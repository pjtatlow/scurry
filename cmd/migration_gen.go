package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

var (
	migrationName string
)

var migrationGenCmd = &cobra.Command{
	Use:   "gen",
	Short: "Generate migration from schema changes",
	Long: `Generate a migration by comparing the local schema with the production schema.
This will detect differences and create a new migration file with the necessary SQL statements.`,
	RunE: migrationGen,
}

func init() {
	migrationCmd.AddCommand(migrationGenCmd)

	flags.AddDefinitionDir(migrationGenCmd)
	flags.AddDbUrl(migrationGenCmd)
	migrationGenCmd.Flags().StringVar(&migrationName, "name", "", "Name for the migration (skips prompt)")
}

func migrationGen(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if flags.DefinitionDir == "" {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}

	errCtx := &ErrorContext{}
	err := doMigrationGen(ctx, errCtx)
	if err != nil {
		reportPath, reportErr := writeErrorReport(errCtx, err)
		if reportErr != nil {
			fmt.Println(ui.Warning(fmt.Sprintf("Failed to write error report: %s", reportErr)))
		} else if reportPath != "" {
			fmt.Println(ui.Info(fmt.Sprintf("Error report written to: %s", reportPath)))
		}
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doMigrationGen(ctx context.Context, errCtx *ErrorContext) error {
	fs := afero.NewOsFs()

	// Validate migrations directory
	if err := validateMigrationsDir(fs); err != nil {
		return err
	}

	// 1. Load local schema from schema-dir
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", flags.DefinitionDir)))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shadow database client: %w", err)
	}
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectory(ctx, fs, flags.DefinitionDir, dbClient)
	if err != nil {
		return fmt.Errorf("failed to load local schema: %w", err)
	}
	errCtx.LocalSchema = localSchema

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views locally",
			len(localSchema.Tables), len(localSchema.Types), len(localSchema.Routines), len(localSchema.Sequences), len(localSchema.Views))))
	}

	// 2. Load production schema from schema.sql
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading production schema from %s...", getSchemaFilePath())))
	}

	prodSchema, err := loadProductionSchema(ctx, fs)
	if err != nil {
		return fmt.Errorf("failed to load production schema: %w", err)
	}
	errCtx.RemoteSchema = prodSchema

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views in production",
			len(prodSchema.Tables), len(prodSchema.Types), len(prodSchema.Routines), len(prodSchema.Sequences), len(prodSchema.Views))))
	}

	// 3. Compare schemas
	if flags.Verbose {
		fmt.Println()
		fmt.Println(ui.Subtle("→ Comparing schemas..."))
	}

	diffResult := schema.Compare(localSchema, prodSchema)

	// 4. Check if there are any changes
	if !diffResult.HasChanges() {
		fmt.Println()
		fmt.Println(ui.Success("✓ No schema changes detected"))
		return nil
	}

	// Prompt for USING expressions on column type changes
	if err := promptForUsingExpressionsGen(diffResult); err != nil {
		return err
	}

	// Generate migration statements
	statements, warnings, err := diffResult.GenerateMigrations(true)
	if err != nil {
		return fmt.Errorf("failed to generate migrations: %w", err)
	}
	errCtx.Statements = statements

	// Show differences
	if flags.Verbose {
		fmt.Println(ui.Header("\nDifferences found:"))
		fmt.Println(diffResult.Summary())
		fmt.Println()
		fmt.Println(ui.Header(fmt.Sprintf("Generated %d migration statement(s) with %d warning(s):", len(statements), len(warnings))))

		for i, stmt := range statements {
			fmt.Printf("%s %s\n\n", ui.Info(fmt.Sprintf("%d.", i+1)), ui.SqlCode(stmt))
		}
	}
	for i, warning := range warnings {
		fmt.Printf("WARNING: %s \n\n", ui.Warning(fmt.Sprintf("%d. %s", i+1, warning)))
	}

	newSchema, err := applyMigrationsToSchema(ctx, prodSchema, statements)
	if err != nil {
		// Migration failed to apply - prompt user to create a manual migration
		fmt.Println(ui.Error(fmt.Sprintf("Failed to apply generated migration: %v", err)))
		fmt.Println()
		fmt.Println(ui.Info("The generated migration could not be applied. This may require manual intervention."))

		confirmed, confirmErr := ui.ConfirmPrompt("Would you like to create a manual migration instead?")
		if confirmErr != nil {
			return fmt.Errorf("confirmation prompt failed: %w", confirmErr)
		}

		if !confirmed {
			return fmt.Errorf("failed to apply migrations to schema: %w", err)
		}

		// Pre-populate with the generated statements
		sqlStatements := strings.Join(statements, ";\n\n") + ";"

		form := huh.NewForm(
			huh.NewGroup(
				huh.NewText().
					Title("SQL Statements").
					Description("Edit the SQL statements to fix the migration issue").
					Placeholder("CREATE TABLE example (\n  id INT PRIMARY KEY\n);").
					Value(&sqlStatements).
					CharLimit(10000).
					Validate(func(s string) error {
						if strings.TrimSpace(s) == "" {
							return fmt.Errorf("SQL statements cannot be empty")
						}
						// Validate SQL
						_, err := parser.Parse(s)
						if err != nil {
							return fmt.Errorf("failed to parse SQL: %w", err)
						}
						return nil
					}),
			),
		).WithTheme(ui.HuhTheme())

		if err := form.Run(); err != nil {
			return fmt.Errorf("migration input canceled: %w", err)
		}

		// Parse edited statements
		parsedStatements, err := parser.Parse(sqlStatements)
		if err != nil {
			return fmt.Errorf("failed to parse SQL: %w", err)
		}

		// Convert parsed statements to strings
		statements = nil
		for _, stmt := range parsedStatements {
			statements = append(statements, stmt.AST.String())
		}

		// Try to apply the edited migration
		newSchema, err = applyMigrationsToSchema(ctx, prodSchema, statements)
		if err != nil {
			return fmt.Errorf("failed to apply edited migrations to schema: %w", err)
		}
	}

	// 5. Get migration name (from flag or prompt)
	var name string
	if migrationName != "" {
		// Use the name from the flag
		name = migrationName
	} else {
		// Check for interactive terminal when prompting for name
		if !ui.IsInteractive() {
			return fmt.Errorf("migration name required in non-interactive mode\nUse --name flag to specify the migration name")
		}
		// Ask user for migration name
		form := huh.NewForm(
			huh.NewGroup(
				huh.NewInput().
					Title("Migration name").
					Description("Enter a descriptive name for this migration").
					Placeholder("add_users_table").
					Value(&name).
					Validate(func(s string) error {
						if s == "" {
							return fmt.Errorf("migration name cannot be empty")
						}
						return nil
					}),
			),
		).WithTheme(ui.HuhTheme())

		err = form.Run()
		if err != nil {
			return fmt.Errorf("migration name input canceled: %w", err)
		}
	}

	// Classify migration as sync or async
	tableSizes, err := migrationpkg.LoadTableSizes(fs, flags.MigrationDir)
	if err != nil {
		return fmt.Errorf("failed to load table_sizes.yaml: %w", err)
	}

	classifyResult := migrationpkg.ClassifyDifferences(diffResult.Differences, tableSizes)

	if classifyResult.Mode == migrationpkg.ModeAsync {
		fmt.Println()
		fmt.Println(ui.Warning("Migration classified as async:"))
		for _, reason := range classifyResult.Reasons {
			fmt.Printf("  - %s\n", reason)
		}
		fmt.Println()
	}

	// Build header with mode and dependency on previous migration
	var header *migrationpkg.Header
	header = &migrationpkg.Header{Mode: classifyResult.Mode}

	// Find previous migration for depends_on
	existingMigrations, err := loadMigrations(fs)
	if err == nil && len(existingMigrations) > 0 {
		lastMigration := existingMigrations[len(existingMigrations)-1]
		header.DependsOn = []string{lastMigration.name}
	}

	// Create migration directory and file
	if flags.Verbose {
		fmt.Println()
		fmt.Println(ui.Subtle("→ Creating migration..."))
	}

	migrationDirName, _, err := createMigration(fs, name, statements, header)
	if err != nil {
		return fmt.Errorf("failed to create migration: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Created migration: %s", migrationDirName)))

	// 6. Apply migrations to production schema
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Updating production schema..."))
	}

	// 7. Dump new schema to schema.sql
	err = dumpProductionSchema(ctx, fs, newSchema)
	if err != nil {
		return fmt.Errorf("failed to update schema.sql: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))

	// 8. If db-url provided, check if local DB matches and mark migration as applied
	if flags.DbUrl != "" {
		if err := markMigrationAsAppliedIfMatches(ctx, migrationDirName, newSchema); err != nil {
			fmt.Println(ui.Warning(fmt.Sprintf("Could not mark migration as applied: %v", err)))
		}
	}

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Migration created successfully! Apply it to your database with: scurry migration apply %s", migrationDirName)))

	return nil
}

// promptForUsingExpressionsGen checks for column type changes and prompts the user
// to optionally provide a USING expression for each one.
// In non-interactive mode, this is skipped (user can edit the migration file manually).
func promptForUsingExpressionsGen(diffResult *schema.ComparisonResult) error {
	// Skip in non-interactive mode
	if !ui.IsInteractive() {
		return nil
	}

	for i := range diffResult.Differences {
		diff := &diffResult.Differences[i]
		if diff.Type != schema.DiffTypeColumnTypeChanged {
			continue
		}

		// Find the AlterTableAlterColumnType command in the migration statements
		for _, stmt := range diff.MigrationStatements {
			alterTable, ok := stmt.(*tree.AlterTable)
			if !ok {
				continue
			}
			for _, cmd := range alterTable.Cmds {
				alterColType, ok := cmd.(*tree.AlterTableAlterColumnType)
				if !ok {
					continue
				}

				// Prompt user if they want to add a USING expression
				fmt.Println()
				confirmed, err := ui.ConfirmPrompt(fmt.Sprintf("Add a USING expression for: %s?", diff.Description))
				if err != nil {
					return fmt.Errorf("confirmation prompt failed: %w", err)
				}

				if !confirmed {
					continue
				}

				// Prompt for the expression
				var exprStr string
				form := huh.NewForm(
					huh.NewGroup(
						huh.NewInput().
							Title("USING expression").
							Description("Enter the expression to convert the column value").
							Placeholder(alterColType.Column.String()).
							Value(&exprStr).
							Validate(func(s string) error {
								if strings.TrimSpace(s) == "" {
									return fmt.Errorf("expression cannot be empty")
								}
								_, err := parser.ParseExpr(s)
								if err != nil {
									return fmt.Errorf("invalid expression: %w", err)
								}
								return nil
							}),
					),
				).WithTheme(ui.HuhTheme())

				if err := form.Run(); err != nil {
					return fmt.Errorf("expression input failed: %w", err)
				}

				// Parse and set the expression
				expr, err := parser.ParseExpr(exprStr)
				if err != nil {
					return fmt.Errorf("failed to parse expression: %w", err)
				}
				alterColType.Using = expr
			}
		}
	}
	return nil
}
