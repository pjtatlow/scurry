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
	migrationGenCmd.Flags().StringVar(&migrationName, "name", "", "Name for the migration (skips prompt)")
}

func migrationGen(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if flags.DefinitionDir == "" {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}

	err := doMigrationGen(ctx)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doMigrationGen(ctx context.Context) error {
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
		return fmt.Errorf("failed to apply migrations to schema: %w", err)
	}

	// 5. Get migration name (from flag or prompt)
	var name string
	if migrationName != "" {
		// Use the name from the flag
		name = migrationName
	} else {
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

	// Create migration directory and file
	if flags.Verbose {
		fmt.Println()
		fmt.Println(ui.Subtle("→ Creating migration..."))
	}

	migrationDirName, err := createMigration(fs, name, statements)
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
	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Migration created successfully! Apply it to your database with: scurry migration apply %s", migrationDirName)))

	return nil
}

// promptForUsingExpressionsGen checks for column type changes and prompts the user
// to optionally provide a USING expression for each one.
func promptForUsingExpressionsGen(diffResult *schema.ComparisonResult) error {
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
