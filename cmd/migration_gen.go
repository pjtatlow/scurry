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

	flags.AddDefinitionDirs(migrationGenCmd)
	migrationGenCmd.Flags().StringVar(&migrationName, "name", "", "Name for the migration (skips prompt)")
}

func migrationGen(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if len(flags.DefinitionDirs) == 0 {
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
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", strings.Join(flags.DefinitionDirs, ", "))))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shadow database client: %w", err)
	}
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectories(ctx, fs, flags.DefinitionDirs, dbClient)
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

	header := &migrationpkg.Header{Mode: classifyResult.Mode}

	// Validate the statements, resolve the name, detect dependencies, and write
	// the migration file (with the interactive manual-edit fallback on failure).
	_, newSchema, err := finalizeAuthoredMigration(ctx, fs, prodSchema, statements, "", header, migrationName, flags.Force, false, flags.Verbose)
	if err != nil {
		return err
	}

	// Update the production schema snapshot (schema.sql).
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Updating production schema..."))
	}

	if err := dumpProductionSchema(ctx, fs, newSchema); err != nil {
		return fmt.Errorf("failed to update schema.sql: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))

	fmt.Println()
	fmt.Println(ui.Info("Migration created successfully! Apply it to your database with: scurry migration execute"))

	return nil
}

// finalizeAuthoredMigration takes a set of migration statements (either generated
// from a diff or supplied directly) and turns them into a migration file: it
// validates them against prodSchema on an ephemeral shadow database, resolves the
// migration name, detects dependencies, and writes the file. It returns the created
// migration directory name and the resulting schema (for advancing schema.sql).
//
// When applyMigrationsToSchema fails and the session is interactive (and not forced),
// the user is dropped into a manual-edit form to fix the SQL. In non-interactive or
// forced mode the validation error is returned directly.
//
// rawBody, when non-empty, is written to migration.sql verbatim (the --migration-sql
// path, which preserves the user's exact SQL and comments); otherwise the file is
// built from statements. If the user manually edits the SQL, rawBody is discarded in
// favor of the edited statements. header.DependsOn is filled from object-level overlap
// only when it is nil, so an explicitly supplied depends_on is respected.
//
// When dryRun is true, the migration is validated but no file is written; the resulting
// schema is still returned.
func finalizeAuthoredMigration(
	ctx context.Context,
	fs afero.Fs,
	prodSchema *schema.Schema,
	statements []string,
	rawBody string,
	header *migrationpkg.Header,
	name string,
	force, dryRun, verbose bool,
) (string, *schema.Schema, error) {
	// 1. Validate the migration against the snapshot on an ephemeral shadow DB.
	newSchema, err := applyMigrationsToSchema(ctx, prodSchema, statements)
	if err != nil {
		// Without a TTY (or when forced) there is no way to fix it interactively.
		if force || !ui.IsInteractive() {
			return "", nil, fmt.Errorf("failed to apply migrations to schema: %w", err)
		}

		fmt.Println(ui.Error(fmt.Sprintf("Failed to apply generated migration: %v", err)))
		fmt.Println()
		fmt.Println(ui.Info("The generated migration could not be applied. This may require manual intervention."))

		confirmed, confirmErr := ui.ConfirmPrompt("Would you like to create a manual migration instead?")
		if confirmErr != nil {
			return "", nil, fmt.Errorf("confirmation prompt failed: %w", confirmErr)
		}
		if !confirmed {
			return "", nil, fmt.Errorf("failed to apply migrations to schema: %w", err)
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
			return "", nil, fmt.Errorf("migration input canceled: %w", err)
		}

		// Parse edited statements
		parsedStatements, err := parser.Parse(sqlStatements)
		if err != nil {
			return "", nil, fmt.Errorf("failed to parse SQL: %w", err)
		}

		// Convert parsed statements to strings; the edited SQL supersedes any raw body.
		statements = nil
		for _, stmt := range parsedStatements {
			statements = append(statements, stmt.AST.String())
		}
		rawBody = ""

		// Try to apply the edited migration
		newSchema, err = applyMigrationsToSchema(ctx, prodSchema, statements)
		if err != nil {
			return "", nil, fmt.Errorf("failed to apply edited migrations to schema: %w", err)
		}
	}

	// 2. Resolve the migration name (from flag/argument or interactive prompt).
	if name == "" {
		if !ui.IsInteractive() {
			return "", nil, fmt.Errorf("migration name required in non-interactive mode\nUse --name flag to specify the migration name")
		}
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

		if err := form.Run(); err != nil {
			return "", nil, fmt.Errorf("migration name input canceled: %w", err)
		}
	}

	// 3. Detect dependencies from object-level overlap (unless already supplied).
	if header.DependsOn == nil {
		var newStmts []tree.Statement
		for _, s := range statements {
			parsed, err := parser.Parse(s)
			if err == nil {
				for _, p := range parsed {
					newStmts = append(newStmts, p.AST)
				}
			}
		}

		existingMigrations, err := loadMigrations(fs)
		if err == nil && len(existingMigrations) > 0 {
			migInfos := make([]migrationpkg.MigrationInfo, len(existingMigrations))
			for i, m := range existingMigrations {
				migInfos[i] = migrationpkg.MigrationInfo{Name: m.Name, SQL: m.SQL}
			}
			header.DependsOn = migrationpkg.FindDependencies(newStmts, migInfos)
		}
	}

	// 4. In dry-run mode, stop before writing anything.
	if dryRun {
		return "", newSchema, nil
	}

	// 5. Write the migration file.
	if verbose {
		fmt.Println()
		fmt.Println(ui.Subtle("→ Creating migration..."))
	}

	var dirName string
	if rawBody != "" {
		if err := migrationpkg.SignHeader(header, rawBody); err != nil {
			return "", nil, fmt.Errorf("failed to sign migration: %w", err)
		}
		content := migrationpkg.FormatHeader(header) + "\n" + rawBody
		dirName, err = writeMigrationFile(fs, name, content)
	} else {
		dirName, _, err = createMigration(fs, name, statements, header)
	}
	if err != nil {
		return "", nil, fmt.Errorf("failed to create migration: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Created migration: %s", dirName)))

	return dirName, newSchema, nil
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
