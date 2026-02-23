package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/flags"
	"github.com/pjtatlow/scurry/internal/ui"
)

var migrationNewCmd = &cobra.Command{
	Use:   "new",
	Short: "Create a new migration manually",
	Long: `Create a new migration by entering SQL statements manually.
You will be prompted to enter SQL statements, which will be validated before creating the migration.`,
	RunE: migrationNew,
}

func init() {
	migrationCmd.AddCommand(migrationNewCmd)
	flags.AddDbUrl(migrationNewCmd)
}

func migrationNew(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	err := doMigrationNew(ctx)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doMigrationNew(ctx context.Context) error {
	// Check for interactive terminal
	if !ui.IsInteractive() {
		return fmt.Errorf("migration new requires an interactive terminal\nRun this command in a terminal with TTY support")
	}

	fs := afero.NewOsFs()

	// Validate migrations directory
	if err := validateMigrationsDir(fs); err != nil {
		return err
	}

	// Load production schema from schema.sql
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
		fmt.Println()
	}

	// Ask user for SQL statements
	var sqlStatements string
	var migrationName string

	form := huh.NewForm(
		huh.NewGroup(
			huh.NewText().
				Title("SQL Statements").
				Description("Enter your SQL statements (ended with semicolons)").
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
		huh.NewGroup(
			huh.NewInput().
				Title("Migration name").
				Description("Enter a descriptive name for this migration").
				Placeholder("add_example_table").
				Value(&migrationName).
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
		return fmt.Errorf("migration input canceled: %w", err)
	}

	// Parse statements
	statements, err := parser.Parse(sqlStatements)
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Convert parsed statements to strings
	var statementStrings []string
	for _, stmt := range statements {
		statementStrings = append(statementStrings, stmt.AST.String())
	}

	if flags.Verbose {
		fmt.Println()
		fmt.Println(ui.Header(fmt.Sprintf("Parsed %d statement(s):", len(statementStrings))))
		for i, stmt := range statementStrings {
			fmt.Printf("%s %s\n\n", ui.Info(fmt.Sprintf("%d.", i+1)), ui.SqlCode(stmt))
		}
	}

	// Apply migrations to get the new schema (for updating schema.sql)
	fmt.Println(ui.Subtle("→ Applying migrations to schema..."))

	newSchema, err := applyMigrationsToSchema(ctx, prodSchema, statementStrings)
	if err != nil {
		return fmt.Errorf("failed to apply migrations to schema: %w", err)
	}

	// Create migration directory and file
	fmt.Println(ui.Subtle("→ Creating migration..."))

	migrationDirName, _, err := createMigration(fs, migrationName, statementStrings, nil)
	if err != nil {
		return fmt.Errorf("failed to create migration: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Created migration: %s", migrationDirName)))

	// Update production schema
	fmt.Println(ui.Subtle("→ Updating production schema..."))

	// Dump new schema to schema.sql
	err = dumpProductionSchema(ctx, fs, newSchema)
	if err != nil {
		return fmt.Errorf("failed to update schema.sql: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))

	// If db-url provided, check if local DB matches and mark migration as applied
	if flags.DbUrl != "" {
		if err := markMigrationAsAppliedIfMatches(ctx, migrationDirName, newSchema, false); err != nil {
			fmt.Println(ui.Warning(fmt.Sprintf("Could not mark migration as applied: %v", err)))
		}
	}

	fmt.Println()
	fmt.Println(ui.Info(("Migration created successfully!")))

	return nil
}
