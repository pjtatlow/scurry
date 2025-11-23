package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/flags"
	"github.com/pjtatlow/scurry/internal/schema"
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
				Description("Enter your SQL statements (one per line)").
				Placeholder("CREATE TABLE example (\n  id INT PRIMARY KEY\n);").
				Value(&sqlStatements).
				CharLimit(10000).
				Validate(func(s string) error {
					if strings.TrimSpace(s) == "" {
						return fmt.Errorf("SQL statements cannot be empty")
					}
					// Validate SQL
					_, err := schema.ParseSQL(s)
					if err != nil {
						return fmt.Errorf("invalid SQL: %w", err)
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
	statements, err := schema.ParseSQL(sqlStatements)
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Convert parsed statements to strings
	var statementStrings []string
	for _, stmt := range statements {
		statementStrings = append(statementStrings, stmt.String())
	}

	if flags.Verbose {
		fmt.Println()
		fmt.Println(ui.Header(fmt.Sprintf("Parsed %d statement(s):", len(statementStrings))))
		for i, stmt := range statementStrings {
			fmt.Printf("%s %s\n\n", ui.Info(fmt.Sprintf("%d.", i+1)), ui.SqlCode(stmt))
		}
	}

	// Create migration directory and file
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Creating migration..."))
	}

	migrationDirName, err := createMigration(fs, migrationName, statementStrings)
	if err != nil {
		return fmt.Errorf("failed to create migration: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Created migration: %s", migrationDirName)))

	// Apply migrations to production schema
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Updating production schema..."))
	}

	newSchema, err := applyMigrationsToSchema(ctx, prodSchema, statementStrings)
	if err != nil {
		return fmt.Errorf("failed to apply migrations to schema: %w", err)
	}

	// Dump new schema to schema.sql
	err = dumpProductionSchema(ctx, fs, newSchema)
	if err != nil {
		return fmt.Errorf("failed to update schema.sql: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))
	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Migration created successfully! Apply it to your database with: scurry migration apply %s", migrationDirName)))

	return nil
}
