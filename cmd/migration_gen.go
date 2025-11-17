package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/charmbracelet/huh"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
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
	migrationGenCmd.Flags().StringVar(&schemaDir, "schema-dir", "./schema", "Directory containing schema SQL files")
}

func migrationGen(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if schemaDir == "" {
		return fmt.Errorf("schema directory is required (use --schema-dir)")
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
	if verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", schemaDir)))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shadow database client: %w", err)
	}
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectory(ctx, fs, schemaDir, dbClient)
	if err != nil {
		return fmt.Errorf("failed to load local schema: %w", err)
	}

	if verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views locally",
			len(localSchema.Tables), len(localSchema.Types), len(localSchema.Routines), len(localSchema.Sequences), len(localSchema.Views))))
	}

	// 2. Load production schema from schema.sql
	if verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading production schema from %s...", getSchemaFilePath())))
	}

	prodSchema, err := loadProductionSchema(ctx, fs)
	if err != nil {
		return fmt.Errorf("failed to load production schema: %w", err)
	}

	if verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views in production",
			len(prodSchema.Tables), len(prodSchema.Types), len(prodSchema.Routines), len(prodSchema.Sequences), len(prodSchema.Views))))
	}

	// 3. Compare schemas
	if verbose {
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

	// Show differences
	if verbose {
		fmt.Println(ui.Header("\nDifferences found:"))
		fmt.Println(diffResult.Summary())
	}

	// Generate migration statements
	statements, err := diffResult.GenerateMigrations(true)
	if err != nil {
		return fmt.Errorf("failed to generate migrations: %w", err)
	}

	newSchema, err := applyMigrationsToSchema(ctx, prodSchema, statements)
	if err != nil {
		return fmt.Errorf("failed to apply migrations to schema: %w", err)
	}

	if verbose {
		fmt.Println()
		fmt.Println(ui.Header(fmt.Sprintf("Generated %d migration statement(s):", len(statements))))
		for i, stmt := range statements {
			fmt.Printf("%s %s\n\n", ui.Info(fmt.Sprintf("%d.", i+1)), ui.SqlCode(stmt))
		}
	}

	// 5. Ask user for migration name
	var migrationName string
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Migration name").
				Description("Enter a descriptive name for this migration").
				Placeholder("add_users_table").
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
		return fmt.Errorf("migration name input canceled: %w", err)
	}

	// Create migration directory and file
	if verbose {
		fmt.Println()
		fmt.Println(ui.Subtle("→ Creating migration..."))
	}

	migrationDirName, err := createMigration(fs, migrationName, statements)
	if err != nil {
		return fmt.Errorf("failed to create migration: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Created migration: %s", migrationDirName)))

	// 6. Apply migrations to production schema
	if verbose {
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
