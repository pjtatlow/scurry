package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/flags"
	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

var (
	validateOverwrite bool
)

type migration struct {
	name string
	sql  string
}

var migrationValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate that migrations produce the expected schema",
	Long: `Validate migrations by applying them to a clean database and comparing
the result with schema.sql. Use --overwrite to update schema.sql instead of comparing.`,
	RunE: migrationValidate,
}

func init() {
	migrationCmd.AddCommand(migrationValidateCmd)
	migrationValidateCmd.Flags().BoolVar(&validateOverwrite, "overwrite", false, "Overwrite schema.sql with the result instead of comparing")
}

func migrationValidate(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	err := doMigrationValidate(ctx)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doMigrationValidate(ctx context.Context) error {
	fs := afero.NewOsFs()

	// Validate migrations directory
	if err := validateMigrationsDir(fs); err != nil {
		return err
	}

	// 1. Load all migration files in order
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Loading migrations..."))
	}

	migrations, err := loadMigrations(fs)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d migration(s)", len(migrations))))
	}

	// 2. Apply migrations to empty shadow database
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Applying migrations to clean database..."))
	}

	resultSchema, err := applyMigrationsToCleanDatabase(ctx, migrations, flags.Verbose)
	if err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Result: %d tables, %d types, %d routines, %d sequences, %d views",
			len(resultSchema.Tables), len(resultSchema.Types), len(resultSchema.Routines), len(resultSchema.Sequences), len(resultSchema.Views))))
	}

	// 3. Handle overwrite flag
	if validateOverwrite {
		if flags.Verbose {
			fmt.Println(ui.Subtle("→ Overwriting schema.sql..."))
		}

		err = dumpProductionSchema(ctx, fs, resultSchema)
		if err != nil {
			return fmt.Errorf("failed to write schema.sql: %w", err)
		}

		fmt.Println()
		fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))
		return nil
	}

	// 4. Load expected schema from schema.sql
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading expected schema from %s...", getSchemaFilePath())))
	}

	expectedSchema, err := loadProductionSchema(ctx, fs)
	if err != nil {
		return fmt.Errorf("failed to load schema.sql: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Expected: %d tables, %d types, %d routines, %d sequences, %d views",
			len(expectedSchema.Tables), len(expectedSchema.Types), len(expectedSchema.Routines), len(expectedSchema.Sequences), len(expectedSchema.Views))))
	}

	// 5. Compare schemas
	if flags.Verbose {
		fmt.Println()
		fmt.Println(ui.Subtle("→ Comparing schemas..."))
	}

	diffResult := schema.Compare(resultSchema, expectedSchema)

	// 6. Check for differences
	if !diffResult.HasChanges() {
		fmt.Println()
		fmt.Println(ui.Success("✓ Migrations match schema.sql"))
		return nil
	}

	// Show differences
	fmt.Println()
	fmt.Println(ui.Error("✗ Migrations do not match schema.sql"))
	fmt.Println()
	fmt.Println(ui.Header("Differences found:"))
	fmt.Println(diffResult.Summary())
	fmt.Println()

	// Prompt user to overwrite schema.sql
	shouldOverwrite, err := ui.ConfirmPrompt("Do you want to overwrite schema.sql with the migration results?")
	if err != nil {
		return fmt.Errorf("confirmation prompt failed: %w", err)
	}

	if !shouldOverwrite {
		return fmt.Errorf("validation failed: schema mismatch")
	}

	// User chose to overwrite
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Overwriting schema.sql..."))
	}

	err = dumpProductionSchema(ctx, fs, resultSchema)
	if err != nil {
		return fmt.Errorf("failed to write schema.sql: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))
	return nil
}

// loadMigrations loads all migration files from the migrations directory in order
func loadMigrations(fs afero.Fs) ([]migration, error) {
	// Read migrations directory
	entries, err := afero.ReadDir(fs, migrationDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	// Filter and sort migration directories
	var migrationDirs []string
	for _, entry := range entries {
		// Skip schema.sql and non-directories
		if !entry.IsDir() {
			continue
		}

		// Migration directories should have the format: YYYYMMDDHHMMSS_name
		name := entry.Name()
		if len(name) >= 14 {
			migrationDirs = append(migrationDirs, name)
		}
	}

	// Sort by timestamp (directory name starts with timestamp)
	sort.Strings(migrationDirs)

	// Read migration.sql from each directory
	var allMigrations []migration
	for _, dir := range migrationDirs {
		migrationFile := filepath.Join(migrationDir, dir, "migration.sql")

		// Check if migration.sql exists
		exists, err := afero.Exists(fs, migrationFile)
		if err != nil {
			return nil, fmt.Errorf("failed to check migration file %s: %w", migrationFile, err)
		}
		if !exists {
			continue
		}

		// Read migration.sql as a single string
		// Don't parse it - migrations can contain ALTER statements and other DDL
		// that the schema parser doesn't support
		content, err := afero.ReadFile(fs, migrationFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %w", migrationFile, err)
		}

		// Add the migration with its name and content
		allMigrations = append(allMigrations, migration{
			name: dir,
			sql:  string(content),
		})
	}

	return allMigrations, nil
}

// applyMigrationsToCleanDatabase creates a clean shadow database and applies all migrations
func applyMigrationsToCleanDatabase(ctx context.Context, migrations []migration, showProgress bool) (*schema.Schema, error) {
	// Use shared test server to get a clean database (no statements executed yet)
	client, err := db.GetShadowDB(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// Apply each migration in its own transaction
	for i, mig := range migrations {
		if showProgress {
			fmt.Println(ui.Subtle(fmt.Sprintf("  Applying migration %d/%d: %s", i+1, len(migrations), mig.name)))
		}

		start := time.Now()
		err = client.ExecuteBulkDDL(ctx, mig.sql)
		duration := time.Since(start)

		if err != nil {
			return nil, fmt.Errorf("failed to apply migration %s: %w", mig.name, err)
		}

		if showProgress {
			fmt.Println(ui.Success(fmt.Sprintf("    ✓ Completed in %v", duration.Round(time.Millisecond))))
		}
	}

	// Get the schema from the database
	return schema.LoadFromDatabase(ctx, client)
}
