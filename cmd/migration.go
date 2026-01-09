package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	"github.com/pjtatlow/scurry/internal/schema"
)

var migrationCmd = &cobra.Command{
	Use:   "migration",
	Short: "Manage database migrations",
	Long: `Manage database migrations by generating migration files from schema changes
or creating new migrations manually.`,
}

func init() {
	rootCmd.AddCommand(migrationCmd)
	flags.AddMigrationDir(rootCmd)
}

// Helper function to validate migrations directory structure
func validateMigrationsDir(fs afero.Fs) error {
	// Check if migrations directory exists
	exists, err := afero.DirExists(fs, flags.MigrationDir)
	if err != nil {
		return fmt.Errorf("failed to check migrations directory: %w", err)
	}
	if !exists {
		return fmt.Errorf("migrations directory does not exist: %s", flags.MigrationDir)
	}

	return nil
}

// Helper function to get schema.sql path
func getSchemaFilePath() string {
	return filepath.Join(flags.MigrationDir, "schema.sql")
}

// Helper function to load production schema from schema.sql
func loadProductionSchema(ctx context.Context, fs afero.Fs) (*schema.Schema, error) {
	schemaPath := getSchemaFilePath()

	// Check if schema.sql exists
	exists, err := afero.Exists(fs, schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check schema.sql: %w", err)
	}

	// If schema.sql doesn't exist, return empty schema
	if !exists {
		return schema.NewSchema(), nil
	}

	// Read schema.sql
	content, err := afero.ReadFile(fs, schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema.sql: %w", err)
	}

	// If file is empty, return empty schema
	if len(content) == 0 || strings.TrimSpace(string(content)) == "" {
		return schema.NewSchema(), nil
	}

	// Parse the SQL and create schema
	sql := string(content)
	statements, err := schema.ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema.sql: %w", err)
	}

	// Load into shadow database to get standardized schema
	return schema.NewSchema(statements...), nil
}

// Helper function to dump schema to schema.sql
func dumpProductionSchema(ctx context.Context, fs afero.Fs, sch *schema.Schema) error {
	schemaPath := getSchemaFilePath()

	statements, _, err := schema.Compare(sch, schema.NewSchema()).GenerateMigrations(true)
	if err != nil {
		return fmt.Errorf("failed to dump new schema: %w", err)
	}

	// Join with semicolons and newlines
	content := strings.Join(statements, ";\n\n\n") + ";\n"

	// Write to schema.sql
	err = afero.WriteFile(fs, schemaPath, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("failed to write schema.sql: %w", err)
	}

	return nil
}

// Helper function to create migration directory and file
func createMigration(fs afero.Fs, name string, statements []string) (string, error) {
	// Generate timestamp prefix
	timestamp := time.Now().Format("20060102150405")
	migrationName := fmt.Sprintf("%s_%s", timestamp, name)
	migrationPath := filepath.Join(flags.MigrationDir, migrationName)

	// Create migration directory
	err := fs.MkdirAll(migrationPath, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create migration directory: %w", err)
	}

	// Write migration.sql
	migrationFile := filepath.Join(migrationPath, "migration.sql")
	content := strings.Join(statements, ";\n\n") + ";\n"
	err = afero.WriteFile(fs, migrationFile, []byte(content), 0644)
	if err != nil {
		return "", fmt.Errorf("failed to write migration.sql: %w", err)
	}

	return migrationName, nil
}

// createMigrationWithCheckpoint creates a migration directory, migration.sql, and checkpoint.sql
func createMigrationWithCheckpoint(fs afero.Fs, name string, statements []string, resultSchema *schema.Schema) (string, error) {
	// Create the basic migration first
	migrationName, err := createMigration(fs, name, statements)
	if err != nil {
		return "", err
	}

	migrationPath := filepath.Join(flags.MigrationDir, migrationName)

	// Load all existing migrations (already sorted by loadMigrations)
	existingMigrations, err := loadMigrations(fs)
	if err != nil {
		return "", fmt.Errorf("failed to load migrations for checkpoint: %w", err)
	}

	// Find the index of our new migration to get all migrations up to it
	var migrationsUpTo []migration
	for i, m := range existingMigrations {
		if m.name == migrationName {
			migrationsUpTo = existingMigrations[:i+1]
			break
		}
	}

	// Create checkpoint
	if err := createCheckpointForMigration(fs, migrationsUpTo, resultSchema, migrationPath); err != nil {
		return "", fmt.Errorf("failed to create checkpoint: %w", err)
	}

	return migrationName, nil
}

// Helper function to apply migrations to production schema
func applyMigrationsToSchema(ctx context.Context, prodSchema *schema.Schema, migrations []string) (*schema.Schema, error) {

	// Use shared test server to apply statements
	client, err := db.GetShadowDB(ctx, prodSchema.OriginalStatements...)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	client.ExecuteBulkDDL(ctx, migrations...)

	// Get the new schema from the database
	return schema.LoadFromDatabase(ctx, client)
}
