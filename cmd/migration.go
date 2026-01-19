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
	"github.com/pjtatlow/scurry/internal/ui"
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
// Returns the migration directory name and the content written to migration.sql
func createMigration(fs afero.Fs, name string, statements []string) (string, string, error) {
	// Generate timestamp prefix
	timestamp := time.Now().Format("20060102150405")
	migrationName := fmt.Sprintf("%s_%s", timestamp, name)
	migrationPath := filepath.Join(flags.MigrationDir, migrationName)

	// Create migration directory
	err := fs.MkdirAll(migrationPath, 0755)
	if err != nil {
		return "", "", fmt.Errorf("failed to create migration directory: %w", err)
	}

	// Write migration.sql
	migrationFile := filepath.Join(migrationPath, "migration.sql")
	content := strings.Join(statements, ";\n\n") + ";\n"
	err = afero.WriteFile(fs, migrationFile, []byte(content), 0644)
	if err != nil {
		return "", "", fmt.Errorf("failed to write migration.sql: %w", err)
	}

	return migrationName, content, nil
}

// Helper function to apply migrations to production schema
func applyMigrationsToSchema(ctx context.Context, prodSchema *schema.Schema, migrations []string) (*schema.Schema, error) {

	// Use shared test server to apply statements
	client, err := db.GetShadowDB(ctx, prodSchema.OriginalStatements...)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if err := client.ExecuteBulkDDL(ctx, migrations...); err != nil {
		return nil, err
	}

	// Get the new schema from the database
	return schema.LoadFromDatabase(ctx, client)
}

// markMigrationAsAppliedIfMatches connects to the local database, compares its schema
// with the expected schema, and if they match, records the migration as applied.
// We record with an empty checksum since the migration was just created locally and
// we don't want false "modified" warnings when the file is later executed elsewhere.
func markMigrationAsAppliedIfMatches(ctx context.Context, migrationName string, expectedSchema *schema.Schema) error {
	// Connect to local database
	dbClient, err := db.Connect(ctx, flags.DbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer dbClient.Close()

	// Initialize migration history table
	if err := dbClient.InitMigrationHistory(ctx); err != nil {
		return fmt.Errorf("failed to initialize migration history: %w", err)
	}

	// Load schema from local database
	localDbSchema, err := schema.LoadFromDatabase(ctx, dbClient)
	if err != nil {
		return fmt.Errorf("failed to load schema from database: %w", err)
	}

	// Compare local DB schema with expected schema
	diffResult := schema.Compare(expectedSchema, localDbSchema)
	if diffResult.HasChanges() {
		fmt.Println(ui.Warning("Local database schema does not match expected schema after migration"))
		fmt.Println(ui.Warning("Migration will not be marked as applied"))
		if flags.Verbose {
			fmt.Println(ui.Subtle("Differences:"))
			fmt.Println(ui.Subtle(diffResult.Summary()))
		}
		return nil
	}

	// Schemas match - record the migration as applied with empty checksum
	// Empty checksum indicates this was marked during creation, not execution
	if err := dbClient.RecordMigration(ctx, migrationName, ""); err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Marked migration as applied in local database: %s", migrationName)))
	return nil
}
