package cmd

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/schema"
)

func TestLoadProductionSchema(t *testing.T) {
	tests := []struct {
		name           string
		schemaContent  string
		expectErr      bool
		expectedTables int
		expectedTypes  int
	}{
		{
			name:           "empty schema.sql",
			schemaContent:  "",
			expectErr:      false,
			expectedTables: 0,
			expectedTypes:  0,
		},
		{
			name: "schema with table",
			schemaContent: `
				CREATE TABLE users (
					id INT PRIMARY KEY,
					name TEXT NOT NULL
				);
			`,
			expectErr:      false,
			expectedTables: 1,
			expectedTypes:  0,
		},
		{
			name: "schema with multiple objects",
			schemaContent: `
				CREATE TYPE status AS ENUM ('active', 'inactive');
				CREATE TABLE users (
					id INT PRIMARY KEY,
					name TEXT NOT NULL,
					status status NOT NULL
				);
			`,
			expectErr:      false,
			expectedTables: 1,
			expectedTypes:  1,
		},
		{
			name:          "invalid SQL",
			schemaContent: "CREATE TABLE (",
			expectErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fs := afero.NewMemMapFs()

			// Create migrations directory
			err := fs.MkdirAll(migrationDir, 0755)
			require.NoError(t, err)

			// Write schema.sql if content provided
			if tt.schemaContent != "" {
				schemaPath := filepath.Join(migrationDir, "schema.sql")
				err = afero.WriteFile(fs, schemaPath, []byte(tt.schemaContent), 0644)
				require.NoError(t, err)
			}

			// Load production schema
			prodSchema, err := loadProductionSchema(ctx, fs)

			if tt.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, prodSchema.Tables, tt.expectedTables)
			assert.Len(t, prodSchema.Types, tt.expectedTypes)
		})
	}
}

func TestDumpProductionSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs := afero.NewMemMapFs()

	// Create migrations directory
	err := fs.MkdirAll(migrationDir, 0755)
	require.NoError(t, err)

	// Create a schema with some objects
	schemaSQL := `
		CREATE TYPE status AS ENUM ('active', 'inactive');
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			status status NOT NULL
		);
	`

	// Write the SQL to schema.sql
	schemaPath := filepath.Join(migrationDir, "schema.sql")
	err = afero.WriteFile(fs, schemaPath, []byte(schemaSQL), 0644)
	require.NoError(t, err)

	// Load production schema
	initialSchema, err := loadProductionSchema(ctx, fs)
	require.NoError(t, err)

	// Apply to database to get standardized schema
	prodSchema, err := applyMigrationsToSchema(ctx, initialSchema, []string{})
	require.NoError(t, err)

	// Dump schema
	err = dumpProductionSchema(ctx, fs, prodSchema)
	require.NoError(t, err)

	// Read schema.sql
	content, err := afero.ReadFile(fs, schemaPath)
	require.NoError(t, err)

	// Verify content
	contentStr := string(content)
	assert.Contains(t, contentStr, "CREATE TYPE")
	assert.Contains(t, contentStr, "status")
	assert.Contains(t, contentStr, "CREATE TABLE")
	assert.Contains(t, contentStr, "users")
}

func TestCreateMigration(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	// Create migrations directory
	err := fs.MkdirAll(migrationDir, 0755)
	require.NoError(t, err)

	// Create a migration
	statements := []string{
		"CREATE TABLE posts (id INT PRIMARY KEY, title TEXT NOT NULL)",
		"CREATE INDEX title_idx ON posts (title)",
	}

	migrationName, err := createMigration(fs, "add_posts_table", statements)
	require.NoError(t, err)

	// Verify migration name format (timestamp_name)
	assert.Contains(t, migrationName, "add_posts_table")
	assert.True(t, len(migrationName) > len("add_posts_table"), "migration name should include timestamp")

	// Verify migration directory exists
	migrationPath := filepath.Join(migrationDir, migrationName)
	exists, err := afero.DirExists(fs, migrationPath)
	require.NoError(t, err)
	assert.True(t, exists)

	// Verify migration.sql exists and has correct content
	migrationFile := filepath.Join(migrationPath, "migration.sql")
	content, err := afero.ReadFile(fs, migrationFile)
	require.NoError(t, err)

	contentStr := string(content)
	assert.Contains(t, contentStr, "CREATE TABLE posts")
	assert.Contains(t, contentStr, "CREATE INDEX title_idx")
}

func TestApplyMigrationsToSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name           string
		initialSQL     string
		migrations     []string
		expectedTables int
	}{
		{
			name:       "empty schema with new table",
			initialSQL: "",
			migrations: []string{
				"CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)",
			},
			expectedTables: 1,
		},
		{
			name: "add column to existing table",
			initialSQL: `
				CREATE TABLE users (
					id INT PRIMARY KEY,
					name TEXT NOT NULL
				);
			`,
			migrations: []string{
				"ALTER TABLE users ADD COLUMN email TEXT",
			},
			expectedTables: 1,
		},
		{
			name: "add table and type",
			initialSQL: `
				CREATE TABLE users (
					id INT PRIMARY KEY,
					name TEXT NOT NULL
				);
			`,
			migrations: []string{
				"CREATE TYPE status AS ENUM ('active', 'inactive')",
				"ALTER TABLE users ADD COLUMN status status NOT NULL DEFAULT 'active'",
			},
			expectedTables: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs := afero.NewMemMapFs()

			// Create migrations directory
			err := fs.MkdirAll(migrationDir, 0755)
			require.NoError(t, err)

			// Write schema.sql with initial schema
			if strings.TrimSpace(tt.initialSQL) != "" {
				schemaPath := filepath.Join(migrationDir, "schema.sql")
				err = afero.WriteFile(fs, schemaPath, []byte(tt.initialSQL), 0644)
				require.NoError(t, err)
			}

			// Load production schema
			prodSchema, err := loadProductionSchema(ctx, fs)
			require.NoError(t, err)

			// Apply migrations
			newSchema, err := applyMigrationsToSchema(ctx, prodSchema, tt.migrations)
			require.NoError(t, err)
			assert.NotNil(t, newSchema)
			assert.Len(t, newSchema.Tables, tt.expectedTables)
		})
	}
}

func TestMigrateGenIntegration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs := afero.NewMemMapFs()

	// Set up directories
	schemaDir := "/schema"

	// Create directories
	err := fs.MkdirAll(migrationDir, 0755)
	require.NoError(t, err)
	err = fs.MkdirAll(schemaDir, 0755)
	require.NoError(t, err)

	// Create initial production schema (empty schema.sql)
	schemaPath := filepath.Join(migrationDir, "schema.sql")
	err = afero.WriteFile(fs, schemaPath, []byte(""), 0644)
	require.NoError(t, err)

	// Create local schema with a table
	localSchemaContent := `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
	`
	err = afero.WriteFile(fs, filepath.Join(schemaDir, "tables/users.sql"), []byte(localSchemaContent), 0644)
	require.NoError(t, err)

	// Load local schema
	dbClient, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectory(ctx, fs, schemaDir, dbClient)
	require.NoError(t, err)

	// Load production schema
	prodSchema, err := loadProductionSchema(ctx, fs)
	require.NoError(t, err)

	// Compare schemas
	diffResult := schema.Compare(localSchema, prodSchema)
	assert.True(t, diffResult.HasChanges())

	// Generate migrations
	statements, err := diffResult.GenerateMigrations(false)
	require.NoError(t, err)
	assert.NotEmpty(t, statements)

	// Create migration
	migrationName, err := createMigration(fs, "add_users_table", statements)
	require.NoError(t, err)
	assert.NotEmpty(t, migrationName)

	// Verify migration file was created
	migrationFile := filepath.Join(migrationDir, migrationName, "migration.sql")
	exists, err := afero.Exists(fs, migrationFile)
	require.NoError(t, err)
	assert.True(t, exists)

	// Apply migrations to production schema
	newSchema, err := applyMigrationsToSchema(ctx, prodSchema, statements)
	require.NoError(t, err)

	// Dump new schema
	err = dumpProductionSchema(ctx, fs, newSchema)
	require.NoError(t, err)

	// Verify schema.sql was updated
	content, err := afero.ReadFile(fs, schemaPath)
	require.NoError(t, err)
	assert.Contains(t, string(content), "CREATE TABLE")
	assert.Contains(t, string(content), "users")
}
