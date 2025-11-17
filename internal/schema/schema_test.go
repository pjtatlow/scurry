package schema

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
)

func TestLoadFromDirectory(t *testing.T) {
	tests := []struct {
		name        string
		files       map[string]string
		crdbVersion string
		expectErr   bool
		errContains string
		validate    func(t *testing.T, s *Schema)
	}{
		{
			name:        "empty directory",
			files:       map[string]string{},
			crdbVersion: "v25.3.4",
			expectErr:   false,
			validate: func(t *testing.T, s *Schema) {
				assert.Empty(t, s.Tables)
				assert.Empty(t, s.Types)
				assert.Empty(t, s.Sequences)
				assert.Empty(t, s.Views)
				assert.Empty(t, s.Routines)
			},
		},
		{
			name: "single table",
			files: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   false,
			validate: func(t *testing.T, s *Schema) {
				require.Len(t, s.Tables, 1)
				assert.Equal(t, "users", s.Tables[0].Name)
				assert.Equal(t, "public", s.Tables[0].Schema)
			},
		},
		{
			name: "multiple tables in subdirectories",
			files: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
				"tables/posts/posts.sql": `
					CREATE TABLE posts (
						id INT PRIMARY KEY,
						user_id INT NOT NULL,
						title TEXT NOT NULL
					);
				`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   false,
			validate: func(t *testing.T, s *Schema) {
				require.Len(t, s.Tables, 2)
				tableNames := []string{s.Tables[0].Name, s.Tables[1].Name}
				assert.Contains(t, tableNames, "users")
				assert.Contains(t, tableNames, "posts")
			},
		},
		{
			name: "mixed object types",
			files: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						status user_status NOT NULL
					);
				`,
				"types/user_status.sql": `
					CREATE TYPE user_status AS ENUM ('active', 'inactive');
				`,
				"sequences/user_id_seq.sql": `
					CREATE SEQUENCE user_id_seq;
				`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   false,
			validate: func(t *testing.T, s *Schema) {
				require.Len(t, s.Tables, 1)
				require.Len(t, s.Types, 1)
				require.Len(t, s.Sequences, 1)
				assert.Equal(t, "users", s.Tables[0].Name)
				assert.Equal(t, "user_status", s.Types[0].Name)
				assert.Equal(t, "user_id_seq", s.Sequences[0].Name)
			},
		},
		{
			name: "ignores non-sql files",
			files: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY
					);
				`,
				"README.md":   "# Schema files",
				"config.yaml": "version: 1",
				"data.json":   `{"test": true}`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   false,
			validate: func(t *testing.T, s *Schema) {
				require.Len(t, s.Tables, 1)
				assert.Equal(t, "users", s.Tables[0].Name)
			},
		},
		{
			name: "case insensitive .sql extension",
			files: map[string]string{
				"tables/users.SQL": `
					CREATE TABLE users (
						id INT PRIMARY KEY
					);
				`,
				"types/status.Sql": `
					CREATE TYPE status AS ENUM ('active');
				`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   false,
			validate: func(t *testing.T, s *Schema) {
				require.Len(t, s.Tables, 1)
				require.Len(t, s.Types, 1)
			},
		},
		{
			name: "invalid sql syntax",
			files: map[string]string{
				"tables/broken.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY
						name TEXT -- missing comma
					);
				`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   true,
			errContains: "in file",
		},
		{
			name: "non-ddl statement",
			files: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY
					);
					INSERT INTO users VALUES (1);
				`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   true,
			errContains: "non-DDL statement found",
		},
		{
			name: "unsupported ddl statement",
			files: map[string]string{
				"indexes/idx_users.sql": `
					CREATE INDEX idx_users ON users (name);
				`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   true,
			errContains: "unsupported DDL statement",
		},
		{
			name: "schema with custom schema name",
			files: map[string]string{
				"schemas/app.sql": `
					CREATE SCHEMA app;
				`,
				"tables/users.sql": `
					CREATE TABLE app.users (
						id INT PRIMARY KEY
					);
				`,
			},
			crdbVersion: "v25.3.4",
			expectErr:   false,
			validate: func(t *testing.T, s *Schema) {
				// Should have at least 2 schemas: public (default) and app
				require.GreaterOrEqual(t, len(s.Schemas), 2)
				schemaNames := make([]string, len(s.Schemas))
				for i, schema := range s.Schemas {
					schemaNames[i] = schema.Name
				}
				assert.Contains(t, schemaNames, "app")
				assert.Contains(t, schemaNames, "public")

				require.Len(t, s.Tables, 1)
				assert.Equal(t, "users", s.Tables[0].Name)
				assert.Equal(t, "app", s.Tables[0].Schema)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create in-memory filesystem
			fs := afero.NewMemMapFs()

			// Create schema directory
			schemaDir := "/schema"
			err := fs.MkdirAll(schemaDir, 0755)
			require.NoError(t, err)

			// Write all test files
			for path, content := range tt.files {
				fullPath := filepath.Join(schemaDir, path)
				// Create parent directories
				dir := filepath.Dir(fullPath)
				err := fs.MkdirAll(dir, 0755)
				require.NoError(t, err)
				err = afero.WriteFile(fs, fullPath, []byte(content), 0644)
				require.NoError(t, err)
			}

			// Load schema
			ctx := context.Background()
			dbClient, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer dbClient.Close()
			schema, err := LoadFromDirectory(ctx, fs, schemaDir, dbClient)

			// Check error expectations
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, schema)

			// Run validation function if provided
			if tt.validate != nil {
				tt.validate(t, schema)
			}
		})
	}
}

func TestParseSQL(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectCount int
		expectErr   bool
		errContains string
	}{
		{
			name: "single create table",
			sql: `
				CREATE TABLE users (
					id INT PRIMARY KEY
				);
			`,
			expectCount: 1,
			expectErr:   false,
		},
		{
			name: "multiple statements",
			sql: `
				CREATE TABLE users (
					id INT PRIMARY KEY
				);
				CREATE TABLE posts (
					id INT PRIMARY KEY
				);
			`,
			expectCount: 2,
			expectErr:   false,
		},
		{
			name: "with comments",
			sql: `
				-- This is a users table
				CREATE TABLE users (
					id INT PRIMARY KEY -- user id
				);
			`,
			expectCount: 1,
			expectErr:   false,
		},
		{
			name:        "empty sql",
			sql:         "",
			expectCount: 0,
			expectErr:   false,
		},
		{
			name:        "invalid syntax",
			sql:         "CREATE TABLE (",
			expectErr:   true,
			errContains: "failed to parse SQL",
		},
		{
			name: "non-ddl statement",
			sql: `
				INSERT INTO users VALUES (1);
			`,
			expectErr:   true,
			errContains: "non-DDL statement found",
		},
		{
			name: "unsupported ddl",
			sql: `
				CREATE INDEX idx_users ON users (name);
			`,
			expectErr:   true,
			errContains: "unsupported DDL statement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := parseSQL(tt.sql)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Len(t, statements, tt.expectCount)
		})
	}
}
