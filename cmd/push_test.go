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
)

func TestPushIntegration(t *testing.T) {
	tests := []struct {
		name              string
		initialSchema     map[string]string
		updatedSchema     map[string]string
		expectedStmtCount int
		expectedStmts     []string // Substrings that should appear in statements
	}{
		{
			name: "add table",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
				"tables/posts.sql": `
					CREATE TABLE posts (
						id INT PRIMARY KEY,
						title TEXT NOT NULL
					);
				`,
			},
			expectedStmtCount: 1,
			expectedStmts:     []string{"CREATE TABLE", "posts"},
		},
		{
			name: "remove table",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
				"tables/posts.sql": `
					CREATE TABLE posts (
						id INT PRIMARY KEY,
						title TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			expectedStmtCount: 1,
			expectedStmts:     []string{"DROP TABLE", "posts"},
		},
		{
			name: "add column",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						email TEXT
					);
				`,
			},
			expectedStmtCount: 1,
			expectedStmts:     []string{"ALTER TABLE", "users", "ADD COLUMN", "email"},
		},
		{
			name: "remove column",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						email TEXT
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			expectedStmtCount: 1,
			expectedStmts:     []string{"ALTER TABLE", "users", "DROP COLUMN", "email"},
		},
		{
			name: "add index",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email TEXT NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			expectedStmtCount: 1,
			expectedStmts:     []string{"CREATE INDEX email_idx"},
		},
		{
			name: "modify index",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email TEXT NOT NULL,
						name TEXT NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email TEXT NOT NULL,
						name TEXT NOT NULL,
						INDEX email_idx (email, name)
					);
				`,
			},
			expectedStmtCount: 4, // DROP, COMMIT, BEGIN, CREATE
			expectedStmts:     []string{"DROP INDEX", "CREATE INDEX email_idx"},
		},
		{
			name: "add enum type",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"types/status.sql": `
					CREATE TYPE status AS ENUM ('active', 'inactive');
				`,
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						status status NOT NULL DEFAULT 'active'
					);
				`,
			},
			expectedStmtCount: 2,
			expectedStmts:     []string{"CREATE TYPE", "status", "ALTER TABLE", "users", "ADD COLUMN"},
		},
		{
			name: "add enum value",
			initialSchema: map[string]string{
				"types/status.sql": `
					CREATE TYPE status AS ENUM ('active', 'inactive');
				`,
			},
			updatedSchema: map[string]string{
				"types/status.sql": `
					CREATE TYPE status AS ENUM ('active', 'inactive', 'pending');
				`,
			},
			expectedStmtCount: 1,
			expectedStmts:     []string{"ALTER TYPE", "status", "ADD VALUE", "pending"},
		},
		{
			name: "add sequence",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"sequences/user_id_seq.sql": `
					CREATE SEQUENCE user_id_seq;
				`,
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY DEFAULT nextval('user_id_seq'),
						name TEXT NOT NULL
					);
				`,
			},
			expectedStmtCount: 2,
			expectedStmts:     []string{"CREATE SEQUENCE", "user_id_seq", "ALTER TABLE", "users", "ALTER COLUMN", "id", "SET DEFAULT"},
		},
		{
			name: "multiple changes",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						email TEXT NOT NULL,
						INDEX email_idx (email)
					);
				`,
				"tables/posts.sql": `
					CREATE TABLE posts (
						id INT PRIMARY KEY,
						user_id INT NOT NULL,
						title TEXT NOT NULL
					);
				`,
			},
			expectedStmtCount: 3, // ADD COLUMN, CREATE INDEX, CREATE TABLE
			expectedStmts:     []string{"ALTER TABLE", "users", "ADD COLUMN", "email", "CREATE INDEX", "email_idx", "CREATE TABLE", "posts"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			// Create in-memory filesystem
			fs := afero.NewMemMapFs()
			schemaDir := "/schema"

			// Helper to write schema files
			writeSchemaFiles := func(files map[string]string) {
				// Clear schema directory
				fs.RemoveAll(schemaDir)
				err := fs.MkdirAll(schemaDir, 0755)
				require.NoError(t, err)

				for path, content := range files {
					fullPath := filepath.Join(schemaDir, path)
					dir := filepath.Dir(fullPath)
					err := fs.MkdirAll(dir, 0755)
					require.NoError(t, err)
					err = afero.WriteFile(fs, fullPath, []byte(content), 0644)
					require.NoError(t, err)
				}
			}

			// Push initial schema
			writeSchemaFiles(tt.initialSchema)

			opts := PushOptions{
				Fs:            fs,
				DefinitionDir: schemaDir,
				DbClient:      client,
				Verbose:       false,
				DryRun:        false,
				Force:         true,
			}

			result, err := executePush(ctx, opts)
			require.NoError(t, err)
			if len(tt.initialSchema) > 0 {
				assert.True(t, result.HasChanges, "Initial push should have changes")
			}

			// Update schema files
			writeSchemaFiles(tt.updatedSchema)

			// Push updated schema
			result, err = executePush(ctx, opts)
			require.NoError(t, err)

			// Verify statements
			assert.Len(t, result.Statements, tt.expectedStmtCount,
				"Expected %d statements, got %d: %v",
				tt.expectedStmtCount, len(result.Statements), result.Statements)

			// Verify expected statement substrings
			allStatements := strings.Join(result.Statements, "\n")
			for _, expected := range tt.expectedStmts {
				assert.Contains(t, allStatements, expected,
					"Expected to find %q in statements:\n%s", expected, allStatements)
			}
		})
	}
}

func TestPushIntegrationComplex(t *testing.T) {
	tests := []struct {
		name              string
		initialSchema     map[string]string
		updatedSchema     map[string]string
		expectedStmtCount int
		expectedStmts     []string
	}{
		{
			name: "add tables with foreign keys",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
				"tables/posts.sql": `
					CREATE TABLE posts (
						id INT PRIMARY KEY,
						user_id INT NOT NULL,
						title TEXT NOT NULL,
						FOREIGN KEY (user_id) REFERENCES users(id)
					);
				`,
				"tables/comments.sql": `
					CREATE TABLE comments (
						id INT PRIMARY KEY,
						post_id INT NOT NULL,
						content TEXT NOT NULL,
						FOREIGN KEY (post_id) REFERENCES posts(id)
					);
				`,
			},
			expectedStmtCount: 2,
			expectedStmts:     []string{"CREATE TABLE", "posts", "comments", "FOREIGN KEY"},
		},
		{
			name: "add enum and use in new column with index",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"types/user_status.sql": `
					CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended');
				`,
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						status user_status NOT NULL DEFAULT 'active',
						INDEX status_idx (status)
					);
				`,
			},
			expectedStmtCount: 3, // CREATE TYPE, ADD COLUMN, CREATE INDEX
			expectedStmts:     []string{"CREATE TYPE", "user_status", "ADD COLUMN", "status", "CREATE INDEX", "status_idx"},
		},
		{
			name: "add foreign key to existing table",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
				"tables/posts.sql": `
					CREATE TABLE posts (
						id INT PRIMARY KEY,
						user_id INT NOT NULL,
						title TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
				"tables/posts.sql": `
					CREATE TABLE posts (
						id INT PRIMARY KEY,
						user_id INT NOT NULL,
						title TEXT NOT NULL,
						FOREIGN KEY (user_id) REFERENCES users(id)
					);
				`,
			},
			expectedStmtCount: 1,
			expectedStmts:     []string{"ALTER TABLE", "posts", "ADD CONSTRAINT", "FOREIGN KEY"},
		},
		{
			name: "change column type",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email TEXT NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email VARCHAR(255) NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			// CockroachDB doesn't support ALTER COLUMN TYPE requiring rewrite inside a transaction,
			// so we need to DROP INDEX, then run ALTER COLUMN TYPE outside transaction, then CREATE INDEX
			// Statements: DROP INDEX, COMMIT, BEGIN, COMMIT, ALTER, BEGIN, COMMIT, BEGIN, CREATE INDEX
			expectedStmtCount: 9,
			expectedStmts:     []string{"DROP INDEX", "ALTER COLUMN", "email", "TYPE", "VARCHAR", "CREATE INDEX"},
		},
		{
			name: "add sequence and use as default with enum",
			initialSchema: map[string]string{
				"tables/orders.sql": `
					CREATE TABLE orders (
						id INT PRIMARY KEY,
						total DECIMAL(10,2) NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"sequences/order_id_seq.sql": `
					CREATE SEQUENCE order_id_seq;
				`,
				"types/order_status.sql": `
					CREATE TYPE order_status AS ENUM ('pending', 'processing', 'completed');
				`,
				"tables/orders.sql": `
					CREATE TABLE orders (
						id INT PRIMARY KEY DEFAULT nextval('order_id_seq'),
						total DECIMAL(10,2) NOT NULL,
						status order_status NOT NULL DEFAULT 'pending'
					);
				`,
			},
			expectedStmtCount: 4, // CREATE SEQUENCE, CREATE TYPE, ALTER id DEFAULT, ADD COLUMN status
			expectedStmts:     []string{"CREATE SEQUENCE", "order_id_seq", "CREATE TYPE", "order_status", "ALTER COLUMN", "id", "SET DEFAULT", "ADD COLUMN", "status"},
		},
		{
			name: "add NOT NULL and CHECK constraint",
			initialSchema: map[string]string{
				"tables/products.sql": `
					CREATE TABLE products (
						id INT PRIMARY KEY,
						name TEXT,
						price DECIMAL(10,2)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/products.sql": `
					CREATE TABLE products (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						price DECIMAL(10,2) NOT NULL CHECK (price > 0)
					);
				`,
			},
			expectedStmtCount: 3, // SET NOT NULL name, SET NOT NULL price, ADD CHECK CONSTRAINT
			expectedStmts:     []string{"ALTER COLUMN", "name", "SET NOT NULL", "price", "ADD CONSTRAINT", "CHECK"},
		},
		{
			name: "complex multi-table schema evolution",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"types/user_role.sql": `
					CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');
				`,
				"sequences/user_id_seq.sql": `
					CREATE SEQUENCE user_id_seq;
				`,
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY DEFAULT nextval('user_id_seq'),
						name TEXT NOT NULL,
						email TEXT NOT NULL UNIQUE,
						role user_role NOT NULL DEFAULT 'user',
						created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
						INDEX role_idx (role)
					);
				`,
				"tables/posts.sql": `
					CREATE TABLE posts (
						id INT PRIMARY KEY,
						user_id INT NOT NULL,
						title TEXT NOT NULL,
						content TEXT,
						published_at TIMESTAMPTZ,
						FOREIGN KEY (user_id) REFERENCES users(id),
						INDEX user_idx (user_id)
					);
				`,
			},
			expectedStmtCount: 9, // CREATE TYPE, CREATE SEQUENCE, ALTER id DEFAULT, ADD email, ADD role, ADD created_at, CREATE role_idx, CREATE TABLE posts, CREATE user_idx
			expectedStmts: []string{
				"CREATE TYPE", "user_role",
				"CREATE SEQUENCE", "user_id_seq",
				"ALTER COLUMN", "id", "SET DEFAULT",
				"ADD COLUMN", "email",
				"ADD COLUMN", "role",
				"ADD COLUMN", "created_at",
				"CREATE INDEX", "role_idx",
				"CREATE TABLE", "posts",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			// Create in-memory filesystem
			fs := afero.NewMemMapFs()
			schemaDir := "/schema"

			// Helper to write schema files
			writeSchemaFiles := func(files map[string]string) {
				// Clear schema directory
				fs.RemoveAll(schemaDir)
				err := fs.MkdirAll(schemaDir, 0755)
				require.NoError(t, err)

				for path, content := range files {
					fullPath := filepath.Join(schemaDir, path)
					dir := filepath.Dir(fullPath)
					err := fs.MkdirAll(dir, 0755)
					require.NoError(t, err)
					err = afero.WriteFile(fs, fullPath, []byte(content), 0644)
					require.NoError(t, err)
				}
			}

			// Push initial schema
			writeSchemaFiles(tt.initialSchema)

			opts := PushOptions{
				Fs:            fs,
				DefinitionDir: schemaDir,
				DbClient:      client,
				Verbose:       false,
				DryRun:        false,
				Force:         true,
			}

			result, err := executePush(ctx, opts)
			require.NoError(t, err)
			if len(tt.initialSchema) > 0 {
				assert.True(t, result.HasChanges, "Initial push should have changes")
			}

			// Update schema files
			writeSchemaFiles(tt.updatedSchema)

			// Push updated schema
			result, err = executePush(ctx, opts)
			require.NoError(t, err)

			// Verify statements
			assert.Len(t, result.Statements, tt.expectedStmtCount,
				"Expected %d statements, got %d: %v",
				tt.expectedStmtCount, len(result.Statements), result.Statements)

			// Verify expected statement substrings
			allStatements := strings.Join(result.Statements, "\n")
			for _, expected := range tt.expectedStmts {
				assert.Contains(t, allStatements, expected,
					"Expected to find %q in statements:\n%s", expected, allStatements)
			}
		})
	}
}

func TestPushIntegrationNoChanges(t *testing.T) {
	ctx := context.Background()
	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Create in-memory filesystem
	fs := afero.NewMemMapFs()
	schemaDir := "/schema"
	err = fs.MkdirAll(schemaDir, 0755)
	require.NoError(t, err)

	// Write initial schema
	schemaContent := `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
	`
	err = afero.WriteFile(fs, filepath.Join(schemaDir, "tables/users.sql"), []byte(schemaContent), 0644)
	require.NoError(t, err)

	opts := PushOptions{
		Fs:            fs,
		DefinitionDir: schemaDir,
		DbClient:      client,
		Verbose:       false,
		DryRun:        false,
		Force:         true,
	}

	// First push
	result, err := executePush(ctx, opts)
	require.NoError(t, err)
	assert.True(t, result.HasChanges)
	assert.NotEmpty(t, result.Statements)

	// Second push with no changes
	result, err = executePush(ctx, opts)
	require.NoError(t, err)
	assert.False(t, result.HasChanges, "Second push should have no changes")
	assert.Empty(t, result.Statements)
}

func TestPushIntegrationDryRun(t *testing.T) {
	ctx := context.Background()

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Create in-memory filesystem
	fs := afero.NewMemMapFs()
	schemaDir := "/schema"
	err = fs.MkdirAll(schemaDir, 0755)
	require.NoError(t, err)

	// Write schema
	schemaContent := `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
	`
	err = afero.WriteFile(fs, filepath.Join(schemaDir, "tables/users.sql"), []byte(schemaContent), 0644)
	require.NoError(t, err)

	opts := PushOptions{
		Fs:            fs,
		DefinitionDir: schemaDir,
		DbClient:      client,
		Verbose:       false,
		DryRun:        true,
		Force:         true,
	}

	// Dry run push
	result, err := executePush(ctx, opts)
	require.NoError(t, err)
	assert.True(t, result.HasChanges)
	assert.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "CREATE TABLE")
	assert.Contains(t, result.Statements[0], "users")

	// Verify nothing was applied (another push should still have changes)
	result, err = executePush(ctx, opts)
	require.NoError(t, err)
	assert.True(t, result.HasChanges, "Dry run should not have applied changes")
	assert.NotEmpty(t, result.Statements)
}

func TestPushIntegrationColumnTypeChanges(t *testing.T) {
	tests := []struct {
		name               string
		initialSchema      map[string]string
		updatedSchema      map[string]string
		expectIndexRebuild bool
		expectedStmts      []string
		unexpectedStmts    []string
	}{
		{
			name: "INT4 to INT8 widening - no index rebuild needed",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						age INT4 NOT NULL,
						INDEX age_idx (age)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						age INT8 NOT NULL,
						INDEX age_idx (age)
					);
				`,
			},
			expectIndexRebuild: false,
			expectedStmts:      []string{"ALTER COLUMN", "age", "INT8"},
			unexpectedStmts:    []string{"DROP INDEX", "CREATE INDEX"},
		},
		{
			name: "INT8 to INT4 narrowing - index rebuild required",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						age INT8 NOT NULL,
						INDEX age_idx (age)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						age INT4 NOT NULL,
						INDEX age_idx (age)
					);
				`,
			},
			expectIndexRebuild: true,
			expectedStmts:      []string{"DROP INDEX", "age_idx", "ALTER COLUMN", "age", "INT4", "CREATE INDEX", "age_idx"},
			unexpectedStmts:    nil,
		},
		{
			name: "VARCHAR(100) to VARCHAR(200) widening - no index rebuild needed",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email VARCHAR(100) NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email VARCHAR(200) NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			expectIndexRebuild: false,
			expectedStmts:      []string{"ALTER COLUMN", "email", "VARCHAR(200)"},
			unexpectedStmts:    []string{"DROP INDEX", "CREATE INDEX"},
		},
		{
			name: "VARCHAR(200) to VARCHAR(100) narrowing - index rebuild required",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email VARCHAR(200) NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email VARCHAR(100) NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			expectIndexRebuild: true,
			expectedStmts:      []string{"DROP INDEX", "email_idx", "ALTER COLUMN", "email", "VARCHAR(100)", "CREATE INDEX", "email_idx"},
			unexpectedStmts:    nil,
		},
		{
			name: "VARCHAR(100) to TEXT unbounded - no index rebuild needed",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email VARCHAR(100) NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email TEXT NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			expectIndexRebuild: false,
			expectedStmts:      []string{"ALTER COLUMN", "email", "STRING"},
			unexpectedStmts:    []string{"DROP INDEX", "CREATE INDEX"},
		},
		{
			name: "TEXT to VARCHAR(100) bounded - index rebuild required",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email TEXT NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						email VARCHAR(100) NOT NULL,
						INDEX email_idx (email)
					);
				`,
			},
			expectIndexRebuild: true,
			expectedStmts:      []string{"DROP INDEX", "email_idx", "ALTER COLUMN", "email", "VARCHAR(100)", "CREATE INDEX", "email_idx"},
			unexpectedStmts:    nil,
		},
		{
			name: "INT to STRING family change - index rebuild required",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						code INT NOT NULL,
						INDEX code_idx (code)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						code STRING NOT NULL,
						INDEX code_idx (code)
					);
				`,
			},
			expectIndexRebuild: true,
			expectedStmts:      []string{"DROP INDEX", "code_idx", "ALTER COLUMN", "code", "STRING", "CREATE INDEX", "code_idx"},
			unexpectedStmts:    nil,
		},
		{
			name: "FLOAT4 to FLOAT8 widening - no index rebuild needed",
			initialSchema: map[string]string{
				"tables/products.sql": `
					CREATE TABLE products (
						id INT PRIMARY KEY,
						price FLOAT4 NOT NULL,
						INDEX price_idx (price)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/products.sql": `
					CREATE TABLE products (
						id INT PRIMARY KEY,
						price FLOAT8 NOT NULL,
						INDEX price_idx (price)
					);
				`,
			},
			expectIndexRebuild: false,
			expectedStmts:      []string{"ALTER COLUMN", "price", "FLOAT8"},
			unexpectedStmts:    []string{"DROP INDEX", "CREATE INDEX"},
		},
		{
			name: "DECIMAL(10,2) to DECIMAL(15,4) widening - no index rebuild needed",
			initialSchema: map[string]string{
				"tables/products.sql": `
					CREATE TABLE products (
						id INT PRIMARY KEY,
						price DECIMAL(10,2) NOT NULL,
						INDEX price_idx (price)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/products.sql": `
					CREATE TABLE products (
						id INT PRIMARY KEY,
						price DECIMAL(15,4) NOT NULL,
						INDEX price_idx (price)
					);
				`,
			},
			expectIndexRebuild: false,
			expectedStmts:      []string{"ALTER COLUMN", "price", "DECIMAL(15,4)"},
			unexpectedStmts:    []string{"DROP INDEX", "CREATE INDEX"},
		},
		{
			name: "DECIMAL(15,4) to DECIMAL(10,2) narrowing - index rebuild required",
			initialSchema: map[string]string{
				"tables/products.sql": `
					CREATE TABLE products (
						id INT PRIMARY KEY,
						price DECIMAL(15,4) NOT NULL,
						INDEX price_idx (price)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/products.sql": `
					CREATE TABLE products (
						id INT PRIMARY KEY,
						price DECIMAL(10,2) NOT NULL,
						INDEX price_idx (price)
					);
				`,
			},
			expectIndexRebuild: true,
			expectedStmts:      []string{"DROP INDEX", "price_idx", "ALTER COLUMN", "price", "DECIMAL(10,2)", "CREATE INDEX", "price_idx"},
			unexpectedStmts:    nil,
		},
		{
			name: "type widening without index - no index statements",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						age INT4 NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						age INT8 NOT NULL
					);
				`,
			},
			expectIndexRebuild: false,
			expectedStmts:      []string{"ALTER COLUMN", "age", "INT8"},
			unexpectedStmts:    []string{"DROP INDEX", "CREATE INDEX"},
		},
		{
			name: "type narrowing without index - no index statements",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						age INT8 NOT NULL
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						age INT4 NOT NULL
					);
				`,
			},
			expectIndexRebuild: false,
			expectedStmts:      []string{"ALTER COLUMN", "age", "INT4"},
			unexpectedStmts:    []string{"DROP INDEX", "CREATE INDEX"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			fs := afero.NewMemMapFs()
			schemaDir := "/schema"

			writeSchemaFiles := func(files map[string]string) {
				fs.RemoveAll(schemaDir)
				err := fs.MkdirAll(schemaDir, 0755)
				require.NoError(t, err)

				for path, content := range files {
					fullPath := filepath.Join(schemaDir, path)
					dir := filepath.Dir(fullPath)
					err := fs.MkdirAll(dir, 0755)
					require.NoError(t, err)
					err = afero.WriteFile(fs, fullPath, []byte(content), 0644)
					require.NoError(t, err)
				}
			}

			// Push initial schema
			writeSchemaFiles(tt.initialSchema)

			opts := PushOptions{
				Fs:            fs,
				DefinitionDir: schemaDir,
				DbClient:      client,
				Verbose:       false,
				DryRun:        false,
				Force:         true,
			}

			result, err := executePush(ctx, opts)
			require.NoError(t, err)
			assert.True(t, result.HasChanges, "Initial push should have changes")

			// Push updated schema
			writeSchemaFiles(tt.updatedSchema)

			result, err = executePush(ctx, opts)
			require.NoError(t, err)
			assert.True(t, result.HasChanges, "Updated push should have changes")

			allStatements := strings.Join(result.Statements, "\n")

			// Verify expected statements are present
			for _, expected := range tt.expectedStmts {
				assert.Contains(t, allStatements, expected,
					"Expected to find %q in statements:\n%s", expected, allStatements)
			}

			// Verify unexpected statements are absent
			for _, unexpected := range tt.unexpectedStmts {
				assert.NotContains(t, allStatements, unexpected,
					"Did not expect to find %q in statements:\n%s", unexpected, allStatements)
			}

			// Verify transaction boundaries when index rebuild is expected
			if tt.expectIndexRebuild && strings.Contains(allStatements, "DROP INDEX") {
				assert.Contains(t, allStatements, "COMMIT",
					"Expected COMMIT for transaction boundary:\n%s", allStatements)
				assert.Contains(t, allStatements, "BEGIN",
					"Expected BEGIN for transaction boundary:\n%s", allStatements)
			}
		})
	}
}

func TestPushDropColumnWithConstraint(t *testing.T) {
	tests := []struct {
		name          string
		initialSchema map[string]string
		updatedSchema map[string]string
	}{
		{
			name: "drop column with check constraint",
			initialSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						age INT,
						CONSTRAINT age_check CHECK (age > 0)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/users.sql": `
					CREATE TABLE users (
						id INT PRIMARY KEY,
						name TEXT NOT NULL
					);
				`,
			},
		},
		{
			name: "drop column with multi-column check constraint",
			initialSchema: map[string]string{
				"tables/events.sql": `
					CREATE TABLE events (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						min_age INT,
						max_age INT,
						CONSTRAINT age_range_check CHECK (min_age < max_age)
					);
				`,
			},
			updatedSchema: map[string]string{
				"tables/events.sql": `
					CREATE TABLE events (
						id INT PRIMARY KEY,
						name TEXT NOT NULL,
						max_age INT
					);
				`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			fs := afero.NewMemMapFs()
			schemaDir := "/schema"

			writeSchemaFiles := func(files map[string]string) {
				fs.RemoveAll(schemaDir)
				err := fs.MkdirAll(schemaDir, 0755)
				require.NoError(t, err)

				for path, content := range files {
					fullPath := filepath.Join(schemaDir, path)
					dir := filepath.Dir(fullPath)
					err := fs.MkdirAll(dir, 0755)
					require.NoError(t, err)
					err = afero.WriteFile(fs, fullPath, []byte(content), 0644)
					require.NoError(t, err)
				}
			}

			// Push initial schema with column and constraint
			writeSchemaFiles(tt.initialSchema)

			opts := PushOptions{
				Fs:            fs,
				DefinitionDir: schemaDir,
				DbClient:      client,
				Verbose:       false,
				DryRun:        false,
				Force:         true,
			}

			result, err := executePush(ctx, opts)
			require.NoError(t, err)
			assert.True(t, result.HasChanges, "Initial push should have changes")

			// Push updated schema that removes the column
			// This should succeed - the constraint should be dropped with the column,
			// not as a separate statement that causes "constraint is in the middle of being dropped"
			writeSchemaFiles(tt.updatedSchema)

			result, err = executePush(ctx, opts)
			require.NoError(t, err, "Push should succeed - dropping column should not cause constraint drop error")
			assert.True(t, result.HasChanges, "Updated push should have changes")
		})
	}
}
