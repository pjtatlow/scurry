package db

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDesiredMigrationsTableSchemaIsValid(t *testing.T) {
	// Ensure the embedded SQL file is valid and can be parsed
	statements, err := SplitStatements(DesiredMigrationsTableSchema)
	require.NoError(t, err, "DesiredMigrationsTableSchema should be valid SQL")
	require.Len(t, statements, 1, "DesiredMigrationsTableSchema should contain exactly one statement")
	assert.Contains(t, statements[0], "CREATE TABLE", "DesiredMigrationsTableSchema should be a CREATE TABLE statement")
	assert.Contains(t, statements[0], "_scurry_.migrations", "DesiredMigrationsTableSchema should create _scurry_.migrations table")

	// Verify all expected columns are present
	expectedColumns := []string{
		"name",
		"checksum",
		"status",
		"started_at",
		"completed_at",
		"applied_at",
		"executed_by",
		"failed_statement",
		"error_msg",
	}
	for _, col := range expectedColumns {
		assert.Contains(t, statements[0], col, "DesiredMigrationsTableSchema should contain column %q", col)
	}
}

func TestGenerateMigrationsTableAlterStatements(t *testing.T) {
	tests := []struct {
		name               string
		currentSchema      string
		desiredSchema      string
		expectedStatements []string
		wantErr            bool
	}{
		{
			name: "original schema to current - adds all new columns",
			currentSchema: `
				CREATE TABLE _scurry_.migrations (
					name STRING PRIMARY KEY,
					checksum STRING NOT NULL,
					applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
					executed_by STRING NOT NULL DEFAULT current_user()
				)
			`,
			desiredSchema: DesiredMigrationsTableSchema,
			expectedStatements: []string{
				`ALTER TABLE _scurry_.migrations ADD COLUMN completed_at TIMESTAMPTZ`,
				`ALTER TABLE _scurry_.migrations ADD COLUMN error_msg STRING`,
				`ALTER TABLE _scurry_.migrations ADD COLUMN failed_statement STRING`,
				`ALTER TABLE _scurry_.migrations ADD COLUMN started_at TIMESTAMPTZ`,
				`ALTER TABLE _scurry_.migrations ADD COLUMN status STRING NOT NULL DEFAULT 'succeeded'`,
			},
			wantErr: false,
		},
		{
			name: "schema already up to date - no changes needed",
			currentSchema: `
				CREATE TABLE _scurry_.migrations (
					name STRING PRIMARY KEY,
					checksum STRING NOT NULL,
					status STRING NOT NULL DEFAULT 'succeeded',
					started_at TIMESTAMPTZ,
					completed_at TIMESTAMPTZ,
					applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
					executed_by STRING NOT NULL DEFAULT current_user(),
					failed_statement STRING,
					error_msg STRING
				)
			`,
			desiredSchema:      DesiredMigrationsTableSchema,
			expectedStatements: nil,
			wantErr:           false,
		},
		{
			name: "partial upgrade - only missing some columns",
			currentSchema: `
				CREATE TABLE _scurry_.migrations (
					name STRING PRIMARY KEY,
					checksum STRING NOT NULL,
					status STRING NOT NULL DEFAULT 'succeeded',
					applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
					executed_by STRING NOT NULL DEFAULT current_user()
				)
			`,
			desiredSchema: DesiredMigrationsTableSchema,
			expectedStatements: []string{
				`ALTER TABLE _scurry_.migrations ADD COLUMN completed_at TIMESTAMPTZ`,
				`ALTER TABLE _scurry_.migrations ADD COLUMN error_msg STRING`,
				`ALTER TABLE _scurry_.migrations ADD COLUMN failed_statement STRING`,
				`ALTER TABLE _scurry_.migrations ADD COLUMN started_at TIMESTAMPTZ`,
			},
			wantErr: false,
		},
		{
			name:          "invalid current schema",
			currentSchema: "CREATE TABLE (",
			desiredSchema: DesiredMigrationsTableSchema,
			wantErr:       true,
		},
		{
			name:          "invalid desired schema",
			currentSchema: DesiredMigrationsTableSchema,
			desiredSchema: "CREATE TABLE (",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := generateMigrationsTableAlterStatements(tt.currentSchema, tt.desiredSchema)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Sort both slices to ensure consistent comparison (map iteration order is not guaranteed)
			sort.Strings(statements)
			sort.Strings(tt.expectedStatements)

			assert.Equal(t, tt.expectedStatements, statements)
		})
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
		wantErr  bool
	}{
		{
			name:     "single statement",
			sql:      "CREATE TABLE users (id INT PRIMARY KEY)",
			expected: []string{"CREATE TABLE users (id INT8 PRIMARY KEY)"},
			wantErr:  false,
		},
		{
			name:     "multiple statements",
			sql:      "CREATE TABLE users (id INT PRIMARY KEY); CREATE TABLE posts (id INT PRIMARY KEY)",
			expected: []string{"CREATE TABLE users (id INT8 PRIMARY KEY)", "CREATE TABLE posts (id INT8 PRIMARY KEY)"},
			wantErr:  false,
		},
		{
			name: "statements with newlines",
			sql: `
				CREATE TABLE users (
					id INT PRIMARY KEY,
					name TEXT NOT NULL
				);
				CREATE TABLE posts (
					id INT PRIMARY KEY,
					title TEXT NOT NULL
				);
			`,
			expected: []string{
				"CREATE TABLE users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
				"CREATE TABLE posts (id INT8 PRIMARY KEY, title STRING NOT NULL)",
			},
			wantErr: false,
		},
		{
			name: "alter statements",
			sql: `
				ALTER TABLE users ADD COLUMN email TEXT;
				ALTER TABLE users ADD COLUMN created_at TIMESTAMPTZ DEFAULT now();
			`,
			expected: []string{
				"ALTER TABLE users ADD COLUMN email STRING",
				"ALTER TABLE users ADD COLUMN created_at TIMESTAMPTZ DEFAULT now()",
			},
			wantErr: false,
		},
		{
			name: "mixed DDL statements",
			sql: `
				CREATE TYPE status AS ENUM ('active', 'inactive');
				CREATE TABLE users (id INT PRIMARY KEY, status status NOT NULL);
			`,
			expected: []string{
				"CREATE TYPE status AS ENUM ('active', 'inactive')",
				"CREATE TABLE users (id INT8 PRIMARY KEY, status status NOT NULL)",
			},
			wantErr: false,
		},
		{
			name:     "empty sql",
			sql:      "",
			expected: nil,
			wantErr:  false,
		},
		{
			name:     "whitespace only",
			sql:      "   \n\t\n   ",
			expected: nil,
			wantErr:  false,
		},
		{
			name:    "invalid sql",
			sql:     "CREATE TABLE (",
			wantErr: true,
		},
		{
			name: "insert statements (DML)",
			sql:  "INSERT INTO users (id, name) VALUES (1, 'test')",
			expected: []string{
				"INSERT INTO users(id, name) VALUES (1, 'test')",
			},
			wantErr: false,
		},
		{
			name: "drop statements",
			sql:  "DROP TABLE IF EXISTS users",
			expected: []string{
				"DROP TABLE IF EXISTS users",
			},
			wantErr: false,
		},
		{
			name: "create index statements",
			sql: `
				CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);
				CREATE INDEX users_name_idx ON users (name);
			`,
			expected: []string{
				"CREATE TABLE users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
				"CREATE INDEX users_name_idx ON users (name)",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := SplitStatements(tt.sql)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, statements)
		})
	}
}

// Integration tests that use a real database

func TestInitMigrationHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Initialize migration history
	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Verify table was created by querying it
	migrations, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	assert.Empty(t, migrations)

	// Calling InitMigrationHistory again should be idempotent
	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)
}

func TestExecuteMigrationWithTracking_Success(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Execute a successful migration
	migration := Migration{
		Name:     "20240101120000_create_users",
		SQL:      "CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL)",
		Checksum: "abc123",
	}

	err = client.ExecuteMigrationWithTracking(ctx, migration)
	require.NoError(t, err)

	// Verify migration was recorded as succeeded
	migrations, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, migrations, 1)

	assert.Equal(t, migration.Name, migrations[0].Name)
	assert.Equal(t, migration.Checksum, migrations[0].Checksum)
	assert.Equal(t, MigrationStatusSucceeded, migrations[0].Status)
	assert.NotNil(t, migrations[0].StartedAt)
	assert.NotNil(t, migrations[0].CompletedAt)
	assert.Nil(t, migrations[0].FailedStatement)
	assert.Nil(t, migrations[0].ErrorMsg)

	// Verify no failed migrations
	failed, err := client.GetFailedMigration(ctx)
	require.NoError(t, err)
	assert.Nil(t, failed)
}

func TestExecuteMigrationWithTracking_Failure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Execute a migration that will fail (second statement references non-existent table)
	migration := Migration{
		Name: "20240101120000_bad_migration",
		SQL: `
			CREATE TABLE posts (id INT PRIMARY KEY, title STRING NOT NULL);
			ALTER TABLE nonexistent_table ADD COLUMN foo STRING;
		`,
		Checksum: "def456",
	}

	err = client.ExecuteMigrationWithTracking(ctx, migration)
	require.Error(t, err)

	// Verify migration was recorded as failed
	migrations, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, migrations, 1)

	assert.Equal(t, migration.Name, migrations[0].Name)
	assert.Equal(t, migration.Checksum, migrations[0].Checksum)
	assert.Equal(t, MigrationStatusFailed, migrations[0].Status)
	assert.NotNil(t, migrations[0].StartedAt)
	assert.NotNil(t, migrations[0].CompletedAt)
	assert.NotNil(t, migrations[0].FailedStatement)
	assert.Contains(t, *migrations[0].FailedStatement, "nonexistent_table")
	assert.NotNil(t, migrations[0].ErrorMsg)

	// Verify GetFailedMigration returns the failed migration
	failed, err := client.GetFailedMigration(ctx)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, migration.Name, failed.Name)
	assert.Equal(t, MigrationStatusFailed, failed.Status)
}

func TestRecoverMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Create a failed migration (valid SQL that fails at execution time)
	migration := Migration{
		Name:     "20240101120000_failed_migration",
		SQL:      "ALTER TABLE nonexistent_recover_table ADD COLUMN foo STRING",
		Checksum: "ghi789",
	}

	err = client.ExecuteMigrationWithTracking(ctx, migration)
	require.Error(t, err)

	// Verify it's failed
	failed, err := client.GetFailedMigration(ctx)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, MigrationStatusFailed, failed.Status)

	// Recover the migration
	err = client.RecoverMigration(ctx, migration.Name)
	require.NoError(t, err)

	// Verify it's now recovered
	migrations, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, migrations, 1)
	assert.Equal(t, MigrationStatusRecovered, migrations[0].Status)
	assert.Nil(t, migrations[0].FailedStatement)
	assert.Nil(t, migrations[0].ErrorMsg)

	// Verify no more failed migrations
	failed, err = client.GetFailedMigration(ctx)
	require.NoError(t, err)
	assert.Nil(t, failed)
}

func TestResetMigrationForRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Create a failed migration
	migration := Migration{
		Name:     "20240101120000_retry_migration",
		SQL:      "SELECT * FROM nonexistent",
		Checksum: "old_checksum",
	}

	err = client.ExecuteMigrationWithTracking(ctx, migration)
	require.Error(t, err)

	// Verify it's failed
	failed, err := client.GetFailedMigration(ctx)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, MigrationStatusFailed, failed.Status)
	assert.Equal(t, "old_checksum", failed.Checksum)

	// Reset for retry with new checksum
	err = client.ResetMigrationForRetry(ctx, migration.Name, "new_checksum")
	require.NoError(t, err)

	// Verify it's now pending with new checksum
	failed, err = client.GetFailedMigration(ctx)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, MigrationStatusPending, failed.Status)
	assert.Equal(t, "new_checksum", failed.Checksum)
	assert.Nil(t, failed.FailedStatement)
	assert.Nil(t, failed.ErrorMsg)
}

func TestSchemaUpgradeFromOldVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Manually create the old schema (v1 without status columns)
	_, err = client.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS _scurry_`)
	require.NoError(t, err)

	_, err = client.ExecContext(ctx, `
		CREATE TABLE _scurry_.migrations (
			name STRING PRIMARY KEY,
			checksum STRING NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			executed_by STRING NOT NULL DEFAULT current_user()
		)
	`)
	require.NoError(t, err)

	// Insert a migration record using the old schema
	_, err = client.ExecContext(ctx, `
		INSERT INTO _scurry_.migrations (name, checksum)
		VALUES ('20230101000000_old_migration', 'old_checksum')
	`)
	require.NoError(t, err)

	// Now call InitMigrationHistory - it should upgrade the schema
	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Verify we can read migrations with the new schema
	migrations, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, migrations, 1)

	// Old migration should have status=succeeded (from DEFAULT)
	assert.Equal(t, "20230101000000_old_migration", migrations[0].Name)
	assert.Equal(t, "old_checksum", migrations[0].Checksum)
	assert.Equal(t, MigrationStatusSucceeded, migrations[0].Status)

	// Verify new migrations work with all the new columns
	migration := Migration{
		Name:     "20240101120000_new_migration",
		SQL:      "CREATE TABLE test_upgrade (id INT PRIMARY KEY)",
		Checksum: "new_checksum",
	}

	err = client.ExecuteMigrationWithTracking(ctx, migration)
	require.NoError(t, err)

	migrations, err = client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, migrations, 2)
}

func TestExecuteRemainingStatements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Create the first table manually (simulating partial execution)
	_, err = client.ExecContext(ctx, "CREATE TABLE remaining_test_1 (id INT PRIMARY KEY)")
	require.NoError(t, err)

	// Migration with 3 statements - pretend the 2nd one failed
	migration := Migration{
		Name: "20240101120000_remaining",
		SQL: `
			CREATE TABLE remaining_test_1 (id INT PRIMARY KEY);
			CREATE TABLE remaining_test_2 (id INT PRIMARY KEY);
			CREATE TABLE remaining_test_3 (id INT PRIMARY KEY);
		`,
		Checksum: "remaining_checksum",
	}

	// The failed statement (normalized by parser)
	failedStatement := "CREATE TABLE remaining_test_2 (id INT8 PRIMARY KEY)"

	// Execute remaining statements (should create tables 2 and 3)
	err = client.ExecuteRemainingStatements(ctx, migration, failedStatement)
	require.NoError(t, err)

	// Verify table 3 was created
	var exists bool
	err = client.GetDB().QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'remaining_test_3'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists, "remaining_test_3 should have been created")
}
