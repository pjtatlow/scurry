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
		"async",
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
				`ALTER TABLE _scurry_.migrations ADD COLUMN async BOOL NOT NULL DEFAULT false`,
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
					error_msg STRING,
					async BOOL NOT NULL DEFAULT false
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
				`ALTER TABLE _scurry_.migrations ADD COLUMN async BOOL NOT NULL DEFAULT false`,
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

func TestAsyncFlagInMigrations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	tests := []struct {
		name          string
		migration     Migration
		expectedAsync bool
	}{
		{
			name: "sync migration has async=false",
			migration: Migration{
				Name:     "20240101120000_sync_mig",
				SQL:      "CREATE TABLE async_test_sync (id INT PRIMARY KEY)",
				Checksum: "sync_checksum",
				Mode:     "sync",
			},
			expectedAsync: false,
		},
		{
			name: "async migration has async=true",
			migration: Migration{
				Name:     "20240101130000_async_mig",
				SQL:      "CREATE TABLE async_test_async (id INT PRIMARY KEY)",
				Checksum: "async_checksum",
				Mode:     "async",
			},
			expectedAsync: true,
		},
		{
			name: "empty mode migration has async=false",
			migration: Migration{
				Name:     "20240101140000_empty_mode_mig",
				SQL:      "CREATE TABLE async_test_empty (id INT PRIMARY KEY)",
				Checksum: "empty_checksum",
				Mode:     "",
			},
			expectedAsync: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.ExecuteMigrationWithTracking(ctx, tt.migration)
			require.NoError(t, err)

			migrations, err := client.GetAppliedMigrations(ctx)
			require.NoError(t, err)

			// Find the migration we just executed
			var found *AppliedMigration
			for i, m := range migrations {
				if m.Name == tt.migration.Name {
					found = &migrations[i]
					break
				}
			}
			require.NotNil(t, found, "migration %s should be in applied migrations", tt.migration.Name)
			assert.Equal(t, tt.expectedAsync, found.Async)
		})
	}
}

func TestRecordMigrationAsync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	tests := []struct {
		name          string
		migName       string
		async         bool
		expectedAsync bool
	}{
		{
			name:          "record sync migration",
			migName:       "20240101120000_record_sync",
			async:         false,
			expectedAsync: false,
		},
		{
			name:          "record async migration",
			migName:       "20240101130000_record_async",
			async:         true,
			expectedAsync: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.RecordMigration(ctx, tt.migName, "checksum", tt.async)
			require.NoError(t, err)

			migrations, err := client.GetAppliedMigrations(ctx)
			require.NoError(t, err)

			var found *AppliedMigration
			for i, m := range migrations {
				if m.Name == tt.migName {
					found = &migrations[i]
					break
				}
			}
			require.NotNil(t, found)
			assert.Equal(t, tt.expectedAsync, found.Async)
		})
	}
}

func TestGetFailedMigration_IgnoresPendingAsync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Start an async migration (leaves it in pending state)
	err = client.StartMigration(ctx, "20240101120000_async_pending", "checksum1", true)
	require.NoError(t, err)

	// GetFailedMigration should NOT return the pending async migration
	failed, err := client.GetFailedMigration(ctx)
	require.NoError(t, err)
	assert.Nil(t, failed, "pending async migration should not block")

	// But a pending sync migration SHOULD block
	err = client.StartMigration(ctx, "20240101130000_sync_pending", "checksum2", false)
	require.NoError(t, err)

	failed, err = client.GetFailedMigration(ctx)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, "20240101130000_sync_pending", failed.Name)
	assert.Equal(t, MigrationStatusPending, failed.Status)
}

func TestHasRunningAsyncMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name        string
		setup       func(t *testing.T, client *Client)
		expectFound bool
		expectName  string
	}{
		{
			name:        "no migrations at all",
			setup:       func(t *testing.T, client *Client) {},
			expectFound: false,
		},
		{
			name: "pending async migration exists",
			setup: func(t *testing.T, client *Client) {
				err := client.StartMigration(ctx, "20240101120000_async_running", "checksum", true)
				require.NoError(t, err)
			},
			expectFound: true,
			expectName:  "20240101120000_async_running",
		},
		{
			name: "pending sync migration does not count",
			setup: func(t *testing.T, client *Client) {
				err := client.StartMigration(ctx, "20240101120000_sync_running", "checksum", false)
				require.NoError(t, err)
			},
			expectFound: false,
		},
		{
			name: "completed async migration does not count",
			setup: func(t *testing.T, client *Client) {
				err := client.RecordMigration(ctx, "20240101120000_async_done", "checksum", true)
				require.NoError(t, err)
			},
			expectFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			err = client.InitMigrationHistory(ctx)
			require.NoError(t, err)

			tt.setup(t, client)

			running, err := client.HasRunningAsyncMigration(ctx)
			require.NoError(t, err)

			if tt.expectFound {
				require.NotNil(t, running)
				assert.Equal(t, tt.expectName, running.Name)
				assert.True(t, running.Async)
			} else {
				assert.Nil(t, running)
			}
		})
	}
}

func TestCheckDependenciesMet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name        string
		setup       func(t *testing.T, client *Client)
		dependsOn   []string
		expectUnmet []string
	}{
		{
			name:        "empty depends_on returns nil",
			setup:       func(t *testing.T, client *Client) {},
			dependsOn:   nil,
			expectUnmet: nil,
		},
		{
			name: "all dependencies succeeded",
			setup: func(t *testing.T, client *Client) {
				err := client.RecordMigration(ctx, "20240101120000_dep_a", "checksum", false)
				require.NoError(t, err)
				err = client.RecordMigration(ctx, "20240101130000_dep_b", "checksum", false)
				require.NoError(t, err)
			},
			dependsOn:   []string{"20240101120000_dep_a", "20240101130000_dep_b"},
			expectUnmet: nil,
		},
		{
			name: "recovered dependency counts as met",
			setup: func(t *testing.T, client *Client) {
				err := client.StartMigration(ctx, "20240101120000_dep_recovered", "checksum", false)
				require.NoError(t, err)
				err = client.FailMigration(ctx, "20240101120000_dep_recovered", "stmt", "error")
				require.NoError(t, err)
				err = client.RecoverMigration(ctx, "20240101120000_dep_recovered")
				require.NoError(t, err)
			},
			dependsOn:   []string{"20240101120000_dep_recovered"},
			expectUnmet: nil,
		},
		{
			name:        "dependency not in table is unmet",
			setup:       func(t *testing.T, client *Client) {},
			dependsOn:   []string{"20240101120000_nonexistent"},
			expectUnmet: []string{"20240101120000_nonexistent"},
		},
		{
			name: "pending dependency is unmet",
			setup: func(t *testing.T, client *Client) {
				err := client.StartMigration(ctx, "20240101120000_dep_pending", "checksum", true)
				require.NoError(t, err)
			},
			dependsOn:   []string{"20240101120000_dep_pending"},
			expectUnmet: []string{"20240101120000_dep_pending"},
		},
		{
			name: "failed dependency is unmet",
			setup: func(t *testing.T, client *Client) {
				err := client.StartMigration(ctx, "20240101120000_dep_failed", "checksum", false)
				require.NoError(t, err)
				err = client.FailMigration(ctx, "20240101120000_dep_failed", "stmt", "error")
				require.NoError(t, err)
			},
			dependsOn:   []string{"20240101120000_dep_failed"},
			expectUnmet: []string{"20240101120000_dep_failed"},
		},
		{
			name: "mix of met and unmet dependencies",
			setup: func(t *testing.T, client *Client) {
				err := client.RecordMigration(ctx, "20240101120000_dep_met", "checksum", false)
				require.NoError(t, err)
			},
			dependsOn:   []string{"20240101120000_dep_met", "20240101130000_dep_missing"},
			expectUnmet: []string{"20240101130000_dep_missing"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			err = client.InitMigrationHistory(ctx)
			require.NoError(t, err)

			tt.setup(t, client)

			unmet, err := client.CheckDependenciesMet(ctx, tt.dependsOn)
			require.NoError(t, err)
			assert.Equal(t, tt.expectUnmet, unmet)
		})
	}
}

func TestSchemaUpgradeAddsAsyncColumn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Manually create a schema without the async column (pre-async schema)
	_, err = client.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS _scurry_`)
	require.NoError(t, err)

	_, err = client.ExecContext(ctx, `
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
	`)
	require.NoError(t, err)

	// Insert a pre-existing migration
	_, err = client.ExecContext(ctx, `
		INSERT INTO _scurry_.migrations (name, checksum)
		VALUES ('20230101000000_pre_async', 'old_checksum')
	`)
	require.NoError(t, err)

	// InitMigrationHistory should add the async column
	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	// Verify old migration defaults to async=false
	migrations, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, migrations, 1)
	assert.Equal(t, false, migrations[0].Async)

	// Verify new async migration works
	err = client.RecordMigration(ctx, "20240101120000_new_async", "checksum", true)
	require.NoError(t, err)

	migrations, err = client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, migrations, 2)

	// Find the new async migration
	var found *AppliedMigration
	for i, m := range migrations {
		if m.Name == "20240101120000_new_async" {
			found = &migrations[i]
			break
		}
	}
	require.NotNil(t, found)
	assert.True(t, found.Async)
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
