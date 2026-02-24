package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
)

func TestTruncateChecksum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		checksum string
		expected string
	}{
		{
			name:     "longer than display length gets truncated",
			checksum: "abcdef1234567890extra",
			expected: "abcdef1234567890...",
		},
		{
			name:     "exactly display length stays unchanged",
			checksum: "abcdef1234567890",
			expected: "abcdef1234567890",
		},
		{
			name:     "shorter than display length stays unchanged",
			checksum: "abc123",
			expected: "abc123",
		},
		{
			name:     "empty string stays unchanged",
			checksum: "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := TruncateChecksum(tt.checksum)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTryAgain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name           string
		migrationSQL   string
		setupSQL       string // SQL to run before the migration to simulate prior state
		wantErr        bool
		wantStatus     string
		wantFailedStmt bool
	}{
		{
			name:         "successful retry of valid SQL",
			migrationSQL: "CREATE TABLE tryagain_success (id INT PRIMARY KEY, name STRING NOT NULL)",
			wantErr:      false,
			wantStatus:   db.MigrationStatusSucceeded,
		},
		{
			name: "retry that fails at a statement records failure",
			migrationSQL: `
				CREATE TABLE tryagain_fail_1 (id INT PRIMARY KEY);
				ALTER TABLE nonexistent_tryagain_table ADD COLUMN foo STRING;
			`,
			wantErr:        true,
			wantStatus:     db.MigrationStatusFailed,
			wantFailedStmt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			err = client.InitMigrationHistory(ctx)
			require.NoError(t, err)

			migrationName := "20240101120000_tryagain_test"
			checksum := "tryagain_checksum"

			// Create a failed migration so TryAgain can reset it
			migration := db.Migration{
				Name:     migrationName,
				SQL:      "SELECT * FROM nonexistent_setup_table",
				Checksum: "old_checksum",
			}
			err = client.ExecuteMigrationWithTracking(ctx, migration)
			require.Error(t, err)

			if tt.setupSQL != "" {
				_, err = client.ExecContext(ctx, tt.setupSQL)
				require.NoError(t, err)
			}

			// Now try again with the real migration SQL
			retryMigration := db.Migration{
				Name:     migrationName,
				SQL:      tt.migrationSQL,
				Checksum: checksum,
			}

			err = TryAgain(ctx, client, retryMigration)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Verify the migration status
			migrations, err := client.GetAppliedMigrations(ctx)
			require.NoError(t, err)
			require.Len(t, migrations, 1)

			assert.Equal(t, tt.wantStatus, migrations[0].Status)
			if tt.wantFailedStmt {
				assert.NotNil(t, migrations[0].FailedStatement)
			} else {
				assert.Nil(t, migrations[0].FailedStatement)
			}
		})
	}
}

func TestMarkSucceeded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	err = client.InitMigrationHistory(ctx)
	require.NoError(t, err)

	migrationName := "20240101120000_marksucceeded_test"
	checksum := "marksucceeded_checksum"

	// Create a failed migration in the database so RecoverMigration can transition it
	err = client.StartMigration(ctx, migrationName, checksum, false)
	require.NoError(t, err)
	err = client.FailMigration(ctx, migrationName, "some statement", "some error")
	require.NoError(t, err)

	migration := db.Migration{
		Name:     migrationName,
		SQL:      "CREATE TABLE marksucceeded_test (id INT PRIMARY KEY)",
		Checksum: checksum,
	}

	err = MarkSucceeded(ctx, client, migration)
	require.NoError(t, err)

	// Verify the migration is marked as recovered
	migrations, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, migrations, 1)

	assert.Equal(t, db.MigrationStatusRecovered, migrations[0].Status)
	assert.Nil(t, migrations[0].FailedStatement)
	assert.Nil(t, migrations[0].ErrorMsg)
}
