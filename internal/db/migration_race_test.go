package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompleteMigration_NotInPendingState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name  string
		setup func(t *testing.T, client *Client) string // returns migration name
	}{
		{
			name: "already succeeded",
			setup: func(t *testing.T, client *Client) string {
				name := "20240101120000_already_succeeded"
				err := client.RecordMigration(ctx, name, "abc123", false)
				require.NoError(t, err)
				return name
			},
		},
		{
			name: "already recovered",
			setup: func(t *testing.T, client *Client) string {
				name := "20240101120000_recovered_then_complete"
				// Create failed migration, then recover it
				err := client.StartMigration(ctx, name, "abc123", false)
				require.NoError(t, err)
				_, err = client.ExecContext(ctx, `
					UPDATE _scurry_.migrations
					SET status = $1, failed_statement = 'SELECT 1', error_msg = 'test'
					WHERE name = $2
				`, MigrationStatusFailed, name)
				require.NoError(t, err)
				err = client.RecoverMigration(ctx, name)
				require.NoError(t, err)
				return name
			},
		},
		{
			name: "non-existent migration",
			setup: func(t *testing.T, client *Client) string {
				return "20240101120000_does_not_exist"
			},
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

			name := tt.setup(t, client)
			err = client.CompleteMigration(ctx, name)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "no longer in pending state")
		})
	}
}

func TestFailMigration_NotInPendingState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name  string
		setup func(t *testing.T, client *Client) string // returns migration name
	}{
		{
			name: "already succeeded",
			setup: func(t *testing.T, client *Client) string {
				name := "20240101120000_already_succeeded"
				err := client.RecordMigration(ctx, name, "abc123", false)
				require.NoError(t, err)
				return name
			},
		},
		{
			name: "non-existent migration",
			setup: func(t *testing.T, client *Client) string {
				return "20240101120000_does_not_exist"
			},
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

			name := tt.setup(t, client)
			err = client.FailMigration(ctx, name, "SELECT 1", "test error")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "no longer in pending state")
		})
	}
}
