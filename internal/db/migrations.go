package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Migration represents a migration file with its metadata
type Migration struct {
	Name      string
	SQL       string
	Checksum  string
	Mode      string // "sync", "async", or "" (treated as sync)
	DependsOn []string
}

// Migration status constants
const (
	MigrationStatusPending   = "pending"
	MigrationStatusSucceeded = "succeeded"
	MigrationStatusFailed    = "failed"
	MigrationStatusRecovered = "recovered"
)

// Migration mode constants
const (
	MigrationModeSync  = "sync"
	MigrationModeAsync = "async"
)

// AppliedMigration represents a migration that has been applied to the database
type AppliedMigration struct {
	Name            string
	Checksum        string
	Status          string // pending, succeeded, failed, recovered
	StartedAt       *time.Time
	CompletedAt     *time.Time
	AppliedAt       time.Time // kept for backwards compatibility
	ExecutedBy      string
	FailedStatement *string
	ErrorMsg        *string
	Async           bool
}

// GetAppliedMigrations returns all migrations that have been applied to the database
func (c *Client) GetAppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT name, checksum, status, started_at, completed_at, applied_at, executed_by, failed_statement, error_msg, async
		FROM _scurry_.migrations
		ORDER BY name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var migrations []AppliedMigration
	for rows.Next() {
		var m AppliedMigration
		if err := rows.Scan(&m.Name, &m.Checksum, &m.Status, &m.StartedAt, &m.CompletedAt, &m.AppliedAt, &m.ExecutedBy, &m.FailedStatement, &m.ErrorMsg, &m.Async); err != nil {
			return nil, fmt.Errorf("failed to scan migration: %w", err)
		}
		migrations = append(migrations, m)
	}

	return migrations, rows.Err()
}

// RecordMigration records a migration as applied in the history table
// This is used for marking migrations as succeeded without tracking execution
func (c *Client) RecordMigration(ctx context.Context, name, checksum string, async bool) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO _scurry_.migrations (name, checksum, status, completed_at, async)
		VALUES ($1, $2, $3, now(), $4)
	`, name, checksum, MigrationStatusSucceeded, async)
	if err != nil {
		return fmt.Errorf("failed to record migration %s: %w", name, err)
	}
	return nil
}

// StartMigration records a migration as pending with started_at timestamp
func (c *Client) StartMigration(ctx context.Context, name, checksum string, async bool) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO _scurry_.migrations (name, checksum, status, started_at, async)
		VALUES ($1, $2, $3, now(), $4)
	`, name, checksum, MigrationStatusPending, async)
	if err != nil {
		return fmt.Errorf("failed to start migration %s: %w", name, err)
	}
	return nil
}

// CompleteMigration marks a migration as succeeded with completed_at timestamp.
// Only succeeds if the migration is currently in pending state.
// Returns an error if the migration was recovered or modified by another process.
func (c *Client) CompleteMigration(ctx context.Context, name string) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE _scurry_.migrations
		SET status = $2, completed_at = now()
		WHERE name = $1 AND status = $3
	`, name, MigrationStatusSucceeded, MigrationStatusPending)
	if err != nil {
		return fmt.Errorf("failed to complete migration %s: %w", name, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rowsAffected == 0 {
		// Migration was not in pending state - likely recovered by another process
		return fmt.Errorf("migration %s is no longer in pending state (may have been recovered by another process)", name)
	}

	return nil
}

// FailMigration marks a migration as failed with the statement and error message.
// Only succeeds if the migration is currently in pending state.
// Returns an error if the migration was recovered or modified by another process.
func (c *Client) FailMigration(ctx context.Context, name, failedStatement, errorMsg string) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE _scurry_.migrations
		SET status = $2, completed_at = now(), failed_statement = $3, error_msg = $4
		WHERE name = $1 AND status = $5
	`, name, MigrationStatusFailed, failedStatement, errorMsg, MigrationStatusPending)
	if err != nil {
		return fmt.Errorf("failed to mark migration %s as failed: %w", name, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rowsAffected == 0 {
		// Migration was not in pending state - likely recovered by another process
		return fmt.Errorf("migration %s is no longer in pending state (may have been recovered by another process)", name)
	}

	return nil
}

// RecoverMigration marks a migration as recovered (manual intervention).
// Only succeeds if the migration is currently in failed state.
func (c *Client) RecoverMigration(ctx context.Context, name string) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE _scurry_.migrations
		SET status = $2, completed_at = now(), failed_statement = NULL, error_msg = NULL
		WHERE name = $1 AND status = $3
	`, name, MigrationStatusRecovered, MigrationStatusFailed)
	if err != nil {
		return fmt.Errorf("failed to recover migration %s: %w", name, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("migration %s is not in failed state", name)
	}

	return nil
}

// ResetMigrationForRetry resets a failed migration to pending state for retry.
// Only succeeds if the migration is currently in failed state.
func (c *Client) ResetMigrationForRetry(ctx context.Context, name, checksum string) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE _scurry_.migrations
		SET status = $2, checksum = $3, started_at = now(), completed_at = NULL, failed_statement = NULL, error_msg = NULL
		WHERE name = $1 AND status = $4
	`, name, MigrationStatusPending, checksum, MigrationStatusFailed)
	if err != nil {
		return fmt.Errorf("failed to reset migration %s for retry: %w", name, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("migration %s is not in failed state", name)
	}

	return nil
}

// GetFailedMigration returns a failed or pending migration if one exists.
// A failed migration always blocks. A pending sync migration blocks.
// A pending async migration does NOT block (it's expected to be in-flight).
func (c *Client) GetFailedMigration(ctx context.Context) (*AppliedMigration, error) {
	var m AppliedMigration
	err := c.db.QueryRowContext(ctx, `
		SELECT name, checksum, status, started_at, completed_at, applied_at, executed_by, failed_statement, error_msg, async
		FROM _scurry_.migrations
		WHERE status = $1
		   OR (status = $2 AND async = false)
		ORDER BY name ASC
		LIMIT 1
	`, MigrationStatusFailed, MigrationStatusPending).Scan(
		&m.Name, &m.Checksum, &m.Status, &m.StartedAt, &m.CompletedAt, &m.AppliedAt, &m.ExecutedBy, &m.FailedStatement, &m.ErrorMsg, &m.Async,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get failed migration: %w", err)
	}
	return &m, nil
}

// CheckDependenciesMet queries the migrations table and returns any dependency names
// that have not succeeded or been recovered. If a name doesn't exist in the table at
// all, it is also considered unmet.
func (c *Client) CheckDependenciesMet(ctx context.Context, dependsOn []string) ([]string, error) {
	if len(dependsOn) == 0 {
		return nil, nil
	}

	// Query all matching migrations that are in a "done" state
	rows, err := c.db.QueryContext(ctx, `
		SELECT name FROM _scurry_.migrations
		WHERE name = ANY($1)
		  AND status IN ($2, $3)
	`, pq.Array(dependsOn), MigrationStatusSucceeded, MigrationStatusRecovered)
	if err != nil {
		return nil, fmt.Errorf("failed to check dependencies: %w", err)
	}
	defer rows.Close()

	met := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan dependency: %w", err)
		}
		met[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var unmet []string
	for _, dep := range dependsOn {
		if !met[dep] {
			unmet = append(unmet, dep)
		}
	}
	return unmet, nil
}

// HasRunningAsyncMigration returns the pending async migration if one exists, nil otherwise.
func (c *Client) HasRunningAsyncMigration(ctx context.Context) (*AppliedMigration, error) {
	var m AppliedMigration
	err := c.db.QueryRowContext(ctx, `
		SELECT name, checksum, status, started_at, completed_at, applied_at, executed_by, failed_statement, error_msg, async
		FROM _scurry_.migrations
		WHERE async = true AND status = $1
		ORDER BY name ASC
		LIMIT 1
	`, MigrationStatusPending).Scan(
		&m.Name, &m.Checksum, &m.Status, &m.StartedAt, &m.CompletedAt, &m.AppliedAt, &m.ExecutedBy, &m.FailedStatement, &m.ErrorMsg, &m.Async,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to check for running async migration: %w", err)
	}
	return &m, nil
}
