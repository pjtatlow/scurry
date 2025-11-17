package db

import (
	"context"
	"fmt"
	"time"
)

// Migration represents a migration file with its metadata
type Migration struct {
	Name     string
	SQL      string
	Checksum string
}

// AppliedMigration represents a migration that has been applied to the database
type AppliedMigration struct {
	Name       string
	Checksum   string
	AppliedAt  time.Time
	ExecutedBy string
}

// InitMigrationHistory creates the _scurry_ schema and migrations table if they don't exist
func (c *Client) InitMigrationHistory(ctx context.Context) error {
	// Create the schema first
	_, err := c.db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS _scurry_`)
	if err != nil {
		return fmt.Errorf("failed to create _scurry_ schema: %w", err)
	}

	// Then create the migrations table
	_, err = c.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS _scurry_.migrations (
			name STRING PRIMARY KEY,
			checksum STRING NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			executed_by STRING NOT NULL DEFAULT current_user()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	return nil
}

// GetAppliedMigrations returns all migrations that have been applied to the database
func (c *Client) GetAppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT name, checksum, applied_at, executed_by
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
		if err := rows.Scan(&m.Name, &m.Checksum, &m.AppliedAt, &m.ExecutedBy); err != nil {
			return nil, fmt.Errorf("failed to scan migration: %w", err)
		}
		migrations = append(migrations, m)
	}

	return migrations, rows.Err()
}

// RecordMigration records a migration as applied in the history table
func (c *Client) RecordMigration(ctx context.Context, name, checksum string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO _scurry_.migrations (name, checksum)
		VALUES ($1, $2)
	`, name, checksum)
	if err != nil {
		return fmt.Errorf("failed to record migration %s: %w", name, err)
	}
	return nil
}

// ExecuteMigration executes a single migration and records it in the history
// This does NOT use a transaction - if it fails, it fails, and we report the error
func (c *Client) ExecuteMigration(ctx context.Context, migration Migration) error {
	// Execute the migration SQL
	err := c.ExecuteBulkDDL(ctx, migration.SQL)
	if err != nil {
		return fmt.Errorf("failed to execute migration %s: %w", migration.Name, err)
	}

	// Record the migration in the history table
	if err := c.RecordMigration(ctx, migration.Name, migration.Checksum); err != nil {
		return fmt.Errorf("migration %s succeeded but failed to record in history: %w", migration.Name, err)
	}

	return nil
}
