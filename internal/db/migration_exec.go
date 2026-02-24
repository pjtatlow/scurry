package db

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
)

// SplitStatements parses SQL into individual statements using the CockroachDB parser
func SplitStatements(sqlContent string) ([]string, error) {
	statements, err := parser.Parse(sqlContent)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	var results []string
	for _, stmt := range statements {
		results = append(results, stmt.AST.String())
	}
	return results, nil
}

// ExecuteMigration executes a single migration and records it in the history
// This does NOT use a transaction - if it fails, it fails, and we report the error
// Deprecated: Use ExecuteMigrationWithTracking instead for better failure tracking
func (c *Client) ExecuteMigration(ctx context.Context, migration Migration) error {
	// Execute the migration SQL
	err := c.ExecuteBulkDDL(ctx, migration.SQL)
	if err != nil {
		return fmt.Errorf("failed to execute migration %s: %w", migration.Name, err)
	}

	// Record the migration in the history table
	if err := c.RecordMigration(ctx, migration.Name, migration.Checksum, migration.Mode == MigrationModeAsync); err != nil {
		return fmt.Errorf("migration %s succeeded but failed to record in history: %w", migration.Name, err)
	}

	return nil
}

// ExecuteMigrationWithTracking executes a migration with statement-level tracking
// Returns the index of the failed statement (0-based) and any error
func (c *Client) ExecuteMigrationWithTracking(ctx context.Context, migration Migration) error {
	// Parse SQL into statements
	statements, err := SplitStatements(migration.SQL)
	if err != nil {
		return fmt.Errorf("failed to parse migration %s: %w", migration.Name, err)
	}

	// Record migration as pending
	if err := c.StartMigration(ctx, migration.Name, migration.Checksum, migration.Mode == MigrationModeAsync); err != nil {
		return err
	}

	// Execute statements one at a time
	for _, stmt := range statements {
		_, err := c.db.ExecContext(ctx, stmt)
		if err != nil {
			// Record failure
			if failErr := c.FailMigration(ctx, migration.Name, stmt, err.Error()); failErr != nil {
				return fmt.Errorf("migration failed and could not record failure: %w (original error: %v)", failErr, err)
			}
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	// Mark as completed
	if err := c.CompleteMigration(ctx, migration.Name); err != nil {
		return fmt.Errorf("migration succeeded but failed to mark as completed: %w", err)
	}

	return nil
}

