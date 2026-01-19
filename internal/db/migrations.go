package db

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// DesiredMigrationsTableSchema is embedded from schema/migrations_table.sql
// This is the source of truth for what the _scurry_.migrations table should look like.
//
//go:embed schema/migrations_table.sql
var DesiredMigrationsTableSchema string

// Migration represents a migration file with its metadata
type Migration struct {
	Name     string
	SQL      string
	Checksum string
}

// Migration status constants
const (
	MigrationStatusPending   = "pending"
	MigrationStatusSucceeded = "succeeded"
	MigrationStatusFailed    = "failed"
	MigrationStatusRecovered = "recovered"
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
}

// InitMigrationHistory creates the _scurry_ schema and migrations table if they don't exist.
// For existing databases with an old schema, it uses schema diffing to migrate to the current schema.
func (c *Client) InitMigrationHistory(ctx context.Context) error {
	// Create the schema first
	_, err := c.db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS _scurry_`)
	if err != nil {
		return fmt.Errorf("failed to create _scurry_ schema: %w", err)
	}

	// Get current CREATE TABLE statement from database (if table exists)
	currentSchema, err := c.getMigrationsTableSchema(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current migrations table schema: %w", err)
	}

	// If table doesn't exist, create it with the desired schema
	if currentSchema == "" {
		_, err = c.db.ExecContext(ctx, DesiredMigrationsTableSchema)
		if err != nil {
			return fmt.Errorf("failed to create migrations table: %w", err)
		}
		return nil
	}

	// Table exists - compare schemas and generate migration statements
	alterStatements, err := generateMigrationsTableAlterStatements(currentSchema, DesiredMigrationsTableSchema)
	if err != nil {
		return fmt.Errorf("failed to generate schema migration: %w", err)
	}

	// Execute any necessary ALTER statements
	for _, stmt := range alterStatements {
		_, err = c.db.ExecContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to migrate migrations table schema: %w", err)
		}
	}

	return nil
}

// MigrationsTableExists checks if the _scurry_.migrations table exists
func (c *Client) MigrationsTableExists(ctx context.Context) (bool, error) {
	schema, err := c.getMigrationsTableSchema(ctx)
	if err != nil {
		return false, err
	}
	return schema != "", nil
}

// getMigrationsTableSchema returns the current CREATE TABLE statement for the migrations table,
// or empty string if the table doesn't exist.
func (c *Client) getMigrationsTableSchema(ctx context.Context) (string, error) {
	var createStatement string
	err := c.db.QueryRowContext(ctx, `
		SELECT create_statement
		FROM crdb_internal.create_statements
		WHERE descriptor_name = 'migrations'
		AND schema_name = '_scurry_'
	`).Scan(&createStatement)

	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return createStatement, nil
}

// generateMigrationsTableAlterStatements compares current and desired schemas and returns
// ALTER statements needed to migrate from current to desired.
func generateMigrationsTableAlterStatements(currentSQL, desiredSQL string) ([]string, error) {
	// Parse both schemas
	currentStmts, err := parser.Parse(currentSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse current schema: %w", err)
	}
	desiredStmts, err := parser.Parse(desiredSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse desired schema: %w", err)
	}

	if len(currentStmts) != 1 || len(desiredStmts) != 1 {
		return nil, fmt.Errorf("expected exactly one statement in each schema")
	}

	currentTable, ok := currentStmts[0].AST.(*tree.CreateTable)
	if !ok {
		return nil, fmt.Errorf("current schema is not a CREATE TABLE statement")
	}
	desiredTable, ok := desiredStmts[0].AST.(*tree.CreateTable)
	if !ok {
		return nil, fmt.Errorf("desired schema is not a CREATE TABLE statement")
	}

	// Build map of current columns
	currentCols := make(map[string]*tree.ColumnTableDef)
	for _, def := range currentTable.Defs {
		if col, ok := def.(*tree.ColumnTableDef); ok {
			currentCols[string(col.Name)] = col
		}
	}

	// Build map of desired columns
	desiredCols := make(map[string]*tree.ColumnTableDef)
	for _, def := range desiredTable.Defs {
		if col, ok := def.(*tree.ColumnTableDef); ok {
			desiredCols[string(col.Name)] = col
		}
	}

	var alterStatements []string

	// Find columns to add (in desired but not in current)
	for name, desiredCol := range desiredCols {
		if _, exists := currentCols[name]; !exists {
			// Generate ADD COLUMN statement
			addCol := &tree.AlterTableAddColumn{
				ColumnDef: desiredCol,
			}
			alter := &tree.AlterTable{
				Table: desiredTable.Table.ToUnresolvedObjectName(),
				Cmds:  tree.AlterTableCmds{addCol},
			}
			alterStatements = append(alterStatements, alter.String())
		}
	}

	return alterStatements, nil
}

// GetAppliedMigrations returns all migrations that have been applied to the database
func (c *Client) GetAppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT name, checksum, status, started_at, completed_at, applied_at, executed_by, failed_statement, error_msg
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
		if err := rows.Scan(&m.Name, &m.Checksum, &m.Status, &m.StartedAt, &m.CompletedAt, &m.AppliedAt, &m.ExecutedBy, &m.FailedStatement, &m.ErrorMsg); err != nil {
			return nil, fmt.Errorf("failed to scan migration: %w", err)
		}
		migrations = append(migrations, m)
	}

	return migrations, rows.Err()
}

// RecordMigration records a migration as applied in the history table
// This is used for marking migrations as succeeded without tracking execution
func (c *Client) RecordMigration(ctx context.Context, name, checksum string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO _scurry_.migrations (name, checksum, status, completed_at)
		VALUES ($1, $2, $3, now())
	`, name, checksum, MigrationStatusSucceeded)
	if err != nil {
		return fmt.Errorf("failed to record migration %s: %w", name, err)
	}
	return nil
}

// MarkAllMigrationsComplete marks all provided migrations as succeeded.
// This is used when the database schema is already in sync (e.g., after a fresh push
// to an empty database or a legacy database without migration tracking).
func (c *Client) MarkAllMigrationsComplete(ctx context.Context, migrations []Migration) error {
	for _, migration := range migrations {
		if err := c.RecordMigration(ctx, migration.Name, migration.Checksum); err != nil {
			return err
		}
	}
	return nil
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
	if err := c.RecordMigration(ctx, migration.Name, migration.Checksum); err != nil {
		return fmt.Errorf("migration %s succeeded but failed to record in history: %w", migration.Name, err)
	}

	return nil
}

// StartMigration records a migration as pending with started_at timestamp
func (c *Client) StartMigration(ctx context.Context, name, checksum string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO _scurry_.migrations (name, checksum, status, started_at)
		VALUES ($1, $2, $3, now())
	`, name, checksum, MigrationStatusPending)
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

// RecoverMigration marks a migration as recovered (manual intervention)
func (c *Client) RecoverMigration(ctx context.Context, name string) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE _scurry_.migrations
		SET status = $2, completed_at = now(), failed_statement = NULL, error_msg = NULL
		WHERE name = $1
	`, name, MigrationStatusRecovered)
	if err != nil {
		return fmt.Errorf("failed to recover migration %s: %w", name, err)
	}
	return nil
}

// ResetMigrationForRetry resets a failed migration to pending state for retry
func (c *Client) ResetMigrationForRetry(ctx context.Context, name, checksum string) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE _scurry_.migrations
		SET status = $2, checksum = $3, started_at = now(), completed_at = NULL, failed_statement = NULL, error_msg = NULL
		WHERE name = $1
	`, name, MigrationStatusPending, checksum)
	if err != nil {
		return fmt.Errorf("failed to reset migration %s for retry: %w", name, err)
	}
	return nil
}

// GetFailedMigration returns the failed migration if one exists
func (c *Client) GetFailedMigration(ctx context.Context) (*AppliedMigration, error) {
	var m AppliedMigration
	err := c.db.QueryRowContext(ctx, `
		SELECT name, checksum, status, started_at, completed_at, applied_at, executed_by, failed_statement, error_msg
		FROM _scurry_.migrations
		WHERE status = $1 OR status = $2
		ORDER BY name ASC
		LIMIT 1
	`, MigrationStatusFailed, MigrationStatusPending).Scan(
		&m.Name, &m.Checksum, &m.Status, &m.StartedAt, &m.CompletedAt, &m.AppliedAt, &m.ExecutedBy, &m.FailedStatement, &m.ErrorMsg,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get failed migration: %w", err)
	}
	return &m, nil
}

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

// ExecuteMigrationWithTracking executes a migration with statement-level tracking
// Returns the index of the failed statement (0-based) and any error
func (c *Client) ExecuteMigrationWithTracking(ctx context.Context, migration Migration) error {
	// Parse SQL into statements
	statements, err := SplitStatements(migration.SQL)
	if err != nil {
		return fmt.Errorf("failed to parse migration %s: %w", migration.Name, err)
	}

	// Record migration as pending
	if err := c.StartMigration(ctx, migration.Name, migration.Checksum); err != nil {
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

// ExecuteRemainingStatements executes statements after the failed one
func (c *Client) ExecuteRemainingStatements(ctx context.Context, migration Migration, failedStatement string) error {
	// Parse SQL into statements
	statements, err := SplitStatements(migration.SQL)
	if err != nil {
		return fmt.Errorf("failed to parse migration: %w", err)
	}

	// Find the failed statement and execute everything after it
	foundFailed := false
	for _, stmt := range statements {
		if !foundFailed {
			if stmt == failedStatement {
				foundFailed = true
			}
			continue
		}
		// Execute remaining statements
		_, err := c.db.ExecContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to execute remaining statement: %w", err)
		}
	}

	return nil
}
