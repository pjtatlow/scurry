package db

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// DesiredMigrationsTableSchema is embedded from schema/migrations_table.sql
// This is the source of truth for what the _scurry_.migrations table should look like.
//
//go:embed schema/migrations_table.sql
var DesiredMigrationsTableSchema string

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

	// Collect column names to add in sorted order for deterministic output
	var colsToAdd []string
	for name := range desiredCols {
		if _, exists := currentCols[name]; !exists {
			colsToAdd = append(colsToAdd, name)
		}
	}
	sort.Strings(colsToAdd)

	var alterStatements []string

	// Generate ADD COLUMN statements in sorted order
	for _, name := range colsToAdd {
		desiredCol := desiredCols[name]
		addCol := &tree.AlterTableAddColumn{
			ColumnDef: desiredCol,
		}
		alter := &tree.AlterTable{
			Table: desiredTable.Table.ToUnresolvedObjectName(),
			Cmds:  tree.AlterTableCmds{addCol},
		}
		alterStatements = append(alterStatements, alter.String())
	}

	return alterStatements, nil
}
