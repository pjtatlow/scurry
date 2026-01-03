package db

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
)

func (c *Client) GetAllCreateStatements(ctx context.Context) ([]string, error) {
	var statements []string
	row := c.db.QueryRowContext(ctx, "SHOW allow_unsafe_internals;")

	var allowUnsafeInternals string
	err := row.Scan(&allowUnsafeInternals)
	allowUnsafeInternalsSupported := true
	if err != nil {
		if strings.Contains(err.Error(), "unrecognized configuration parameter \"allow_unsafe_internals\"") {
			allowUnsafeInternalsSupported = false
		} else {
			return nil, err
		}
	}

	shouldSetUnsafeInternals := allowUnsafeInternalsSupported && allowUnsafeInternals == "off"

	err = crdb.ExecuteTx(ctx, c.db, &sql.TxOptions{ReadOnly: true}, func(tx *sql.Tx) error {
		var err error
		if shouldSetUnsafeInternals {
			_, err := tx.ExecContext(ctx, "SET LOCAL allow_unsafe_internals = 'on';")
			if err != nil {
				return fmt.Errorf("failed to set allow_unsafe_internals: %w", err)
			}
		}
		statements, err = queryAndScanCreateStatements(tx, `
			WITH create_schema_statements AS (
				SELECT schema_name, create_statement
				FROM crdb_internal.create_schema_statements
				WHERE schema_name != '_scurry_'
			),
			create_statements AS (
				SELECT create_statement
				FROM crdb_internal.create_statements
				WHERE schema_name IN (SELECT schema_name FROM create_schema_statements)
			),
			create_type_statements AS (
				SELECT create_statement
				FROM crdb_internal.create_type_statements
				WHERE schema_name IN (SELECT schema_name FROM create_schema_statements)
			),
			create_function_statements AS (
				SELECT create_statement
				FROM crdb_internal.create_function_statements
				WHERE schema_name IN (SELECT schema_name FROM create_schema_statements)
			),
			create_procedure_statements AS (
				SELECT create_statement
				FROM crdb_internal.create_procedure_statements
				WHERE schema_name IN (SELECT schema_name FROM create_schema_statements)
			),
			create_trigger_statements AS (
				SELECT create_statement
				FROM crdb_internal.create_trigger_statements
				WHERE schema_name IN (SELECT schema_name FROM create_schema_statements)
			)
			SELECT create_statement FROM create_statements

			UNION ALL

			SELECT create_statement FROM create_type_statements

			UNION ALL

			SELECT create_statement FROM create_function_statements

			UNION ALL

			SELECT create_statement FROM create_procedure_statements

			UNION ALL

			SELECT create_statement FROM create_trigger_statements

			UNION ALL

			SELECT create_statement FROM create_schema_statements
		`)
		return err
	})

	return statements, err
}

func queryAndScanCreateStatements(
	tx *sql.Tx,
	query string,
) ([]string, error) {
	rows, err := tx.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query create statements: %w", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var definition string
		if err := rows.Scan(&definition); err != nil {
			return nil, fmt.Errorf("failed to scan create statement: %w", err)
		}
		results = append(results, definition)
	}

	return results, rows.Err()
}

// ExecuteBulkDDL executes multiple DDL statements, respecting COMMIT/BEGIN
// transaction boundaries. Statements are grouped into chunks that are executed
// within transactions. COMMIT and BEGIN statements in the input are used as
// natural chunk boundaries (and are removed from execution since crdb.ExecuteTx
// handles transaction management). If a chunk exceeds 50 statements, it is
// further split into sub-chunks of 50.
func (c *Client) ExecuteBulkDDL(ctx context.Context, statements ...string) error {
	chunks := chunkStatementsByTransaction(statements, 50)

	for _, chunk := range chunks {
		if len(chunk) == 0 {
			continue
		}
		if err := crdb.ExecuteTx(ctx, c.db, &sql.TxOptions{}, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "SET LOCAL autocommit_before_ddl = false")
			if err != nil {
				return fmt.Errorf("failed to set autocommit_before_ddl: %w", err)
			}

			_, err = tx.ExecContext(ctx, strings.Join(chunk, ";"))
			return err
		}); err != nil {
			return err
		}
	}

	return nil
}

// chunkStatementsByTransaction splits statements into chunks based on COMMIT/BEGIN
// boundaries. COMMIT and BEGIN statements are removed since transaction management
// is handled by the caller. If a chunk exceeds maxChunkSize, it is further split.
func chunkStatementsByTransaction(statements []string, maxChunkSize int) [][]string {
	var chunks [][]string
	var currentChunk []string

	for _, stmt := range statements {
		normalized := strings.ToUpper(strings.TrimSpace(stmt))

		// Check if this is a transaction boundary statement
		if normalized == "COMMIT" || normalized == "COMMIT TRANSACTION" ||
			normalized == "BEGIN" || normalized == "BEGIN TRANSACTION" {
			// Flush current chunk if non-empty
			if len(currentChunk) > 0 {
				chunks = append(chunks, splitChunk(currentChunk, maxChunkSize)...)
				currentChunk = nil
			}
			continue
		}

		currentChunk = append(currentChunk, stmt)
	}

	// Don't forget the last chunk
	if len(currentChunk) > 0 {
		chunks = append(chunks, splitChunk(currentChunk, maxChunkSize)...)
	}

	return chunks
}

// splitChunk splits a chunk into sub-chunks of at most maxSize statements
func splitChunk(chunk []string, maxSize int) [][]string {
	var result [][]string
	for subChunk := range slices.Chunk(chunk, maxSize) {
		result = append(result, subChunk)
	}
	return result
}
