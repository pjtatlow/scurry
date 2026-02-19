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
// within transactions. COMMIT/BEGIN pairs in the input signal transaction
// boundaries (and are removed from execution since crdb.ExecuteTx handles
// transaction management).
//
// A COMMIT without an immediately following BEGIN signals that the next
// statements should run outside a transaction (needed for operations like
// ALTER COLUMN TYPE requiring on-disk rewrite in CockroachDB). These chunks
// are preceded by a nil marker from chunkStatementsByTransaction.
//
// If a chunk exceeds 50 statements, it is further split into sub-chunks.
func (c *Client) ExecuteBulkDDL(ctx context.Context, statements ...string) error {
	chunks := chunkStatementsByTransaction(statements, 50)

	for i := 0; i < len(chunks); i++ {
		chunk := chunks[i]

		// nil chunk signals the next chunk should run without a transaction
		if chunk == nil {
			i++
			if i >= len(chunks) {
				break
			}
			chunk = chunks[i]
			if len(chunk) == 0 {
				continue
			}
			// Execute without transaction wrapper
			_, err := c.db.ExecContext(ctx, strings.Join(chunk, ";"))
			if err != nil {
				return err
			}
			continue
		}

		if len(chunk) == 0 {
			continue
		}
		if err := crdb.ExecuteTx(ctx, c.db, &sql.TxOptions{}, func(tx *sql.Tx) error {
			// Shadow databases don't have schema_locked tables, so we can safely
			// disable autocommit to keep multiple DDL statements in one transaction.
			// Production databases may have schema_locked tables which require
			// autocommit to remain enabled so CockroachDB can auto-unlock them.
			if c.isShadow {
				_, err := tx.ExecContext(ctx, "SET LOCAL autocommit_before_ddl = false")
				if err != nil {
					return fmt.Errorf("failed to set autocommit_before_ddl: %w", err)
				}
			}

			_, err := tx.ExecContext(ctx, strings.Join(chunk, ";"))
			return err
		}); err != nil {
			return err
		}
	}

	return nil
}

// chunkStatementsByTransaction splits statements into chunks based on COMMIT/BEGIN
// pair boundaries. A COMMIT immediately followed by BEGIN signals a transaction
// boundary - statements before the pair go in one chunk, statements after go in
// another. Both COMMIT and BEGIN are removed since transaction management is
// handled by the caller.
//
// A COMMIT without an immediately following BEGIN (or BEGIN without preceding
// COMMIT) means statements in between should run outside a transaction - these
// are placed in a chunk by themselves with a nil marker that tells ExecuteBulkDDL
// to run them without a transaction wrapper.
//
// If a chunk exceeds maxChunkSize, it is further split into sub-chunks.
func chunkStatementsByTransaction(statements []string, maxChunkSize int) [][]string {
	var chunks [][]string
	var currentChunk []string
	inNonTransactionalSection := false

	for i := 0; i < len(statements); i++ {
		stmt := statements[i]
		normalized := strings.ToUpper(strings.TrimSpace(stmt))

		isCommit := normalized == "COMMIT" || normalized == "COMMIT TRANSACTION"
		isBegin := normalized == "BEGIN" || normalized == "BEGIN TRANSACTION"

		if isCommit {
			// Check if next statement is BEGIN
			nextIsBegin := false
			if i+1 < len(statements) {
				nextNorm := strings.ToUpper(strings.TrimSpace(statements[i+1]))
				nextIsBegin = nextNorm == "BEGIN" || nextNorm == "BEGIN TRANSACTION"
			}

			if nextIsBegin {
				// COMMIT/BEGIN pair - flush current chunk and skip both
				if len(currentChunk) > 0 {
					chunks = append(chunks, splitChunk(currentChunk, maxChunkSize)...)
					currentChunk = nil
				}
				i++ // skip the BEGIN
				inNonTransactionalSection = false
			} else {
				// COMMIT without BEGIN - entering non-transactional section
				if len(currentChunk) > 0 {
					chunks = append(chunks, splitChunk(currentChunk, maxChunkSize)...)
					currentChunk = nil
				}
				inNonTransactionalSection = true
			}
			continue
		}

		if isBegin {
			// BEGIN without preceding COMMIT - exiting non-transactional section
			if len(currentChunk) > 0 {
				if inNonTransactionalSection {
					// Mark this chunk as non-transactional by prepending nil marker
					chunks = append(chunks, nil) // nil signals non-transactional
					chunks = append(chunks, currentChunk)
				} else {
					chunks = append(chunks, splitChunk(currentChunk, maxChunkSize)...)
				}
				currentChunk = nil
			}
			inNonTransactionalSection = false
			continue
		}

		currentChunk = append(currentChunk, stmt)
	}

	// Don't forget the last chunk
	if len(currentChunk) > 0 {
		if inNonTransactionalSection {
			chunks = append(chunks, nil)
			chunks = append(chunks, currentChunk)
		} else {
			chunks = append(chunks, splitChunk(currentChunk, maxChunkSize)...)
		}
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
