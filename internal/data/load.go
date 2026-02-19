package data

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/lib/pq"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/schema"
)

// LoadOptions configures the load behavior.
type LoadOptions struct {
	DryRun        bool
	TruncateFirst bool
	CreateSchema  bool
}

// LoadResult contains summary information about the load operation.
type LoadResult struct {
	TablesLoaded int
	RowsInserted int
}

// Load imports data from a DumpFile into the target database.
func Load(ctx context.Context, client *db.Client, dump *DumpFile, opts LoadOptions) (*LoadResult, error) {
	// Parse dump schema
	dumpStatements, err := parseDumpSchema(dump.SchemaSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dump schema: %w", err)
	}
	dumpSchema := schema.NewSchema(dumpStatements...)

	if opts.CreateSchema {
		// Apply the dump's schema to the target DB
		if err := applySchema(ctx, client, dumpSchema); err != nil {
			return nil, fmt.Errorf("failed to create schema: %w", err)
		}
	} else {
		// Load target schema and check compatibility
		targetSchema, err := schema.LoadFromDatabase(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("failed to load target schema: %w", err)
		}

		issues := CheckCompatibility(dumpSchema, targetSchema)
		hasErrors := false
		for _, issue := range issues {
			if issue.Severity == "error" {
				hasErrors = true
			}
		}

		if hasErrors {
			return nil, &CompatibilityError{Issues: issues}
		}
	}

	if opts.DryRun {
		// Just return summary
		totalRows := 0
		for _, td := range dump.TableData {
			totalRows += td.RowCount
		}
		return &LoadResult{
			TablesLoaded: len(dump.TableData),
			RowsInserted: totalRows,
		}, nil
	}

	// Compute insertion order for truncation
	insertionOrder, err := schema.ComputeTableInsertionOrder(dumpSchema.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to compute table insertion order: %w", err)
	}

	// Truncate tables in reverse order if requested
	if opts.TruncateFirst {
		if err := truncateTables(ctx, client, insertionOrder.Order); err != nil {
			return nil, fmt.Errorf("failed to truncate tables: %w", err)
		}
	}

	// Build self-ref column info from dump schema
	selfRefNotNullCols := findSelfRefNotNullColumns(dumpSchema, insertionOrder)

	// Execute table data
	totalRows := 0
	tablesLoaded := 0
	for _, td := range dump.TableData {
		if len(td.Statements) == 0 {
			continue
		}

		notNullCols := selfRefNotNullCols[td.QualifiedName]

		// Temporarily drop NOT NULL on self-ref columns if needed
		if len(notNullCols) > 0 {
			if err := alterSelfRefNotNull(ctx, client, td.QualifiedName, notNullCols, false); err != nil {
				return nil, fmt.Errorf("failed to drop NOT NULL on self-ref columns for %s: %w", td.QualifiedName, err)
			}
		}

		// Execute all statements for this table
		if err := executeTableStatements(ctx, client, td.Statements); err != nil {
			return nil, fmt.Errorf("failed to load data for table %s: %w", td.QualifiedName, err)
		}

		// Restore NOT NULL on self-ref columns
		if len(notNullCols) > 0 {
			if err := alterSelfRefNotNull(ctx, client, td.QualifiedName, notNullCols, true); err != nil {
				return nil, fmt.Errorf("failed to restore NOT NULL on self-ref columns for %s: %w", td.QualifiedName, err)
			}
		}

		totalRows += td.RowCount
		tablesLoaded++
	}

	// Execute sequence setvals
	for _, seq := range dump.Sequences {
		stmt := fmt.Sprintf("SELECT setval('%s', %d)", seq.QualifiedName, seq.Value)
		if _, err := client.GetDB().ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("failed to set sequence %s: %w", seq.QualifiedName, err)
		}
	}

	return &LoadResult{
		TablesLoaded: tablesLoaded,
		RowsInserted: totalRows,
	}, nil
}

// CompatibilityError wraps compatibility issues for structured error handling.
type CompatibilityError struct {
	Issues []CompatibilityIssue
}

func (e *CompatibilityError) Error() string {
	var sb strings.Builder
	sb.WriteString("schema compatibility check failed:\n")
	for _, issue := range e.Issues {
		fmt.Fprintf(&sb, "  [%s] %s\n", issue.Severity, issue.Description)
	}
	return sb.String()
}

func parseDumpSchema(schemaSQL string) ([]tree.Statement, error) {
	if schemaSQL == "" {
		return nil, nil
	}
	return schema.ParseSQL(schemaSQL)
}

func applySchema(ctx context.Context, client *db.Client, dumpSchema *schema.Schema) error {
	diff := schema.Compare(dumpSchema, schema.NewSchema())
	statements, _, err := diff.GenerateMigrations(false)
	if err != nil {
		return err
	}
	return client.ExecuteBulkDDL(ctx, statements...)
}

func truncateTables(ctx context.Context, client *db.Client, orderedTables []string) error {
	// Truncate in reverse order (dependents first)
	for i := len(orderedTables) - 1; i >= 0; i-- {
		parts := strings.SplitN(orderedTables[i], ".", 2)
		quotedTable := pq.QuoteIdentifier(parts[0]) + "." + pq.QuoteIdentifier(parts[1])
		stmt := fmt.Sprintf("TRUNCATE %s CASCADE", quotedTable)
		if _, err := client.GetDB().ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to truncate %s: %w", orderedTables[i], err)
		}
	}
	return nil
}

// findSelfRefNotNullColumns identifies self-referencing FK columns that have NOT NULL constraints.
func findSelfRefNotNullColumns(s *schema.Schema, order *schema.TableInsertionOrder) map[string][]string {
	result := make(map[string][]string)

	// Build column nullability map
	colNullability := make(map[string]map[string]bool) // table -> col -> isNotNull
	for _, t := range s.Tables {
		name := t.ResolvedName()
		cols := make(map[string]bool)
		for _, def := range t.Ast.Defs {
			if col, ok := def.(*tree.ColumnTableDef); ok {
				cols[col.Name.Normalize()] = col.Nullable.Nullability == tree.NotNull
			}
		}
		colNullability[name] = cols
	}

	for tableName, selfRefCols := range order.SelfRefColumns {
		var notNullCols []string
		if cols, ok := colNullability[tableName]; ok {
			for _, colName := range selfRefCols {
				if cols[colName] {
					notNullCols = append(notNullCols, colName)
				}
			}
		}
		if len(notNullCols) > 0 {
			result[tableName] = notNullCols
		}
	}

	return result
}

func alterSelfRefNotNull(ctx context.Context, client *db.Client, qualifiedName string, columns []string, setNotNull bool) error {
	parts := strings.SplitN(qualifiedName, ".", 2)
	quotedTable := pq.QuoteIdentifier(parts[0]) + "." + pq.QuoteIdentifier(parts[1])

	for _, col := range columns {
		var stmt string
		if setNotNull {
			stmt = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", quotedTable, pq.QuoteIdentifier(col))
		} else {
			stmt = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", quotedTable, pq.QuoteIdentifier(col))
		}
		if _, err := client.GetDB().ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func executeTableStatements(ctx context.Context, client *db.Client, statements []string) error {
	return crdb.ExecuteTx(ctx, client.GetDB(), &sql.TxOptions{}, func(tx *sql.Tx) error {
		for _, stmt := range statements {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("statement failed: %w\nSQL: %s", err, truncateSQL(stmt, 200))
			}
		}
		return nil
	})
}

func truncateSQL(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
