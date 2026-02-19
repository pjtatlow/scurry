package data

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/lib/pq"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/schema"
)

type rowData struct {
	values []*string
}

// Dump reads all data from the database and returns a DumpFile.
func Dump(ctx context.Context, client *db.Client, batchSize int) (*DumpFile, error) {
	// Load schema from database
	dbSchema, err := schema.LoadFromDatabase(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to load database schema: %w", err)
	}

	// Compute table insertion order
	insertionOrder, err := schema.ComputeTableInsertionOrder(dbSchema.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to compute table insertion order: %w", err)
	}

	// Generate schema SQL (CREATE statements)
	schemaSQL, err := generateSchemaSQL(dbSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to generate schema SQL: %w", err)
	}

	// Build table lookup
	tableMap := make(map[string]*tree.CreateTable)
	for _, t := range dbSchema.Tables {
		tableMap[t.ResolvedName()] = t.Ast
	}

	// Dump each table in FK-safe order
	var tableDumps []TableDump
	for _, tableName := range insertionOrder.Order {
		tableAST, ok := tableMap[tableName]
		if !ok {
			continue
		}

		selfRefCols := insertionOrder.SelfRefColumns[tableName]
		td, err := dumpTable(ctx, client, tableName, tableAST, selfRefCols, batchSize)
		if err != nil {
			return nil, fmt.Errorf("failed to dump table %s: %w", tableName, err)
		}
		tableDumps = append(tableDumps, td)
	}

	// Dump sequences
	sequences, err := dumpSequences(ctx, client, dbSchema.Sequences)
	if err != nil {
		return nil, fmt.Errorf("failed to dump sequences: %w", err)
	}

	return &DumpFile{
		Version:   1,
		CreatedAt: time.Now(),
		Tables:    insertionOrder.Order,
		SchemaSQL: schemaSQL,
		TableData: tableDumps,
		Sequences: sequences,
	}, nil
}

func generateSchemaSQL(s *schema.Schema) (string, error) {
	diff := schema.Compare(s, schema.NewSchema())
	statements, _, err := diff.GenerateMigrations(true)
	if err != nil {
		return "", err
	}
	return strings.Join(statements, ";\n") + ";", nil
}

func dumpTable(ctx context.Context, client *db.Client, qualifiedName string, tableAST *tree.CreateTable, selfRefCols []string, batchSize int) (TableDump, error) {
	// Get column info
	columns, pkColumns := getTableColumns(tableAST)
	if len(columns) == 0 {
		return TableDump{QualifiedName: qualifiedName}, nil
	}

	// Build self-ref column set for quick lookup
	selfRefSet := make(map[string]bool)
	for _, col := range selfRefCols {
		selfRefSet[col] = true
	}

	// Build column lists
	var allColNames []string
	var selectColNames []string
	for _, col := range columns {
		colName := col.Name.Normalize()
		allColNames = append(allColNames, colName)
		selectColNames = append(selectColNames, pq.QuoteIdentifier(colName))
	}

	// Build ORDER BY from PK columns
	var orderBy []string
	for _, pk := range pkColumns {
		orderBy = append(orderBy, pq.QuoteIdentifier(pk))
	}

	// Split qualified name for quoting
	parts := strings.SplitN(qualifiedName, ".", 2)
	quotedTable := pq.QuoteIdentifier(parts[0]) + "." + pq.QuoteIdentifier(parts[1])

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectColNames, ", "), quotedTable)
	if len(orderBy) > 0 {
		query += " ORDER BY " + strings.Join(orderBy, ", ")
	}

	rows, err := client.GetDB().QueryContext(ctx, query)
	if err != nil {
		return TableDump{}, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Scan all rows
	var allRows []rowData
	numCols := len(allColNames)

	for rows.Next() {
		values := make([]*string, numCols)
		scanArgs := make([]interface{}, numCols)
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return TableDump{}, fmt.Errorf("scan failed: %w", err)
		}
		allRows = append(allRows, rowData{values: values})
	}
	if err := rows.Err(); err != nil {
		return TableDump{}, fmt.Errorf("rows iteration failed: %w", err)
	}

	if len(allRows) == 0 {
		return TableDump{QualifiedName: qualifiedName, RowCount: 0}, nil
	}

	var statements []string

	if len(selfRefCols) > 0 {
		// Two-phase insert for self-referential tables
		stmts := generateSelfRefInserts(quotedTable, allColNames, selfRefSet, allRows, pkColumns, batchSize)
		statements = append(statements, stmts...)
	} else {
		// Normal insert
		stmts := generateInserts(quotedTable, allColNames, allRows, batchSize)
		statements = append(statements, stmts...)
	}

	return TableDump{
		QualifiedName: qualifiedName,
		RowCount:      len(allRows),
		Statements:    statements,
	}, nil
}

// getTableColumns returns the non-computed columns and primary key column names for a table.
func getTableColumns(tableAST *tree.CreateTable) ([]*tree.ColumnTableDef, []string) {
	var columns []*tree.ColumnTableDef
	var pkColumns []string

	for _, def := range tableAST.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.Computed.Computed {
				continue
			}
			columns = append(columns, d)
			// Handle inline PRIMARY KEY syntax
			if d.PrimaryKey.IsPrimaryKey {
				pkColumns = append(pkColumns, d.Name.Normalize())
			}
		case *tree.UniqueConstraintTableDef:
			if d.PrimaryKey {
				for _, col := range d.Columns {
					pkColumns = append(pkColumns, col.Column.Normalize())
				}
			}
		}
	}

	return columns, pkColumns
}

func generateInserts(quotedTable string, colNames []string, rows []rowData, batchSize int) []string {
	quotedCols := make([]string, len(colNames))
	for i, name := range colNames {
		quotedCols[i] = pq.QuoteIdentifier(name)
	}
	colList := strings.Join(quotedCols, ", ")

	var statements []string
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		var sb strings.Builder
		fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES\n", quotedTable, colList)
		for j, row := range batch {
			sb.WriteByte('(')
			for k, val := range row.values {
				if k > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(formatValue(val))
			}
			sb.WriteByte(')')
			if j < len(batch)-1 {
				sb.WriteString(",\n")
			} else {
				sb.WriteByte(';')
			}
		}
		statements = append(statements, sb.String())
	}

	return statements
}

func generateSelfRefInserts(quotedTable string, colNames []string, selfRefSet map[string]bool, rows []rowData, pkColumns []string, batchSize int) []string {
	// Phase 1: INSERT with self-ref columns set to NULL
	quotedCols := make([]string, len(colNames))
	for i, name := range colNames {
		quotedCols[i] = pq.QuoteIdentifier(name)
	}
	colList := strings.Join(quotedCols, ", ")

	var statements []string

	// Build the INSERT batches with self-ref cols nulled out
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		var sb strings.Builder
		fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES\n", quotedTable, colList)
		for j, row := range batch {
			sb.WriteByte('(')
			for k, val := range row.values {
				if k > 0 {
					sb.WriteString(", ")
				}
				if selfRefSet[colNames[k]] {
					sb.WriteString("NULL")
				} else {
					sb.WriteString(formatValue(val))
				}
			}
			sb.WriteByte(')')
			if j < len(batch)-1 {
				sb.WriteString(",\n")
			} else {
				sb.WriteByte(';')
			}
		}
		statements = append(statements, sb.String())
	}

	// Phase 2: UPDATE statements to set the real self-ref values
	// Find PK column indices
	pkIndices := make([]int, 0, len(pkColumns))
	for _, pk := range pkColumns {
		for i, col := range colNames {
			if col == pk {
				pkIndices = append(pkIndices, i)
				break
			}
		}
	}

	// Find self-ref column indices
	selfRefIndices := make([]int, 0)
	for i, col := range colNames {
		if selfRefSet[col] {
			selfRefIndices = append(selfRefIndices, i)
		}
	}

	// Generate UPDATE for each row that has non-NULL self-ref values
	for _, row := range rows {
		hasNonNull := false
		for _, idx := range selfRefIndices {
			if row.values[idx] != nil {
				hasNonNull = true
				break
			}
		}
		if !hasNonNull {
			continue
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "UPDATE %s SET ", quotedTable)

		first := true
		for _, idx := range selfRefIndices {
			if row.values[idx] == nil {
				continue
			}
			if !first {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "%s = %s", pq.QuoteIdentifier(colNames[idx]), formatValue(row.values[idx]))
			first = false
		}

		sb.WriteString(" WHERE ")
		for i, pkIdx := range pkIndices {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			fmt.Fprintf(&sb, "%s = %s", pq.QuoteIdentifier(colNames[pkIdx]), formatValue(row.values[pkIdx]))
		}
		sb.WriteByte(';')

		statements = append(statements, sb.String())
	}

	return statements
}

func formatValue(val *string) string {
	if val == nil {
		return "NULL"
	}
	return pq.QuoteLiteral(*val)
}

func dumpSequences(ctx context.Context, client *db.Client, sequences []schema.ObjectSchema[*tree.CreateSequence]) ([]SequenceValue, error) {
	var results []SequenceValue
	for _, seq := range sequences {
		qualifiedName := seq.ResolvedName()
		parts := strings.SplitN(qualifiedName, ".", 2)
		quotedSeq := pq.QuoteIdentifier(parts[0]) + "." + pq.QuoteIdentifier(parts[1])

		var lastVal sql.NullInt64
		query := fmt.Sprintf("SELECT last_value FROM %s", quotedSeq)
		err := client.GetDB().QueryRowContext(ctx, query).Scan(&lastVal)
		if err != nil {
			return nil, fmt.Errorf("failed to get sequence value for %s: %w", qualifiedName, err)
		}

		if lastVal.Valid {
			results = append(results, SequenceValue{
				QualifiedName: qualifiedName,
				Value:         lastVal.Int64,
			})
		}
	}
	return results, nil
}
