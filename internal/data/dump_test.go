package data

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
)

func TestDump(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name         string
		setupSQL     []string
		insertSQL    []string
		batchSize    int
		expectTables []string
		expectRows   map[string]int
	}{
		{
			name: "basic dump",
			setupSQL: []string{
				"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			},
			insertSQL: []string{
				"INSERT INTO public.users VALUES (1, 'Alice'), (2, 'Bob')",
			},
			batchSize:    100,
			expectTables: []string{"public.users"},
			expectRows:   map[string]int{"public.users": 2},
		},
		{
			name: "FK ordering",
			setupSQL: []string{
				"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
				"CREATE TABLE public.posts (id INT8 PRIMARY KEY, user_id INT8 REFERENCES public.users(id), title STRING)",
			},
			insertSQL: []string{
				"INSERT INTO public.users VALUES (1, 'Alice')",
				"INSERT INTO public.posts VALUES (1, 1, 'Hello')",
			},
			batchSize:    100,
			expectTables: []string{"public.users", "public.posts"},
			expectRows:   map[string]int{"public.users": 1, "public.posts": 1},
		},
		{
			name: "empty table",
			setupSQL: []string{
				"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			},
			insertSQL:    nil,
			batchSize:    100,
			expectTables: []string{"public.users"},
			expectRows:   map[string]int{"public.users": 0},
		},
		{
			name: "batch splitting",
			setupSQL: []string{
				"CREATE TABLE public.items (id INT8 PRIMARY KEY)",
			},
			insertSQL: []string{
				"INSERT INTO public.items VALUES (1), (2), (3), (4), (5)",
			},
			batchSize:    2,
			expectTables: []string{"public.items"},
			expectRows:   map[string]int{"public.items": 5},
		},
		{
			name: "NULL values",
			setupSQL: []string{
				"CREATE TABLE public.data (id INT8 PRIMARY KEY, val STRING)",
			},
			insertSQL: []string{
				"INSERT INTO public.data VALUES (1, 'hello'), (2, NULL)",
			},
			batchSize:    100,
			expectTables: []string{"public.data"},
			expectRows:   map[string]int{"public.data": 2},
		},
		{
			name: "self-referential FK",
			setupSQL: []string{
				"CREATE TABLE public.categories (id INT8 PRIMARY KEY, name STRING NOT NULL, parent_id INT8 REFERENCES public.categories(id))",
			},
			insertSQL: []string{
				"INSERT INTO public.categories VALUES (1, 'Root', NULL)",
				"INSERT INTO public.categories VALUES (2, 'Child', 1)",
			},
			batchSize:    100,
			expectTables: []string{"public.categories"},
			expectRows:   map[string]int{"public.categories": 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := db.GetShadowDB(ctx, tt.setupSQL...)
			require.NoError(t, err)
			defer client.Close()

			for _, sql := range tt.insertSQL {
				_, err := client.GetDB().ExecContext(ctx, sql)
				require.NoError(t, err)
			}

			dumpFile, err := Dump(ctx, client, tt.batchSize)
			require.NoError(t, err)

			assert.Equal(t, tt.expectTables, dumpFile.Tables)

			for _, td := range dumpFile.TableData {
				expectedRows, ok := tt.expectRows[td.QualifiedName]
				if ok {
					assert.Equal(t, expectedRows, td.RowCount, "row count mismatch for %s", td.QualifiedName)
				}
			}

			// Verify round-trip
			var buf strings.Builder
			err = dumpFile.Write(&buf)
			require.NoError(t, err)

			parsed, err := ParseDumpFile(strings.NewReader(buf.String()))
			require.NoError(t, err)
			assert.Equal(t, dumpFile.Version, parsed.Version)
			assert.Equal(t, len(dumpFile.TableData), len(parsed.TableData))
		})
	}
}

func TestDumpSelfRefInserts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := db.GetShadowDB(ctx,
		"CREATE TABLE public.categories (id INT8 PRIMARY KEY, name STRING NOT NULL, parent_id INT8 REFERENCES public.categories(id))",
	)
	require.NoError(t, err)
	defer client.Close()

	_, err = client.GetDB().ExecContext(ctx, "INSERT INTO public.categories VALUES (1, 'Root', NULL)")
	require.NoError(t, err)
	_, err = client.GetDB().ExecContext(ctx, "INSERT INTO public.categories VALUES (2, 'Child', 1)")
	require.NoError(t, err)
	_, err = client.GetDB().ExecContext(ctx, "INSERT INTO public.categories VALUES (3, 'Grandchild', 2)")
	require.NoError(t, err)

	dumpFile, err := Dump(ctx, client, 100)
	require.NoError(t, err)

	var catDump *TableDump
	for i := range dumpFile.TableData {
		if dumpFile.TableData[i].QualifiedName == "public.categories" {
			catDump = &dumpFile.TableData[i]
			break
		}
	}
	require.NotNil(t, catDump, "categories table not found in dump")
	assert.Equal(t, 3, catDump.RowCount)

	hasInsert := false
	hasUpdate := false
	for _, stmt := range catDump.Statements {
		if strings.HasPrefix(stmt, "INSERT") {
			hasInsert = true
		}
		if strings.HasPrefix(stmt, "UPDATE") {
			hasUpdate = true
		}
	}
	assert.True(t, hasInsert, "expected INSERT statements")
	assert.True(t, hasUpdate, "expected UPDATE statements for self-ref columns")
}

func TestGenerateInserts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		table     string
		colNames  []string
		rows      []rowData
		batchSize int
		wantCount int
	}{
		{
			name:     "single batch",
			table:    `"public"."users"`,
			colNames: []string{"id", "name"},
			rows: []rowData{
				{values: []*string{strPtr("1"), strPtr("Alice")}},
				{values: []*string{strPtr("2"), strPtr("Bob")}},
			},
			batchSize: 100,
			wantCount: 1,
		},
		{
			name:     "multiple batches",
			table:    `"public"."items"`,
			colNames: []string{"id"},
			rows: []rowData{
				{values: []*string{strPtr("1")}},
				{values: []*string{strPtr("2")}},
				{values: []*string{strPtr("3")}},
			},
			batchSize: 2,
			wantCount: 2,
		},
		{
			name:     "with NULLs",
			table:    `"public"."data"`,
			colNames: []string{"id", "val"},
			rows: []rowData{
				{values: []*string{strPtr("1"), nil}},
			},
			batchSize: 100,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts := generateInserts(tt.table, tt.colNames, tt.rows, tt.batchSize)
			assert.Len(t, stmts, tt.wantCount)

			for _, stmt := range stmts {
				assert.True(t, strings.HasPrefix(stmt, "INSERT INTO"))
				assert.True(t, strings.HasSuffix(stmt, ";"))
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		val  *string
		want string
	}{
		{name: "NULL", val: nil, want: "NULL"},
		{name: "string", val: strPtr("hello"), want: "'hello'"},
		{name: "string with quote", val: strPtr("it's"), want: "'it''s'"},
		{name: "number", val: strPtr("42"), want: "'42'"},
		{name: "empty string", val: strPtr(""), want: "''"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := formatValue(tt.val)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetTableColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sql          string
		expectCols   []string
		expectPKCols []string
	}{
		{
			name:         "basic columns",
			sql:          "CREATE TABLE t (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			expectCols:   []string{"id", "name"},
			expectPKCols: []string{"id"},
		},
		{
			name:         "composite PK",
			sql:          "CREATE TABLE t (a INT8, b INT8, c STRING, PRIMARY KEY (a, b))",
			expectCols:   []string{"a", "b", "c"},
			expectPKCols: []string{"a", "b"},
		},
		{
			name:         "skips computed columns",
			sql:          "CREATE TABLE t (id INT8 PRIMARY KEY, name STRING, full_name STRING AS (name) STORED)",
			expectCols:   []string{"id", "name"},
			expectPKCols: []string{"id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts, err := parser.Parse(tt.sql)
			require.NoError(t, err)
			ct := stmts[0].AST.(*tree.CreateTable)
			ct.HoistConstraints()

			cols, pkCols := getTableColumns(ct)

			var colNames []string
			for _, col := range cols {
				colNames = append(colNames, col.Name.Normalize())
			}

			assert.Equal(t, tt.expectCols, colNames)
			assert.Equal(t, tt.expectPKCols, pkCols)
		})
	}
}

func strPtr(s string) *string {
	return &s
}
