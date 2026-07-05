package schema

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// parseColumn parses a single-column CREATE TABLE and returns that column,
// without touching a database. The column's Type carries the resolved *types.T
// exactly as it would after round-tripping through SHOW CREATE.
func parseColumn(t *testing.T, colType string) *tree.ColumnTableDef {
	t.Helper()
	stmts, err := parser.Parse("CREATE TABLE t (c " + colType + ")")
	if err != nil {
		t.Fatalf("parse %q: %v", colType, err)
	}
	ct := stmts[0].AST.(*tree.CreateTable)
	for _, d := range ct.Defs {
		if col, ok := d.(*tree.ColumnTableDef); ok {
			return col
		}
	}
	t.Fatalf("no column parsed from %q", colType)
	return nil
}

func TestColumnTypesEqual(t *testing.T) {
	tests := []struct {
		name  string
		a, b  string
		equal bool
	}{
		// String aliases fold together.
		{"varchar vs string same width", "VARCHAR(64)", "STRING(64)", true},
		{"string vs text unbounded", "STRING", "TEXT", true},
		{"varchar vs text unbounded", "VARCHAR", "TEXT", true},
		{"varchar vs char varying", "VARCHAR(60)", "CHARACTER VARYING(60)", true},

		// String width still matters.
		{"varchar width differs", "VARCHAR(64)", "VARCHAR(32)", false},
		{"varchar bounded vs unbounded string", "VARCHAR(255)", "TEXT", false},

		// CHAR is a distinct fixed-length type, not an alias of STRING.
		{"char vs string same width", "CHAR(3)", "STRING(3)", false},
		{"char vs varchar same width", "CHAR(3)", "VARCHAR(3)", false},
		{"char vs character same", "CHAR(3)", "CHARACTER(3)", true},

		// Default time precision folds; explicit non-default does not.
		{"timestamptz default vs 6", "TIMESTAMPTZ", "TIMESTAMPTZ(6)", true},
		{"timestamp default vs 6", "TIMESTAMP", "TIMESTAMP(6)", true},
		{"time default vs 6", "TIME", "TIME(6)", true},
		{"timestamptz 3 differs from default", "TIMESTAMPTZ", "TIMESTAMPTZ(3)", false},
		{"timestamp vs timestamptz differ", "TIMESTAMP", "TIMESTAMPTZ", false},

		// Numeric and other aliases already canonicalize in the parser, so the
		// SQLString fast path folds them.
		{"bigint vs int8", "BIGINT", "INT8", true},
		{"int vs int8", "INT", "INT8", true},
		{"smallint vs int2", "SMALLINT", "INT2", true},
		{"numeric vs decimal", "NUMERIC", "DECIMAL", true},
		{"dec vs decimal", "DEC", "DECIMAL", true},
		{"double precision vs float8", "DOUBLE PRECISION", "FLOAT8", true},
		{"bool vs boolean", "BOOL", "BOOLEAN", true},

		// Genuinely different numeric widths stay different.
		{"int4 vs int8", "INT4", "INT8", false},
		{"real vs float8", "REAL", "FLOAT8", false},
		{"int vs string", "INT8", "STRING", false},
		{"decimal precision differs", "DECIMAL(10,2)", "DECIMAL(8,2)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := parseColumn(t, tt.a).Type
			b := parseColumn(t, tt.b).Type
			if got := columnTypesEqual(a, b); got != tt.equal {
				t.Errorf("columnTypesEqual(%s, %s) = %v, want %v", tt.a, tt.b, got, tt.equal)
			}
			// Equality must be symmetric.
			if got := columnTypesEqual(b, a); got != tt.equal {
				t.Errorf("columnTypesEqual(%s, %s) = %v, want %v (asymmetric)", tt.b, tt.a, got, tt.equal)
			}
		})
	}
}

// TestCompareColumnIgnoresTypeAliases drives the actual column comparison path
// (no database) to confirm that alias-only spelling differences produce no
// migration, while a real type change still does.
func TestCompareColumnIgnoresTypeAliases(t *testing.T) {
	enumCtx := newEnumChangeContext(&Schema{}, &Schema{})
	tableRef := tree.MakeUnqualifiedTableName("t")

	tests := []struct {
		name          string
		local, remote string
		wantDiff      bool
	}{
		{"varchar vs string", "VARCHAR(64)", "STRING(64)", false},
		{"timestamptz precision default", "TIMESTAMPTZ", "TIMESTAMPTZ(6)", false},
		{"char vs string is real", "CHAR(3)", "STRING(3)", true},
		{"varchar narrowing is real", "VARCHAR(32)", "VARCHAR(64)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := parseColumn(t, tt.local)
			remote := parseColumn(t, tt.remote)
			diffs := compareColumn("t", "c", tableRef, local, remote, enumCtx)
			hasTypeDiff := false
			for _, d := range diffs {
				if d.Type == DiffTypeColumnTypeChanged {
					hasTypeDiff = true
				}
			}
			if hasTypeDiff != tt.wantDiff {
				t.Errorf("compareColumn(%s, %s) type-diff = %v, want %v", tt.local, tt.remote, hasTypeDiff, tt.wantDiff)
			}
		})
	}
}
