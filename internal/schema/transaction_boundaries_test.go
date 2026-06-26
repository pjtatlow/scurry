package schema

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// stmtsFromSpec builds a statement list from a compact spec string where each
// rune is a statement: 'C' = COMMIT, 'B' = BEGIN, and any other rune is a
// "real" (non transaction-control) statement identified by that rune.
func stmtsFromSpec(spec string) []tree.Statement {
	stmts := make([]tree.Statement, 0, len(spec))
	for _, r := range spec {
		switch r {
		case 'C':
			stmts = append(stmts, &tree.CommitTransaction{})
		case 'B':
			stmts = append(stmts, &tree.BeginTransaction{})
		default:
			stmts = append(stmts, &tree.CreateIndex{Name: tree.Name(string(r))})
		}
	}
	return stmts
}

// specFromStmts is the inverse of stmtsFromSpec, used to assert on output.
func specFromStmts(stmts []tree.Statement) string {
	var b strings.Builder
	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *tree.CommitTransaction:
			b.WriteByte('C')
		case *tree.BeginTransaction:
			b.WriteByte('B')
		case *tree.CreateIndex:
			b.WriteString(string(s.Name))
		default:
			b.WriteByte('?')
		}
	}
	return b.String()
}

func TestCoalesceTransactionBoundaries(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "no boundaries",
			in:   "ab",
			want: "ab",
		},
		{
			name: "single boundary preserved",
			in:   "aCBb",
			want: "aCBb",
		},
		{
			name: "leading boundary removed",
			in:   "CBab",
			want: "ab",
		},
		{
			name: "trailing boundary removed",
			in:   "abCB",
			want: "ab",
		},
		{
			name: "leading and trailing removed",
			in:   "CBabCB",
			want: "ab",
		},
		{
			name: "consecutive pairs collapse to one",
			in:   "aCBCBb",
			want: "aCBb",
		},
		{
			name: "the reported symptom: three pairs collapse to one",
			in:   "aCBCBCBb",
			want: "aCBb",
		},
		{
			name: "multiple distinct boundaries each kept",
			in:   "aCBbCBc",
			want: "aCBbCBc",
		},
		{
			name: "redundant pairs between several real statements",
			in:   "aCBCBbCBCBCBc",
			want: "aCBbCBc",
		},
		{
			name: "only boundaries collapse to nothing",
			in:   "CBCBCB",
			want: "",
		},
		// Non-transactional sections use a lone COMMIT to enter and a lone BEGIN
		// to exit (e.g. ALTER COLUMN TYPE rewrites). These must be preserved.
		{
			name: "lone commit/begin wrapping a non-txn statement preserved",
			in:   "aCxBb",
			want: "aCxBb",
		},
		{
			name: "redundant pair before non-txn entry dropped",
			in:   "aCBCxBb",
			want: "aCxBb",
		},
		{
			name: "redundant pair after non-txn exit dropped",
			in:   "aCxBCBb",
			want: "aCxBb",
		},
		{
			name: "non-txn section then real statement",
			in:   "CxBb",
			want: "CxBb",
		},
		{
			name: "two adjacent non-txn sections",
			in:   "aCxBCyBb",
			want: "aCxBCyBb",
		},
		{
			name: "trailing non-txn section keeps its leading commit",
			in:   "abCx",
			want: "abCx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := specFromStmts(coalesceTransactionBoundaries(stmtsFromSpec(tt.in)))
			if got != tt.want {
				t.Errorf("coalesceTransactionBoundaries(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
