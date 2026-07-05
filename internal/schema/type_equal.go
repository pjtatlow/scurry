package schema

import (
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// columnTypesEqual reports whether two column types are semantically identical,
// folding the alias/precision spellings that SHOW CREATE and a hand-written
// schema disagree on but a naive SQLString comparison would flag as drift.
// Most aliases (BIGINT/INT8, NUMERIC/DECIMAL, ...) already canonicalize in the
// parser; only string OIDs and default time precision need help.
func columnTypesEqual(a, b tree.ResolvableTypeReference) bool {
	if a.SQLString() == b.SQLString() {
		return true
	}
	at, aok := a.(*types.T)
	bt, bok := b.(*types.T)
	if !aok || !bok {
		// Unresolved (e.g. an enum type name); leave it to the caller.
		return false
	}
	if at.Family() != bt.Family() {
		return false
	}
	switch at.Family() {
	case types.StringFamily:
		// STRING/VARCHAR/TEXT are one type; width still matters.
		return canonicalStringOid(at.Oid()) == canonicalStringOid(bt.Oid()) && at.Width() == bt.Width()
	case types.TimestampFamily, types.TimestampTZFamily, types.TimeFamily, types.TimeTZFamily:
		// Same family already checked, so this only folds default precision (6).
		return at.Precision() == bt.Precision()
	default:
		return false
	}
}

// canonicalStringOid folds STRING/TEXT and VARCHAR together; CHAR (bpchar),
// "char" and NAME keep their identity (CHAR is fixed-length, not a STRING alias).
func canonicalStringOid(o oid.Oid) oid.Oid {
	if o == oid.T_varchar {
		return oid.T_text
	}
	return o
}
