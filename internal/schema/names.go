package schema

import (
	"sort"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

func getTableName(name tree.TableName) (string, string) {
	schemaName := "public"
	if name.ExplicitSchema {
		schemaName = name.SchemaName.Normalize()
	}
	tableName := name.ObjectName.Normalize()
	return schemaName, tableName
}

func getRoutineName(name tree.RoutineName) (string, string) {
	schemaName := "public"
	if name.ExplicitSchema {
		schemaName = name.SchemaName.Normalize()
	}
	routineName := name.ObjectName.Normalize()
	return schemaName, routineName
}

func getObjectName(name *tree.UnresolvedObjectName) (string, string) {
	schemaName := "public"
	if name.HasExplicitSchema() {
		schemaName = strings.ToLower(name.Schema())
	}
	objectName := strings.ToLower(name.Object())

	return schemaName, objectName
}

// indexElemColumnNames returns the normalized column names from an
// IndexElemList. Returns ok=false if any element is an expression rather
// than a plain column reference — expression-based unique indexes cannot
// satisfy a foreign-key reference, so callers should skip them.
func indexElemColumnNames(elems tree.IndexElemList) ([]string, bool) {
	cols := make([]string, 0, len(elems))
	for _, elem := range elems {
		if elem.Expr != nil || elem.Column == "" {
			return nil, false
		}
		cols = append(cols, elem.Column.Normalize())
	}
	return cols, true
}

// nameListStrings returns the normalized strings of a NameList.
func nameListStrings(names tree.NameList) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = n.Normalize()
	}
	return out
}

// uniqueProviderName returns a synthetic dependency-graph name that
// represents "this table has a unique constraint covering these columns."
// Both unique-providing statements (PK, UNIQUE constraint, unique CREATE
// INDEX) and FK consumers reference this name so the migration generator
// can order a new unique constraint before any FK that depends on it.
//
// Columns are sorted so the order in which they appear in the unique
// constraint vs. the FK doesn't matter for matching.
func uniqueProviderName(schemaName, tableName string, cols []string) string {
	sorted := make([]string, len(cols))
	copy(sorted, cols)
	sort.Strings(sorted)
	return "unique:" + schemaName + "." + tableName + "[" + strings.Join(sorted, ",") + "]"
}
