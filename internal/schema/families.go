package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// buildColumnFamilyMap returns a column-name -> family-name mapping for the
// given table. Columns not present in any FAMILY clause map to the empty
// string. CockroachDB's normalized form (returned by SHOW CREATE TABLE) places
// family info exclusively in FamilyTableDef entries, but this helper also
// honors column-level FAMILY qualifiers so unnormalized local SQL behaves the
// same way.
func buildColumnFamilyMap(stmt *tree.CreateTable) map[string]string {
	colToFamily := make(map[string]string)
	for _, def := range stmt.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.HasColumnFamily() {
				colToFamily[d.Name.Normalize()] = d.Family.Name.Normalize()
			}
		case *tree.FamilyTableDef:
			famName := d.Name.Normalize()
			for _, col := range d.Columns {
				colToFamily[col.Normalize()] = famName
			}
		}
	}
	return colToFamily
}

// compareFamilies detects column-family changes on columns that already exist
// in both schemas. CockroachDB does not support moving an existing column
// between families or adding/removing the family of an existing column: family
// membership can only be set when the column is first added (CREATE TABLE or
// ALTER TABLE ADD COLUMN). The resulting Difference carries a BlockingError so
// GenerateMigrations refuses to produce a migration the user would have to
// manually un-break.
//
// New columns are not handled here — compareColumns enriches the ADD COLUMN
// statement with the family qualifier directly.
func compareFamilies(tableName string, local, remote *tree.CreateTable, localCols, remoteCols map[string]*tree.ColumnTableDef) []Difference {
	localFams := buildColumnFamilyMap(local)
	remoteFams := buildColumnFamilyMap(remote)

	diffs := make([]Difference, 0)
	for colName := range localCols {
		if _, exists := remoteCols[colName]; !exists {
			continue
		}
		localFam := localFams[colName]
		remoteFam := remoteFams[colName]
		if localFam == remoteFam {
			continue
		}
		diffs = append(diffs, Difference{
			Type:        DiffTypeTableModified,
			ObjectName:  tableName,
			Description: fmt.Sprintf("Column '%s.%s' family changed from %q to %q", tableName, colName, remoteFam, localFam),
			Dangerous:   true,
			BlockingError: fmt.Sprintf(
				"Column '%s.%s' family changed from %q to %q, but CockroachDB does not support changing a column's family on an existing table. Recreate the column or table to apply this change.",
				tableName, colName, remoteFam, localFam,
			),
		})
	}
	return diffs
}

// applyColumnFamilyForAdd returns a copy of col with its Family field populated
// from the local table's family declarations, so the generated ALTER TABLE ADD
// COLUMN statement includes the correct FAMILY clause. CockroachDB normalizes
// column-level FAMILY qualifiers into table-level FamilyTableDef entries, so
// after the local schema is round-tripped through the shadow database the
// ColumnTableDef.Family field is empty even when the column belongs to a
// family.
//
// If the target family does not exist on the remote table, CREATE IF NOT
// EXISTS FAMILY is emitted so adding multiple columns to a new family in the
// same migration is order-independent.
//
// Returns col unchanged if the column has no family in local.
func applyColumnFamilyForAdd(col *tree.ColumnTableDef, localFamily string, familyExistsInRemote bool) *tree.ColumnTableDef {
	if localFamily == "" {
		return col
	}
	enriched := *col
	enriched.Family.Name = tree.Name(localFamily)
	enriched.Family.Create = !familyExistsInRemote
	enriched.Family.IfNotExists = !familyExistsInRemote
	return &enriched
}

// remoteFamilyNames returns the set of family names declared on the remote
// table, used to decide whether ADD COLUMN should emit CREATE FAMILY for a
// newly-introduced family.
func remoteFamilyNames(remote *tree.CreateTable) map[string]bool {
	names := make(map[string]bool)
	for _, def := range remote.Defs {
		if fam, ok := def.(*tree.FamilyTableDef); ok {
			names[fam.Name.Normalize()] = true
		}
	}
	return names
}
