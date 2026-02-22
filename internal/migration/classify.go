package migration

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/schema"
)

// ClassifyResult holds the classification outcome for a set of differences
type ClassifyResult struct {
	Mode    MigrationMode
	Reasons []string
}

// ClassifyDifferences determines whether a migration should be sync or async
// based on the diff types and table sizes. If any operation is async, the whole
// migration is classified as async.
func ClassifyDifferences(diffs []schema.Difference, tableSizes *TableSizes) *ClassifyResult {
	result := &ClassifyResult{Mode: ModeSync}

	for i := range diffs {
		classifyDifference(&diffs[i], tableSizes, result)
	}

	return result
}

func classifyDifference(diff *schema.Difference, ts *TableSizes, result *ClassifyResult) {
	switch diff.Type {
	case schema.DiffTypeTableAdded:
		// CREATE TABLE is always sync
		return

	case schema.DiffTypeTableRemoved:
		// DROP TABLE is always sync
		return

	case schema.DiffTypeTableModified, schema.DiffTypeTableColumnModified, schema.DiffTypeColumnTypeChanged:
		classifyTableModification(diff, ts, result)

	default:
		// Schema/type/sequence/view/routine operations are always sync
		return
	}
}

func classifyTableModification(diff *schema.Difference, ts *TableSizes, result *ClassifyResult) {
	for _, stmt := range diff.MigrationStatements {
		switch s := stmt.(type) {
		case *tree.CreateIndex:
			tableName := qualifiedTableName(s.Table)
			if ts.IsLargeTable(tableName) {
				markAsync(result, fmt.Sprintf("CREATE INDEX on large table %s", tableName))
			}

		case *tree.AlterTable:
			tableName := qualifiedTableName(s.Table.ToTableName())
			for _, cmd := range s.Cmds {
				classifyAlterTableCmd(cmd, tableName, ts, result)
			}
		}
	}
}

func classifyAlterTableCmd(cmd tree.AlterTableCmd, tableName string, ts *TableSizes, result *ClassifyResult) {
	switch c := cmd.(type) {
	case *tree.AlterTableAddColumn:
		if isAddColumnWithNonNullDefault(c.ColumnDef) && ts.IsLargeTable(tableName) {
			markAsync(result, fmt.Sprintf("ADD COLUMN with NOT NULL DEFAULT on large table %s", tableName))
		}

	case *tree.AlterTableSetNotNull:
		if ts.IsLargeTable(tableName) {
			markAsync(result, fmt.Sprintf("SET NOT NULL on large table %s", tableName))
		}

	case *tree.AlterTableAddConstraint:
		if isValidatingConstraint(c) && ts.IsLargeTable(tableName) {
			markAsync(result, fmt.Sprintf("ADD CONSTRAINT on large table %s", tableName))
		}

	case *tree.AlterTableAlterColumnType:
		if ts.IsLargeTable(tableName) {
			markAsync(result, fmt.Sprintf("ALTER COLUMN TYPE on large table %s", tableName))
		}
	}
}

// isAddColumnWithNonNullDefault returns true if the column is NOT NULL with a DEFAULT expression.
func isAddColumnWithNonNullDefault(col *tree.ColumnTableDef) bool {
	return col.Nullable.Nullability == tree.NotNull && col.HasDefaultExpr()
}

// isValidatingConstraint returns true if the constraint is a FK or CHECK that will be validated.
func isValidatingConstraint(c *tree.AlterTableAddConstraint) bool {
	if c.ValidationBehavior == tree.ValidationSkip {
		return false
	}
	switch c.ConstraintDef.(type) {
	case *tree.ForeignKeyConstraintTableDef, *tree.CheckConstraintTableDef:
		return true
	default:
		return false
	}
}

func qualifiedTableName(name tree.TableName) string {
	schemaName := "public"
	if name.ExplicitSchema {
		schemaName = name.SchemaName.Normalize()
	}
	tableName := name.ObjectName.Normalize()
	return schemaName + "." + tableName
}

func markAsync(result *ClassifyResult, reason string) {
	result.Mode = ModeAsync
	result.Reasons = append(result.Reasons, reason)
}
