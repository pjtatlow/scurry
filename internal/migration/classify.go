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

// ClassifyStatements determines whether a migration should be sync or async based on
// its raw statements and table sizes. It applies the same per-statement rules as
// ClassifyDifferences, for migrations authored directly (e.g. custom SQL supplied to
// `migration local`) rather than generated from a schema diff.
func ClassifyStatements(stmts []tree.Statement, tableSizes *TableSizes) *ClassifyResult {
	result := &ClassifyResult{Mode: ModeSync}

	for _, stmt := range stmts {
		classifyStatement(stmt, tableSizes, result)
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
		classifyStatement(stmt, ts, result)
	}
}

// classifyStatement marks the result async if the single statement is an expensive
// operation against a large table. Statements that don't touch a large table (or aren't
// index/alter/bulk-DML operations, e.g. CREATE TABLE) leave the result unchanged (sync).
func classifyStatement(stmt tree.Statement, ts *TableSizes, result *ClassifyResult) {
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

	case *tree.Update:
		// Data backfills (UPDATE across a large table) should roll out async.
		if name, ok := dmlTargetTable(s.Table); ok && ts.IsLargeTable(name) {
			markAsync(result, fmt.Sprintf("UPDATE on large table %s", name))
		}

	case *tree.Delete:
		// Bulk deletes on a large table should roll out async.
		if name, ok := dmlTargetTable(s.Table); ok && ts.IsLargeTable(name) {
			markAsync(result, fmt.Sprintf("DELETE on large table %s", name))
		}

	case *tree.Insert:
		// A bulk INSERT ... SELECT into a large table is expensive; a small
		// INSERT ... VALUES (seed data) is not.
		if isSelectSourcedInsert(s) {
			if name, ok := dmlTargetTable(s.Table); ok && ts.IsLargeTable(name) {
				markAsync(result, fmt.Sprintf("INSERT ... SELECT into large table %s", name))
			}
		}
	}
}

// dmlTargetTable extracts the target table name from a DML statement's table expression.
// It returns false when the target is not a simple table reference (e.g. a subquery).
func dmlTargetTable(te tree.TableExpr) (string, bool) {
	aliased, ok := te.(*tree.AliasedTableExpr)
	if !ok {
		return "", false
	}
	tn, ok := aliased.Expr.(*tree.TableName)
	if !ok {
		return "", false
	}
	return qualifiedTableName(*tn), true
}

// isSelectSourcedInsert reports whether an INSERT draws its rows from a SELECT (a bulk
// copy) rather than a literal VALUES clause (small seed data).
func isSelectSourcedInsert(ins *tree.Insert) bool {
	if ins.Rows == nil || ins.Rows.Select == nil {
		return false
	}
	_, isValues := ins.Rows.Select.(*tree.ValuesClause)
	return !isValues
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
