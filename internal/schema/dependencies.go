package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/set"
)

func getDependencyNames(stmt tree.Statement) set.Set[string] {
	switch stmt := stmt.(type) {
	case *tree.CreateTable:
		return getCreateTableDependencies(stmt)
	case *tree.CreateView:
		return getCreateViewDependencies(stmt)
	case *tree.CreateRoutine:
		return getCreateRoutineDependencies(stmt)
	case *tree.CreateType:
		return getCreateTypeDependencies(stmt)
	case *tree.CreateSequence:
		return getCreateSequenceDependencies(stmt)
	case *tree.AlterType:
		return getAlterTypeDependencies(stmt)
	case *tree.AlterTable:
		return getAlterTableDependencies(stmt)
	case *tree.CreateIndex:
		return getIndexDependencies(stmt.Table, stmt.Columns, stmt.Storing)
	case *tree.CreateTrigger:
		return getCreateTriggerDependencies(stmt)

	// Drop statements have no dependencies, if we made one, then the objects already exist
	// Can't think of a situation where we would create an object, then need to drop it in the same schema change...
	case *tree.DropRoutine:
	case *tree.DropTrigger:
	case *tree.DropTable:
	case *tree.DropSequence:
	case *tree.DropType:
	case *tree.DropView:
	case *tree.DropIndex:
	case *tree.BeginTransaction:
	case *tree.CommitTransaction:

	// Schemas have no dependencies.
	case *tree.CreateSchema:
	case *tree.DropSchema:
	default:
		panic(fmt.Sprintf("unexpected statement type: %s", stmt.StatementTag()))
	}
	return set.New[string]()
}

func getCreateTableDependencies(stmt *tree.CreateTable) set.Set[string] {
	deps := set.New[string]()
	schemaName, tableName := getTableName(stmt.Table)
	deps.Add("schema:" + schemaName)

	for _, def := range stmt.Defs {

		switch d := def.(type) {
		case *tree.ColumnTableDef:
			deps = addColumnDeps(schemaName, tableName, d, deps)
		case *tree.ForeignKeyConstraintTableDef:
			schema, table := getTableName(d.Table)
			if table != tableName {
				deps.Add(fmt.Sprintf("%s.%s", schema, table))
			}
		// None of these TableDefs can have dependencies afaik
		case *tree.FamilyTableDef:
		case *tree.CheckConstraintTableDef:
		case *tree.UniqueConstraintTableDef:
		case *tree.IndexTableDef:

		// Not supported in scurry
		case *tree.LikeTableDef:

		}
	}

	return deps
}

func addColumnDeps(schemaName, tableName string, d *tree.ColumnTableDef, deps set.Set[string]) set.Set[string] {
	if d.Computed.Computed {
		// Computed column expressions reference other columns in the same table,
		// so we need to use getExprColumnDeps to properly prefix column references
		// with the full table path (e.g., "public.table.column").
		deps = deps.Union(getExprColumnDeps(schemaName, tableName, d.Computed.Expr))
	}
	if d.DefaultExpr.Expr != nil {
		deps = deps.Union(getExprDeps(d.DefaultExpr.Expr))
	}
	if name, ok := getResolvableTypeReferenceDepName(d.Type); ok {
		deps.Add(name)
	}
	return deps
}

func getCreateViewDependencies(stmt *tree.CreateView) set.Set[string] {
	deps := set.New[string]()

	schemaName, _ := getTableName(stmt.Name)
	deps.Add("schema:" + schemaName)
	// TODO: find dependencies in the view definition

	return deps
}

func getCreateRoutineDependencies(stmt *tree.CreateRoutine) set.Set[string] {
	deps := set.New[string]()

	schemaName, _ := getRoutineName(stmt.Name)
	deps.Add("schema:" + schemaName)
	// TODO: find dependencies in the routine definition

	return deps
}

func getCreateTypeDependencies(stmt *tree.CreateType) set.Set[string] {
	deps := set.New[string]()

	schemaName, _ := getObjectName(stmt.TypeName)
	deps.Add("schema:" + schemaName)
	// TODO: find dependencies in the type definition

	return deps
}

func getCreateSequenceDependencies(stmt *tree.CreateSequence) set.Set[string] {
	deps := set.New[string]()

	schemaName, _ := getTableName(stmt.Name)
	deps.Add("schema:" + schemaName)
	// TODO: find dependencies in the sequence definition
	_ = stmt

	return deps
}

func getCreateTriggerDependencies(stmt *tree.CreateTrigger) set.Set[string] {
	deps := set.New[string]()

	schemaName, tableName := getObjectName(stmt.TableName)
	deps.Add("schema:" + schemaName)
	deps.Add(schemaName + "." + tableName)
	if schemaName == "public" {
		deps.Add(tableName)
	}

	if stmt.When != nil {
		deps = deps.Union(getExprColumnDeps(schemaName, tableName, stmt.When))
	}

	return deps
}

func getAlterTypeDependencies(stmt *tree.AlterType) set.Set[string] {
	deps := set.New[string]()

	schemaName, typeName := getObjectName(stmt.Type)
	deps.Add(schemaName + "." + typeName)
	if schemaName == "public" {
		deps.Add(typeName)
	}

	return deps
}

func getAlterTableDependencies(stmt *tree.AlterTable) set.Set[string] {
	deps := set.New[string]()

	// Get table name from the statement
	schemaName, tableName := getObjectName(stmt.Table)

	// AlterTable depends on the table existing
	deps.Add(schemaName + "." + tableName)
	if schemaName == "public" {
		deps.Add(tableName)
	}

	// Check each command for additional dependencies
	for _, cmd := range stmt.Cmds {
		switch c := cmd.(type) {
		case *tree.AlterTableAddColumn:
			deps = addColumnDeps(schemaName, tableName, c.ColumnDef, deps)
		case *tree.AlterTableAlterColumnType:
			if name, ok := getResolvableTypeReferenceDepName(c.ToType); ok {
				deps.Add(name)
			}

			if c.Using != nil {
				deps = deps.Union(getExprDeps(c.Using))

			}
		case *tree.AlterTableSetDefault:
			if c.Default != nil {
				deps = deps.Union(getExprDeps(c.Default))
			}
		case *tree.AlterTableAddConstraint:
			schemaName, tableName := getTableName(stmt.Table.ToTableName())

			switch constraint := c.ConstraintDef.(type) {
			case *tree.UniqueConstraintTableDef:
				deps = deps.Union(getIndexDependencies(stmt.Table.ToTableName(), constraint.Columns, constraint.Storing))
			case *tree.CheckConstraintTableDef:
				deps = deps.Union(getExprColumnDeps(schemaName, tableName, constraint.Expr))
			case *tree.ForeignKeyConstraintTableDef:
				otherSchemaName, otherTableName := getTableName(constraint.Table)
				deps.Add(otherSchemaName + "." + otherTableName)
				for _, col := range constraint.ToCols {
					deps.Add(otherSchemaName + "." + otherTableName + "." + col.Normalize())
				}
				for _, col := range constraint.FromCols {
					deps.Add(schemaName + "." + tableName + "." + col.Normalize())
				}

			default:
				panic(fmt.Sprintf("unexpected constraint type: %T", constraint))
			}
		case *tree.AlterTableSetOnUpdate:
			if c.Expr != nil {
				deps = deps.Union(getExprDeps(c.Expr))
			}
		case *tree.AlterTableAlterPrimaryKey:
			deps = deps.Union(getIndexDependencies(stmt.Table.ToTableName(), c.Columns, tree.NameList{}))

		// These have no dependencies
		case *tree.AlterTableDropColumn:
		case *tree.AlterTableDropNotNull:
		case *tree.AlterTableDropStored:
		case *tree.AlterTableSetNotNull:
		case *tree.AlterTableDropConstraint:
		case *tree.AlterTableSetVisible:
		case *tree.AlterTableSetStorageParams:
		case *tree.AlterTableResetStorageParams:

		default:
			panic(fmt.Sprintf("unexpected ALTER TABLE command type: %T", cmd))
		}
	}

	return deps
}

func getIndexDependencies(table tree.TableName, columns tree.IndexElemList, storing tree.NameList) set.Set[string] {
	deps := set.New[string]()

	// Get table name
	schemaName, tableName := getTableName(table)

	// Index depends on the table existing
	deps.Add(schemaName + "." + tableName)
	if schemaName == "public" {
		deps.Add(tableName)
	}

	// Index depends on all columns it references
	for _, col := range columns {
		if col.Expr != nil {
			deps = deps.Union(getExprColumnDeps(schemaName, tableName, col.Expr))
		} else {
			deps.Add(schemaName + "." + tableName + "." + col.Column.Normalize())
		}
	}

	// Index depends on all columns it stores
	for _, col := range storing {
		deps.Add(schemaName + "." + tableName + "." + col.Normalize())
	}

	return deps
}
