package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/set"
)

func getProvidedNames(stmt tree.Statement) set.Set[string] {
	names := set.New[string]()
	switch s := stmt.(type) {
	case *tree.CreateSchema:
		names.Add("schema:" + s.Schema.Schema())

	case *tree.CreateTable:
		{
			// Tables provide their own name, as well as their columns, indexes, constraints, and foreign keys
			schemaName, tableName := getTableName(s.Table)
			names.Add(schemaName + "." + tableName)

			if schemaName == "public" {
				names.Add(tableName)
			}

			for _, def := range s.Defs {

				switch d := def.(type) {
				case *tree.ColumnTableDef:
					names.Add(schemaName + "." + tableName + "." + d.Name.Normalize())

				// None of these can be depended on by other objects
				case *tree.ForeignKeyConstraintTableDef:
				case *tree.FamilyTableDef:
				case *tree.UniqueConstraintTableDef:
				case *tree.CheckConstraintTableDef:
				case *tree.IndexTableDef:
				// Not supported in scurry
				case *tree.LikeTableDef:

				}
			}
		}
	case *tree.CreateType:
		{
			schemaName, typeName := getObjectName(s.TypeName)
			names.Add(schemaName + "." + typeName)
			if schemaName == "public" {
				names.Add(typeName)
			}
		}
	case *tree.CreateSequence:
		{
			schemaName, typeName := getTableName(s.Name)
			names.Add(schemaName + "." + typeName)
			if schemaName == "public" {
				names.Add(typeName)
			}
		}
	case *tree.CreateView:
		{
			schemaName, typeName := getTableName(s.Name)
			names.Add(schemaName + "." + typeName)
			if schemaName == "public" {
				names.Add(typeName)
			}
		}
	case *tree.CreateRoutine:
		{
			schemaName, typeName := getRoutineName(s.Name)
			names.Add(schemaName + "." + typeName)
			if schemaName == "public" {
				names.Add(typeName)
			}
		}

	case *tree.AlterTable:
		{
			// AlterTable can provide new columns when adding them
			schemaName, tableName := getObjectName(s.Table)
			for _, cmd := range s.Cmds {
				switch c := cmd.(type) {
				case *tree.AlterTableAddColumn:
					// Adding a column provides the column name
					colName := c.ColumnDef.Name.Normalize()
					names.Add(schemaName + "." + tableName + "." + colName)

				case *tree.AlterTableAlterColumnType:
					// Altering a column's type provides the column name as well
					colName := c.Column.Normalize()
					names.Add(schemaName + "." + tableName + "." + colName)

				// None of these provide anything that can be depended on elsewhere
				case *tree.AlterTableSetDefault:
				case *tree.AlterTableDropColumn:
				case *tree.AlterTableDropNotNull:
				case *tree.AlterTableDropStored:
				case *tree.AlterTableSetNotNull:
				case *tree.AlterTableAddConstraint:
				case *tree.AlterTableDropConstraint:
				case *tree.AlterTableSetVisible:
				case *tree.AlterTableSetOnUpdate:
				case *tree.AlterTableAlterPrimaryKey:
				case *tree.AlterTableSetStorageParams:
				case *tree.AlterTableResetStorageParams:

				default:
					panic(fmt.Sprintf("unexpected ALTER TABLE command type: %T", cmd))
				}
			}
		}
	case *tree.CreateIndex:
		{
			// Index provides its own name
			indexName := s.Name.Normalize()
			if indexName != "" {
				names.Add(indexName)
			}
		}

	case *tree.AlterType:
		{
			schemaName, typeName := getObjectName(s.Type)
			if addVal, ok := s.Cmd.(*tree.AlterTypeAddValue); ok {
				enumVal := string(addVal.NewVal)
				names.Add(schemaName + "." + typeName + "." + enumVal)
				if schemaName == "public" {
					names.Add(typeName + "." + enumVal)
				}
			}
		}

	// These are possible statements we could encounter, but don't provide anything.
	case *tree.DropRoutine:
	case *tree.DropTable:
	case *tree.DropSequence:
	case *tree.DropType:
	case *tree.DropView:
	case *tree.DropIndex:
	case *tree.BeginTransaction:
	case *tree.CommitTransaction:
	case *tree.DropSchema:
	default:
		panic(fmt.Sprintf("unexpected statement type: %s", stmt.StatementTag()))
	}
	return names

}
