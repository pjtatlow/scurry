package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// compareTables finds differences in tables
func compareTables(local, remote *Schema) []Difference {
	diffs := make([]Difference, 0)

	// Build maps for quick lookup
	localTables := make(map[string]ObjectSchema[*tree.CreateTable])
	remoteTables := make(map[string]ObjectSchema[*tree.CreateTable])

	for _, t := range local.Tables {
		localTables[t.ResolvedName()] = t
	}
	for _, t := range remote.Tables {
		remoteTables[t.ResolvedName()] = t
	}

	// Find added and modified tables
	for name, localTable := range localTables {
		remoteTable, existsInRemote := remoteTables[name]
		if !existsInRemote {
			// Table added - create it
			diffs = append(diffs, Difference{
				Type:                DiffTypeTableAdded,
				ObjectName:          name,
				Description:         fmt.Sprintf("Table '%s' added", name),
				MigrationStatements: []tree.Statement{localTable.Ast},
			})
		} else {
			// Table exists in both - check for modifications
			tableDiffs := compareTableModifications(name, localTable.Ast, remoteTable.Ast)
			diffs = append(diffs, tableDiffs...)
		}
	}

	// Find removed tables
	for name, remoteTable := range remoteTables {
		if _, existsInLocal := localTables[name]; !existsInLocal {
			// Table removed - drop it
			drop := &tree.DropTable{
				Names:        tree.TableNames{remoteTable.Ast.Table},
				IfExists:     true,
				DropBehavior: tree.DropRestrict,
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeTableRemoved,
				ObjectName:          name,
				Description:         fmt.Sprintf("Table '%s' removed", name),
				Dangerous:           true,
				MigrationStatements: []tree.Statement{drop},
			})
		}
	}

	return diffs
}

type tableComponents struct {
	columns     map[string]*tree.ColumnTableDef
	constraints map[string]tree.ConstraintTableDef
	indexes     map[string]*tree.IndexTableDef
	families    map[string]*tree.FamilyTableDef
}

// extractTableComponents breaks down a CreateTable AST into organized maps
// for easier comparison. Components are keyed by their names.
func extractTableComponents(stmt *tree.CreateTable) *tableComponents {
	tc := &tableComponents{
		columns:     make(map[string]*tree.ColumnTableDef),
		constraints: make(map[string]tree.ConstraintTableDef),
		indexes:     make(map[string]*tree.IndexTableDef),
		families:    make(map[string]*tree.FamilyTableDef),
	}

	for _, def := range stmt.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			colName := d.Name.Normalize()
			tc.columns[colName] = d

		// The three types of constraints cannot be altered, only dropped and created.
		// So we can use their string representations as keys since we don't need to match them up.
		case *tree.ForeignKeyConstraintTableDef:
			key := formatNode(d)
			tc.constraints[key] = d
		case *tree.CheckConstraintTableDef:
			key := formatNode(d)
			tc.constraints[key] = d
		case *tree.UniqueConstraintTableDef:
			key := formatNode(d)
			tc.constraints[key] = d

		case *tree.IndexTableDef:
			indexName := d.Name.Normalize()
			if indexName != "" {
				tc.indexes[indexName] = d
			}

		case *tree.FamilyTableDef:
			familyName := d.Name.Normalize()
			if familyName != "" {
				tc.families[familyName] = d
			}
		}
	}

	return tc
}

// compareTableModifications compares two versions of the same table and returns differences
func compareTableModifications(tableName string, local, remote *tree.CreateTable) []Difference {
	diffs := make([]Difference, 0)

	localComponents := extractTableComponents(local)
	remoteComponents := extractTableComponents(remote)

	// Compare columns
	columnDiffs := compareColumns(tableName, local.Table, localComponents.columns, remoteComponents.columns)
	diffs = append(diffs, columnDiffs...)

	// Compare indexes
	indexDiffs := compareIndexes(tableName, local.Table, localComponents.indexes, remoteComponents.indexes)
	diffs = append(diffs, indexDiffs...)

	// Compare constraints (foreign keys, check constraints, unique constraints)
	constraintDiffs := compareCheckConstraints(tableName, local.Table, localComponents.constraints, remoteComponents.constraints)
	diffs = append(diffs, constraintDiffs...)

	// TODO: Compare column families

	return diffs
}

// compareColumns finds differences in table columns
func compareColumns(tableName string, tableRef tree.TableName, localCols, remoteCols map[string]*tree.ColumnTableDef) []Difference {
	diffs := make([]Difference, 0)

	// Find added columns
	for colName, localCol := range localCols {
		if _, existsInRemote := remoteCols[colName]; !existsInRemote {

			var warningMessage string
			requiredWithoutDefault := localCol.Nullable.Nullability == tree.NotNull && (!localCol.HasDefaultExpr() && !localCol.IsComputed())
			if requiredWithoutDefault {
				warningMessage = fmt.Sprintf("Column '%s.%s' is non-nullable but has no default value. Will fail to add column if the table is not empty.", tableName, colName)
			}
			createColumn := &tree.AlterTable{
				Table: tableRef.ToUnresolvedObjectName(),
				Cmds: tree.AlterTableCmds{
					&tree.AlterTableAddColumn{
						ColumnDef: localCol,
					},
				},
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeTableModified,
				ObjectName:          tableName,
				Description:         fmt.Sprintf("Column '%s.%s' added", tableName, colName),
				Dangerous:           warningMessage != "",
				WarningMessage:      warningMessage,
				MigrationStatements: []tree.Statement{createColumn},
			})
		} else {
			diffs = append(diffs, compareColumn(tableName, colName, tableRef, localCol, remoteCols[colName])...)
		}
	}

	// Find removed columns
	for colName := range remoteCols {
		if _, existsInLocal := localCols[colName]; !existsInLocal {
			removeColumn := &tree.AlterTable{
				Table: tableRef.ToUnresolvedObjectName(),
				Cmds: tree.AlterTableCmds{
					&tree.AlterTableDropColumn{
						Column:       tree.Name(colName),
						DropBehavior: tree.DropRestrict,
					},
				},
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeTableModified,
				ObjectName:          tableName,
				Description:         fmt.Sprintf("Column '%s.%s' removed", tableName, colName),
				MigrationStatements: []tree.Statement{removeColumn},
			})
		}
	}

	return diffs
}

func compareColumn(tableName, colName string, tableRef tree.TableName, localCol, remoteCol *tree.ColumnTableDef) []Difference {

	dropAndCreate := func(description string) []Difference {
		return []Difference{
			{
				Type:           DiffTypeTableColumnModified,
				ObjectName:     tableName,
				Description:    description,
				Dangerous:      true,
				WarningMessage: fmt.Sprintf("Column '%s.%s' will be dropped and re-created, can result in data loss.", tableName, colName),
				IsDropCreate:   true,
				MigrationStatements: []tree.Statement{
					&tree.AlterTable{
						Table: tableRef.ToUnresolvedObjectName(),
						Cmds: tree.AlterTableCmds{
							&tree.AlterTableDropColumn{
								Column:       tree.Name(colName),
								DropBehavior: tree.DropRestrict,
							},
						},
					},
					&tree.AlterTable{
						Table: tableRef.ToUnresolvedObjectName(),
						Cmds: tree.AlterTableCmds{
							&tree.AlterTableAddColumn{
								ColumnDef: localCol,
							},
						},
					},
				},
			},
		}
	}

	diffs := make([]Difference, 0)
	cmds := make(tree.AlterTableCmds, 0)
	dangerous := false

	// Check types
	if localCol.Type.SQLString() != remoteCol.Type.SQLString() {
		// TODO: collation? using? Maybe we ask the user for the "using" expression...
		cmds = append(cmds, &tree.AlterTableAlterColumnType{
			Column: localCol.Name,
			ToType: localCol.Type,
		})
		dangerous = true
	}

	// Check nullability
	localNotNull := localCol.Nullable.Nullability == tree.NotNull
	remoteNotNull := remoteCol.Nullable.Nullability == tree.NotNull
	if localNotNull != remoteNotNull {
		if localNotNull {
			cmds = append(cmds, &tree.AlterTableSetNotNull{
				Column: localCol.Name,
			})
			dangerous = true
		} else {
			cmds = append(cmds, &tree.AlterTableDropNotNull{
				Column: localCol.Name,
			})
			dangerous = true
		}
	}

	// Check DEFAULT expression
	if localCol.HasDefaultExpr() && (!remoteCol.HasDefaultExpr() || localCol.DefaultExpr.Expr.String() != remoteCol.DefaultExpr.Expr.String()) {
		// Set default
		cmds = append(cmds, &tree.AlterTableSetDefault{
			Column:  localCol.Name,
			Default: localCol.DefaultExpr.Expr,
		})
	} else if !localCol.HasDefaultExpr() && remoteCol.HasDefaultExpr() {
		// Drop default
		cmds = append(cmds, &tree.AlterTableSetDefault{
			Column:  localCol.Name,
			Default: nil, // Does a drop default if the expression is nil
		})
		dangerous = true
	}

	// Check ON UPDATE expression
	if localCol.HasOnUpdateExpr() && (!remoteCol.HasOnUpdateExpr() || localCol.OnUpdateExpr.Expr.String() != remoteCol.OnUpdateExpr.Expr.String() || localCol.OnUpdateExpr.ConstraintName.Normalize() != remoteCol.OnUpdateExpr.ConstraintName.Normalize()) {
		// Set ON UPDATE
		cmds = append(cmds, &tree.AlterTableSetOnUpdate{
			Column: localCol.Name,
			Expr:   localCol.OnUpdateExpr.Expr,
		})
	} else if !localCol.HasOnUpdateExpr() && remoteCol.HasOnUpdateExpr() {
		// Drop ON UPDATE
		cmds = append(cmds, &tree.AlterTableSetOnUpdate{
			Column: localCol.Name,
			Expr:   nil, // Does a drop on update if the expression is nil
		})
	}

	// Computed field changes
	if localCol.IsComputed() {
		if remoteCol.IsComputed() {
			// Both are computed, but if anything changed we need to drop / add the whole column.
			if localCol.Computed.Virtual != remoteCol.Computed.Virtual || localCol.Computed.Expr.String() != remoteCol.Computed.Expr.String() {
				return dropAndCreate(fmt.Sprintf("Column '%s.%s' computed expression modified, needs to be dropped and recreated", tableName, colName))
			}
		} else {
			// Needs to be computed, drop and create
			return dropAndCreate(fmt.Sprintf("Column '%s.%s' is now computed, needs to be dropped and recreated", tableName, colName))
		}
	} else if remoteCol.IsComputed() {
		// No longer computed, drop STORED
		cmds = append(cmds, &tree.AlterTableDropStored{
			Column: localCol.Name,
		})
	}

	// Hidden flag
	if localCol.Hidden != remoteCol.Hidden {
		cmds = append(cmds, &tree.AlterTableSetVisible{
			Column:  localCol.Name,
			Visible: !localCol.Hidden,
		})
	}

	// TODO: Column families?
	// TODO: Unique constraints?
	// TODO: Other check constraints?

	if len(cmds) > 0 {
		alterTable := &tree.AlterTable{
			Table: tableRef.ToUnresolvedObjectName(),
			Cmds:  cmds,
		}
		diffs = append(diffs, Difference{
			Type:                DiffTypeTableModified,
			ObjectName:          tableName,
			Description:         fmt.Sprintf("Column '%s.%s' modified", tableName, colName),
			Dangerous:           dangerous,
			MigrationStatements: []tree.Statement{alterTable},
		})
	}
	return diffs
}

// compareIndexes finds differences in table indexes
func compareIndexes(tableName string, tableRef tree.TableName, localIndexes, remoteIndexes map[string]*tree.IndexTableDef) []Difference {
	diffs := make([]Difference, 0)

	// Find added indexes
	for indexName, localIndex := range localIndexes {
		createIndex := &tree.CreateIndex{
			Name:             tree.Name(indexName),
			Table:            tableRef,
			Columns:          localIndex.Columns,
			Storing:          localIndex.Storing,
			Type:             localIndex.Type,
			Sharded:          localIndex.Sharded,
			PartitionByIndex: localIndex.PartitionByIndex,
			StorageParams:    localIndex.StorageParams,
			Predicate:        localIndex.Predicate,
			Invisibility:     localIndex.Invisibility,
		}
		if remoteIndex, existsInRemote := remoteIndexes[indexName]; !existsInRemote {
			// Index added - generate CREATE INDEX
			diffs = append(diffs, Difference{
				Type:                DiffTypeTableModified,
				ObjectName:          tableName,
				Description:         fmt.Sprintf("Index '%s.%s' added", tableName, indexName),
				MigrationStatements: []tree.Statement{createIndex},
			})
		} else {
			// Compare index definitions, if they differ at all, drop / create them.
			localIndexStr := formatNode(localIndex)
			remoteIndexStr := formatNode(remoteIndex)

			if localIndexStr != remoteIndexStr {
				dropIndex := &tree.DropIndex{
					IndexList:    tree.TableIndexNames{{Table: tableRef, Index: tree.UnrestrictedName(indexName)}},
					DropBehavior: tree.DropRestrict,
				}
				diffs = append(diffs, Difference{
					Type:                DiffTypeTableModified,
					ObjectName:          tableName,
					Description:         fmt.Sprintf("Index '%s.%s' modified", tableName, indexName),
					Dangerous:           true,
					IsDropCreate:        true,
					MigrationStatements: []tree.Statement{dropIndex, &tree.CommitTransaction{}, &tree.BeginTransaction{}, createIndex},
				})
			}
		}
	}

	// Find removed indexes
	for indexName := range remoteIndexes {
		if _, existsInLocal := localIndexes[indexName]; !existsInLocal {
			// Index removed - generate DROP INDEX
			dropIndex := &tree.DropIndex{
				IndexList:    tree.TableIndexNames{{Table: tableRef, Index: tree.UnrestrictedName(indexName)}},
				DropBehavior: tree.DropRestrict,
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeTableModified,
				ObjectName:          tableName,
				Description:         fmt.Sprintf("Index '%s.%s' removed", tableName, indexName),
				Dangerous:           true,
				MigrationStatements: []tree.Statement{dropIndex},
			})
		}
	}

	return diffs
}

func compareCheckConstraints(tableName string, tableRef tree.TableName, localConstraints, remoteConstraints map[string]tree.ConstraintTableDef) []Difference {
	diffs := make([]Difference, 0)

	// Find removed constraints, we may be adding the same ones back in later so we need to drop them first
	for constraintKey, remoteConstraint := range remoteConstraints {
		if _, existsInLocal := localConstraints[constraintKey]; !existsInLocal {
			if uniqueConstraint, ok := remoteConstraint.(*tree.UniqueConstraintTableDef); ok && !uniqueConstraint.PrimaryKey {
				dropIndex := &tree.DropIndex{
					IndexList:    tree.TableIndexNames{{Table: tableRef, Index: tree.UnrestrictedName(uniqueConstraint.Name)}},
					DropBehavior: tree.DropCascade,
				}

				diffs = append(diffs, Difference{
					Type:                DiffTypeTableModified,
					ObjectName:          tableName,
					Description:         fmt.Sprintf("Unique index '%s.%s' removed", tableName, uniqueConstraint.Name),
					Dangerous:           true,
					MigrationStatements: []tree.Statement{dropIndex},
				})
			} else {

				name := getConstraintName(remoteConstraint)
				if name != "" {
					dropConstraint := &tree.AlterTable{
						Table: tableRef.ToUnresolvedObjectName(),
						Cmds: tree.AlterTableCmds{
							&tree.AlterTableDropConstraint{
								IfExists:     true,
								DropBehavior: tree.DropRestrict,
								Constraint:   tree.Name(name),
							},
						},
					}
					diffs = append(diffs, Difference{
						Type:                DiffTypeTableModified,
						ObjectName:          tableName,
						Description:         fmt.Sprintf("Constraint '%s.%s' removed", tableName, name),
						Dangerous:           true,
						MigrationStatements: []tree.Statement{dropConstraint},
					})
				}
			}
		}
	}

	// Find added constraints
	for constraintKey, localConstraint := range localConstraints {
		if _, existsInRemote := remoteConstraints[constraintKey]; !existsInRemote {
			if uniqueConstraint, ok := localConstraint.(*tree.UniqueConstraintTableDef); ok && !uniqueConstraint.PrimaryKey {
				createIndex := &tree.CreateIndex{
					Name:             uniqueConstraint.Name,
					Table:            tableRef,
					Unique:           true,
					Columns:          uniqueConstraint.Columns,
					Storing:          uniqueConstraint.Storing,
					Type:             uniqueConstraint.Type,
					Sharded:          uniqueConstraint.Sharded,
					PartitionByIndex: uniqueConstraint.PartitionByIndex,
					StorageParams:    uniqueConstraint.StorageParams,
					Predicate:        uniqueConstraint.Predicate,
					Invisibility:     uniqueConstraint.Invisibility,
				}
				diffs = append(diffs, Difference{
					Type:                DiffTypeTableModified,
					ObjectName:          tableName,
					Description:         fmt.Sprintf("Unique index '%s.%s' created", tableName, uniqueConstraint.Name),
					MigrationStatements: []tree.Statement{createIndex},
				})
			} else {

				createConstraint := &tree.AlterTable{
					Table: tableRef.ToUnresolvedObjectName(),
					Cmds: tree.AlterTableCmds{
						&tree.AlterTableAddConstraint{
							ConstraintDef:      localConstraint,
							ValidationBehavior: tree.ValidationDefault, // TODO: support deferred constraints?
						},
					},
				}
				name := getConstraintName(localConstraint)

				diffs = append(diffs, Difference{
					Type:                DiffTypeTableModified,
					ObjectName:          tableName,
					Description:         fmt.Sprintf("Constraint '%s.%s' added", tableName, name),
					MigrationStatements: []tree.Statement{createConstraint},
				})
			}
		}
	}

	return diffs
}

func getConstraintName(constraint tree.ConstraintTableDef) string {
	name := ""
	switch constraint := constraint.(type) {
	case *tree.UniqueConstraintTableDef:
		name = constraint.Name.Normalize()
	case *tree.ForeignKeyConstraintTableDef:
		name = constraint.Name.Normalize()
	case *tree.CheckConstraintTableDef:
		name = constraint.Name.Normalize()
	}
	return name
}
