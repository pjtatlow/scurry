package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
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
			originalDeps := getCreateTableDependencies(remoteTable.Ast)
			diffs = append(diffs, Difference{
				Type:                 DiffTypeTableRemoved,
				ObjectName:           name,
				Description:          fmt.Sprintf("Table '%s' removed", name),
				Dangerous:            true,
				MigrationStatements:  []tree.Statement{drop},
				OriginalDependencies: originalDeps,
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

		case *tree.ForeignKeyConstraintTableDef:
			tc.constraints[d.Name.Normalize()] = d
		case *tree.CheckConstraintTableDef:
			tc.constraints[d.Name.Normalize()] = d
		case *tree.UniqueConstraintTableDef:
			tc.constraints[d.Name.Normalize()] = d

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

	// Handle column type changes first - these need special handling because indexes/constraints
	// that reference the changed columns must be dropped before the type change and recreated after.
	// This function removes handled columns/indexes/constraints from the component maps.
	typeChangeDiffs := handleColumnTypeChanges(tableName, local.Table, localComponents, remoteComponents)
	diffs = append(diffs, typeChangeDiffs...)

	// Compare remaining columns (type changes already handled above)
	columnDiffs := compareColumns(tableName, local.Table, localComponents.columns, remoteComponents.columns)
	diffs = append(diffs, columnDiffs...)

	// Remove indexes on dropped columns before comparing - these will be
	// automatically dropped when the column is dropped.
	removeIndexesOnDroppedColumns(localComponents.columns, remoteComponents.columns, remoteComponents.indexes)

	// Compare remaining indexes
	indexDiffs := compareIndexes(tableName, local.Table, localComponents.indexes, remoteComponents.indexes)
	diffs = append(diffs, indexDiffs...)

	// Remove constraints on dropped columns before comparing - these will be
	// automatically dropped when the column is dropped, so we don't need to
	// generate separate DROP CONSTRAINT statements.
	removeConstraintsOnDroppedColumns(localComponents.columns, remoteComponents.columns, remoteComponents.constraints)

	// Compare remaining constraints
	constraintDiffs := compareConstraints(tableName, local.Table, localComponents.constraints, remoteComponents.constraints)
	diffs = append(diffs, constraintDiffs...)

	// Compare storage params (TTL settings, etc.)
	storageParamDiffs := compareStorageParams(tableName, local.Table, local.StorageParams, remote.StorageParams)
	diffs = append(diffs, storageParamDiffs...)

	// TODO: Compare column families

	return diffs
}

// handleColumnTypeChanges handles column type changes that require an on-disk data rewrite.
// Such changes (narrowing, family changes) cannot run inside a transaction and require
// any indexes on the column to be dropped and recreated. This function generates statements
// in the correct order: DROP indexes/constraints, ALTER COLUMN TYPE, CREATE indexes/constraints.
//
// Safe type changes (widening within same family) are left for compareColumn to handle.
// Modifies the component maps to remove handled items.
func handleColumnTypeChanges(
	tableName string,
	tableRef tree.TableName,
	localComponents, remoteComponents *tableComponents,
) []Difference {
	typeChangedLocalCols := make(map[string]*tree.ColumnTableDef)
	var typeChangedColNames []string
	for colName, localCol := range localComponents.columns {
		if remoteCol, exists := remoteComponents.columns[colName]; exists {
			if localCol.Type.SQLString() != remoteCol.Type.SQLString() {
				localType, localOk := localCol.Type.(*types.T)
				remoteType, remoteOk := remoteCol.Type.(*types.T)
				if localOk && remoteOk && typeChangeRequiresRewrite(localType, remoteType) {
					typeChangedColNames = append(typeChangedColNames, colName)
					typeChangedLocalCols[colName] = localCol
				} else if !localOk || !remoteOk {
					// If we can't determine the type, assume rewrite is needed
					typeChangedColNames = append(typeChangedColNames, colName)
					typeChangedLocalCols[colName] = localCol
				}
			}
		}
	}

	if len(typeChangedColNames) == 0 {
		return nil
	}

	affectedRemoteIndexes := findAndRemoveAffectedIndexes(remoteComponents.indexes, typeChangedColNames)
	affectedLocalIndexes := findAndRemoveAffectedIndexes(localComponents.indexes, typeChangedColNames)
	affectedRemoteUniqueConstraints := findAndRemoveAffectedUniqueConstraints(remoteComponents.constraints, typeChangedColNames)
	affectedLocalUniqueConstraints := findAndRemoveAffectedUniqueConstraints(localComponents.constraints, typeChangedColNames)

	for _, colName := range typeChangedColNames {
		delete(localComponents.columns, colName)
		delete(remoteComponents.columns, colName)
	}

	statements := make([]tree.Statement, 0)
	hasDrops := len(affectedRemoteIndexes) > 0 || len(affectedRemoteUniqueConstraints) > 0
	hasCreates := len(affectedLocalIndexes) > 0 || len(affectedLocalUniqueConstraints) > 0

	// Type changes requiring rewrite cannot run inside a transaction
	statements = append(statements, &tree.CommitTransaction{}, &tree.BeginTransaction{})

	if hasDrops {
		for indexName := range affectedRemoteIndexes {
			statements = append(statements, &tree.DropIndex{
				IndexList:    tree.TableIndexNames{{Table: tableRef, Index: tree.UnrestrictedName(indexName)}},
				DropBehavior: tree.DropRestrict,
			})
		}

		for constraintName := range affectedRemoteUniqueConstraints {
			statements = append(statements, &tree.DropIndex{
				IndexList:    tree.TableIndexNames{{Table: tableRef, Index: tree.UnrestrictedName(constraintName)}},
				DropBehavior: tree.DropCascade,
			})
		}

		statements = append(statements, &tree.CommitTransaction{}, &tree.BeginTransaction{})
	}

	// ALTER COLUMN TYPE requiring rewrite must run outside a transaction.
	// Each column type change must be in its own statement - CockroachDB doesn't
	// allow combining multiple ALTER COLUMN TYPE operations in a single ALTER TABLE.
	for _, colName := range typeChangedColNames {
		localCol := typeChangedLocalCols[colName]
		statements = append(statements,
			&tree.CommitTransaction{},
			&tree.AlterTable{
				Table: tableRef.ToUnresolvedObjectName(),
				Cmds: tree.AlterTableCmds{
					&tree.AlterTableAlterColumnType{
						Column: localCol.Name,
						ToType: localCol.Type,
					},
				},
			},
			&tree.BeginTransaction{},
		)
	}

	if hasCreates {
		statements = append(statements, &tree.CommitTransaction{}, &tree.BeginTransaction{})

		for _, uniqueConstraint := range affectedLocalUniqueConstraints {
			statements = append(statements, &tree.CreateIndex{
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
			})
		}

		for indexName, localIndex := range affectedLocalIndexes {
			statements = append(statements, &tree.CreateIndex{
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
			})
		}
	}

	// Build description
	description := fmt.Sprintf("Column type changed for: %s", typeChangedColNames[0])
	if len(typeChangedColNames) > 1 {
		description = fmt.Sprintf("Column types changed for: %v", typeChangedColNames)
	}

	return []Difference{{
		Type:                DiffTypeColumnTypeChanged,
		ObjectName:          tableName,
		Description:         description,
		Dangerous:           true,
		MigrationStatements: statements,
	}}
}

// compareColumns finds differences in table columns.
func compareColumns(tableName string, tableRef tree.TableName, localCols, remoteCols map[string]*tree.ColumnTableDef) []Difference {
	diffs := make([]Difference, 0)

	// Skip the crdb_internal_expiration column - this is managed automatically by CockroachDB
	// when row-level TTL is enabled/disabled via storage params. We handle TTL via
	// compareStorageParams, so we shouldn't try to add/remove this column directly.
	delete(localCols, "crdb_internal_expiration")
	delete(remoteCols, "crdb_internal_expiration")

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

	// Check types - handle separately so we can prompt for USING expression
	if localCol.Type.SQLString() != remoteCol.Type.SQLString() {
		typeChangeCmd := &tree.AlterTableAlterColumnType{
			Column: localCol.Name,
			ToType: localCol.Type,
		}
		diffs = append(diffs, Difference{
			Type:        DiffTypeColumnTypeChanged,
			ObjectName:  tableName,
			Description: fmt.Sprintf("Column '%s.%s' type changed from %s to %s", tableName, colName, remoteCol.Type.SQLString(), localCol.Type.SQLString()),
			Dangerous:   true,
			MigrationStatements: []tree.Statement{
				&tree.AlterTable{
					Table: tableRef.ToUnresolvedObjectName(),
					Cmds:  tree.AlterTableCmds{typeChangeCmd},
				},
			},
		})
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

// compareIndexes finds differences in table indexes.
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

// compareConstraints finds differences in table constraints.
func compareConstraints(tableName string, tableRef tree.TableName, localConstraints, remoteConstraints map[string]tree.ConstraintTableDef) []Difference {
	diffs := make([]Difference, 0)
	localPrimaryKey := findPrimaryKey(localConstraints)
	remotePrimaryKey := findPrimaryKey(remoteConstraints)
	if localPrimaryKey == nil {
		panic(fmt.Sprintf("Could not find primary key for table %s in local constraints", tableName))
	}
	if remotePrimaryKey == nil {
		panic(fmt.Sprintf("Could not find primary key for table %s in remote constraints", tableName))
	}
	localPrimaryKeyStr := formatNode(localPrimaryKey)
	remotePrimaryKeyStr := formatNode(remotePrimaryKey)

	if localPrimaryKeyStr != remotePrimaryKeyStr {
		diffs = append(diffs, Difference{
			Type:         DiffTypeTableModified,
			ObjectName:   tableName,
			Description:  "Primary key modified",
			Dangerous:    true,
			IsDropCreate: false,
			MigrationStatements: []tree.Statement{
				&tree.CommitTransaction{}, &tree.BeginTransaction{},
				&tree.AlterTable{
					Table: tableRef.ToUnresolvedObjectName(),
					Cmds: tree.AlterTableCmds{
						&tree.AlterTableDropConstraint{
							Constraint: remotePrimaryKey.Name,
						},
						&tree.AlterTableAddConstraint{
							ConstraintDef: localPrimaryKey,
						},
					},
				},
				&tree.CommitTransaction{}, &tree.BeginTransaction{},
			},
		})
	}

	// Find added constraints
	for constraintName, localConstraint := range localConstraints {
		if remoteConstraint, existsInRemote := remoteConstraints[constraintName]; existsInRemote {
			localConstraintStr := formatNode(localConstraint)
			remoteConstraintStr := formatNode(remoteConstraint)

			if localConstraintStr != remoteConstraintStr {
				diffs = append(diffs, Difference{
					Type:         DiffTypeTableModified,
					ObjectName:   tableName,
					Description:  fmt.Sprintf("Constraint '%s' modified", constraintName),
					Dangerous:    true,
					IsDropCreate: true,
					MigrationStatements: []tree.Statement{
						removeConstraint(tableRef, remoteConstraint),
						&tree.CommitTransaction{}, &tree.BeginTransaction{},
						createConstraint(tableRef, localConstraint),
					},
				})
			}
		} else {
			createStatement := createConstraint(tableRef, localConstraint)
			diffs = append(diffs, Difference{
				Type:                DiffTypeTableModified,
				ObjectName:          tableName,
				Description:         fmt.Sprintf("Constraint '%s' added", constraintName),
				MigrationStatements: []tree.Statement{createStatement},
			})
		}
	}

	// Find removed constraints
	for constraintName, remoteConstraint := range remoteConstraints {
		if _, existsInLocal := localConstraints[constraintName]; !existsInLocal {
			dropStatement := removeConstraint(tableRef, remoteConstraint)
			diffs = append(diffs, Difference{
				Type:                DiffTypeTableModified,
				ObjectName:          tableName,
				Description:         fmt.Sprintf("Constraint %s removed", constraintName),
				Dangerous:           true,
				MigrationStatements: []tree.Statement{dropStatement},
			})
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

func createConstraint(tableRef tree.TableName, constraint tree.ConstraintTableDef) tree.Statement {
	if uniqueConstraint, ok := constraint.(*tree.UniqueConstraintTableDef); ok && !uniqueConstraint.PrimaryKey {
		return &tree.CreateIndex{
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
	}
	return &tree.AlterTable{
		Table: tableRef.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddConstraint{
				ConstraintDef:      constraint,
				ValidationBehavior: tree.ValidationDefault, // TODO: support deferred constraints?
			},
		},
	}
}

func removeConstraint(tableRef tree.TableName, constraint tree.ConstraintTableDef) tree.Statement {
	if uniqueConstraint, ok := constraint.(*tree.UniqueConstraintTableDef); ok && !uniqueConstraint.PrimaryKey {
		return &tree.DropIndex{
			IndexList:    tree.TableIndexNames{{Table: tableRef, Index: tree.UnrestrictedName(uniqueConstraint.Name)}},
			DropBehavior: tree.DropCascade,
		}
	}
	return &tree.AlterTable{
		Table: tableRef.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableDropConstraint{
				IfExists:     true,
				DropBehavior: tree.DropRestrict,
				Constraint:   tree.Name(getConstraintName(constraint)),
			},
		},
	}
}

// Finds the primary key constraint from the given constraints map.
// Removes it if it's found.
func findPrimaryKey(constraints map[string]tree.ConstraintTableDef) *tree.UniqueConstraintTableDef {
	for name, constraint := range constraints {
		if uniqueConstraint, ok := constraint.(*tree.UniqueConstraintTableDef); ok && uniqueConstraint.PrimaryKey {
			delete(constraints, name)
			return uniqueConstraint
		}
	}
	return nil
}

// getIndexColumnNames returns the column names referenced by an index.
func getIndexColumnNames(index *tree.IndexTableDef) []string {
	cols := make([]string, 0, len(index.Columns)+len(index.Storing))
	for _, col := range index.Columns {
		if col.Column != "" {
			cols = append(cols, col.Column.Normalize())
		}
	}
	for _, col := range index.Storing {
		cols = append(cols, col.Normalize())
	}
	return cols
}

// getUniqueConstraintColumnNames returns the column names referenced by a unique constraint.
func getUniqueConstraintColumnNames(constraint *tree.UniqueConstraintTableDef) []string {
	cols := make([]string, 0, len(constraint.Columns)+len(constraint.Storing))
	for _, col := range constraint.Columns {
		if col.Column != "" {
			cols = append(cols, col.Column.Normalize())
		}
	}
	for _, col := range constraint.Storing {
		cols = append(cols, col.Normalize())
	}
	return cols
}

// findAndRemoveAffectedIndexes returns indexes that reference any of the given columns
// and removes them from the input map.
func findAndRemoveAffectedIndexes(indexes map[string]*tree.IndexTableDef, changedCols []string) map[string]*tree.IndexTableDef {
	changedColSet := make(map[string]bool)
	for _, col := range changedCols {
		changedColSet[col] = true
	}

	affected := make(map[string]*tree.IndexTableDef)
	for name, index := range indexes {
		for _, col := range getIndexColumnNames(index) {
			if changedColSet[col] {
				affected[name] = index
				delete(indexes, name)
				break
			}
		}
	}
	return affected
}

// findAndRemoveAffectedUniqueConstraints returns non-PK unique constraints that reference any of the given columns
// and removes them from the input map.
func findAndRemoveAffectedUniqueConstraints(constraints map[string]tree.ConstraintTableDef, changedCols []string) map[string]*tree.UniqueConstraintTableDef {
	changedColSet := make(map[string]bool)
	for _, col := range changedCols {
		changedColSet[col] = true
	}

	affected := make(map[string]*tree.UniqueConstraintTableDef)
	for name, constraint := range constraints {
		if uniqueConstraint, ok := constraint.(*tree.UniqueConstraintTableDef); ok && !uniqueConstraint.PrimaryKey {
			for _, col := range getUniqueConstraintColumnNames(uniqueConstraint) {
				if changedColSet[col] {
					affected[name] = uniqueConstraint
					delete(constraints, name)
					break
				}
			}
		}
	}
	return affected
}

// getConstraintColumns returns the column names referenced by a constraint.
func getConstraintColumns(constraint tree.ConstraintTableDef) []string {
	switch c := constraint.(type) {
	case *tree.UniqueConstraintTableDef:
		return getUniqueConstraintColumnNames(c)
	case *tree.ForeignKeyConstraintTableDef:
		cols := make([]string, len(c.FromCols))
		for i, col := range c.FromCols {
			cols[i] = col.Normalize()
		}
		return cols
	case *tree.CheckConstraintTableDef:
		return getCheckConstraintColumns(c.Expr)
	}
	return nil
}

// getCheckConstraintColumns extracts column names from a CHECK constraint expression.
func getCheckConstraintColumns(expr tree.Expr) []string {
	cols := make([]string, 0)
	visitor := &checkConstraintColumnVisitor{columns: &cols}
	tree.WalkExpr(visitor, expr)
	return cols
}

type checkConstraintColumnVisitor struct {
	columns *[]string
}

func (v *checkConstraintColumnVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch e := expr.(type) {
	case *tree.ColumnItem:
		*v.columns = append(*v.columns, e.ColumnName.Normalize())
	case *tree.UnresolvedName:
		// Column references in CHECK constraints appear as UnresolvedName with NumParts == 1
		if e.NumParts == 1 {
			*v.columns = append(*v.columns, tree.Name(e.Parts[0]).Normalize())
		}
	}
	return true, expr
}

func (v *checkConstraintColumnVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

// removeConstraintsOnDroppedColumns removes from remoteConstraints any constraints
// that reference columns being dropped (columns in remote but not in local).
// This prevents generating DROP CONSTRAINT statements for constraints that will
// be automatically dropped when their column is dropped.
func removeConstraintsOnDroppedColumns(localColumns, remoteColumns map[string]*tree.ColumnTableDef, remoteConstraints map[string]tree.ConstraintTableDef) {
	// Find dropped columns
	droppedCols := make(map[string]bool)
	for colName := range remoteColumns {
		if _, existsInLocal := localColumns[colName]; !existsInLocal {
			droppedCols[colName] = true
		}
	}

	if len(droppedCols) == 0 {
		return
	}

	// Remove constraints that reference any dropped column
	for constraintName, constraint := range remoteConstraints {
		for _, col := range getConstraintColumns(constraint) {
			if droppedCols[col] {
				delete(remoteConstraints, constraintName)
				break
			}
		}
	}
}

// removeIndexesOnDroppedColumns removes from remoteIndexes any indexes
// that reference columns being dropped (columns in remote but not in local).
// This prevents generating DROP INDEX statements for indexes that will
// be automatically dropped when their column is dropped.
func removeIndexesOnDroppedColumns(localColumns, remoteColumns map[string]*tree.ColumnTableDef, remoteIndexes map[string]*tree.IndexTableDef) {
	// Find dropped columns
	droppedCols := make(map[string]bool)
	for colName := range remoteColumns {
		if _, existsInLocal := localColumns[colName]; !existsInLocal {
			droppedCols[colName] = true
		}
	}

	if len(droppedCols) == 0 {
		return
	}

	// Remove indexes that reference any dropped column
	for indexName, index := range remoteIndexes {
		for _, col := range getIndexColumnNames(index) {
			if droppedCols[col] {
				delete(remoteIndexes, indexName)
				break
			}
		}
	}
}

// typeChangeRequiresRewrite returns true if the type change requires an on-disk
// data rewrite. Such changes cannot run inside a transaction and require indexes
// to be dropped and recreated. Widening within the same family is safe; narrowing
// or changing families requires rewrite.
func typeChangeRequiresRewrite(localType, remoteType *types.T) bool {
	if localType.Family() != remoteType.Family() {
		return true
	}

	switch localType.Family() {
	case types.IntFamily, types.FloatFamily:
		return localType.Width() < remoteType.Width()

	case types.StringFamily, types.CollatedStringFamily:
		// Width of 0 means unbounded (TEXT, VARCHAR without limit)
		localWidth := localType.Width()
		remoteWidth := remoteType.Width()
		if localWidth == 0 {
			return false
		}
		if remoteWidth == 0 {
			return true
		}
		return localWidth < remoteWidth

	case types.DecimalFamily:
		// Precision/Scale of 0 means unbounded
		localPrecision := localType.Precision()
		remotePrecision := remoteType.Precision()
		localScale := localType.Scale()
		remoteScale := remoteType.Scale()

		if localPrecision == 0 {
			if localScale == 0 {
				return false
			}
			return localScale < remoteScale
		}
		if localPrecision < remotePrecision {
			return true
		}
		if localScale == 0 {
			return false
		}
		return localScale < remoteScale

	case types.BitFamily:
		if localType.Width() == 0 {
			return false
		}
		return localType.Width() < remoteType.Width()

	default:
		return true
	}
}

// compareStorageParams compares table-level storage parameters (like TTL settings)
// and generates ALTER TABLE SET/RESET statements for changes.
func compareStorageParams(tableName string, tableRef tree.TableName, localParams, remoteParams tree.StorageParams) []Difference {
	diffs := make([]Difference, 0)

	// Build maps for comparison
	localParamMap := make(map[string]tree.Expr)
	remoteParamMap := make(map[string]tree.Expr)

	for _, p := range localParams {
		localParamMap[p.Key] = p.Value
	}
	for _, p := range remoteParams {
		remoteParamMap[p.Key] = p.Value
	}

	// Find added or modified params
	var addedOrModified tree.StorageParams
	for key, localValue := range localParamMap {
		remoteValue, existsInRemote := remoteParamMap[key]
		if !existsInRemote {
			// Param added
			addedOrModified = append(addedOrModified, tree.StorageParam{Key: key, Value: localValue})
		} else if formatExpr(localValue) != formatExpr(remoteValue) {
			// Param modified
			addedOrModified = append(addedOrModified, tree.StorageParam{Key: key, Value: localValue})
		}
	}

	// Find removed params
	var removed []string
	for key := range remoteParamMap {
		if _, existsInLocal := localParamMap[key]; !existsInLocal {
			removed = append(removed, key)
		}
	}

	// Generate SET statement for added/modified params
	if len(addedOrModified) > 0 {
		setCmd := &tree.AlterTable{
			Table: tableRef.ToUnresolvedObjectName(),
			Cmds: tree.AlterTableCmds{
				&tree.AlterTableSetStorageParams{
					StorageParams: addedOrModified,
				},
			},
		}

		description := fmt.Sprintf("Storage params changed for '%s'", tableName)
		if len(addedOrModified) == 1 {
			description = fmt.Sprintf("Storage param '%s' set on '%s'", addedOrModified[0].Key, tableName)
		}

		diffs = append(diffs, Difference{
			Type:                DiffTypeTableModified,
			ObjectName:          tableName,
			Description:         description,
			MigrationStatements: []tree.Statement{setCmd},
		})
	}

	// Generate RESET statement for removed params
	if len(removed) > 0 {
		resetCmd := &tree.AlterTable{
			Table: tableRef.ToUnresolvedObjectName(),
			Cmds: tree.AlterTableCmds{
				&tree.AlterTableResetStorageParams{
					Params: removed,
				},
			},
		}

		description := fmt.Sprintf("Storage params removed from '%s'", tableName)
		if len(removed) == 1 {
			description = fmt.Sprintf("Storage param '%s' removed from '%s'", removed[0], tableName)
		}

		diffs = append(diffs, Difference{
			Type:                DiffTypeTableModified,
			ObjectName:          tableName,
			Description:         description,
			MigrationStatements: []tree.Statement{resetCmd},
		})
	}

	return diffs
}

// formatExpr returns a string representation of an expression for comparison.
func formatExpr(expr tree.Expr) string {
	if expr == nil {
		return ""
	}
	return tree.AsString(expr)
}
