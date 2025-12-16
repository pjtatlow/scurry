package schema

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// compareTypes finds differences in types (enums, composite types, etc.)
func compareTypes(local, remote *Schema) []Difference {
	diffs := make([]Difference, 0)

	// Build maps for quick lookup
	localTypes := make(map[string]ObjectSchema[*tree.CreateType])
	remoteTypes := make(map[string]ObjectSchema[*tree.CreateType])

	for _, t := range local.Types {
		localTypes[t.ResolvedName()] = t
	}
	for _, t := range remote.Types {
		remoteTypes[t.ResolvedName()] = t
	}

	// Find added and modified types
	for _, name := range sortedKeys(localTypes) {
		localType := localTypes[name]
		remoteType, existsInRemote := remoteTypes[name]
		if !existsInRemote {
			// Type added - create it
			diffs = append(diffs, Difference{
				Type:                DiffTypeTypeAdded,
				ObjectName:          name,
				Description:         fmt.Sprintf("Type '%s' added", name),
				MigrationStatements: []tree.Statement{localType.Ast},
			})
		} else {
			// Check if type was modified
			if localType.Ast.String() != remoteType.Ast.String() {
				// Type modified - need to check what kind of modification
				diff := compareTypeDetails(name, localType, remoteType)
				if diff != nil {
					diffs = append(diffs, *diff)
				}
			}
		}
	}

	// Find removed types
	for _, name := range sortedKeys(remoteTypes) {
		remoteType := remoteTypes[name]
		if _, existsInLocal := localTypes[name]; !existsInLocal {
			// Type removed - drop it
			drop := tree.DropType{
				IfExists:     true,
				DropBehavior: tree.DropRestrict,
				Names:        []*tree.UnresolvedObjectName{remoteType.Ast.TypeName},
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeTypeRemoved,
				ObjectName:          name,
				Description:         fmt.Sprintf("Type '%s' removed", name),
				Dangerous:           true,
				MigrationStatements: []tree.Statement{&drop},
			})
		}
	}

	return diffs
}

// compareTypeDetails compares the details of two types and generates appropriate migration DDL
func compareTypeDetails(name string, local, remote ObjectSchema[*tree.CreateType]) *Difference {
	// Check if both are enum types
	localEnum := getEnumValues(local.Ast)
	remoteEnum := getEnumValues(remote.Ast)

	if localEnum != nil && remoteEnum != nil {
		// Both are enums - compare values
		return compareEnumTypes(name, local, remote, localEnum, remoteEnum)
	}

	// For non-enum types or mixed types, require DROP and CREATE
	// This is destructive but necessary for composite types, domains, etc.
	migrationDDL := []tree.Statement{
		&tree.DropType{
			IfExists:     true,
			DropBehavior: tree.DropRestrict,
			Names:        []*tree.UnresolvedObjectName{remote.Ast.TypeName},
		},
		local.Ast,
	}

	return &Difference{
		Type:                DiffTypeTypeModified,
		ObjectName:          name,
		Description:         fmt.Sprintf("Type '%s' modified (requires DROP and CREATE)", name),
		IsDropCreate:        true,
		Dangerous:           true,
		MigrationStatements: migrationDDL,
	}
}

// compareEnumTypes compares two enum types and generates ALTER TYPE statements
func compareEnumTypes(name string, local, remote ObjectSchema[*tree.CreateType], localValues, remoteValues []string) *Difference {
	// Find added and removed values
	added := findAddedValues(remoteValues, localValues)
	removed := findRemovedValues(remoteValues, localValues)

	if len(added) == 0 && len(removed) == 0 {
		// No changes (shouldn't happen as caller already checked DDL equality)
		return nil
	}

	migrationDDL := make([]tree.Statement, 0)
	descParts := make([]string, 0)

	// Handle removed values
	if len(removed) > 0 {
		for _, value := range removed {
			alter := &tree.AlterType{
				Type: remote.Ast.TypeName,
				Cmd: &tree.AlterTypeDropValue{
					Val: tree.EnumValue(value),
				},
			}
			migrationDDL = append(migrationDDL, alter)
		}
		descParts = append(descParts, fmt.Sprintf("-%d values", len(removed)))
	}

	// Handle added values
	if len(added) > 0 {
		for _, value := range added {
			alter := &tree.AlterType{
				Type: local.Ast.TypeName,
				Cmd: &tree.AlterTypeAddValue{
					IfNotExists: true,
					NewVal:      tree.EnumValue(value),
				},
			}
			migrationDDL = append(migrationDDL, alter)
		}
		descParts = append(descParts, fmt.Sprintf("+%d values", len(added)))
	}

	description := fmt.Sprintf("Type '%s' modified (%s)", name, strings.Join(descParts, ", "))

	return &Difference{
		Type:                DiffTypeTypeModified,
		ObjectName:          name,
		Description:         description,
		Dangerous:           len(removed) > 0,
		MigrationStatements: migrationDDL,
	}
}

// getEnumValues extracts enum values from a CREATE TYPE statement if it's an enum
func getEnumValues(createType *tree.CreateType) []string {
	if createType.Variety != tree.Enum {
		return nil
	}

	values := make([]string, 0, len(createType.EnumLabels))
	for _, label := range createType.EnumLabels {
		values = append(values, string(label))
	}
	return values
}

// findAddedValues returns values that are in newValues but not in oldValues
func findAddedValues(oldValues, newValues []string) []string {
	oldSet := make(map[string]bool)
	for _, v := range oldValues {
		oldSet[v] = true
	}

	added := make([]string, 0)
	for _, v := range newValues {
		if !oldSet[v] {
			added = append(added, v)
		}
	}
	return added
}

// findRemovedValues returns values that are in oldValues but not in newValues
func findRemovedValues(oldValues, newValues []string) []string {
	newSet := make(map[string]bool)
	for _, v := range newValues {
		newSet[v] = true
	}

	removed := make([]string, 0)
	for _, v := range oldValues {
		if !newSet[v] {
			removed = append(removed, v)
		}
	}
	return removed
}
