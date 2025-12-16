package schema

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// getQualifiedRoutineName returns the schema-qualified name of a routine
func getQualifiedRoutineName(name tree.RoutineName) string {
	schema := "public"
	if name.ExplicitSchema {
		schema = name.SchemaName.String()
	}
	return fmt.Sprintf("%s.%s", schema, name.ObjectName.String())
}

// getRoutineSignature creates a unique identifier for a routine including parameter and return types
// This handles function overloading (same name, different parameters/return types)
func getRoutineSignature(routine *tree.CreateRoutine) string {
	paramTypes := make([]string, 0, len(routine.Params))
	for _, param := range routine.Params {
		paramTypes = append(paramTypes, param.Type.SQLString())
	}

	routineName := getQualifiedRoutineName(routine.Name)
	if routine.ReturnType != nil {
		returnType := routine.ReturnType.Type.SQLString()
		return fmt.Sprintf("%s(%s) -> %s", routineName, strings.Join(paramTypes, ", "), returnType)
	}
	return fmt.Sprintf("%s(%s)", routineName, strings.Join(paramTypes, ", "))
}

// compareRoutines finds differences in routines (functions/procedures)
func compareRoutines(local, remote *Schema) []Difference {
	diffs := make([]Difference, 0)

	// Build maps for quick lookup
	// Use signature (name + params) as key to handle function overloading
	localRoutines := make(map[string]ObjectSchema[*tree.CreateRoutine])
	remoteRoutines := make(map[string]ObjectSchema[*tree.CreateRoutine])

	for _, r := range local.Routines {
		localRoutines[getRoutineSignature(r.Ast)] = r
	}
	for _, r := range remote.Routines {
		remoteRoutines[getRoutineSignature(r.Ast)] = r
	}

	// Find added and modified routines
	for _, name := range sortedKeys(localRoutines) {
		localRoutine := localRoutines[name]
		remoteRoutine, existsInRemote := remoteRoutines[name]
		if !existsInRemote {
			// Routine added - create it
			diffs = append(diffs, Difference{
				Type:                DiffTypeRoutineAdded,
				ObjectName:          name,
				Description:         fmt.Sprintf("Routine '%s' added", name),
				MigrationStatements: []tree.Statement{localRoutine.Ast},
			})
		} else {
			// Check if routine was modified
			if localRoutine.Ast.String() != remoteRoutine.Ast.String() {
				// For modified routines, use CREATE OR REPLACE
				// CockroachDB supports this for functions/procedures
				ast := *localRoutine.Ast
				ast.Replace = true
				diffs = append(diffs, Difference{
					Type:                DiffTypeRoutineModified,
					ObjectName:          name,
					Description:         fmt.Sprintf("Routine '%s' modified", name),
					MigrationStatements: []tree.Statement{&ast},
				})
			}
		}
	}

	// Find removed routines
	for _, name := range sortedKeys(remoteRoutines) {
		routine := remoteRoutines[name]
		if _, existsInLocal := localRoutines[name]; !existsInLocal {
			// Routine removed - drop it
			drop := &tree.DropRoutine{
				IfExists:     true,
				Procedure:    routine.Ast.IsProcedure,
				DropBehavior: tree.DropRestrict,
				Routines: tree.RoutineObjs{tree.RoutineObj{
					FuncName: routine.Ast.Name,
					Params:   routine.Ast.Params,
				}},
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeRoutineRemoved,
				ObjectName:          name,
				Description:         fmt.Sprintf("Routine '%s' removed", name),
				Dangerous:           true,
				MigrationStatements: []tree.Statement{drop},
			})
		}
	}

	return diffs
}
