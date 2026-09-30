package schema

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/set"
)

// normalizeTriggerNames rewrites a trigger's table and function names as schema.name, dropping any catalog and defaulting the schema to public
func normalizeTriggerNames(trigger *tree.CreateTrigger) {
	if trigger.TableName != nil {
		if trigger.TableName.NumParts == 1 {
			trigger.TableName.Parts[1] = "public"
		}
		trigger.TableName.NumParts = 2
		trigger.TableName.Parts[2] = ""
	}
	if trigger.FuncName != nil {
		if trigger.FuncName.NumParts == 1 {
			trigger.FuncName.Parts[1] = "public"
		}
		trigger.FuncName.NumParts = 2
		trigger.FuncName.Parts[2] = ""
		trigger.FuncName.Parts[3] = ""
	}
}

// getTriggerFuncName returns the schema and name of the function a trigger executes
func getTriggerFuncName(name *tree.UnresolvedName) (string, string) {
	schemaName := "public"
	if name.NumParts >= 2 {
		schemaName = strings.ToLower(name.Parts[1])
	}
	return schemaName, strings.ToLower(name.Parts[0])
}

// getTriggerKey returns the schema-qualified table name plus trigger name that identifies a trigger
func getTriggerKey(trigger *tree.CreateTrigger) string {
	schemaName, tableName := getObjectName(trigger.TableName)
	return schemaName + "." + tableName + "." + trigger.Name.Normalize()
}

func dropTriggerStatement(trigger *tree.CreateTrigger) *tree.DropTrigger {
	return &tree.DropTrigger{
		IfExists: true,
		Trigger:  trigger.Name,
		Table:    trigger.TableName,
	}
}

// outsideTransaction wraps statements in a lone COMMIT and BEGIN so they execute in their own implicit transactions; CockroachDB only implements CREATE TRIGGER and DROP TRIGGER in the declarative schema changer, which refuses them inside a multi-statement transaction
func outsideTransaction(stmts ...tree.Statement) []tree.Statement {
	wrapped := make([]tree.Statement, 0, len(stmts)+2)
	wrapped = append(wrapped, &tree.CommitTransaction{})
	wrapped = append(wrapped, stmts...)
	wrapped = append(wrapped, &tree.BeginTransaction{})
	return wrapped
}

func getCreateTriggerDependencies(trigger *tree.CreateTrigger) set.Set[string] {
	deps := set.New[string]()

	schemaName, tableName := getObjectName(trigger.TableName)
	deps.Add("schema:" + schemaName)
	deps.Add(schemaName + "." + tableName)
	if schemaName == "public" {
		deps.Add(tableName)
	}

	funcSchema, funcName := getTriggerFuncName(trigger.FuncName)
	deps.Add(funcSchema + "." + funcName)
	if funcSchema == "public" {
		deps.Add(funcName)
	}

	return deps
}

// triggersUsingRoutine returns the triggers that execute the given schema-qualified routine
func triggersUsingRoutine(s *Schema, routineName string) []ObjectSchema[*tree.CreateTrigger] {
	matches := make([]ObjectSchema[*tree.CreateTrigger], 0)
	for _, trigger := range s.Triggers {
		funcSchema, funcName := getTriggerFuncName(trigger.Ast.FuncName)
		if funcSchema+"."+funcName == routineName {
			matches = append(matches, trigger)
		}
	}
	return matches
}

// triggersToDropBeforeReplacing returns the remote triggers that execute the routine plus any remote trigger a re-created local trigger will replace by name
func triggersToDropBeforeReplacing(remote *Schema, routineName string, recreated []ObjectSchema[*tree.CreateTrigger]) []ObjectSchema[*tree.CreateTrigger] {
	recreatedKeys := set.New[string]()
	for _, trigger := range recreated {
		recreatedKeys.Add(getTriggerKey(trigger.Ast))
	}
	matches := make([]ObjectSchema[*tree.CreateTrigger], 0)
	for _, trigger := range remote.Triggers {
		funcSchema, funcName := getTriggerFuncName(trigger.Ast.FuncName)
		if funcSchema+"."+funcName == routineName || recreatedKeys.Contains(getTriggerKey(trigger.Ast)) {
			matches = append(matches, trigger)
		}
	}
	return matches
}

// modifiedRoutineNames returns the schema-qualified names of routines whose definition differs between local and remote
func modifiedRoutineNames(local, remote *Schema) set.Set[string] {
	names := set.New[string]()
	remoteRoutines := make(map[string]ObjectSchema[*tree.CreateRoutine])
	for _, r := range remote.Routines {
		remoteRoutines[getRoutineSignature(r.Ast)] = r
	}
	for _, r := range local.Routines {
		remoteRoutine, ok := remoteRoutines[getRoutineSignature(r.Ast)]
		if ok && r.Ast.String() != remoteRoutine.Ast.String() {
			names.Add(getQualifiedRoutineName(r.Ast.Name))
		}
	}
	return names
}

// compareTriggers finds differences in triggers, skipping those re-created by a modified routine in compareRoutines
func compareTriggers(local, remote *Schema) []Difference {
	diffs := make([]Difference, 0)

	rebuiltByRoutine := modifiedRoutineNames(local, remote)
	usesModifiedRoutine := func(trigger *tree.CreateTrigger) bool {
		funcSchema, funcName := getTriggerFuncName(trigger.FuncName)
		return rebuiltByRoutine.Contains(funcSchema + "." + funcName)
	}

	localTriggers := make(map[string]ObjectSchema[*tree.CreateTrigger])
	remoteTriggers := make(map[string]ObjectSchema[*tree.CreateTrigger])
	for _, t := range local.Triggers {
		localTriggers[getTriggerKey(t.Ast)] = t
	}
	for _, t := range remote.Triggers {
		remoteTriggers[getTriggerKey(t.Ast)] = t
	}

	for key, localTrigger := range localTriggers {
		if usesModifiedRoutine(localTrigger.Ast) {
			continue
		}
		remoteTrigger, existsInRemote := remoteTriggers[key]
		if !existsInRemote {
			diffs = append(diffs, Difference{
				Type:                DiffTypeTriggerAdded,
				ObjectName:          key,
				Description:         fmt.Sprintf("Trigger '%s' added", key),
				MigrationStatements: outsideTransaction(localTrigger.Ast),
			})
			continue
		}
		if usesModifiedRoutine(remoteTrigger.Ast) {
			continue
		}
		if localTrigger.Ast.String() != remoteTrigger.Ast.String() {
			diffs = append(diffs, Difference{
				Type:                 DiffTypeTriggerModified,
				ObjectName:           key,
				Description:          fmt.Sprintf("Trigger '%s' modified", key),
				IsDropCreate:         true,
				MigrationStatements:  outsideTransaction(dropTriggerStatement(remoteTrigger.Ast), localTrigger.Ast),
				OriginalDependencies: getCreateTriggerDependencies(remoteTrigger.Ast),
			})
		}
	}

	for key, remoteTrigger := range remoteTriggers {
		if usesModifiedRoutine(remoteTrigger.Ast) {
			continue
		}
		if _, existsInLocal := localTriggers[key]; !existsInLocal {
			diffs = append(diffs, Difference{
				Type:                 DiffTypeTriggerRemoved,
				ObjectName:           key,
				Description:          fmt.Sprintf("Trigger '%s' removed", key),
				Dangerous:            true,
				MigrationStatements:  outsideTransaction(dropTriggerStatement(remoteTrigger.Ast)),
				OriginalDependencies: getCreateTriggerDependencies(remoteTrigger.Ast),
			})
		}
	}

	return diffs
}
