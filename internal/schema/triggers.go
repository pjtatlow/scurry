package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

func compareTriggers(local, remote *Schema) []Difference {
	diffs := make([]Difference, 0)

	localTriggers := make(map[string]ObjectSchema[*tree.CreateTrigger])
	remoteTriggers := make(map[string]ObjectSchema[*tree.CreateTrigger])

	for _, t := range local.Triggers {
		localTriggers[t.ResolvedName()] = t
	}
	for _, t := range remote.Triggers {
		remoteTriggers[t.ResolvedName()] = t
	}

	for name, localTrigger := range localTriggers {
		remoteTrigger, existsInRemote := remoteTriggers[name]
		if !existsInRemote {
			diffs = append(diffs, Difference{
				Type:                DiffTypeTriggerAdded,
				ObjectName:          name,
				Description:         fmt.Sprintf("Trigger '%s' added", name),
				MigrationStatements: []tree.Statement{localTrigger.Ast},
			})
		} else {
			if localTrigger.Ast.String() != remoteTrigger.Ast.String() {
				drop := &tree.DropTrigger{
					IfExists:     true,
					Trigger:      remoteTrigger.Ast.Name,
					Table:        remoteTrigger.Ast.TableName,
					DropBehavior: tree.DropRestrict,
				}
				diffs = append(diffs, Difference{
					Type:        DiffTypeTriggerModified,
					ObjectName:  name,
					Description: fmt.Sprintf("Trigger '%s' modified", name),
					MigrationStatements: []tree.Statement{
						drop,
						&tree.CommitTransaction{},
						&tree.BeginTransaction{},
						localTrigger.Ast,
					},
				})
			}
		}
	}

	for name, remoteTrigger := range remoteTriggers {
		if _, existsInLocal := localTriggers[name]; !existsInLocal {
			drop := &tree.DropTrigger{
				IfExists:     true,
				Trigger:      remoteTrigger.Ast.Name,
				Table:        remoteTrigger.Ast.TableName,
				DropBehavior: tree.DropRestrict,
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeTriggerRemoved,
				ObjectName:          name,
				Description:         fmt.Sprintf("Trigger '%s' removed", name),
				Dangerous:           true,
				MigrationStatements: []tree.Statement{drop},
			})
		}
	}

	return diffs
}
