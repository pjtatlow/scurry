package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// compareSequences finds differences in sequences
func compareSequences(local, remote *Schema) []Difference {
	diffs := make([]Difference, 0)

	// Build maps for quick lookup
	localSequences := make(map[string]ObjectSchema[*tree.CreateSequence])
	remoteSequences := make(map[string]ObjectSchema[*tree.CreateSequence])

	for _, s := range local.Sequences {
		localSequences[s.ResolvedName()] = s
	}
	for _, s := range remote.Sequences {
		remoteSequences[s.ResolvedName()] = s
	}

	// Find added and modified sequences
	for _, name := range sortedKeys(localSequences) {
		localSeq := localSequences[name]
		remoteSeq, existsInRemote := remoteSequences[name]
		if !existsInRemote {
			// Sequence added - create it
			diffs = append(diffs, Difference{
				Type:                DiffTypeSequenceAdded,
				ObjectName:          name,
				Description:         fmt.Sprintf("Sequence '%s' added", name),
				MigrationStatements: []tree.Statement{localSeq.Ast},
			})
		} else {
			// Check if sequence was modified
			if localSeq.Ast.String() != remoteSeq.Ast.String() {
				// Sequence modified - drop and recreate
				drop := &tree.DropSequence{
					Names:        []tree.TableName{remoteSeq.Ast.Name},
					IfExists:     true,
					DropBehavior: tree.DropRestrict,
				}
				diffs = append(diffs, Difference{
					Type:                DiffTypeSequenceModified,
					ObjectName:          name,
					Description:         fmt.Sprintf("Sequence '%s' modified", name),
					MigrationStatements: []tree.Statement{drop, localSeq.Ast},
				})
			}
		}
	}

	// Find removed sequences
	for _, name := range sortedKeys(remoteSequences) {
		remoteSeq := remoteSequences[name]
		if _, existsInLocal := localSequences[name]; !existsInLocal {
			// Sequence removed - drop it
			drop := &tree.DropSequence{
				Names:        []tree.TableName{remoteSeq.Ast.Name},
				IfExists:     true,
				DropBehavior: tree.DropRestrict,
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeSequenceRemoved,
				ObjectName:          name,
				Description:         fmt.Sprintf("Sequence '%s' removed", name),
				MigrationStatements: []tree.Statement{drop},
			})
		}
	}

	return diffs
}
