package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// compareViews finds differences in views (regular and materialized)
func compareViews(local, remote *Schema) []Difference {
	diffs := make([]Difference, 0)

	// Build maps for quick lookup
	localViews := make(map[string]ObjectSchema[*tree.CreateView])
	remoteViews := make(map[string]ObjectSchema[*tree.CreateView])

	for _, v := range local.Views {
		localViews[v.ResolvedName()] = v
	}
	for _, v := range remote.Views {
		remoteViews[v.ResolvedName()] = v
	}

	// Find added and modified views
	for name, localView := range localViews {
		remoteView, existsInRemote := remoteViews[name]
		if !existsInRemote {
			// View added - create it
			diffs = append(diffs, Difference{
				Type:                DiffTypeViewAdded,
				ObjectName:          name,
				Description:         fmt.Sprintf("View '%s' added", name),
				MigrationStatements: []tree.Statement{localView.Ast},
			})
		} else {
			// Check if view was modified
			if localView.Ast.String() != remoteView.Ast.String() {
				// View modified - drop and recreate
				// Use DROP VIEW which works for both regular and materialized views
				drop := &tree.DropView{
					Names:          []tree.TableName{remoteView.Ast.Name},
					IfExists:       true,
					DropBehavior:   tree.DropRestrict,
					IsMaterialized: remoteView.Ast.Materialized,
				}
				diffs = append(diffs, Difference{
					Type:                DiffTypeViewModified,
					ObjectName:          name,
					Description:         fmt.Sprintf("View '%s' modified", name),
					MigrationStatements: []tree.Statement{drop, localView.Ast},
				})
			}
		}
	}

	// Find removed views
	for name, remoteView := range remoteViews {
		if _, existsInLocal := localViews[name]; !existsInLocal {
			// View removed - drop it
			drop := &tree.DropView{
				Names:          []tree.TableName{remoteView.Ast.Name},
				IfExists:       true,
				DropBehavior:   tree.DropRestrict,
				IsMaterialized: remoteView.Ast.Materialized,
			}
			diffs = append(diffs, Difference{
				Type:                DiffTypeViewRemoved,
				ObjectName:          name,
				Description:         fmt.Sprintf("View '%s' removed", name),
				MigrationStatements: []tree.Statement{drop},
			})
		}
	}

	return diffs
}
