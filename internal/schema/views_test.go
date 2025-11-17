package schema

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// Helper to convert statements to DDL strings
func statementsToStringsViews(stmts []tree.Statement) []string {
	strings := make([]string, len(stmts))
	for i, stmt := range stmts {
		strings[i] = stmt.String()
	}
	return strings
}

// Helper function to create a schema with views
func createSchemaWithViews(views []string) *Schema {
	s := &Schema{
		Views: make([]ObjectSchema[*tree.CreateView], 0),
	}

	for _, viewSQL := range views {
		statements, err := parser.Parse(viewSQL)
		if err != nil {
			panic(err)
		}
		for _, stmt := range statements {
			if createView, ok := stmt.AST.(*tree.CreateView); ok {
				schemaName := "public"
				if createView.Name.ExplicitSchema {
					schemaName = createView.Name.SchemaName.String()
				}
				viewName := createView.Name.ObjectName.String()

				s.Views = append(s.Views, ObjectSchema[*tree.CreateView]{
					Name:   viewName,
					Schema: schemaName,
					Ast:    createView,
				})
			}
		}
	}

	return s
}

func TestCompareViews(t *testing.T) {
	tests := []struct {
		name          string
		localViews    []string
		remoteViews   []string
		wantDiffCount int
		wantDiffTypes []DiffType
	}{
		{
			name:          "no differences",
			localViews:    []string{"CREATE VIEW active_users AS SELECT * FROM users WHERE active = true"},
			remoteViews:   []string{"CREATE VIEW active_users AS SELECT * FROM users WHERE active = true"},
			wantDiffCount: 0,
		},
		{
			name:          "view added",
			localViews:    []string{"CREATE VIEW v1 AS SELECT 1", "CREATE VIEW v2 AS SELECT 2"},
			remoteViews:   []string{"CREATE VIEW v1 AS SELECT 1"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeViewAdded},
		},
		{
			name:          "view removed",
			localViews:    []string{"CREATE VIEW v1 AS SELECT 1"},
			remoteViews:   []string{"CREATE VIEW v1 AS SELECT 1", "CREATE VIEW v2 AS SELECT 2"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeViewRemoved},
		},
		{
			name:          "view modified - query changed",
			localViews:    []string{"CREATE VIEW active_users AS SELECT * FROM users WHERE active = true"},
			remoteViews:   []string{"CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active'"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeViewModified},
		},
		{
			name:          "materialized view added",
			localViews:    []string{"CREATE MATERIALIZED VIEW mv AS SELECT * FROM users"},
			remoteViews:   []string{},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeViewAdded},
		},
		{
			name:          "materialized view modified",
			localViews:    []string{"CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM users"},
			remoteViews:   []string{"CREATE MATERIALIZED VIEW mv AS SELECT * FROM users"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeViewModified},
		},
		{
			name:          "multiple changes",
			localViews:    []string{"CREATE VIEW v1 AS SELECT 1", "CREATE VIEW v2 AS SELECT 99"},
			remoteViews:   []string{"CREATE VIEW v2 AS SELECT 2", "CREATE VIEW v3 AS SELECT 3"},
			wantDiffCount: 3,
			wantDiffTypes: []DiffType{
				DiffTypeViewAdded,    // v1 added
				DiffTypeViewModified, // v2 modified
				DiffTypeViewRemoved,  // v3 removed
			},
		},
		{
			name:          "empty schemas",
			localViews:    []string{},
			remoteViews:   []string{},
			wantDiffCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithViews(tt.localViews)
			remoteSchema := createSchemaWithViews(tt.remoteViews)

			diffs := compareViews(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Errorf("compareViews() returned %d diffs, want %d", len(diffs), tt.wantDiffCount)
			}

			if tt.wantDiffTypes != nil {
				gotTypes := make([]DiffType, len(diffs))
				for i, d := range diffs {
					gotTypes[i] = d.Type
				}

				// Check that all expected types are present (order doesn't matter)
				typeMatches := make(map[DiffType]int)
				for _, dt := range gotTypes {
					typeMatches[dt]++
				}
				for _, dt := range tt.wantDiffTypes {
					if typeMatches[dt] == 0 {
						t.Errorf("expected diff type %s not found in results", dt)
					}
					typeMatches[dt]--
				}
			}
		})
	}
}

func TestViewModifiedMigration(t *testing.T) {
	tests := []struct {
		name          string
		localView     string
		remoteView    string
		wantStmtCount int
		wantContains  []string
	}{
		{
			name:          "regular view modified generates drop and create",
			localView:     "CREATE VIEW v AS SELECT 2",
			remoteView:    "CREATE VIEW v AS SELECT 1",
			wantStmtCount: 2, // DROP + CREATE
			wantContains:  []string{"DROP VIEW", "CREATE VIEW", "SELECT 2"},
		},
		{
			name:          "materialized view modified generates drop and create",
			localView:     "CREATE MATERIALIZED VIEW mv AS SELECT id FROM users",
			remoteView:    "CREATE MATERIALIZED VIEW mv AS SELECT * FROM users",
			wantStmtCount: 2, // DROP + CREATE
			wantContains:  []string{"DROP MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW", "SELECT id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithViews([]string{tt.localView})
			remoteSchema := createSchemaWithViews([]string{tt.remoteView})

			diffs := compareViews(localSchema, remoteSchema)

			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}

			diff := diffs[0]

			if diff.Type != DiffTypeViewModified {
				t.Errorf("expected DiffTypeViewModified, got %s", diff.Type)
			}

			if len(diff.MigrationStatements) != tt.wantStmtCount {
				t.Errorf("expected %d migration statements, got %d", tt.wantStmtCount, len(diff.MigrationStatements))
			}

			// Check for expected strings in migration DDL
			allDDL := strings.Join(statementsToStringsViews(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}

func TestViewAddedRemoved(t *testing.T) {
	tests := []struct {
		name          string
		localView     string
		remoteView    string
		diffType      DiffType
		wantStmtCount int
		wantContains  []string
	}{
		{
			name:          "view added",
			localView:     "CREATE VIEW new_view AS SELECT 1",
			remoteView:    "",
			diffType:      DiffTypeViewAdded,
			wantStmtCount: 1,
			wantContains:  []string{"CREATE VIEW", "new_view"},
		},
		{
			name:          "view removed",
			localView:     "",
			remoteView:    "CREATE VIEW old_view AS SELECT 1",
			diffType:      DiffTypeViewRemoved,
			wantStmtCount: 1,
			wantContains:  []string{"DROP VIEW", "old_view"},
		},
		{
			name:          "materialized view added",
			localView:     "CREATE MATERIALIZED VIEW new_mv AS SELECT 1",
			remoteView:    "",
			diffType:      DiffTypeViewAdded,
			wantStmtCount: 1,
			wantContains:  []string{"CREATE MATERIALIZED VIEW", "new_mv"},
		},
		{
			name:          "materialized view removed",
			localView:     "",
			remoteView:    "CREATE MATERIALIZED VIEW old_mv AS SELECT 1",
			diffType:      DiffTypeViewRemoved,
			wantStmtCount: 1,
			wantContains:  []string{"DROP MATERIALIZED VIEW", "old_mv"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localViews := []string{}
			remoteViews := []string{}

			if tt.localView != "" {
				localViews = append(localViews, tt.localView)
			}
			if tt.remoteView != "" {
				remoteViews = append(remoteViews, tt.remoteView)
			}

			localSchema := createSchemaWithViews(localViews)
			remoteSchema := createSchemaWithViews(remoteViews)

			diffs := compareViews(localSchema, remoteSchema)

			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}

			diff := diffs[0]

			if diff.Type != tt.diffType {
				t.Errorf("expected %s, got %s", tt.diffType, diff.Type)
			}

			if len(diff.MigrationStatements) != tt.wantStmtCount {
				t.Errorf("expected %d migration statements, got %d", tt.wantStmtCount, len(diff.MigrationStatements))
			}

			// Check for expected strings in migration DDL
			allDDL := strings.Join(statementsToStringsViews(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}
