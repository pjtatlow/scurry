package schema

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

func statementsToStringsTriggers(stmts []tree.Statement) []string {
	result := make([]string, len(stmts))
	for i, stmt := range stmts {
		result[i] = stmt.String()
	}
	return result
}

func createSchemaWithTriggers(triggers []string) *Schema {
	s := &Schema{
		Triggers: make([]ObjectSchema[*tree.CreateTrigger], 0),
	}

	for _, triggerSQL := range triggers {
		statements, err := parser.Parse(triggerSQL)
		if err != nil {
			panic(err)
		}
		for _, stmt := range statements {
			if createTrigger, ok := stmt.AST.(*tree.CreateTrigger); ok {
				schemaName, tableName, triggerName := getTriggerName(createTrigger)
				s.Triggers = append(s.Triggers, ObjectSchema[*tree.CreateTrigger]{
					Name:   tableName + "." + triggerName,
					Schema: schemaName,
					Ast:    createTrigger,
				})
			}
		}
	}

	return s
}

func TestCompareTriggers(t *testing.T) {
	tests := []struct {
		name           string
		localTriggers  []string
		remoteTriggers []string
		wantDiffCount  int
		wantDiffTypes  []DiffType
	}{
		{
			name:           "no differences",
			localTriggers:  []string{"CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()"},
			remoteTriggers: []string{"CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()"},
			wantDiffCount:  0,
		},
		{
			name:           "trigger added",
			localTriggers:  []string{"CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f1()", "CREATE TRIGGER t2 AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f2()"},
			remoteTriggers: []string{"CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f1()"},
			wantDiffCount:  1,
			wantDiffTypes:  []DiffType{DiffTypeTriggerAdded},
		},
		{
			name:           "trigger removed",
			localTriggers:  []string{"CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f1()"},
			remoteTriggers: []string{"CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f1()", "CREATE TRIGGER t2 AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f2()"},
			wantDiffCount:  1,
			wantDiffTypes:  []DiffType{DiffTypeTriggerRemoved},
		},
		{
			name:           "trigger modified - function changed",
			localTriggers:  []string{"CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION new_audit_func()"},
			remoteTriggers: []string{"CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION old_audit_func()"},
			wantDiffCount:  1,
			wantDiffTypes:  []DiffType{DiffTypeTriggerModified},
		},
		{
			name:           "trigger modified - timing changed",
			localTriggers:  []string{"CREATE TRIGGER audit_trigger BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()"},
			remoteTriggers: []string{"CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()"},
			wantDiffCount:  1,
			wantDiffTypes:  []DiffType{DiffTypeTriggerModified},
		},
		{
			name:           "multiple changes",
			localTriggers:  []string{"CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f1()", "CREATE TRIGGER t2 AFTER UPDATE ON users FOR EACH ROW EXECUTE FUNCTION f2_new()"},
			remoteTriggers: []string{"CREATE TRIGGER t2 AFTER UPDATE ON users FOR EACH ROW EXECUTE FUNCTION f2_old()", "CREATE TRIGGER t3 AFTER DELETE ON users FOR EACH ROW EXECUTE FUNCTION f3()"},
			wantDiffCount:  3,
			wantDiffTypes: []DiffType{
				DiffTypeTriggerAdded,    // t1 added
				DiffTypeTriggerModified, // t2 modified
				DiffTypeTriggerRemoved,  // t3 removed
			},
		},
		{
			name:           "empty schemas",
			localTriggers:  []string{},
			remoteTriggers: []string{},
			wantDiffCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithTriggers(tt.localTriggers)
			remoteSchema := createSchemaWithTriggers(tt.remoteTriggers)

			diffs := compareTriggers(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Errorf("compareTriggers() returned %d diffs, want %d", len(diffs), tt.wantDiffCount)
			}

			if tt.wantDiffTypes != nil {
				gotTypes := make([]DiffType, len(diffs))
				for i, d := range diffs {
					gotTypes[i] = d.Type
				}

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

func TestTriggerModifiedMigration(t *testing.T) {
	tests := []struct {
		name          string
		localTrigger  string
		remoteTrigger string
		wantStmtCount int
		wantContains  []string
	}{
		{
			name:          "modified trigger generates drop, commit, begin, create",
			localTrigger:  "CREATE TRIGGER t AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION new_func()",
			remoteTrigger: "CREATE TRIGGER t AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION old_func()",
			wantStmtCount: 4,
			wantContains:  []string{"DROP TRIGGER", "COMMIT", "BEGIN", "CREATE TRIGGER", "new_func"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithTriggers([]string{tt.localTrigger})
			remoteSchema := createSchemaWithTriggers([]string{tt.remoteTrigger})

			diffs := compareTriggers(localSchema, remoteSchema)

			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}

			diff := diffs[0]

			if diff.Type != DiffTypeTriggerModified {
				t.Errorf("expected DiffTypeTriggerModified, got %s", diff.Type)
			}

			if len(diff.MigrationStatements) != tt.wantStmtCount {
				t.Errorf("expected %d migration statements, got %d", tt.wantStmtCount, len(diff.MigrationStatements))
			}

			allDDL := strings.Join(statementsToStringsTriggers(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}

func TestTriggerAddedRemoved(t *testing.T) {
	tests := []struct {
		name          string
		localTrigger  string
		remoteTrigger string
		diffType      DiffType
		wantStmtCount int
		wantContains  []string
		wantDangerous bool
	}{
		{
			name:          "trigger added",
			localTrigger:  "CREATE TRIGGER new_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()",
			remoteTrigger: "",
			diffType:      DiffTypeTriggerAdded,
			wantStmtCount: 1,
			wantContains:  []string{"CREATE TRIGGER", "new_trigger"},
			wantDangerous: false,
		},
		{
			name:          "trigger removed",
			localTrigger:  "",
			remoteTrigger: "CREATE TRIGGER old_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()",
			diffType:      DiffTypeTriggerRemoved,
			wantStmtCount: 1,
			wantContains:  []string{"DROP TRIGGER", "old_trigger"},
			wantDangerous: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localTriggers := []string{}
			remoteTriggers := []string{}

			if tt.localTrigger != "" {
				localTriggers = append(localTriggers, tt.localTrigger)
			}
			if tt.remoteTrigger != "" {
				remoteTriggers = append(remoteTriggers, tt.remoteTrigger)
			}

			localSchema := createSchemaWithTriggers(localTriggers)
			remoteSchema := createSchemaWithTriggers(remoteTriggers)

			diffs := compareTriggers(localSchema, remoteSchema)

			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}

			diff := diffs[0]

			if diff.Type != tt.diffType {
				t.Errorf("expected %s, got %s", tt.diffType, diff.Type)
			}

			if diff.Dangerous != tt.wantDangerous {
				t.Errorf("expected Dangerous=%v, got %v", tt.wantDangerous, diff.Dangerous)
			}

			if len(diff.MigrationStatements) != tt.wantStmtCount {
				t.Errorf("expected %d migration statements, got %d", tt.wantStmtCount, len(diff.MigrationStatements))
			}

			allDDL := strings.Join(statementsToStringsTriggers(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}
