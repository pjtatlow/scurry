package schema

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// Helper to convert statements to DDL strings
func statementsToStrings(stmts []tree.Statement) []string {
	strings := make([]string, len(stmts))
	for i, stmt := range stmts {
		strings[i] = stmt.String()
	}
	return strings
}

// Helper function to create a schema with sequences
func createSchemaWithSequences(sequences []string) *Schema {
	s := &Schema{
		Sequences: make([]ObjectSchema[*tree.CreateSequence], 0),
	}

	for _, seqSQL := range sequences {
		statements, err := parser.Parse(seqSQL)
		if err != nil {
			panic(err)
		}
		for _, stmt := range statements {
			if createSeq, ok := stmt.AST.(*tree.CreateSequence); ok {
				schemaName := "public"
				if createSeq.Name.ExplicitSchema {
					schemaName = createSeq.Name.SchemaName.String()
				}
				seqName := createSeq.Name.ObjectName.String()

				s.Sequences = append(s.Sequences, ObjectSchema[*tree.CreateSequence]{
					Name:   seqName,
					Schema: schemaName,
					Ast:    createSeq,
				})
			}
		}
	}

	return s
}

func TestCompareSequences(t *testing.T) {
	tests := []struct {
		name          string
		localSeqs     []string
		remoteSeqs    []string
		wantDiffCount int
		wantDiffTypes []DiffType
	}{
		{
			name:          "no differences",
			localSeqs:     []string{"CREATE SEQUENCE user_id_seq"},
			remoteSeqs:    []string{"CREATE SEQUENCE user_id_seq"},
			wantDiffCount: 0,
		},
		{
			name:          "sequence added",
			localSeqs:     []string{"CREATE SEQUENCE user_id_seq", "CREATE SEQUENCE post_id_seq"},
			remoteSeqs:    []string{"CREATE SEQUENCE user_id_seq"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeSequenceAdded},
		},
		{
			name:          "sequence removed",
			localSeqs:     []string{"CREATE SEQUENCE user_id_seq"},
			remoteSeqs:    []string{"CREATE SEQUENCE user_id_seq", "CREATE SEQUENCE post_id_seq"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeSequenceRemoved},
		},
		{
			name:          "sequence modified - increment changed",
			localSeqs:     []string{"CREATE SEQUENCE user_id_seq INCREMENT BY 2"},
			remoteSeqs:    []string{"CREATE SEQUENCE user_id_seq INCREMENT BY 1"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeSequenceModified},
		},
		{
			name:          "sequence modified - minvalue changed",
			localSeqs:     []string{"CREATE SEQUENCE user_id_seq MINVALUE 100"},
			remoteSeqs:    []string{"CREATE SEQUENCE user_id_seq MINVALUE 1"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeSequenceModified},
		},
		{
			name:          "sequence modified - maxvalue changed",
			localSeqs:     []string{"CREATE SEQUENCE user_id_seq MAXVALUE 10000"},
			remoteSeqs:    []string{"CREATE SEQUENCE user_id_seq MAXVALUE 9223372036854775807"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeSequenceModified},
		},
		{
			name:          "sequence modified - cache changed",
			localSeqs:     []string{"CREATE SEQUENCE user_id_seq CACHE 10"},
			remoteSeqs:    []string{"CREATE SEQUENCE user_id_seq CACHE 1"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeSequenceModified},
		},
		{
			name:          "multiple changes",
			localSeqs:     []string{"CREATE SEQUENCE seq1", "CREATE SEQUENCE seq2 INCREMENT BY 5"},
			remoteSeqs:    []string{"CREATE SEQUENCE seq2 INCREMENT BY 1", "CREATE SEQUENCE seq3"},
			wantDiffCount: 3,
			wantDiffTypes: []DiffType{
				DiffTypeSequenceAdded,    // seq1 added
				DiffTypeSequenceModified, // seq2 modified
				DiffTypeSequenceRemoved,  // seq3 removed
			},
		},
		{
			name:          "empty schemas",
			localSeqs:     []string{},
			remoteSeqs:    []string{},
			wantDiffCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithSequences(tt.localSeqs)
			remoteSchema := createSchemaWithSequences(tt.remoteSeqs)

			diffs := compareSequences(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Errorf("compareSequences() returned %d diffs, want %d", len(diffs), tt.wantDiffCount)
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

func TestSequenceModifiedMigration(t *testing.T) {
	tests := []struct {
		name          string
		localSeq      string
		remoteSeq     string
		wantStmtCount int
		wantContains  []string
	}{
		{
			name:          "sequence modified generates drop and create",
			localSeq:      "CREATE SEQUENCE user_id_seq INCREMENT BY 2",
			remoteSeq:     "CREATE SEQUENCE user_id_seq INCREMENT BY 1",
			wantStmtCount: 2, // DROP + CREATE
			wantContains:  []string{"DROP SEQUENCE", "user_id_seq", "CREATE SEQUENCE", "INCREMENT BY 2"},
		},
		{
			name:          "sequence with multiple changes",
			localSeq:      "CREATE SEQUENCE user_id_seq INCREMENT BY 5 MINVALUE 100 MAXVALUE 10000 CACHE 10",
			remoteSeq:     "CREATE SEQUENCE user_id_seq",
			wantStmtCount: 2, // DROP + CREATE
			wantContains:  []string{"DROP SEQUENCE", "CREATE SEQUENCE", "INCREMENT BY 5", "MINVALUE 100"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithSequences([]string{tt.localSeq})
			remoteSchema := createSchemaWithSequences([]string{tt.remoteSeq})

			diffs := compareSequences(localSchema, remoteSchema)

			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}

			diff := diffs[0]

			if diff.Type != DiffTypeSequenceModified {
				t.Errorf("expected DiffTypeSequenceModified, got %s", diff.Type)
			}

			if len(diff.MigrationStatements) != tt.wantStmtCount {
				t.Errorf("expected %d migration statements, got %d", tt.wantStmtCount, len(diff.MigrationStatements))
			}

			// Check for expected strings in migration DDL
			allDDL := strings.Join(statementsToStrings(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}

func TestSequenceAddedRemoved(t *testing.T) {
	tests := []struct {
		name          string
		localSeq      string
		remoteSeq     string
		diffType      DiffType
		wantStmtCount int
		wantContains  []string
	}{
		{
			name:          "sequence added",
			localSeq:      "CREATE SEQUENCE new_seq",
			remoteSeq:     "",
			diffType:      DiffTypeSequenceAdded,
			wantStmtCount: 1,
			wantContains:  []string{"CREATE SEQUENCE", "new_seq"},
		},
		{
			name:          "sequence removed",
			localSeq:      "",
			remoteSeq:     "CREATE SEQUENCE old_seq",
			diffType:      DiffTypeSequenceRemoved,
			wantStmtCount: 1,
			wantContains:  []string{"DROP SEQUENCE", "old_seq"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSeqs := []string{}
			remoteSeqs := []string{}

			if tt.localSeq != "" {
				localSeqs = append(localSeqs, tt.localSeq)
			}
			if tt.remoteSeq != "" {
				remoteSeqs = append(remoteSeqs, tt.remoteSeq)
			}

			localSchema := createSchemaWithSequences(localSeqs)
			remoteSchema := createSchemaWithSequences(remoteSeqs)

			diffs := compareSequences(localSchema, remoteSchema)

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
			allDDL := strings.Join(statementsToStrings(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}
