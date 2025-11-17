package schema

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// Helper function to parse SQL strings into statements for tests
func parseStatements(sqls ...string) []tree.Statement {
	statements := make([]tree.Statement, 0, len(sqls))
	for _, sql := range sqls {
		parsed, err := parser.Parse(sql)
		if err != nil {
			panic(err)
		}
		for _, stmt := range parsed {
			statements = append(statements, stmt.AST)
		}
	}
	return statements
}

// Helper function to create a schema with routines
func createSchemaWithRoutines(routines []string) *Schema {
	s := &Schema{
		Routines: make([]ObjectSchema[*tree.CreateRoutine], 0),
	}

	for _, routineSQL := range routines {
		statements, err := parser.Parse(routineSQL)
		if err != nil {
			panic(err)
		}
		for _, stmt := range statements {
			if createRoutine, ok := stmt.AST.(*tree.CreateRoutine); ok {
				schemaName := "public"
				if createRoutine.Name.ExplicitSchema {
					schemaName = createRoutine.Name.SchemaName.String()
				}
				routineName := createRoutine.Name.ObjectName.String()

				s.Routines = append(s.Routines, ObjectSchema[*tree.CreateRoutine]{
					Name:   routineName,
					Schema: schemaName,
					Ast:    createRoutine,
				})
			}
		}
	}

	return s
}

func TestCompareRoutines(t *testing.T) {
	tests := []struct {
		name             string
		localRoutines    []string
		remoteRoutines   []string
		wantDiffCount    int
		wantDiffTypes    []DiffType
		wantDescriptions []string
	}{
		{
			name: "no differences",
			localRoutines: []string{
				"CREATE FUNCTION random_id() RETURNS STRING LANGUAGE SQL AS $$ SELECT gen_random_uuid()::STRING $$",
			},
			remoteRoutines: []string{
				"CREATE FUNCTION random_id() RETURNS STRING LANGUAGE SQL AS $$ SELECT gen_random_uuid()::STRING $$",
			},
			wantDiffCount: 0,
		},
		{
			name: "routine added",
			localRoutines: []string{
				"CREATE FUNCTION random_id() RETURNS STRING LANGUAGE SQL AS $$ SELECT gen_random_uuid()::STRING $$",
				"CREATE FUNCTION new_func() RETURNS INT LANGUAGE SQL AS $$ SELECT 42 $$",
			},
			remoteRoutines: []string{
				"CREATE FUNCTION random_id() RETURNS STRING LANGUAGE SQL AS $$ SELECT gen_random_uuid()::STRING $$",
			},
			wantDiffCount:    1,
			wantDiffTypes:    []DiffType{DiffTypeRoutineAdded},
			wantDescriptions: []string{"Routine 'public.new_func() -> INT8' added"},
		},
		{
			name: "routine removed",
			localRoutines: []string{
				"CREATE FUNCTION random_id() RETURNS STRING LANGUAGE SQL AS $$ SELECT gen_random_uuid()::STRING $$",
			},
			remoteRoutines: []string{
				"CREATE FUNCTION random_id() RETURNS STRING LANGUAGE SQL AS $$ SELECT gen_random_uuid()::STRING $$",
				"CREATE FUNCTION old_func() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
			},
			wantDiffCount:    1,
			wantDiffTypes:    []DiffType{DiffTypeRoutineRemoved},
			wantDescriptions: []string{"Routine 'public.old_func() -> INT8' removed"},
		},
		{
			name: "routine modified",
			localRoutines: []string{
				"CREATE FUNCTION random_id() RETURNS STRING LANGUAGE SQL AS $$ SELECT gen_random_uuid()::STRING $$",
			},
			remoteRoutines: []string{
				"CREATE FUNCTION random_id() RETURNS STRING LANGUAGE SQL AS $$ SELECT 'different' $$",
			},
			wantDiffCount:    1,
			wantDiffTypes:    []DiffType{DiffTypeRoutineModified},
			wantDescriptions: []string{"Routine 'public.random_id() -> STRING' modified"},
		},
		{
			name: "multiple changes",
			localRoutines: []string{
				"CREATE FUNCTION func1() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
				"CREATE FUNCTION func2() RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$",
				"CREATE FUNCTION func3_modified() RETURNS INT LANGUAGE SQL AS $$ SELECT 99 $$",
			},
			remoteRoutines: []string{
				"CREATE FUNCTION func2() RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$",
				"CREATE FUNCTION func3_modified() RETURNS INT LANGUAGE SQL AS $$ SELECT 3 $$",
				"CREATE FUNCTION func4() RETURNS INT LANGUAGE SQL AS $$ SELECT 4 $$",
			},
			wantDiffCount: 3,
			wantDiffTypes: []DiffType{
				DiffTypeRoutineAdded,    // func1 added
				DiffTypeRoutineModified, // func3_modified changed
				DiffTypeRoutineRemoved,  // func4 removed
			},
		},
		{
			name:           "empty schemas",
			localRoutines:  []string{},
			remoteRoutines: []string{},
			wantDiffCount:  0,
		},
		{
			name: "procedure vs function",
			localRoutines: []string{
				"CREATE PROCEDURE my_proc() LANGUAGE SQL AS $$ SELECT 1 $$",
			},
			remoteRoutines:   []string{},
			wantDiffCount:    1,
			wantDiffTypes:    []DiffType{DiffTypeRoutineAdded},
			wantDescriptions: []string{"Routine 'public.my_proc()' added"},
		},
		{
			name: "overloaded functions - different parameter counts",
			localRoutines: []string{
				"CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$",
				"CREATE FUNCTION add(a INT, b INT, c INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b + c $$",
			},
			remoteRoutines: []string{},
			wantDiffCount:  2,
			wantDiffTypes:  []DiffType{DiffTypeRoutineAdded, DiffTypeRoutineAdded},
		},
		{
			name: "overloaded functions - different parameter types",
			localRoutines: []string{
				"CREATE FUNCTION concat(a STRING, b STRING) RETURNS STRING LANGUAGE SQL AS $$ SELECT a || b $$",
				"CREATE FUNCTION concat(a INT, b INT) RETURNS STRING LANGUAGE SQL AS $$ SELECT a::STRING || b::STRING $$",
			},
			remoteRoutines: []string{},
			wantDiffCount:  2,
			wantDiffTypes:  []DiffType{DiffTypeRoutineAdded, DiffTypeRoutineAdded},
		},
		{
			name: "overloaded functions - one modified, one unchanged",
			localRoutines: []string{
				"CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b + 1 $$",
				"CREATE FUNCTION add(a INT, b INT, c INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b + c $$",
			},
			remoteRoutines: []string{
				"CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$",
				"CREATE FUNCTION add(a INT, b INT, c INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b + c $$",
			},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeRoutineModified},
		},
		{
			name: "overloaded functions - remove one overload",
			localRoutines: []string{
				"CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$",
			},
			remoteRoutines: []string{
				"CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$",
				"CREATE FUNCTION add(a INT, b INT, c INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b + c $$",
			},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeRoutineRemoved},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithRoutines(tt.localRoutines)
			remoteSchema := createSchemaWithRoutines(tt.remoteRoutines)

			diffs := compareRoutines(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Errorf("compareRoutines() returned %d diffs, want %d", len(diffs), tt.wantDiffCount)
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

			if tt.wantDescriptions != nil {
				gotDescs := make([]string, len(diffs))
				for i, d := range diffs {
					gotDescs[i] = d.Description
				}

				// Check that all expected descriptions are present
				descMatches := make(map[string]int)
				for _, desc := range gotDescs {
					descMatches[desc]++
				}
				for _, desc := range tt.wantDescriptions {
					if descMatches[desc] == 0 {
						t.Errorf("expected description %q not found in results. Got: %v", desc, gotDescs)
					}
					descMatches[desc]--
				}
			}
		})
	}
}

func TestComparisonResult_Summary(t *testing.T) {
	tests := []struct {
		name         string
		differences  []Difference
		wantContains string
	}{
		{
			name:         "no differences",
			differences:  []Difference{},
			wantContains: "No differences found",
		},
		{
			name: "one difference",
			differences: []Difference{
				{
					Type:        DiffTypeRoutineAdded,
					ObjectName:  "public.func1",
					Description: "Routine 'public.func1' added",
				},
			},
			wantContains: "Found 1 difference(s)",
		},
		{
			name: "multiple differences",
			differences: []Difference{
				{
					Type:        DiffTypeRoutineAdded,
					ObjectName:  "public.func1",
					Description: "Routine 'public.func1' added",
				},
				{
					Type:        DiffTypeRoutineRemoved,
					ObjectName:  "public.func2",
					Description: "Routine 'public.func2' removed",
				},
			},
			wantContains: "Found 2 difference(s)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &ComparisonResult{
				Differences: tt.differences,
			}

			summary := result.Summary()

			if summary == "" {
				t.Error("Summary() returned empty string")
			}

			// Check that summary contains expected text
			if tt.wantContains != "" {
				found := false
				for i := range len(summary) {
					if i+len(tt.wantContains) <= len(summary) {
						if summary[i:i+len(tt.wantContains)] == tt.wantContains {
							found = true
							break
						}
					}
				}
				if !found {
					t.Errorf("Summary() = %q, want it to contain %q", summary, tt.wantContains)
				}
			}
		})
	}
}
