package schema

import (
	"strings"
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

// Helper function to create a schema with both types and tables
func createSchemaWithTypesAndTables(types []string, tables []string) *Schema {
	s := &Schema{
		Types:  make([]ObjectSchema[*tree.CreateType], 0),
		Tables: make([]ObjectSchema[*tree.CreateTable], 0),
	}

	for _, typeSQL := range types {
		statements, err := parser.Parse(typeSQL)
		if err != nil {
			panic(err)
		}
		for _, stmt := range statements {
			if createType, ok := stmt.AST.(*tree.CreateType); ok {
				schemaName := "public"
				if createType.TypeName.HasExplicitSchema() {
					schemaName = createType.TypeName.Schema()
				}
				typeName := createType.TypeName.Object()

				s.Types = append(s.Types, ObjectSchema[*tree.CreateType]{
					Name:   typeName,
					Schema: schemaName,
					Ast:    createType,
				})
			}
		}
	}

	for _, tableSQL := range tables {
		statements, err := parser.Parse(tableSQL)
		if err != nil {
			panic(err)
		}
		for _, stmt := range statements {
			if createTable, ok := stmt.AST.(*tree.CreateTable); ok {
				schemaName := "public"
				if createTable.Table.ExplicitSchema {
					schemaName = createTable.Table.SchemaName.String()
				}
				tableName := createTable.Table.ObjectName.String()

				s.Tables = append(s.Tables, ObjectSchema[*tree.CreateTable]{
					Name:   tableName,
					Schema: schemaName,
					Ast:    createTable,
				})
			}
		}
	}

	return s
}

func TestMigrationOrderingDropTableBeforeDropType(t *testing.T) {
	tests := []struct {
		name         string
		remoteTypes  []string
		remoteTables []string
		localTypes   []string
		localTables  []string
		// wantOrder specifies substrings that must appear in order in the migration output
		wantOrder []string
	}{
		{
			name:        "drop table before drop enum it uses",
			remoteTypes: []string{"CREATE TYPE status AS ENUM ('active', 'inactive')"},
			remoteTables: []string{
				"CREATE TABLE users (id INT PRIMARY KEY, status status NOT NULL)",
			},
			localTypes:  []string{},
			localTables: []string{},
			wantOrder:   []string{"DROP TABLE", "users", "DROP TYPE", "status"},
		},
		{
			name:        "drop table before drop enum - multiple tables using same enum",
			remoteTypes: []string{"CREATE TYPE priority AS ENUM ('low', 'medium', 'high')"},
			remoteTables: []string{
				"CREATE TABLE tasks (id INT PRIMARY KEY, priority priority NOT NULL)",
				"CREATE TABLE tickets (id INT PRIMARY KEY, priority priority NOT NULL)",
			},
			localTypes:  []string{},
			localTables: []string{},
			// Both DROP TABLEs should come before DROP TYPE
			wantOrder: []string{"DROP TABLE", "DROP TABLE", "DROP TYPE", "priority"},
		},
		{
			name: "drop tables before drop enums - multiple enums",
			remoteTypes: []string{
				"CREATE TYPE status AS ENUM ('active', 'inactive')",
				"CREATE TYPE role AS ENUM ('admin', 'user')",
			},
			remoteTables: []string{
				"CREATE TABLE users (id INT PRIMARY KEY, status status NOT NULL, role role NOT NULL)",
			},
			localTypes:  []string{},
			localTables: []string{},
			// DROP TABLE should come before both DROP TYPEs
			wantOrder: []string{"DROP TABLE", "users", "DROP TYPE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithTypesAndTables(tt.localTypes, tt.localTables)
			remoteSchema := createSchemaWithTypesAndTables(tt.remoteTypes, tt.remoteTables)

			diffResult := Compare(localSchema, remoteSchema)

			if !diffResult.HasChanges() {
				t.Fatal("expected changes but got none")
			}

			migrations, _, err := diffResult.GenerateMigrations(false)
			if err != nil {
				t.Fatalf("GenerateMigrations() error: %v", err)
			}

			// Join all migrations into a single string to check ordering
			allDDL := strings.Join(migrations, "\n")

			// Verify that wantOrder substrings appear in the correct order
			lastIndex := -1
			for _, want := range tt.wantOrder {
				index := strings.Index(allDDL[lastIndex+1:], want)
				if index == -1 {
					t.Errorf("expected %q to appear in migration output after position %d.\nGot:\n%s", want, lastIndex, allDDL)
					break
				}
				// Adjust index to be relative to the full string
				index = lastIndex + 1 + index
				lastIndex = index
			}
		})
	}
}

func TestMigrationOrderingAddComputedColumnDependency(t *testing.T) {
	tests := []struct {
		name         string
		remoteTables []string
		localTables  []string
		// wantOrder specifies substrings that must appear in order in the migration output
		wantOrder []string
	}{
		{
			name: "add regular column before computed column that depends on it",
			remoteTables: []string{
				"CREATE TABLE inventory (id INT NOT NULL, quantity INT8 NOT NULL, CONSTRAINT inventory_pkey PRIMARY KEY (id))",
			},
			localTables: []string{
				"CREATE TABLE inventory (id INT NOT NULL, quantity INT8 NOT NULL, committed INT8 DEFAULT 0 NOT NULL, available INT8 AS (quantity - committed) STORED NOT NULL, CONSTRAINT inventory_pkey PRIMARY KEY (id))",
			},
			// "committed" column must be added before "available" computed column that references it
			wantOrder: []string{"committed", "available"},
		},
		{
			name: "computed column depends on multiple new columns",
			remoteTables: []string{
				"CREATE TABLE prices (id INT NOT NULL, CONSTRAINT prices_pkey PRIMARY KEY (id))",
			},
			localTables: []string{
				"CREATE TABLE prices (id INT NOT NULL, base_price INT8 NOT NULL, discount INT8 DEFAULT 0 NOT NULL, final_price INT8 AS (base_price - discount) STORED NOT NULL, CONSTRAINT prices_pkey PRIMARY KEY (id))",
			},
			// Both base_price and discount must be added before final_price
			wantOrder: []string{"base_price", "final_price"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithTypesAndTables(nil, tt.localTables)
			remoteSchema := createSchemaWithTypesAndTables(nil, tt.remoteTables)

			diffResult := Compare(localSchema, remoteSchema)

			if !diffResult.HasChanges() {
				t.Fatal("expected changes but got none")
			}

			migrations, _, err := diffResult.GenerateMigrations(false)
			if err != nil {
				t.Fatalf("GenerateMigrations() error: %v", err)
			}

			// Join all migrations into a single string to check ordering
			allDDL := strings.Join(migrations, "\n")

			// Verify that wantOrder substrings appear in the correct order
			lastIndex := -1
			for _, want := range tt.wantOrder {
				index := strings.Index(allDDL[lastIndex+1:], want)
				if index == -1 {
					t.Errorf("expected %q to appear in migration output after position %d.\nGot:\n%s", want, lastIndex, allDDL)
					break
				}
				// Adjust index to be relative to the full string
				index = lastIndex + 1 + index
				lastIndex = index
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
