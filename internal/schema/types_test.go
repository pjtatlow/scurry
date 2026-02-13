package schema

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// Helper to convert statements to DDL strings
func statementsToStringsTypes(stmts []tree.Statement) []string {
	strings := make([]string, len(stmts))
	for i, stmt := range stmts {
		strings[i] = stmt.String()
	}
	return strings
}

// Helper function to create a schema with types
func createSchemaWithTypes(types []string) *Schema {
	s := &Schema{
		Types: make([]ObjectSchema[*tree.CreateType], 0),
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

	return s
}

func TestCompareTypes(t *testing.T) {
	tests := []struct {
		name          string
		localTypes    []string
		remoteTypes   []string
		wantDiffCount int
		wantDiffTypes []DiffType
	}{
		{
			name:          "no differences",
			localTypes:    []string{"CREATE TYPE status AS ENUM ('active', 'inactive')"},
			remoteTypes:   []string{"CREATE TYPE status AS ENUM ('active', 'inactive')"},
			wantDiffCount: 0,
		},
		{
			name:          "type added",
			localTypes:    []string{"CREATE TYPE status AS ENUM ('active', 'inactive')", "CREATE TYPE role AS ENUM ('admin', 'user')"},
			remoteTypes:   []string{"CREATE TYPE status AS ENUM ('active', 'inactive')"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTypeAdded},
		},
		{
			name:          "type removed",
			localTypes:    []string{"CREATE TYPE status AS ENUM ('active', 'inactive')"},
			remoteTypes:   []string{"CREATE TYPE status AS ENUM ('active', 'inactive')", "CREATE TYPE role AS ENUM ('admin', 'user')"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTypeRemoved},
		},
		{
			name:          "enum value added",
			localTypes:    []string{"CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')"},
			remoteTypes:   []string{"CREATE TYPE status AS ENUM ('active', 'inactive')"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTypeModified},
		},
		{
			name:          "enum value removed",
			localTypes:    []string{"CREATE TYPE status AS ENUM ('active')"},
			remoteTypes:   []string{"CREATE TYPE status AS ENUM ('active', 'inactive')"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTypeModified},
		},
		{
			name:          "enum values added and removed",
			localTypes:    []string{"CREATE TYPE status AS ENUM ('active', 'pending', 'suspended')"},
			remoteTypes:   []string{"CREATE TYPE status AS ENUM ('active', 'inactive')"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTypeModified},
		},
		{
			name:          "multiple changes",
			localTypes:    []string{"CREATE TYPE status AS ENUM ('active', 'inactive')", "CREATE TYPE color AS ENUM ('red', 'green', 'blue')"},
			remoteTypes:   []string{"CREATE TYPE status AS ENUM ('active')", "CREATE TYPE priority AS ENUM ('high', 'low')"},
			wantDiffCount: 3,
			wantDiffTypes: []DiffType{DiffTypeTypeModified, DiffTypeTypeAdded, DiffTypeTypeRemoved},
		},
		{
			name:          "empty schemas",
			localTypes:    []string{},
			remoteTypes:   []string{},
			wantDiffCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithTypes(tt.localTypes)
			remoteSchema := createSchemaWithTypes(tt.remoteTypes)

			diffs := compareTypes(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Errorf("compareTypes() returned %d diffs, want %d", len(diffs), tt.wantDiffCount)
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

func TestCompareEnumTypes(t *testing.T) {
	tests := []struct {
		name          string
		localType     string
		remoteType    string
		wantStmtCount int
		wantContains  []string
	}{
		{
			name:          "enum value added",
			localType:     "CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')",
			remoteType:    "CREATE TYPE status AS ENUM ('active', 'inactive')",
			wantStmtCount: 3, // 1 ADD VALUE + COMMIT/BEGIN
			wantContains:  []string{"ALTER TYPE", "ADD VALUE", "'pending'"},
		},
		{
			name:          "multiple enum values added",
			localType:     "CREATE TYPE status AS ENUM ('active', 'inactive', 'pending', 'suspended')",
			remoteType:    "CREATE TYPE status AS ENUM ('active', 'inactive')",
			wantStmtCount: 4, // 2 ADD VALUE + COMMIT/BEGIN
			wantContains:  []string{"ALTER TYPE", "ADD VALUE", "'pending'", "'suspended'"},
		},
		{
			name:          "enum value removed",
			localType:     "CREATE TYPE status AS ENUM ('active')",
			remoteType:    "CREATE TYPE status AS ENUM ('active', 'inactive')",
			wantStmtCount: 1, // 1 ALTER TYPE DROP VALUE
			wantContains:  []string{"ALTER TYPE", "DROP VALUE", "'inactive'"},
		},
		{
			name:          "enum values added and removed",
			localType:     "CREATE TYPE status AS ENUM ('active', 'pending')",
			remoteType:    "CREATE TYPE status AS ENUM ('active', 'inactive')",
			wantStmtCount: 4, // 1 DROP VALUE + 1 ADD VALUE + COMMIT/BEGIN
			wantContains:  []string{"DROP VALUE", "'inactive'", "ADD VALUE", "'pending'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithTypes([]string{tt.localType})
			remoteSchema := createSchemaWithTypes([]string{tt.remoteType})

			diffs := compareTypes(localSchema, remoteSchema)

			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}

			diff := diffs[0]

			if len(diff.MigrationStatements) != tt.wantStmtCount {
				t.Errorf("expected %d migration statements, got %d", tt.wantStmtCount, len(diff.MigrationStatements))
			}

			// Check for expected strings in migration DDL
			allDDL := strings.Join(statementsToStringsTypes(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}

func TestGetEnumValues(t *testing.T) {
	tests := []struct {
		name       string
		typeSQL    string
		wantValues []string
	}{
		{
			name:       "simple enum",
			typeSQL:    "CREATE TYPE status AS ENUM ('active', 'inactive')",
			wantValues: []string{"active", "inactive"},
		},
		{
			name:       "enum with many values",
			typeSQL:    "CREATE TYPE color AS ENUM ('red', 'green', 'blue', 'yellow')",
			wantValues: []string{"red", "green", "blue", "yellow"},
		},
		{
			name:       "enum with single value",
			typeSQL:    "CREATE TYPE single AS ENUM ('only')",
			wantValues: []string{"only"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := parser.Parse(tt.typeSQL)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			createType := statements[0].AST.(*tree.CreateType)
			values := getEnumValues(createType)

			if len(values) != len(tt.wantValues) {
				t.Errorf("got %d values, want %d", len(values), len(tt.wantValues))
			}

			for i, want := range tt.wantValues {
				if i < len(values) && values[i] != want {
					t.Errorf("value[%d] = %q, want %q", i, values[i], want)
				}
			}
		})
	}
}

func TestFindAddedRemovedValues(t *testing.T) {
	tests := []struct {
		name        string
		oldValues   []string
		newValues   []string
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:        "value added",
			oldValues:   []string{"a", "b"},
			newValues:   []string{"a", "b", "c"},
			wantAdded:   []string{"c"},
			wantRemoved: []string{},
		},
		{
			name:        "value removed",
			oldValues:   []string{"a", "b", "c"},
			newValues:   []string{"a", "b"},
			wantAdded:   []string{},
			wantRemoved: []string{"c"},
		},
		{
			name:        "values added and removed",
			oldValues:   []string{"a", "b", "c"},
			newValues:   []string{"a", "d", "e"},
			wantAdded:   []string{"d", "e"},
			wantRemoved: []string{"b", "c"},
		},
		{
			name:        "no changes",
			oldValues:   []string{"a", "b"},
			newValues:   []string{"a", "b"},
			wantAdded:   []string{},
			wantRemoved: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			added := findAddedValues(tt.oldValues, tt.newValues)
			removed := findRemovedValues(tt.oldValues, tt.newValues)

			if len(added) != len(tt.wantAdded) {
				t.Errorf("findAddedValues() returned %d values, want %d", len(added), len(tt.wantAdded))
			}

			if len(removed) != len(tt.wantRemoved) {
				t.Errorf("findRemovedValues() returned %d values, want %d", len(removed), len(tt.wantRemoved))
			}

			// Check added values
			for _, want := range tt.wantAdded {
				found := false
				for _, got := range added {
					if got == want {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected added value %q not found", want)
				}
			}

			// Check removed values
			for _, want := range tt.wantRemoved {
				found := false
				for _, got := range removed {
					if got == want {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected removed value %q not found", want)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
