package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/db"
)

// Helper to convert statements to DDL strings
func statementsToStringsTables(stmts []tree.Statement) []string {
	strings := make([]string, len(stmts))
	for i, stmt := range stmts {
		strings[i] = stmt.String()
	}
	return strings
}

// Helper function to create a schema with tables.
// This runs the statements through a real CockroachDB instance to get
// properly formatted CREATE TABLE statements with explicit constraints.
func createSchemaWithTables(tables []string) *Schema {
	s := &Schema{
		Tables: make([]ObjectSchema[*tree.CreateTable], 0),
	}

	if len(tables) == 0 {
		return s
	}

	ctx := context.Background()

	// Create a shadow DB and execute the statements
	client, err := db.GetShadowDB(ctx, tables...)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Get the properly formatted CREATE statements back from the database
	createStatements, err := client.GetAllCreateStatements(ctx)
	if err != nil {
		panic(err)
	}

	for _, stmt := range createStatements {
		statements, err := parser.Parse(stmt)
		if err != nil {
			panic(err)
		}
		for _, parsed := range statements {
			if createTable, ok := parsed.AST.(*tree.CreateTable); ok {
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

func TestCompareTables(t *testing.T) {
	tests := []struct {
		name          string
		localTables   []string
		remoteTables  []string
		wantDiffCount int
		wantDiffTypes []DiffType
	}{
		{
			name:          "no differences",
			localTables:   []string{"CREATE TABLE users (id INT PRIMARY KEY, name STRING)"},
			remoteTables:  []string{"CREATE TABLE users (id INT PRIMARY KEY, name STRING)"},
			wantDiffCount: 0,
		},
		{
			name:          "table added",
			localTables:   []string{"CREATE TABLE users (id INT PRIMARY KEY)", "CREATE TABLE posts (id INT PRIMARY KEY)"},
			remoteTables:  []string{"CREATE TABLE users (id INT PRIMARY KEY)"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTableAdded},
		},
		{
			name:          "table removed",
			localTables:   []string{"CREATE TABLE users (id INT PRIMARY KEY)"},
			remoteTables:  []string{"CREATE TABLE users (id INT PRIMARY KEY)", "CREATE TABLE posts (id INT PRIMARY KEY)"},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTableRemoved},
		},
		{
			name:          "multiple tables added",
			localTables:   []string{"CREATE TABLE users (id INT PRIMARY KEY)", "CREATE TABLE posts (id INT PRIMARY KEY)", "CREATE TABLE comments (id INT PRIMARY KEY)"},
			remoteTables:  []string{"CREATE TABLE users (id INT PRIMARY KEY)"},
			wantDiffCount: 2,
			wantDiffTypes: []DiffType{DiffTypeTableAdded, DiffTypeTableAdded},
		},
		{
			name:          "multiple tables removed",
			localTables:   []string{"CREATE TABLE users (id INT PRIMARY KEY)"},
			remoteTables:  []string{"CREATE TABLE users (id INT PRIMARY KEY)", "CREATE TABLE posts (id INT PRIMARY KEY)", "CREATE TABLE comments (id INT PRIMARY KEY)"},
			wantDiffCount: 2,
			wantDiffTypes: []DiffType{DiffTypeTableRemoved, DiffTypeTableRemoved},
		},
		{
			name:        "mixed add and remove",
			localTables: []string{"CREATE TABLE users (id INT PRIMARY KEY)", "CREATE TABLE posts (id INT PRIMARY KEY)"},
			remoteTables: []string{
				"CREATE TABLE users (id INT PRIMARY KEY)",
				"CREATE TABLE comments (id INT PRIMARY KEY)",
			},
			wantDiffCount: 2,
			wantDiffTypes: []DiffType{DiffTypeTableAdded, DiffTypeTableRemoved},
		},
		{
			name:          "empty schemas",
			localTables:   []string{},
			remoteTables:  []string{},
			wantDiffCount: 0,
		},
		{
			name: "table with multiple columns added",
			localTables: []string{
				"CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, email STRING UNIQUE, created_at TIMESTAMP DEFAULT now())",
			},
			remoteTables:  []string{},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTableAdded},
		},
		{
			name:          "table with foreign key added",
			localTables:   []string{"CREATE TABLE users (id INT PRIMARY KEY)", "CREATE TABLE posts (id INT PRIMARY KEY, user_id INT REFERENCES users(id))"},
			remoteTables:  []string{},
			wantDiffCount: 2,
			wantDiffTypes: []DiffType{DiffTypeTableAdded, DiffTypeTableAdded},
		},
		{
			name:          "table with indexes added",
			localTables:   []string{"CREATE TABLE users (id INT PRIMARY KEY, email STRING, INDEX email_idx (email))"},
			remoteTables:  []string{},
			wantDiffCount: 1,
			wantDiffTypes: []DiffType{DiffTypeTableAdded},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables(tt.localTables)
			remoteSchema := createSchemaWithTables(tt.remoteTables)

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Errorf("compareTables() returned %d diffs, want %d", len(diffs), tt.wantDiffCount)
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

func TestTableAddedMigration(t *testing.T) {
	tests := []struct {
		name         string
		localTable   string
		wantContains []string
	}{
		{
			name:       "simple table added",
			localTable: "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			wantContains: []string{
				"CREATE TABLE",
				"users",
				"id INT",
				"name STRING",
				"PRIMARY KEY",
			},
		},
		{
			name:       "table with constraints added",
			localTable: "CREATE TABLE users (id INT PRIMARY KEY, email STRING NOT NULL UNIQUE)",
			wantContains: []string{
				"CREATE TABLE",
				"users",
				"email STRING NOT NULL",
				"UNIQUE",
			},
		},
		{
			name:       "table with index added",
			localTable: "CREATE TABLE users (id INT PRIMARY KEY, email STRING, INDEX email_idx (email))",
			wantContains: []string{
				"CREATE TABLE",
				"users",
				"INDEX email_idx",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}

			diff := diffs[0]

			if diff.Type != DiffTypeTableAdded {
				t.Errorf("expected DiffTypeTableAdded, got %s", diff.Type)
			}

			if len(diff.MigrationStatements) != 1 {
				t.Errorf("expected 1 migration statement, got %d", len(diff.MigrationStatements))
			}

			// Check for expected strings in migration DDL
			allDDL := strings.Join(statementsToStringsTables(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}

func TestTableRemovedMigration(t *testing.T) {
	tests := []struct {
		name         string
		remoteTable  string
		wantContains []string
	}{
		{
			name:        "simple table removed",
			remoteTable: "CREATE TABLE users (id INT PRIMARY KEY)",
			wantContains: []string{
				"DROP TABLE",
				"users",
			},
		},
		{
			name:        "table with IF EXISTS and RESTRICT",
			remoteTable: "CREATE TABLE posts (id INT PRIMARY KEY, user_id INT)",
			wantContains: []string{
				"DROP TABLE",
				"IF EXISTS",
				"posts",
				"RESTRICT",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}

			diff := diffs[0]

			if diff.Type != DiffTypeTableRemoved {
				t.Errorf("expected DiffTypeTableRemoved, got %s", diff.Type)
			}

			if len(diff.MigrationStatements) != 1 {
				t.Errorf("expected 1 migration statement, got %d", len(diff.MigrationStatements))
			}

			// Check for expected strings in migration DDL
			allDDL := strings.Join(statementsToStringsTables(diff.MigrationStatements), "\n")
			for _, expected := range tt.wantContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("migration DDL missing expected string %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}

func TestCompareTablesColumnAdditions(t *testing.T) {
	tests := []struct {
		name             string
		localTable       string
		remoteTable      string
		wantDiffCount    int
		wantDiffType     DiffType
		wantDescContains string
		wantDDLContains  []string
	}{
		{
			name:             "single column added",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING)",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "email",
			wantDDLContains:  []string{"ALTER TABLE", "ADD COLUMN", "email", "STRING"},
		},
		{
			name:             "column with NOT NULL added",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING NOT NULL)",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY)",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "email",
			wantDDLContains:  []string{"ALTER TABLE", "ADD COLUMN", "email", "NOT NULL"},
		},
		{
			name:             "column with DEFAULT added",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT now())",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY)",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "created_at",
			wantDDLContains:  []string{"ALTER TABLE", "ADD COLUMN", "created_at", "DEFAULT"},
		},
		{
			name:             "multiple columns added",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING, age INT)",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			wantDiffCount:    2,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "",
			wantDDLContains:  []string{"ALTER TABLE", "ADD COLUMN"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Fatalf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
			}

			for _, diff := range diffs {
				if diff.Type != tt.wantDiffType {
					t.Errorf("expected diff type %s, got %s", tt.wantDiffType, diff.Type)
				}

				if tt.wantDescContains != "" && !strings.Contains(diff.Description, tt.wantDescContains) {
					t.Errorf("description %q does not contain %q", diff.Description, tt.wantDescContains)
				}

				if len(diff.MigrationStatements) != 1 {
					t.Errorf("expected 1 migration statement, got %d", len(diff.MigrationStatements))
					continue
				}

				ddl := diff.MigrationStatements[0].String()
				for _, expected := range tt.wantDDLContains {
					if !strings.Contains(ddl, expected) {
						t.Errorf("DDL %q does not contain %q", ddl, expected)
					}
				}
			}
		})
	}
}

func TestCompareTablesColumnRemovals(t *testing.T) {
	tests := []struct {
		name             string
		localTable       string
		remoteTable      string
		wantDiffCount    int
		wantDiffType     DiffType
		wantDescContains string
		wantDDLContains  []string
	}{
		{
			name:             "single column removed",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING)",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "email",
			wantDDLContains:  []string{"ALTER TABLE", "DROP COLUMN", "email", "RESTRICT"},
		},
		{
			name:             "multiple columns removed",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING, age INT)",
			wantDiffCount:    2,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "",
			wantDDLContains:  []string{"ALTER TABLE", "DROP COLUMN", "RESTRICT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Fatalf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
			}

			for _, diff := range diffs {
				if diff.Type != tt.wantDiffType {
					t.Errorf("expected diff type %s, got %s", tt.wantDiffType, diff.Type)
				}

				if tt.wantDescContains != "" && !strings.Contains(diff.Description, tt.wantDescContains) {
					t.Errorf("description %q does not contain %q", diff.Description, tt.wantDescContains)
				}

				if len(diff.MigrationStatements) != 1 {
					t.Errorf("expected 1 migration statement, got %d", len(diff.MigrationStatements))
					continue
				}

				ddl := diff.MigrationStatements[0].String()
				for _, expected := range tt.wantDDLContains {
					if !strings.Contains(ddl, expected) {
						t.Errorf("DDL %q does not contain %q", ddl, expected)
					}
				}
			}
		})
	}
}

func TestCompareTablesColumnAdditionsAndRemovals(t *testing.T) {
	tests := []struct {
		name          string
		localTable    string
		remoteTable   string
		wantDiffCount int
	}{
		{
			name:          "columns added and removed",
			localTable:    "CREATE TABLE users (id INT PRIMARY KEY, email STRING, age INT)",
			remoteTable:   "CREATE TABLE users (id INT PRIMARY KEY, name STRING, phone STRING)",
			wantDiffCount: 4, // add email, add age, remove name, remove phone
		},
		{
			name:          "no changes when columns identical",
			localTable:    "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING)",
			remoteTable:   "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING)",
			wantDiffCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Errorf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
				for i, diff := range diffs {
					t.Logf("  diff[%d]: %s - %s", i, diff.Type, diff.Description)
				}
			}
		})
	}
}

func TestCompareTablesIndexAdditions(t *testing.T) {
	tests := []struct {
		name             string
		localTable       string
		remoteTable      string
		wantDiffCount    int
		wantDiffType     DiffType
		wantDescContains string
		wantDDLContains  []string
	}{
		{
			name:             "single index added",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING, INDEX email_idx (email))",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING)",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "email_idx",
			wantDDLContains:  []string{"CREATE INDEX", "email_idx", "email"},
		},
		{
			name:             "index with STORING added",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email) STORING (name))",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING)",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "email_idx",
			wantDDLContains:  []string{"CREATE INDEX", "email_idx", "STORING", "name"},
		},
		{
			name:             "multiple indexes added",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email), INDEX name_idx (name))",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING)",
			wantDiffCount:    2,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "",
			wantDDLContains:  []string{"CREATE INDEX"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Fatalf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
			}

			for _, diff := range diffs {
				if diff.Type != tt.wantDiffType {
					t.Errorf("expected diff type %s, got %s", tt.wantDiffType, diff.Type)
				}

				if tt.wantDescContains != "" && !strings.Contains(diff.Description, tt.wantDescContains) {
					t.Errorf("description %q does not contain %q", diff.Description, tt.wantDescContains)
				}

				if len(diff.MigrationStatements) != 1 {
					t.Errorf("expected 1 migration statement, got %d", len(diff.MigrationStatements))
					continue
				}

				ddl := diff.MigrationStatements[0].String()
				for _, expected := range tt.wantDDLContains {
					if !strings.Contains(ddl, expected) {
						t.Errorf("DDL %q does not contain %q", ddl, expected)
					}
				}
			}
		})
	}
}

func TestCompareTablesIndexRemovals(t *testing.T) {
	tests := []struct {
		name             string
		localTable       string
		remoteTable      string
		wantDiffCount    int
		wantDiffType     DiffType
		wantDescContains string
		wantDDLContains  []string
	}{
		{
			name:             "single index removed",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING)",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING, INDEX email_idx (email))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "email_idx",
			wantDDLContains:  []string{"DROP INDEX", "email_idx", "RESTRICT"},
		},
		{
			name:             "multiple indexes removed",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING)",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email), INDEX name_idx (name))",
			wantDiffCount:    2,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "",
			wantDDLContains:  []string{"DROP INDEX", "RESTRICT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Fatalf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
			}

			for _, diff := range diffs {
				if diff.Type != tt.wantDiffType {
					t.Errorf("expected diff type %s, got %s", tt.wantDiffType, diff.Type)
				}

				if tt.wantDescContains != "" && !strings.Contains(diff.Description, tt.wantDescContains) {
					t.Errorf("description %q does not contain %q", diff.Description, tt.wantDescContains)
				}

				if len(diff.MigrationStatements) != 1 {
					t.Errorf("expected 1 migration statement, got %d", len(diff.MigrationStatements))
					continue
				}

				ddl := diff.MigrationStatements[0].String()
				for _, expected := range tt.wantDDLContains {
					if !strings.Contains(ddl, expected) {
						t.Errorf("DDL %q does not contain %q", ddl, expected)
					}
				}
			}
		})
	}
}

func TestCompareTablesIndexAdditionsAndRemovals(t *testing.T) {
	tests := []struct {
		name          string
		localTable    string
		remoteTable   string
		wantDiffCount int
	}{
		{
			name:          "indexes added and removed",
			localTable:    "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email))",
			remoteTable:   "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX name_idx (name))",
			wantDiffCount: 2, // add email_idx, remove name_idx
		},
		{
			name:          "no changes when indexes identical",
			localTable:    "CREATE TABLE users (id INT PRIMARY KEY, email STRING, INDEX email_idx (email))",
			remoteTable:   "CREATE TABLE users (id INT PRIMARY KEY, email STRING, INDEX email_idx (email))",
			wantDiffCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Errorf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
				for i, diff := range diffs {
					t.Logf("  diff[%d]: %s - %s", i, diff.Type, diff.Description)
				}
			}
		})
	}
}

func TestCompareTablesIndexModifications(t *testing.T) {
	tests := []struct {
		name             string
		localTable       string
		remoteTable      string
		wantDiffCount    int
		wantDiffType     DiffType
		wantDescContains string
		wantDDLContains  []string
	}{
		{
			name:             "index columns changed",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email, name))",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "modified",
			wantDDLContains:  []string{"DROP INDEX", "email_idx", "CREATE INDEX", "email_idx"},
		},
		{
			name:             "index STORING changed",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, age INT, INDEX email_idx (email) STORING (name, age))",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, age INT, INDEX email_idx (email) STORING (name))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "modified",
			wantDDLContains:  []string{"DROP INDEX", "CREATE INDEX", "STORING"},
		},
		{
			name:             "index STORING added",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email) STORING (name))",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "modified",
			wantDDLContains:  []string{"DROP INDEX", "CREATE INDEX", "STORING"},
		},
		{
			name:             "index STORING removed",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email))",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, email STRING, name STRING, INDEX email_idx (email) STORING (name))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "modified",
			wantDDLContains:  []string{"DROP INDEX", "CREATE INDEX"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Fatalf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
			}

			diff := diffs[0]

			if diff.Type != tt.wantDiffType {
				t.Errorf("expected diff type %s, got %s", tt.wantDiffType, diff.Type)
			}

			if tt.wantDescContains != "" && !strings.Contains(diff.Description, tt.wantDescContains) {
				t.Errorf("description %q does not contain %q", diff.Description, tt.wantDescContains)
			}

			// Index modifications require transaction boundaries:
			// DROP INDEX, COMMIT, BEGIN, CREATE INDEX (4 statements)
			if len(diff.MigrationStatements) != 4 {
				t.Errorf("expected 4 migration statements (DROP + COMMIT + BEGIN + CREATE), got %d", len(diff.MigrationStatements))
				for i, stmt := range diff.MigrationStatements {
					t.Logf("Statement %d: %s", i, stmt.String())
				}
				return
			}

			// Verify DROP, COMMIT, BEGIN, and CREATE are present
			allDDL := ""
			for _, stmt := range diff.MigrationStatements {
				allDDL += stmt.String() + "\n"
			}
			for _, expected := range tt.wantDDLContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("DDL does not contain %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}

func TestCompareTablesPrimaryKeyChanges(t *testing.T) {
	tests := []struct {
		name             string
		localTable       string
		remoteTable      string
		wantDiffCount    int
		wantDiffType     DiffType
		wantDescContains string
		wantDDLContains  []string
		wantDangerous    bool
	}{
		{
			name:             "primary key column changed",
			localTable:       "CREATE TABLE users (id INT NOT NULL, email STRING NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (email))",
			remoteTable:      "CREATE TABLE users (id INT NOT NULL, email STRING NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "Primary key",
			wantDDLContains:  []string{"DROP CONSTRAINT", "ADD CONSTRAINT", "PRIMARY KEY", "email"},
			wantDangerous:    true,
		},
		{
			name:             "primary key add column",
			localTable:       "CREATE TABLE users (id INT, name STRING NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id, name))",
			remoteTable:      "CREATE TABLE users (id INT, name STRING NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "Primary key",
			wantDDLContains:  []string{"DROP CONSTRAINT", "ADD CONSTRAINT", "PRIMARY KEY", "id", "name"},
			wantDangerous:    true,
		},
		{
			name:             "primary key remove column",
			localTable:       "CREATE TABLE users (id INT, name STRING NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id))",
			remoteTable:      "CREATE TABLE users (id INT, name STRING NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id, name))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "Primary key",
			wantDDLContains:  []string{"DROP CONSTRAINT", "ADD CONSTRAINT", "PRIMARY KEY"},
			wantDangerous:    true,
		},
		{
			name:             "primary key column order changed",
			localTable:       "CREATE TABLE users (id INT, name STRING NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (name, id))",
			remoteTable:      "CREATE TABLE users (id INT, name STRING NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id, name))",
			wantDiffCount:    1,
			wantDiffType:     DiffTypeTableModified,
			wantDescContains: "Primary key",
			wantDDLContains:  []string{"DROP CONSTRAINT", "ADD CONSTRAINT", "PRIMARY KEY"},
			wantDangerous:    true,
		},
		{
			name:             "no change when primary key identical",
			localTable:       "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			remoteTable:      "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			wantDiffCount:    0,
			wantDiffType:     "",
			wantDescContains: "",
			wantDDLContains:  nil,
			wantDangerous:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Fatalf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
			}

			if tt.wantDiffCount == 0 {
				return
			}

			diff := diffs[0]

			if diff.Type != tt.wantDiffType {
				t.Errorf("expected diff type %s, got %s", tt.wantDiffType, diff.Type)
			}

			if tt.wantDescContains != "" && !strings.Contains(diff.Description, tt.wantDescContains) {
				t.Errorf("description %q does not contain %q", diff.Description, tt.wantDescContains)
			}

			if diff.Dangerous != tt.wantDangerous {
				t.Errorf("expected Dangerous=%v, got %v", tt.wantDangerous, diff.Dangerous)
			}

			// Primary key modifications require transaction boundaries:
			// COMMIT, BEGIN, ALTER TABLE (DROP + ADD), COMMIT, BEGIN (5 statements)
			if len(diff.MigrationStatements) != 5 {
				t.Errorf("expected 5 migration statements (COMMIT + BEGIN + ALTER + COMMIT + BEGIN), got %d", len(diff.MigrationStatements))
				for i, stmt := range diff.MigrationStatements {
					t.Logf("Statement %d: %s", i, stmt.String())
				}
				return
			}

			// Verify expected DDL is present
			allDDL := ""
			for _, stmt := range diff.MigrationStatements {
				allDDL += stmt.String() + "\n"
			}
			for _, expected := range tt.wantDDLContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("DDL does not contain %q.\nGot:\n%s", expected, allDDL)
				}
			}
		})
	}
}

func TestCompareTablesColumnTypeChangeWithIndex(t *testing.T) {
	tests := []struct {
		name            string
		localTable      string
		remoteTable     string
		wantDDLContains []string
	}{
		{
			name:        "column type change with index on column",
			localTable:  "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL, INDEX email_idx (email))",
			remoteTable: "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL, INDEX email_idx (email))",
			// Generates: DROP INDEX diff, ALTER COLUMN diff, CREATE INDEX diff
			wantDDLContains: []string{"DROP INDEX", "email_idx", "ALTER COLUMN", "email", "VARCHAR", "CREATE INDEX", "email_idx"},
		},
		{
			name:        "column type change with column in STORING clause",
			localTable:  "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL, name STRING, INDEX name_idx (name) STORING (email))",
			remoteTable: "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL, name STRING, INDEX name_idx (name) STORING (email))",
			// Generates: DROP INDEX diff, ALTER COLUMN diff, CREATE INDEX diff
			wantDDLContains: []string{"DROP INDEX", "name_idx", "ALTER COLUMN", "email", "VARCHAR", "CREATE INDEX", "name_idx", "STORING"},
		},
		{
			name:        "column type change with multiple indexes",
			localTable:  "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL, INDEX email_idx (email), INDEX email_id_idx (email, id))",
			remoteTable: "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL, INDEX email_idx (email), INDEX email_id_idx (email, id))",
			// Generates: DROP INDEX diff (both indexes), ALTER COLUMN diff, CREATE INDEX diffs
			wantDDLContains: []string{"DROP INDEX", "ALTER TABLE", "ALTER COLUMN", "email", "VARCHAR", "CREATE INDEX"},
		},
		{
			name:            "column type change without index",
			localTable:      "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL)",
			remoteTable:     "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL)",
			wantDDLContains: []string{"ALTER COLUMN", "email", "VARCHAR"},
		},
		{
			name:        "column type change with index removed",
			localTable:  "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL)",
			remoteTable: "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL, INDEX email_idx (email))",
			// Generates: DROP INDEX diff, ALTER COLUMN diff (no CREATE INDEX since index not in local)
			wantDDLContains: []string{"DROP INDEX", "email_idx", "ALTER TABLE", "ALTER COLUMN", "email", "VARCHAR"},
		},
		{
			name:        "multiple columns type change in same index",
			localTable:  "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL, name VARCHAR(100) NOT NULL, INDEX combo_idx (email, name))",
			remoteTable: "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL, name TEXT NOT NULL, INDEX combo_idx (email, name))",
			// Generates: DROP INDEX diff, ALTER COLUMN diffs, CREATE INDEX diff
			wantDDLContains: []string{"DROP INDEX", "combo_idx", "ALTER TABLE", "ALTER COLUMN", "CREATE INDEX"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) == 0 {
				t.Fatalf("expected at least 1 diff, got 0")
			}

			// Collect all DDL from all diffs
			allDDL := ""
			for _, diff := range diffs {
				for _, stmt := range diff.MigrationStatements {
					allDDL += stmt.String() + "\n"
				}
			}

			// Verify expected DDL is present across all diffs
			for _, expected := range tt.wantDDLContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("DDL does not contain %q.\nGot:\n%s", expected, allDDL)
				}
			}

			// Verify transaction boundaries are present when there are index drops
			if strings.Contains(allDDL, "DROP INDEX") {
				if !strings.Contains(allDDL, "COMMIT") || !strings.Contains(allDDL, "BEGIN") {
					t.Errorf("expected transaction boundaries (COMMIT/BEGIN) when dropping indexes.\nGot:\n%s", allDDL)
				}
			}
		})
	}
}

func TestRemoveIndexesOnDroppedColumns(t *testing.T) {
	tests := []struct {
		name               string
		localTable         string
		remoteTable        string
		wantDiffCount      int
		wantDDLContains    []string
		wantDDLNotContains []string
	}{
		{
			name:               "drop column with index suppresses index drop",
			localTable:         "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			remoteTable:        "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING, INDEX email_idx (email))",
			wantDiffCount:      1,
			wantDDLContains:    []string{"DROP COLUMN"},
			wantDDLNotContains: []string{"DROP INDEX"},
		},
		{
			name:               "drop column referenced in multi-column index suppresses index drop",
			localTable:         "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			remoteTable:        "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING, INDEX name_email_idx (name, email))",
			wantDiffCount:      1,
			wantDDLContains:    []string{"DROP COLUMN"},
			wantDDLNotContains: []string{"DROP INDEX"},
		},
		{
			name:               "drop column without index works normally",
			localTable:         "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			remoteTable:        "CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING)",
			wantDiffCount:      1,
			wantDDLContains:    []string{"DROP COLUMN"},
			wantDDLNotContains: []string{"DROP INDEX"},
		},
		{
			name:       "drop column referenced only in partial index WHERE clause suppresses index drop",
			localTable: "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
			remoteTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name STRING,
				is_active BOOL,
				INDEX idx_active_users (name) WHERE is_active = true
			)`,
			wantDiffCount:      1,
			wantDDLContains:    []string{"DROP COLUMN"},
			wantDDLNotContains: []string{"DROP INDEX"},
		},
		{
			name:       "drop column referenced in both index key and WHERE clause suppresses index drop",
			localTable: "CREATE TABLE users (id INT PRIMARY KEY)",
			remoteTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING,
				UNIQUE INDEX idx_email (email) WHERE email IS NOT NULL
			)`,
			wantDiffCount:      1,
			wantDDLContains:    []string{"DROP COLUMN"},
			wantDDLNotContains: []string{"DROP INDEX"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			localSchema := createSchemaWithTables([]string{tt.localTable})
			remoteSchema := createSchemaWithTables([]string{tt.remoteTable})

			diffs := compareTables(localSchema, remoteSchema)

			if len(diffs) != tt.wantDiffCount {
				t.Fatalf("expected %d diffs, got %d", tt.wantDiffCount, len(diffs))
				for i, diff := range diffs {
					t.Logf("  diff[%d]: %s - %s", i, diff.Type, diff.Description)
				}
			}

			allDDL := ""
			for _, diff := range diffs {
				for _, stmt := range diff.MigrationStatements {
					allDDL += stmt.String() + "\n"
				}
			}

			for _, expected := range tt.wantDDLContains {
				if !strings.Contains(allDDL, expected) {
					t.Errorf("DDL should contain %q.\nGot:\n%s", expected, allDDL)
				}
			}

			for _, notExpected := range tt.wantDDLNotContains {
				if strings.Contains(allDDL, notExpected) {
					t.Errorf("DDL should NOT contain %q.\nGot:\n%s", notExpected, allDDL)
				}
			}
		})
	}
}
