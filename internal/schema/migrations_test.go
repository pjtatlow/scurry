package schema

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/set"
)

func TestGenerateMigrations_LexographicOrdering(t *testing.T) {
	tests := []struct {
		name            string
		differences     []Difference
		expectedOrdered []string
	}{
		{
			name: "independent statements ordered lexographically",
			differences: []Difference{
				{
					Type:        DiffTypeTableAdded,
					Description: "table 'users' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TABLE users (id INT PRIMARY KEY)"),
					},
				},
				{
					Type:        DiffTypeTableAdded,
					Description: "table 'posts' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TABLE posts (id INT PRIMARY KEY)"),
					},
				},
				{
					Type:        DiffTypeTableAdded,
					Description: "table 'comments' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TABLE comments (id INT PRIMARY KEY)"),
					},
				},
			},
			expectedOrdered: []string{
				"CREATE TABLE comments (id INT8 PRIMARY KEY)",
				"CREATE TABLE posts (id INT8 PRIMARY KEY)",
				"CREATE TABLE users (id INT8 PRIMARY KEY)",
			},
		},
		{
			name: "dependent statements maintain dependency order",
			differences: []Difference{
				{
					Type:        DiffTypeTypeAdded,
					Description: "type 'status' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TYPE status AS ENUM ('active', 'inactive')"),
					},
				},
				{
					Type:        DiffTypeTableAdded,
					Description: "table 'users' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TABLE users (id INT PRIMARY KEY, status status NOT NULL)"),
					},
				},
			},
			expectedOrdered: []string{
				"CREATE TYPE status AS ENUM ('active', 'inactive')",
				"CREATE TABLE users (id INT8 PRIMARY KEY, status status NOT NULL)",
			},
		},
		{
			name: "mixed dependent and independent statements",
			differences: []Difference{
				{
					Type:        DiffTypeTableAdded,
					Description: "table 'posts' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TABLE posts (id INT PRIMARY KEY)"),
					},
				},
				{
					Type:        DiffTypeTypeAdded,
					Description: "type 'z_status' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TYPE z_status AS ENUM ('active', 'inactive')"),
					},
				},
				{
					Type:        DiffTypeTableAdded,
					Description: "table 'users' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TABLE users (id INT PRIMARY KEY, status z_status NOT NULL)"),
					},
				},
				{
					Type:        DiffTypeTableAdded,
					Description: "table 'comments' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TABLE comments (id INT PRIMARY KEY)"),
					},
				},
			},
			expectedOrdered: []string{
				// Independent statements sorted lexographically
				"CREATE TABLE comments (id INT8 PRIMARY KEY)",
				"CREATE TABLE posts (id INT8 PRIMARY KEY)",
				// Type must come before users table (dependency)
				"CREATE TYPE z_status AS ENUM ('active', 'inactive')",
				"CREATE TABLE users (id INT8 PRIMARY KEY, status z_status NOT NULL)",
			},
		},
		{
			name: "multiple types ordered lexographically",
			differences: []Difference{
				{
					Type:        DiffTypeTypeAdded,
					Description: "type 'zebra' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TYPE zebra AS ENUM ('stripe')"),
					},
				},
				{
					Type:        DiffTypeTypeAdded,
					Description: "type 'apple' added",
					MigrationStatements: []tree.Statement{
						mustParse("CREATE TYPE apple AS ENUM ('red', 'green')"),
					},
				},
			},
			expectedOrdered: []string{
				"CREATE TYPE apple AS ENUM ('red', 'green')",
				"CREATE TYPE zebra AS ENUM ('stripe')",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &ComparisonResult{
				Differences: tt.differences,
			}

			statements, err := result.GenerateMigrations(false)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedOrdered, statements, "statements should be ordered correctly")
		})
	}
}

func TestTopologicalSort_CircularDependency(t *testing.T) {
	// This is a more advanced test to ensure circular dependency detection still works
	// Create two statements that depend on each other (impossible, but let's test error handling)
	stmt1 := &migrationStatement{
		stmt:     mustParse("CREATE TABLE a (id INT PRIMARY KEY)"),
		requires: set.New[*migrationStatement](),
	}
	stmt2 := &migrationStatement{
		stmt:     mustParse("CREATE TABLE b (id INT PRIMARY KEY)"),
		requires: set.New[*migrationStatement](),
	}

	// Create circular dependency
	stmt1.requires.Add(stmt2)
	stmt2.requires.Add(stmt1)

	statements := []*migrationStatement{stmt1, stmt2}

	_, err := topologicalSort(statements, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular dependency")
}

// Helper function to parse SQL
func mustParse(sql string) tree.Statement {
	stmts, err := ParseSQL(sql)
	if err != nil {
		panic(err)
	}
	if len(stmts) != 1 {
		panic("expected exactly one statement")
	}
	return stmts[0]
}
