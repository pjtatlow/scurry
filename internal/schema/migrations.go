package schema

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/set"
	"github.com/pjtatlow/scurry/internal/ui"
)

type migrationStatement struct {
	stmt     tree.Statement
	requires set.Set[*migrationStatement]
}

type dependencyMap map[string]set.Set[*migrationStatement]

func (dm dependencyMap) add(name string, stmt *migrationStatement) {
	if _, ok := dm[name]; !ok {
		dm[name] = set.New(stmt)
	} else {
		dm[name].Add(stmt)
	}
}

func (r *ComparisonResult) GenerateMigrations(pretty bool) ([]string, error) {
	statements := make([]*migrationStatement, 0)
	// Multiple statements can provide the same name, like functions with overloads
	providers := make(dependencyMap)

	// Dropping the schema has to come last, save them for the end
	dropSchemaStmts := make([]*migrationStatement, 0)
	for _, difference := range r.Differences {
		var prev *migrationStatement
		for _, ddl := range difference.MigrationStatements {

			stmt := &migrationStatement{
				stmt:     ddl,
				requires: set.New[*migrationStatement](),
			}
			if prev != nil {
				// Statements in the same difference are dependant on any previous statements
				// This lets us make a difference that drops / creates, and they will be executed in that order.
				stmt.requires.Add(prev)
			}
			if _, ok := ddl.(*tree.DropSchema); ok {
				dropSchemaStmts = append(dropSchemaStmts, stmt)
			} else {
				statements = append(statements, stmt)
			}
			prev = stmt
		}
	}
	statements = append(statements, dropSchemaStmts...)

	// Collect all of the names provided by each statement, so as we explore dependencies we can connect statements together.
	for _, migration := range statements {
		for name := range getProvidedNames(migration.stmt).Values() {
			providers.add(name, migration)
		}
	}

	// Add dependencies between statements by checking the requirements against the things that other statements provide.
	// If we don't have a provider for a requirement, we will assume it is already present or a builtin.
	for _, migration := range statements {
		for name := range getDependencyNames(migration.stmt).Values() {
			if others, ok := providers[name]; ok {
				migration.requires = migration.requires.Union(others)
			}
		}
	}

	// Perform topological sort with lexicographic tiebreaking
	ddl, err := topologicalSort(statements, pretty)
	if err != nil {
		return nil, err
	}
	return ddl, nil
}

func topologicalSort(statements []*migrationStatement, pretty bool) ([]string, error) {
	// Build a map of migrations to their string representations for sorting
	stmtStrings := make(map[*migrationStatement]string, len(statements))
	for _, migration := range statements {
		var str string
		if pretty {
			s, err := tree.Pretty(migration.stmt)
			if err == nil {
				str = s
			} else {
				str = migration.stmt.String()
			}
		} else {
			str = migration.stmt.String()
		}
		stmtStrings[migration] = str
	}

	// Build reverse adjacency list (who depends on me?) and calculate in-degree
	inDegree := make(map[*migrationStatement]int, len(statements))
	dependents := make(map[*migrationStatement][]*migrationStatement)

	for _, migration := range statements {
		inDegree[migration] = 0
	}

	for _, migration := range statements {
		for dep := range migration.requires.Values() {
			inDegree[migration]++
			dependents[dep] = append(dependents[dep], migration)
		}
	}

	// Start with all statements that have no dependencies
	ready := make([]*migrationStatement, 0)
	for _, migration := range statements {
		if inDegree[migration] == 0 {
			ready = append(ready, migration)
		}
	}

	// Sort ready statements lexicographically
	slices.SortFunc(ready, func(a, b *migrationStatement) int {
		return strings.Compare(stmtStrings[a], stmtStrings[b])
	})

	result := make([]string, 0, len(statements))

	for len(ready) > 0 {
		// Pop the first (lexicographically smallest) statement
		current := ready[0]
		ready = ready[1:]

		// Add to result
		result = append(result, stmtStrings[current])

		// Process dependents of the current statement
		newReady := make([]*migrationStatement, 0)
		for _, dependent := range dependents[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				newReady = append(newReady, dependent)
			}
		}

		// Sort newly ready statements lexicographically and add to ready list
		slices.SortFunc(newReady, func(a, b *migrationStatement) int {
			return strings.Compare(stmtStrings[a], stmtStrings[b])
		})
		ready = append(ready, newReady...)
	}

	// Check if all statements were processed (detect cycles)
	if len(result) != len(statements) {
		// Find a cycle for error reporting
		for _, migration := range statements {
			if inDegree[migration] > 0 {
				_, err := exploreDeps(migration, set.New[*migrationStatement]())
				if err != nil {
					return nil, err
				}
			}
		}
		return nil, fmt.Errorf("unable to process all statements due to dependency issues")
	}

	return result, nil
}

func exploreDeps(migration *migrationStatement, pending set.Set[*migrationStatement]) (set.Set[*migrationStatement], error) {
	result := set.New[*migrationStatement]()
	if pending.Contains(migration) {
		pending := slices.Collect(pending.Values())

		parts := make([]string, 0, len(pending)+1)
		for _, m := range pending {
			parts = append(parts, ui.SqlCode(m.stmt.String()))
		}
		parts = append(parts, ui.SqlCode(migration.stmt.String()))

		return nil, fmt.Errorf("circular dependency detected\n\n%s", strings.Join(parts, fmt.Sprintf("\n\n%s\n\n", ui.Warning("REQUIRES"))))
	}

	pending.Add(migration)
	for dependency := range migration.requires.Values() {
		other, err := exploreDeps(dependency, pending)
		if err != nil {
			return nil, err
		}
		result = result.Union(other)
	}
	pending.Remove(migration)
	result.Add(migration)

	return result, nil
}
