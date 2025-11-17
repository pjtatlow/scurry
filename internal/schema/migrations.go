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

	// Collect all of the statements in a set, making sure dependencies are put in first.
	// Then convert them into a big list of strings.
	orderedStatements := set.New[*migrationStatement]()
	for _, migration := range statements {
		if orderedStatements.Contains(migration) {
			continue
		}
		result, err := exploreDeps(migration, set.New[*migrationStatement]())
		if err != nil {
			return nil, err
		}
		orderedStatements = orderedStatements.Union(result)

	}

	ddl := make([]string, 0)
	for migration := range orderedStatements.Values() {
		if pretty {
			s, err := tree.Pretty(migration.stmt)
			if err == nil {
				ddl = append(ddl, s)
				continue
			}

		}
		ddl = append(ddl, migration.stmt.String())
	}
	return ddl, nil
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
