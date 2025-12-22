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

func (r *ComparisonResult) GenerateMigrations(pretty bool) ([]string, []string, error) {
	statements := make([]*migrationStatement, 0)
	warnings := make([]string, 0)
	// Multiple statements can provide the same name, like functions with overloads
	providers := make(dependencyMap)

	// For reverse dependency resolution when dropping objects
	dropStatements := make(dependencyMap)
	originalDependencies := make(map[*migrationStatement]set.Set[string])

	// Dropping the schema has to come last, save them for the end
	dropSchemaStmts := make([]*migrationStatement, 0)
	for _, difference := range r.Differences {
		if difference.WarningMessage != "" {
			warnings = append(warnings, difference.WarningMessage)
		}

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

			if difference.OriginalDependencies != nil && difference.OriginalDependencies.Size() > 0 {
				originalDependencies[stmt] = difference.OriginalDependencies
			}
			switch d := ddl.(type) {
			case *tree.DropType:
				for _, name := range d.Names {
					schemaName, objectName := getObjectName(name)
					dropStatements.add(schemaName+"."+objectName, stmt)
				}
			case *tree.DropTable:
				for _, name := range d.Names {
					schemaName, tableName := getTableName(name)
					dropStatements.add(schemaName+"."+tableName, stmt)
				}
			case *tree.DropView:
				for _, name := range d.Names {
					schemaName, viewName := getTableName(name)
					dropStatements.add(schemaName+"."+viewName, stmt)
				}
			case *tree.DropSequence:
				for _, name := range d.Names {
					schemaName, seqName := getTableName(name)
					dropStatements.add(schemaName+"."+seqName, stmt)
				}
			case *tree.DropRoutine:
				for _, routine := range d.Routines {
					schemaName, routineName := getRoutineName(routine.FuncName)
					dropStatements.add(schemaName+"."+routineName, stmt)
				}
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

	// Add reverse dependencies for DROP statements so we drop dependent objects first
	for dropStmt, origDeps := range originalDependencies {
		for depName := range origDeps.Values() {
			if stmts, ok := dropStatements[depName]; ok {
				for stmt := range stmts.Values() {
					stmt.requires.Add(dropStmt)
				}
			}
		}
	}

	slices.SortFunc(statements, func(a, b *migrationStatement) int {
		return strings.Compare(a.stmt.String(), b.stmt.String())
	})

	// Collect all of the statements in a set, making sure dependencies are put in first.
	// Then convert them into a big list of strings.
	statementSet := set.New[*migrationStatement]()
	for _, migration := range statements {
		if statementSet.Contains(migration) {
			continue
		}
		result, err := exploreDeps(migration, set.New[*migrationStatement]())
		if err != nil {
			return nil, nil, err
		}
		statementSet = statementSet.Union(result)
	}
	orderedStatements := slices.Collect(statementSet.Values())

	start := 0
	currentChunk := set.New[*migrationStatement]()
	for end := range orderedStatements {
		stmt := orderedStatements[end]
		// check if any of this statement's dependencies are in the current chunk
		chunkHasDep := false
		for dep := range stmt.requires.Values() {
			if currentChunk.Contains(dep) {
				chunkHasDep = true
				break
			}
		}
		// If this statement has no dependencies in the current chunk, add it to the current chunk
		if !chunkHasDep {
			currentChunk.Add(stmt)
		} else {
			// otherwise we need to end that chunk and make a new one
			slices.SortFunc(orderedStatements[start:end], func(a, b *migrationStatement) int {
				return strings.Compare(a.stmt.String(), b.stmt.String())
			})
			start = end
			currentChunk = set.New[*migrationStatement]()
			currentChunk.Add(stmt)
		}
	}
	slices.SortFunc(orderedStatements[start:], func(a, b *migrationStatement) int {
		return strings.Compare(a.stmt.String(), b.stmt.String())
	})

	ddl := make([]string, 0)
	for _, migration := range orderedStatements {
		var s string
		var err error
		if pretty {
			s, err = tree.Pretty(migration.stmt)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to pretty print migration statement: %w", err)
			}
		} else {
			s = migration.stmt.String()
		}
		ddl = append(ddl, s)
	}
	return ddl, warnings, nil
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
