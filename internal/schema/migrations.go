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

	// Compute topological levels and sort within each level for deterministic output.
	// Level 0: statements with no dependencies on other statements being created
	// Level N: statements whose maximum dependency level is N-1
	// Within each level, statements are sorted alphabetically by their SQL representation.
	levels, err := computeLevels(statements)
	if err != nil {
		return nil, nil, err
	}

	// Flatten levels into ordered list
	orderedStatements := make([]*migrationStatement, 0, len(statements))
	for _, level := range levels {
		orderedStatements = append(orderedStatements, level...)
	}

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

// computeLevels performs a topological sort and groups statements into levels.
// Level 0 contains statements with no dependencies, Level N contains statements
// whose maximum dependency level is N-1. Within each level, statements are sorted
// alphabetically by their SQL representation for deterministic output.
func computeLevels(statements []*migrationStatement) ([][]*migrationStatement, error) {
	levels := make(map[*migrationStatement]int)

	// Recursive function to compute level with cycle detection
	var computeLevel func(stmt *migrationStatement, pending set.Set[*migrationStatement]) (int, error)
	computeLevel = func(stmt *migrationStatement, pending set.Set[*migrationStatement]) (int, error) {
		// Check for cached result
		if level, ok := levels[stmt]; ok {
			return level, nil
		}

		// Check for circular dependency
		if pending.Contains(stmt) {
			pendingSlice := slices.Collect(pending.Values())
			parts := make([]string, 0, len(pendingSlice)+1)
			for _, m := range pendingSlice {
				parts = append(parts, ui.SqlCode(m.stmt.String()))
			}
			parts = append(parts, ui.SqlCode(stmt.stmt.String()))
			return -1, fmt.Errorf("circular dependency detected\n\n%s", strings.Join(parts, fmt.Sprintf("\n\n%s\n\n", ui.Warning("REQUIRES"))))
		}

		// No dependencies = level 0
		if stmt.requires.Size() == 0 {
			levels[stmt] = 0
			return 0, nil
		}

		// Compute max dependency level
		pending.Add(stmt)
		maxDepLevel := -1
		for dep := range stmt.requires.Values() {
			depLevel, err := computeLevel(dep, pending)
			if err != nil {
				return -1, err
			}
			if depLevel > maxDepLevel {
				maxDepLevel = depLevel
			}
		}
		pending.Remove(stmt)

		level := maxDepLevel + 1
		levels[stmt] = level
		return level, nil
	}

	// Compute level for all statements
	maxLevel := 0
	for _, stmt := range statements {
		level, err := computeLevel(stmt, set.New[*migrationStatement]())
		if err != nil {
			return nil, err
		}
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Handle empty statements case
	if len(statements) == 0 {
		return [][]*migrationStatement{}, nil
	}

	// Group statements by level
	result := make([][]*migrationStatement, maxLevel+1)
	for i := range result {
		result[i] = make([]*migrationStatement, 0)
	}
	for stmt, level := range levels {
		result[level] = append(result[level], stmt)
	}

	// Sort each level alphabetically by SQL string for determinism
	for i := range result {
		slices.SortFunc(result[i], func(a, b *migrationStatement) int {
			return strings.Compare(a.stmt.String(), b.stmt.String())
		})
	}

	return result, nil
}
