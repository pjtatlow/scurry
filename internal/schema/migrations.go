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
	stmts    []tree.Statement
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

	// Map to track warnings for each migration statement
	statementWarnings := make(map[*migrationStatement]string)

	// Dropping the schema has to come last, save them for the end
	dropSchemaStmts := make([]*migrationStatement, 0)
	for _, difference := range r.Differences {
		if difference.WarningMessage != "" {
			warnings = append(warnings, difference.WarningMessage)
		}

		if len(difference.MigrationStatements) == 0 {
			continue
		}

		// Create one migrationStatement per Difference, containing all its statements
		// This ensures all statements from a single diff stay together and execute in order
		stmt := &migrationStatement{
			stmts:    difference.MigrationStatements,
			requires: set.New[*migrationStatement](),
		}

		// Store warning for this statement if it exists
		if difference.WarningMessage != "" {
			statementWarnings[stmt] = difference.WarningMessage
		}

		// Check if this is a drop schema statement (they go last)
		isDropSchema := false
		for _, ddl := range difference.MigrationStatements {
			if _, ok := ddl.(*tree.DropSchema); ok {
				isDropSchema = true
				break
			}
		}

		if isDropSchema {
			dropSchemaStmts = append(dropSchemaStmts, stmt)
		} else {
			statements = append(statements, stmt)
		}

		if difference.OriginalDependencies != nil && difference.OriginalDependencies.Size() > 0 {
			originalDependencies[stmt] = difference.OriginalDependencies
		}

		// Track what each statement group drops for reverse dependency resolution
		for _, ddl := range difference.MigrationStatements {
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
		}
	}
	statements = append(statements, dropSchemaStmts...)

	// Collect all of the names provided by each statement group, so as we explore dependencies we can connect statements together.
	for _, migration := range statements {
		for _, ddl := range migration.stmts {
			for name := range GetProvidedNames(ddl, false).Values() {
				providers.add(name, migration)
			}
		}
	}

	// Add dependencies between statement groups by checking the requirements against the things that other groups provide.
	// If we don't have a provider for a requirement, we will assume it is already present or a builtin.
	for _, migration := range statements {
		for _, ddl := range migration.stmts {
			for name := range GetDependencyNames(ddl, false).Values() {
				if others, ok := providers[name]; ok {
					for other := range others.Values() {
						// Don't add self-references - a group can't depend on itself
						if other != migration {
							migration.requires.Add(other)
						}
					}
				}
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
		return strings.Compare(a.stmts[0].String(), b.stmts[0].String())
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
				return strings.Compare(a.stmts[0].String(), b.stmts[0].String())
			})
			start = end
			currentChunk = set.New[*migrationStatement]()
			currentChunk.Add(stmt)
		}
	}
	slices.SortFunc(orderedStatements[start:], func(a, b *migrationStatement) int {
		return strings.Compare(a.stmts[0].String(), b.stmts[0].String())
	})

	// Build a map to track which tree.Statement belongs to which migrationStatement
	// This lets us identify the first statement of each migration group
	stmtToMigration := make(map[tree.Statement]*migrationStatement)
	for _, migration := range orderedStatements {
		if len(migration.stmts) > 0 {
			stmtToMigration[migration.stmts[0]] = migration
		}
	}

	// Flatten all statements from each group, preserving their order
	allStatements := make([]tree.Statement, 0)
	for _, migration := range orderedStatements {
		allStatements = append(allStatements, migration.stmts...)
	}

	// If we begin a transaction change, skip it
	l := len(allStatements)
	if l > 2 && isCommitBegin(allStatements[:2]) {
		allStatements = allStatements[2:]
	}
	// If we end a transaction change, skip it
	l = len(allStatements)
	if l > 2 && isCommitBegin(allStatements[l-2:]) {
		allStatements = allStatements[:l-2]
	}

	ddl := make([]string, 0)
	for _, stmt := range allStatements {
		var s string
		var err error
		if pretty {
			s, err = tree.Pretty(stmt)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to pretty print migration statement: %w", err)
			}
		} else {
			s = stmt.String()
		}

		// If this is the first statement of a migration group with a warning, prepend the warning comment
		if migration, isFirst := stmtToMigration[stmt]; isFirst {
			if warning, hasWarning := statementWarnings[migration]; hasWarning {
				warningComment := formatWarningComment(warning)
				if warningComment != "" {
					s = warningComment + "\n" + s
				}
			}
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
			parts = append(parts, ui.SqlCode(m.stmts[0].String()))
		}
		parts = append(parts, ui.SqlCode(migration.stmts[0].String()))

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

func formatWarningComment(warning string) string {
	if warning == "" {
		return ""
	}
	lines := strings.Split(warning, "\n")
	var commentLines []string
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			commentLines = append(commentLines, "-- WARNING: "+line)
		}
	}
	return strings.Join(commentLines, "\n")
}

func isCommitBegin(stmts []tree.Statement) bool {
	if len(stmts) != 2 {
		return false
	}
	if _, ok := stmts[0].(*tree.CommitTransaction); !ok {
		return false
	}
	if _, ok := stmts[1].(*tree.BeginTransaction); !ok {
		return false
	}
	return true
}
