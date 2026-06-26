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
	// Refuse to generate a migration if any differences are flagged as
	// unrepresentable in DDL (e.g. column-family changes on existing columns).
	// Reporting all of them at once avoids a fix-one-find-another loop.
	var blockingErrors []string
	for _, d := range r.Differences {
		if d.BlockingError != "" {
			blockingErrors = append(blockingErrors, d.BlockingError)
		}
	}
	if len(blockingErrors) > 0 {
		return nil, nil, fmt.Errorf("schema change cannot be applied:\n  - %s", strings.Join(blockingErrors, "\n  - "))
	}

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
			case *tree.AlterTable:
				// ALTER TABLE ... DROP COLUMN registers the qualified column name so
				// that a separate DROP INDEX diff with OriginalDependencies pointing
				// at the column forces itself to run before this drop.
				schemaName, tableName := getObjectName(d.Table)
				for _, cmd := range d.Cmds {
					if dropCol, ok := cmd.(*tree.AlterTableDropColumn); ok {
						dropStatements.add(schemaName+"."+tableName+"."+dropCol.Column.Normalize(), stmt)
					}
				}
			}
		}
	}
	statements = append(statements, dropSchemaStmts...)

	// Collect all of the names provided by each statement group, so as we explore dependencies we can connect statements together.
	for _, migration := range statements {
		for _, ddl := range migration.stmts {
			for name := range GetProvidedNames(ddl, true).Values() {
				providers.add(name, migration)
			}
		}
	}

	// Add dependencies between statement groups by checking the requirements against the things that other groups provide.
	// If we don't have a provider for a requirement, we will assume it is already present or a builtin.
	for _, migration := range statements {
		for _, ddl := range migration.stmts {
			for name := range GetDependencyNames(ddl, true).Values() {
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

	// Each Difference independently prepends/appends COMMIT/BEGIN transaction
	// boundaries to its statements. Once flattened, consecutive Differences can
	// produce redundant runs of boundaries (e.g. "COMMIT; BEGIN; COMMIT; BEGIN;")
	// as well as leading/trailing boundaries. Coalesce them down to the minimal
	// set that produces the same transaction structure at execution time.
	allStatements = coalesceTransactionBoundaries(allStatements)

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

func isCommit(stmt tree.Statement) bool {
	_, ok := stmt.(*tree.CommitTransaction)
	return ok
}

func isBegin(stmt tree.Statement) bool {
	_, ok := stmt.(*tree.BeginTransaction)
	return ok
}

// txnChunk is a contiguous run of real (non transaction-control) statements
// that execute together, optionally outside of a transaction.
type txnChunk struct {
	stmts  []tree.Statement
	nonTxn bool
}

// coalesceTransactionBoundaries removes redundant COMMIT/BEGIN transaction
// control statements while preserving the exact transaction structure that the
// executor (db.chunkStatementsByTransaction) derives from them.
//
// Statements are first partitioned into logical chunks using the same rules as
// the executor — a COMMIT immediately followed by BEGIN is a transaction
// boundary, a lone COMMIT enters a non-transactional section, and a lone BEGIN
// exits it. Empty chunks (the artifacts of redundant boundaries) are dropped.
// The chunks are then re-emitted with the minimal canonical set of control
// statements needed to reproduce the same partition, so the generated SQL no
// longer contains runs like "COMMIT; BEGIN; COMMIT; BEGIN;".
func coalesceTransactionBoundaries(stmts []tree.Statement) []tree.Statement {
	var chunks []txnChunk
	var cur []tree.Statement
	inNonTxn := false

	flush := func(nonTxn bool) {
		if len(cur) > 0 {
			chunks = append(chunks, txnChunk{stmts: cur, nonTxn: nonTxn})
			cur = nil
		}
	}

	for i := 0; i < len(stmts); i++ {
		stmt := stmts[i]
		switch {
		case isCommit(stmt):
			if i+1 < len(stmts) && isBegin(stmts[i+1]) {
				// COMMIT/BEGIN pair: transaction boundary.
				flush(false)
				i++ // consume the BEGIN
				inNonTxn = false
			} else {
				// Lone COMMIT: enter non-transactional section.
				flush(false)
				inNonTxn = true
			}
		case isBegin(stmt):
			// Lone BEGIN: exit non-transactional section.
			flush(inNonTxn)
			inNonTxn = false
		default:
			cur = append(cur, stmt)
		}
	}
	flush(inNonTxn)

	out := make([]tree.Statement, 0, len(stmts))
	for j, chunk := range chunks {
		if j == 0 {
			if chunk.nonTxn {
				out = append(out, &tree.CommitTransaction{})
			}
		} else {
			prev := chunks[j-1]
			switch {
			case !prev.nonTxn && !chunk.nonTxn:
				// txn -> txn: a single transaction boundary.
				out = append(out, &tree.CommitTransaction{}, &tree.BeginTransaction{})
			case !prev.nonTxn && chunk.nonTxn:
				// txn -> non-txn: enter non-transactional section.
				out = append(out, &tree.CommitTransaction{})
			case prev.nonTxn && !chunk.nonTxn:
				// non-txn -> txn: exit non-transactional section.
				out = append(out, &tree.BeginTransaction{})
			default:
				// non-txn -> non-txn: exit then re-enter.
				out = append(out, &tree.BeginTransaction{}, &tree.CommitTransaction{})
			}
		}
		out = append(out, chunk.stmts...)
	}
	return out
}
