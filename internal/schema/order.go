package schema

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/set"
)

// TableInsertionOrder describes FK-safe table insertion order and
// any self-referencing FK columns per table.
type TableInsertionOrder struct {
	Order          []string            // qualified table names in FK-safe insertion order
	SelfRefColumns map[string][]string // table -> self-referencing FK column names
}

// ComputeTableInsertionOrder computes a topological ordering of tables
// based on foreign key dependencies, suitable for inserting data.
// It reuses the same dependency infrastructure as GenerateMigrations.
func ComputeTableInsertionOrder(tables []ObjectSchema[*tree.CreateTable]) (*TableInsertionOrder, error) {
	statements := make([]*migrationStatement, 0, len(tables))
	tableNames := make(map[*migrationStatement]string) // migrationStatement -> qualified table name
	providers := make(dependencyMap)

	// Create one migrationStatement per table
	for _, t := range tables {
		stmt := &migrationStatement{
			stmts:    []tree.Statement{t.Ast},
			requires: set.New[*migrationStatement](),
		}
		statements = append(statements, stmt)
		tableNames[stmt] = t.ResolvedName()

		// Register provided names
		for name := range GetProvidedNames(t.Ast, false).Values() {
			providers.add(name, stmt)
		}
	}

	// Resolve dependencies (skip self-references — those are handled separately)
	for _, migration := range statements {
		for name := range GetDependencyNames(migration.stmts[0], false).Values() {
			if others, ok := providers[name]; ok {
				for other := range others.Values() {
					if other != migration {
						migration.requires.Add(other)
					}
				}
			}
		}
	}

	// Sort alphabetically for deterministic output
	slices.SortFunc(statements, func(a, b *migrationStatement) int {
		return strings.Compare(tableNames[a], tableNames[b])
	})

	// Topological sort via exploreDeps
	statementSet := set.New[*migrationStatement]()
	for _, migration := range statements {
		if statementSet.Contains(migration) {
			continue
		}
		result, err := exploreDeps(migration, set.New[*migrationStatement]())
		if err != nil {
			return nil, fmt.Errorf("failed to compute table insertion order: %w", err)
		}
		statementSet = statementSet.Union(result)
	}
	ordered := slices.Collect(statementSet.Values())

	// Extract table names in order
	order := make([]string, 0, len(ordered))
	for _, stmt := range ordered {
		order = append(order, tableNames[stmt])
	}

	// Detect self-referential FKs
	selfRefColumns := make(map[string][]string)
	for _, t := range tables {
		qualifiedName := t.ResolvedName()
		_, tableName := getTableName(t.Ast.Table)
		for _, def := range t.Ast.Defs {
			fk, ok := def.(*tree.ForeignKeyConstraintTableDef)
			if !ok {
				continue
			}
			_, refTable := getTableName(fk.Table)
			if refTable == tableName {
				cols := make([]string, len(fk.FromCols))
				for i, col := range fk.FromCols {
					cols[i] = col.Normalize()
				}
				selfRefColumns[qualifiedName] = append(selfRefColumns[qualifiedName], cols...)
			}
		}
	}

	return &TableInsertionOrder{
		Order:          order,
		SelfRefColumns: selfRefColumns,
	}, nil
}
