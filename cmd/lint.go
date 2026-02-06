package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

var lintCmd = &cobra.Command{
	Use:   "lint",
	Short: "Check schema for potential issues",
	Long: `Lint the local schema for potential issues.

Currently checks:
  - Foreign keys without covering indexes (can cause full table scans)
  - Unique indexes/constraints with nullable columns (NULL != NULL, so uniqueness is not enforced)`,
	RunE: lint,
}

func init() {
	rootCmd.AddCommand(lintCmd)

	flags.AddDefinitionDir(lintCmd)
}

func lint(cmd *cobra.Command, args []string) error {
	if flags.DefinitionDir == "" {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}

	err := doLint(cmd.Context())
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

// LintIssue represents a potential problem found in the schema
type LintIssue struct {
	Table       string
	Constraint  string
	Description string
	Suggestion  string
}

func doLint(ctx context.Context) error {
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", flags.DefinitionDir)))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shadow database client: %w", err)
	}
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectory(ctx, afero.NewOsFs(), flags.DefinitionDir, dbClient)
	if err != nil {
		return fmt.Errorf("failed to load local schema: %w", err)
	}

	var issues []LintIssue
	issues = append(issues, checkForeignKeyIndexes(localSchema)...)
	issues = append(issues, checkNullableUniqueColumns(localSchema)...)

	if len(issues) == 0 {
		fmt.Println(ui.Success("✓ No issues found!"))
		return nil
	}

	fmt.Println(ui.Warning(fmt.Sprintf("Found %d issue(s):\n", len(issues))))

	for _, issue := range issues {
		fmt.Println(ui.Error(fmt.Sprintf("  ✗ %s.%s", issue.Table, issue.Constraint)))
		fmt.Println(ui.Subtle(fmt.Sprintf("    %s", issue.Description)))
		fmt.Println(ui.Info(fmt.Sprintf("    Suggestion: %s", issue.Suggestion)))
		fmt.Println()
	}

	os.Exit(1)
	return nil
}

// checkForeignKeyIndexes checks that all foreign keys have a covering index
func checkForeignKeyIndexes(s *schema.Schema) []LintIssue {
	var issues []LintIssue

	for _, table := range s.Tables {
		tableName := table.ResolvedName()
		tableIssues := checkTableForeignKeyIndexes(tableName, table.Ast)
		issues = append(issues, tableIssues...)
	}

	return issues
}

func checkTableForeignKeyIndexes(tableName string, table *tree.CreateTable) []LintIssue {
	var issues []LintIssue

	// Collect all indexes (explicit indexes + primary key + unique constraints)
	indexedPrefixes := collectIndexPrefixes(table)

	// Check each foreign key
	for _, def := range table.Defs {
		fk, ok := def.(*tree.ForeignKeyConstraintTableDef)
		if !ok {
			continue
		}

		// Get the columns in this foreign key
		fkCols := make([]string, len(fk.FromCols))
		for i, col := range fk.FromCols {
			fkCols[i] = col.Normalize()
		}

		// Check if any index covers these columns as a prefix
		if !hasCoveringIndex(fkCols, indexedPrefixes) {
			constraintName := fk.Name.Normalize()
			if constraintName == "" {
				constraintName = fmt.Sprintf("fk_%s", fkCols[0])
			}

			issues = append(issues, LintIssue{
				Table:       tableName,
				Constraint:  constraintName,
				Description: fmt.Sprintf("Foreign key on (%s) has no covering index", formatColumnList(fkCols)),
				Suggestion:  fmt.Sprintf("Add INDEX (%s) to the table definition", formatColumnList(fkCols)),
			})
		}
	}

	return issues
}

// collectIndexPrefixes returns all column prefixes that are covered by indexes
// An index on (a, b, c) covers prefixes: [a], [a, b], [a, b, c]
func collectIndexPrefixes(table *tree.CreateTable) [][]string {
	var prefixes [][]string

	for _, def := range table.Defs {
		switch d := def.(type) {
		case *tree.IndexTableDef:
			// Regular index
			cols := getIndexKeyColumns(d.Columns)
			prefixes = append(prefixes, allPrefixes(cols)...)

		case *tree.UniqueConstraintTableDef:
			// Unique constraint (including primary key) creates an index
			cols := getIndexKeyColumns(d.Columns)
			prefixes = append(prefixes, allPrefixes(cols)...)
		}
	}

	return prefixes
}

// getIndexKeyColumns extracts the key column names from an index element list
// (excludes STORING columns which don't help with index lookups)
func getIndexKeyColumns(columns tree.IndexElemList) []string {
	cols := make([]string, 0, len(columns))
	for _, col := range columns {
		if col.Column != "" {
			cols = append(cols, col.Column.Normalize())
		}
	}
	return cols
}

// allPrefixes returns all prefixes of a column list
// e.g., [a, b, c] -> [[a], [a, b], [a, b, c]]
func allPrefixes(cols []string) [][]string {
	if len(cols) == 0 {
		return nil
	}
	prefixes := make([][]string, len(cols))
	for i := range cols {
		prefix := make([]string, i+1)
		copy(prefix, cols[:i+1])
		prefixes[i] = prefix
	}
	return prefixes
}

// hasCoveringIndex checks if the foreign key columns are covered by any index prefix
func hasCoveringIndex(fkCols []string, indexPrefixes [][]string) bool {
	for _, prefix := range indexPrefixes {
		if columnsMatch(fkCols, prefix) {
			return true
		}
	}
	return false
}

// columnsMatch checks if two column lists are equal
func columnsMatch(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func formatColumnList(cols []string) string {
	if len(cols) == 1 {
		return cols[0]
	}
	result := cols[0]
	for _, col := range cols[1:] {
		result += ", " + col
	}
	return result
}

// checkNullableUniqueColumns checks that unique indexes/constraints don't contain nullable columns.
// In SQL, NULL != NULL, so a unique constraint on a nullable column doesn't actually enforce uniqueness
// for NULL values — multiple rows can have NULL in the same unique column.
func checkNullableUniqueColumns(s *schema.Schema) []LintIssue {
	var issues []LintIssue

	for _, table := range s.Tables {
		tableName := table.ResolvedName()
		tableIssues := checkTableNullableUniqueColumns(tableName, table.Ast)
		issues = append(issues, tableIssues...)
	}

	return issues
}

func checkTableNullableUniqueColumns(tableName string, table *tree.CreateTable) []LintIssue {
	var issues []LintIssue

	// Build a map of column name -> column definition for nullability lookups
	columns := make(map[string]*tree.ColumnTableDef)
	for _, def := range table.Defs {
		col, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		columns[col.Name.Normalize()] = col
	}

	for _, def := range table.Defs {
		switch d := def.(type) {
		case *tree.UniqueConstraintTableDef:
			// Skip primary keys — PK columns are implicitly NOT NULL
			if d.PrimaryKey {
				continue
			}
			notNullGuarded := collectIsNotNullColumns(d.Predicate)
			checkUniqueColumnsNullability(tableName, d.Name.Normalize(), getIndexKeyColumns(d.Columns), columns, notNullGuarded, &issues)

		}
	}

	return issues
}

// collectIsNotNullColumns extracts column names guarded by IS NOT NULL in a predicate expression.
// It handles single IS NOT NULL expressions and AND-combined expressions.
func collectIsNotNullColumns(predicate tree.Expr) map[string]bool {
	cols := make(map[string]bool)
	collectIsNotNullColumnsRecursive(predicate, cols)
	return cols
}

func collectIsNotNullColumnsRecursive(expr tree.Expr, cols map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *tree.IsNotNullExpr:
		if name, ok := e.Expr.(*tree.UnresolvedName); ok {
			cols[name.String()] = true
		}
	case *tree.AndExpr:
		collectIsNotNullColumnsRecursive(e.Left, cols)
		collectIsNotNullColumnsRecursive(e.Right, cols)
	}
}

func checkUniqueColumnsNullability(tableName, constraintName string, cols []string, columns map[string]*tree.ColumnTableDef, notNullGuarded map[string]bool, issues *[]LintIssue) {
	for _, colName := range cols {
		col, ok := columns[colName]
		if !ok {
			continue
		}
		if col.Nullable.Nullability == tree.NotNull {
			continue
		}
		// Skip columns that are guarded by a WHERE col IS NOT NULL predicate
		if notNullGuarded[colName] {
			continue
		}
		// Column is nullable (either explicitly NULL or default/silent null)
		if constraintName == "" {
			constraintName = fmt.Sprintf("unique_%s", formatColumnList(cols))
		}
		*issues = append(*issues, LintIssue{
			Table:       tableName,
			Constraint:  constraintName,
			Description: fmt.Sprintf("Unique constraint on (%s) includes nullable column %q (NULL values are not considered equal, so uniqueness is not enforced for NULLs)", formatColumnList(cols), colName),
			Suggestion:  fmt.Sprintf("Make column %q NOT NULL, or add a partial unique index with a WHERE %s IS NOT NULL clause", colName, colName),
		})
	}
}
