package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
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
  - Unique indexes/constraints with nullable columns (NULL != NULL, so uniqueness is not enforced)
  - TTL expiration expressions without a covering index (TTL deletion job cannot efficiently find expired rows)

Suppress specific checks with SQL comments in definition files:
  -- scurry:lint-disable=nullable-unique
  -- scurry:lint-disable=nullable-unique:users
  -- scurry:lint-disable=nullable-unique:users.phone_key`,
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
	Rule        string
	Table       string
	Constraint  string
	Description string
	Suggestion  string
}

// lintDisable represents a parsed -- scurry:lint-disable directive
type lintDisable struct {
	Rule       string // e.g. "nullable-unique"
	Table      string // e.g. "users" (empty = all tables in file)
	Constraint string // e.g. "phone_key" (empty = all constraints on table)
}

func doLint(ctx context.Context) error {
	fs := afero.NewOsFs()

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", flags.DefinitionDir)))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shadow database client: %w", err)
	}
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectory(ctx, fs, flags.DefinitionDir, dbClient)
	if err != nil {
		return fmt.Errorf("failed to load local schema: %w", err)
	}

	disables, err := loadLintDisables(fs, flags.DefinitionDir)
	if err != nil {
		return fmt.Errorf("failed to load lint directives: %w", err)
	}

	var issues []LintIssue
	issues = append(issues, checkForeignKeyIndexes(localSchema)...)
	issues = append(issues, checkNullableUniqueColumns(localSchema)...)
	issues = append(issues, checkTTLIndexes(localSchema)...)

	// Filter out suppressed issues
	var filtered []LintIssue
	for _, issue := range issues {
		if isSuppressed(issue, disables) {
			if flags.Verbose {
				fmt.Println(ui.Subtle(fmt.Sprintf("  suppressed %s.%s (%s) by lint-disable directive", issue.Table, issue.Constraint, issue.Rule)))
			}
			continue
		}
		filtered = append(filtered, issue)
	}

	if len(filtered) == 0 {
		fmt.Println(ui.Success("✓ No issues found!"))
		return nil
	}

	fmt.Println(ui.Warning(fmt.Sprintf("Found %d issue(s):\n", len(filtered))))
	for _, issue := range filtered {
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
				Rule:        "fk-missing-index",
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
			Rule:        "nullable-unique",
			Table:       tableName,
			Constraint:  constraintName,
			Description: fmt.Sprintf("Unique constraint on (%s) includes nullable column %q (NULL values are not considered equal, so uniqueness is not enforced for NULLs)", formatColumnList(cols), colName),
			Suggestion:  fmt.Sprintf("Make column %q NOT NULL, or add a partial unique index with a WHERE %s IS NOT NULL clause", colName, colName),
		})
	}
}

// checkTTLIndexes checks that tables with ttl_expiration_expression have an index
// on the column(s) referenced in the expression. Without such an index, the TTL
// deletion job must perform full table scans to find expired rows.
func checkTTLIndexes(s *schema.Schema) []LintIssue {
	var issues []LintIssue

	for _, table := range s.Tables {
		tableName := table.ResolvedName()
		tableIssues := checkTableTTLIndexes(tableName, table.Ast)
		issues = append(issues, tableIssues...)
	}

	return issues
}

func checkTableTTLIndexes(tableName string, table *tree.CreateTable) []LintIssue {
	// Find ttl_expiration_expression in storage params
	var ttlExpr string
	for _, param := range table.StorageParams {
		if param.Key == "ttl_expiration_expression" {
			ttlExpr = getStorageParamStringValue(param.Value)
			break
		}
	}

	if ttlExpr == "" {
		return nil
	}

	// Parse the expression to extract column references
	cols := extractColumnsFromExpression(ttlExpr)
	if len(cols) == 0 {
		return nil
	}

	// Collect first columns of all non-partial indexes
	indexFirstCols := collectIndexFirstColumns(table)

	// Check if any column from the TTL expression is the first column of an index
	for _, col := range cols {
		if indexFirstCols[col] {
			return nil
		}
	}

	return []LintIssue{{
		Rule:        "ttl-missing-index",
		Table:       tableName,
		Constraint:  "ttl_expiration_expression",
		Description: fmt.Sprintf("TTL expression references column(s) (%s) but no index starts with any of these columns — the TTL deletion job will not be able to use an index to find expired rows", formatColumnList(cols)),
		Suggestion:  fmt.Sprintf("Add INDEX (%s) to the table definition", cols[0]),
	}}
}

// getStorageParamStringValue extracts the raw string value from a storage param expression.
func getStorageParamStringValue(value tree.Expr) string {
	switch v := value.(type) {
	case *tree.StrVal:
		return v.RawString()
	case *tree.DString:
		return string(*v)
	default:
		// Fallback: use AsString and strip surrounding quotes
		s := tree.AsString(value)
		if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
			s = s[1 : len(s)-1]
			s = strings.ReplaceAll(s, "''", "'")
		}
		return s
	}
}

// extractColumnsFromExpression parses a SQL expression string and returns the
// column names referenced in it.
func extractColumnsFromExpression(expr string) []string {
	// Wrap in SELECT to make it a valid statement for parsing
	stmts, err := parser.Parse(fmt.Sprintf("SELECT %s", expr))
	if err != nil {
		return nil
	}
	if len(stmts) == 0 {
		return nil
	}

	selectStmt, ok := stmts[0].AST.(*tree.Select)
	if !ok {
		return nil
	}

	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(selectClause.Exprs) == 0 {
		return nil
	}

	var cols []string
	seen := make(map[string]bool)
	collectColumnRefs(selectClause.Exprs[0].Expr, &cols, seen)
	return cols
}

// collectColumnRefs recursively walks an expression tree and collects column references.
func collectColumnRefs(expr tree.Expr, cols *[]string, seen map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *tree.UnresolvedName:
		name := strings.ToLower(e.Parts[0])
		if !seen[name] {
			seen[name] = true
			*cols = append(*cols, name)
		}
	case *tree.BinaryExpr:
		collectColumnRefs(e.Left, cols, seen)
		collectColumnRefs(e.Right, cols, seen)
	case *tree.ParenExpr:
		collectColumnRefs(e.Expr, cols, seen)
	case *tree.FuncExpr:
		for _, arg := range e.Exprs {
			collectColumnRefs(arg, cols, seen)
		}
	case *tree.CastExpr:
		collectColumnRefs(e.Expr, cols, seen)
	case *tree.CoalesceExpr:
		for _, arg := range e.Exprs {
			collectColumnRefs(arg, cols, seen)
		}
	case *tree.CaseExpr:
		collectColumnRefs(e.Expr, cols, seen)
		for _, w := range e.Whens {
			collectColumnRefs(w.Cond, cols, seen)
			collectColumnRefs(w.Val, cols, seen)
		}
		collectColumnRefs(e.Else, cols, seen)
	}
}

// collectIndexFirstColumns returns a set of column names that are the first column
// of any non-partial index (including primary key and unique constraints).
func collectIndexFirstColumns(table *tree.CreateTable) map[string]bool {
	firstCols := make(map[string]bool)
	for _, def := range table.Defs {
		switch d := def.(type) {
		case *tree.IndexTableDef:
			if d.Predicate != nil {
				continue // Skip partial indexes
			}
			cols := getIndexKeyColumns(d.Columns)
			if len(cols) > 0 {
				firstCols[cols[0]] = true
			}
		case *tree.UniqueConstraintTableDef:
			if d.Predicate != nil {
				continue // Skip partial indexes
			}
			cols := getIndexKeyColumns(d.Columns)
			if len(cols) > 0 {
				firstCols[cols[0]] = true
			}
		}
	}
	return firstCols
}

const lintDisablePrefix = "-- scurry:lint-disable="

// parseLintDisables scans lines from the top of a SQL file for
// -- scurry:lint-disable=<rule>[:<table>[.<constraint>]] directives.
// It stops at the first non-comment, non-empty line.
func parseLintDisables(sql string) []lintDisable {
	var directives []lintDisable
	scanner := bufio.NewScanner(strings.NewReader(sql))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "--") {
			break
		}
		if !strings.HasPrefix(line, lintDisablePrefix) {
			continue
		}
		value := strings.TrimPrefix(line, lintDisablePrefix)
		// Strip inline comments: "nullable-unique -- explanation" → "nullable-unique"
		if idx := strings.Index(value, " "); idx != -1 {
			value = value[:idx]
		}
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		d := lintDisable{}
		// Split rule from optional table.constraint qualifier
		if colonIdx := strings.IndexByte(value, ':'); colonIdx != -1 {
			d.Rule = value[:colonIdx]
			qualifier := value[colonIdx+1:]
			if dotIdx := strings.IndexByte(qualifier, '.'); dotIdx != -1 {
				d.Table = qualifier[:dotIdx]
				d.Constraint = qualifier[dotIdx+1:]
			} else {
				d.Table = qualifier
			}
		} else {
			d.Rule = value
		}
		directives = append(directives, d)
	}
	return directives
}

// loadLintDisables walks the definition directory, parses lint-disable directives
// from each .sql file, and associates them with the table names defined in that file.
// Returns a map from table name to the directives that apply to it.
func loadLintDisables(fs afero.Fs, dirPath string) (map[string][]lintDisable, error) {
	result := make(map[string][]lintDisable)

	err := afero.Walk(fs, dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(strings.ToLower(path), ".sql") {
			return nil
		}

		content, err := afero.ReadFile(fs, path)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}

		sql := string(content)
		directives := parseLintDisables(sql)
		if len(directives) == 0 {
			return nil
		}

		// Parse SQL to find table names defined in this file
		stmts, err := parser.Parse(sql)
		if err != nil {
			return nil // Parsing errors will be caught by schema loading
		}

		for _, stmt := range stmts {
			ct, ok := stmt.AST.(*tree.CreateTable)
			if !ok {
				continue
			}
			tableName := ct.Table.Table()
			result[tableName] = append(result[tableName], directives...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// isSuppressed checks if an issue is suppressed by any lint-disable directive.
func isSuppressed(issue LintIssue, disables map[string][]lintDisable) bool {
	directives, ok := disables[issue.Table]
	if !ok {
		return false
	}
	for _, d := range directives {
		if d.Rule != issue.Rule {
			continue
		}
		// File-wide: no table qualifier
		if d.Table == "" {
			return true
		}
		// Table-wide: matches table, no constraint qualifier
		if d.Table == issue.Table && d.Constraint == "" {
			return true
		}
		// Specific constraint
		if d.Table == issue.Table && d.Constraint == issue.Constraint {
			return true
		}
	}
	return false
}
