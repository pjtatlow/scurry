package data

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/schema"
)

// CompatibilityIssue represents a compatibility issue found during schema comparison.
type CompatibilityIssue struct {
	Table       string
	Column      string
	Severity    string // "error" or "warning"
	Description string
}

// CheckCompatibility compares the dump's schema against the target database schema
// to detect issues before loading data.
func CheckCompatibility(dumpSchema, targetSchema *schema.Schema) []CompatibilityIssue {
	var issues []CompatibilityIssue

	// Build target table map
	targetTables := make(map[string]*tree.CreateTable)
	for _, t := range targetSchema.Tables {
		targetTables[t.ResolvedName()] = t.Ast
	}

	// Build dump table map
	dumpTables := make(map[string]*tree.CreateTable)
	for _, t := range dumpSchema.Tables {
		dumpTables[t.ResolvedName()] = t.Ast
	}

	// Check each dump table exists in target
	for _, t := range dumpSchema.Tables {
		qualifiedName := t.ResolvedName()
		targetTable, exists := targetTables[qualifiedName]
		if !exists {
			issues = append(issues, CompatibilityIssue{
				Table:       qualifiedName,
				Severity:    "error",
				Description: fmt.Sprintf("Table '%s' exists in dump but not in target database", qualifiedName),
			})
			continue
		}

		issues = append(issues, checkTableCompatibility(qualifiedName, t.Ast, targetTable)...)
	}

	return issues
}

func checkTableCompatibility(tableName string, dumpTable, targetTable *tree.CreateTable) []CompatibilityIssue {
	var issues []CompatibilityIssue

	// Extract columns from both tables
	dumpCols := extractColumns(dumpTable)
	targetCols := extractColumns(targetTable)

	// Check dump columns exist in target
	for colName := range dumpCols {
		if _, exists := targetCols[colName]; !exists {
			issues = append(issues, CompatibilityIssue{
				Table:       tableName,
				Column:      colName,
				Severity:    "error",
				Description: fmt.Sprintf("Column '%s.%s' exists in dump but not in target table", tableName, colName),
			})
		}
	}

	// Check target columns not in dump
	for colName, targetCol := range targetCols {
		if _, exists := dumpCols[colName]; exists {
			// Column exists in both — check type compatibility
			dumpCol := dumpCols[colName]
			if dumpCol.Type.SQLString() != targetCol.Type.SQLString() {
				issues = append(issues, CompatibilityIssue{
					Table:       tableName,
					Column:      colName,
					Severity:    "warning",
					Description: fmt.Sprintf("Column '%s.%s' type differs: dump has %s, target has %s", tableName, colName, dumpCol.Type.SQLString(), targetCol.Type.SQLString()),
				})
			}
			continue
		}

		// Column in target but not in dump — check if it's okay to skip
		if targetCol.Computed.Computed {
			// Computed columns don't need data
			continue
		}

		isNotNull := targetCol.Nullable.Nullability == tree.NotNull
		hasDefault := targetCol.HasDefaultExpr()

		if isNotNull && !hasDefault {
			issues = append(issues, CompatibilityIssue{
				Table:       tableName,
				Column:      colName,
				Severity:    "error",
				Description: fmt.Sprintf("Target has NOT NULL column '%s.%s' without DEFAULT that is not in the dump", tableName, colName),
			})
		} else {
			issues = append(issues, CompatibilityIssue{
				Table:       tableName,
				Column:      colName,
				Severity:    "warning",
				Description: fmt.Sprintf("Target has column '%s.%s' not present in dump (has default or is nullable)", tableName, colName),
			})
		}
	}

	return issues
}

// extractColumns returns a map of column name -> column def for a table.
func extractColumns(table *tree.CreateTable) map[string]*tree.ColumnTableDef {
	cols := make(map[string]*tree.ColumnTableDef)
	for _, def := range table.Defs {
		if col, ok := def.(*tree.ColumnTableDef); ok {
			cols[col.Name.Normalize()] = col
		}
	}
	return cols
}
