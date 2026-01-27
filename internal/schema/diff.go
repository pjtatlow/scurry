package schema

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/set"
)

// DiffType represents the type of schema difference
type DiffType string

const (
	DiffSchemaAdded   DiffType = "schema_added"
	DiffSchemaRemoved DiffType = "schema_removed"

	DiffTypeRoutineAdded    DiffType = "routine_added"
	DiffTypeRoutineRemoved  DiffType = "routine_removed"
	DiffTypeRoutineModified DiffType = "routine_modified"

	DiffTypeTypeAdded    DiffType = "type_added"
	DiffTypeTypeRemoved  DiffType = "type_removed"
	DiffTypeTypeModified DiffType = "type_modified"

	DiffTypeSequenceAdded    DiffType = "sequence_added"
	DiffTypeSequenceRemoved  DiffType = "sequence_removed"
	DiffTypeSequenceModified DiffType = "sequence_modified"

	DiffTypeViewAdded    DiffType = "view_added"
	DiffTypeViewRemoved  DiffType = "view_removed"
	DiffTypeViewModified DiffType = "view_modified"

	DiffTypeTableAdded          DiffType = "table_added"
	DiffTypeTableRemoved        DiffType = "table_removed"
	DiffTypeTableModified       DiffType = "table_modified"
	DiffTypeTableColumnModified DiffType = "table_column_modified"
	DiffTypeColumnTypeChanged   DiffType = "column_type_changed"

	DiffTypeTriggerAdded    DiffType = "trigger_added"
	DiffTypeTriggerRemoved  DiffType = "trigger_removed"
	DiffTypeTriggerModified DiffType = "trigger_modified"
)

// Difference represents a single schema difference
type Difference struct {
	Type                 DiffType
	ObjectName           string
	Description          string
	Dangerous            bool
	WarningMessage       string
	IsDropCreate         bool
	MigrationStatements  []tree.Statement
	OriginalDependencies set.Set[string] // For DROP ordering: what the dropped object depended on
}

// ComparisonResult holds all differences between two schemas
type ComparisonResult struct {
	Differences []Difference
}

// Compare compares two schemas and returns all differences
func Compare(local, remote *Schema) *ComparisonResult {
	result := ComparisonResult{
		Differences: make([]Difference, 0),
	}

	result.Differences = append(result.Differences, compareSchemas(local, remote)...)
	result.Differences = append(result.Differences, compareTypes(local, remote)...)
	result.Differences = append(result.Differences, compareSequences(local, remote)...)
	result.Differences = append(result.Differences, compareRoutines(local, remote)...)
	result.Differences = append(result.Differences, compareTables(local, remote)...)
	result.Differences = append(result.Differences, compareViews(local, remote)...)
	result.Differences = append(result.Differences, compareTriggers(local, remote)...)

	return &result
}

// HasChanges returns true if there are any differences
func (r *ComparisonResult) HasChanges() bool {
	return len(r.Differences) > 0
}

// Summary returns a human-readable summary of differences
func (r *ComparisonResult) Summary() string {
	if !r.HasChanges() {
		return "No differences found"
	}

	summary := fmt.Sprintf("Found %d difference(s):\n", len(r.Differences))
	for i, diff := range r.Differences {
		summary += fmt.Sprintf("%d. %s\n", i+1, diff.Description)
	}
	return summary
}

func compareSchemas(local, remote *Schema) []Difference {
	diffs := make([]Difference, 0)

	// Build maps for quick lookup
	localSchemas := set.New[string]()
	remoteSchemas := set.New[string]()

	for _, s := range local.Schemas {
		localSchemas.Add(s.Name)
	}
	for _, s := range remote.Schemas {
		remoteSchemas.Add(s.Name)
	}

	missing := localSchemas.Difference(remoteSchemas)
	extra := remoteSchemas.Difference(localSchemas)

	for name := range missing.Values() {
		diffs = append(diffs, Difference{
			Type:        DiffSchemaAdded,
			ObjectName:  "schema:" + name,
			Description: fmt.Sprintf("Schema \"%s\" added", name),
			MigrationStatements: []tree.Statement{
				&tree.CreateSchema{
					IfNotExists: true,
					Schema:      tree.ObjectNamePrefix{SchemaName: tree.Name(name), ExplicitSchema: true},
				},
			},
		})
	}

	for name := range extra.Values() {
		diffs = append(diffs, Difference{
			Type:        DiffSchemaRemoved,
			ObjectName:  "schema:" + name,
			Description: fmt.Sprintf("Schema \"%s\" removed", name),
			MigrationStatements: []tree.Statement{
				&tree.DropSchema{
					Names: []tree.ObjectNamePrefix{{SchemaName: tree.Name(name), ExplicitSchema: true}},
				},
			},
		})
	}

	return diffs
}
