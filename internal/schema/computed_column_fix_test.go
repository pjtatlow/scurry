package schema

import (
	"strings"
	"testing"
)

// TestComputedColumnMigrationSimulation simulates the exact scenario from the error report
// where adding a computed column (available) that depends on another new column (committed)
// was failing because the migrations were generated in the wrong order.
func TestComputedColumnMigrationSimulation(t *testing.T) {
	// Remote schema from the error report (line 1827) - missing committed and available columns
	remoteTable := `CREATE TABLE public.storage_location_inventory (
		id INT8 NOT NULL DEFAULT public.random_id(),
		account_id INT8 NOT NULL,
		storage_location_id INT8 NOT NULL,
		location_inventory_id INT8 NOT NULL,
		quantity INT8 NOT NULL DEFAULT 0:::INT8,
		last_counted_at TIMESTAMPTZ(6) NULL,
		created_at TIMESTAMPTZ(6) NOT NULL DEFAULT current_timestamp():::TIMESTAMPTZ,
		updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT current_timestamp():::TIMESTAMPTZ,
		CONSTRAINT storage_location_inventory_pkey PRIMARY KEY (id ASC)
	)`

	// Local schema from the error report (line 631) - has committed and available columns
	localTable := `CREATE TABLE public.storage_location_inventory (
		id INT8 NOT NULL DEFAULT public.random_id(),
		account_id INT8 NOT NULL,
		storage_location_id INT8 NOT NULL,
		location_inventory_id INT8 NOT NULL,
		quantity INT8 NOT NULL DEFAULT 0:::INT8,
		committed INT8 NOT NULL DEFAULT 0:::INT8,
		available INT8 NOT NULL AS (quantity - committed) STORED,
		last_counted_at TIMESTAMPTZ(6) NULL,
		created_at TIMESTAMPTZ(6) NOT NULL DEFAULT current_timestamp():::TIMESTAMPTZ,
		updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT current_timestamp():::TIMESTAMPTZ,
		CONSTRAINT storage_location_inventory_pkey PRIMARY KEY (id ASC)
	)`

	localSchema := createSchemaWithTypesAndTables(nil, []string{localTable})
	remoteSchema := createSchemaWithTypesAndTables(nil, []string{remoteTable})

	diffResult := Compare(localSchema, remoteSchema)

	if !diffResult.HasChanges() {
		t.Fatal("expected changes but got none")
	}

	migrations, _, err := diffResult.GenerateMigrations(true)
	if err != nil {
		t.Fatalf("GenerateMigrations() error: %v", err)
	}

	// Print the migrations for visibility
	t.Log("Generated migrations:")
	for i, m := range migrations {
		t.Logf("  %d: %s", i+1, m)
	}

	// Join all migrations into a single string to check ordering
	allDDL := strings.Join(migrations, "\n")

	// The committed column MUST appear before the available column in the migrations
	// because available depends on committed via the computed expression (quantity - committed)
	committedIdx := strings.Index(allDDL, "committed")
	availableIdx := strings.Index(allDDL, "available")

	if committedIdx == -1 {
		t.Fatal("expected 'committed' column in migration output but not found")
	}
	if availableIdx == -1 {
		t.Fatal("expected 'available' column in migration output but not found")
	}

	if committedIdx > availableIdx {
		t.Errorf("FAIL: 'committed' column (position %d) must be added BEFORE 'available' column (position %d)\n\nGenerated DDL:\n%s",
			committedIdx, availableIdx, allDDL)
	} else {
		t.Logf("SUCCESS: 'committed' column (position %d) is correctly ordered before 'available' column (position %d)",
			committedIdx, availableIdx)
	}
}
