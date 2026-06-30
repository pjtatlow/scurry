package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateDDLFor builds the local/remote schemas from raw SQL and returns the
// full migration DDL scurry would emit to turn remote into local. Tables carry
// an explicit PRIMARY KEY constraint so they look like the round-tripped form
// scurry sees in production (where every table has a named PK).
func generateDDLFor(t *testing.T, localSQL, remoteSQL []string) []string {
	t.Helper()
	local := NewSchema(parseStatements(localSQL...)...)
	remote := NewSchema(parseStatements(remoteSQL...)...)
	result := Compare(local, remote)
	ddl, _, err := result.GenerateMigrations(false)
	require.NoError(t, err)
	return ddl
}

func countContaining(ddl []string, substr string) int {
	n := 0
	for _, s := range ddl {
		if strings.Contains(s, substr) {
			n++
		}
	}
	return n
}

// --- #3: enum-scoped text bridge -------------------------------------------

func TestColumnTypeChange_EnumTargetBridgesThroughText(t *testing.T) {
	// Column retargeted from one enum to a different (superset-labeled) enum.
	// Labels differ, so this is NOT a rename: it must go through the rewrite
	// path with a text-bridged USING cast (CRDB rejects a direct enum->enum cast).
	local := []string{
		`CREATE TYPE old_e AS ENUM ('a', 'b')`,
		`CREATE TYPE new_e AS ENUM ('a', 'b', 'c')`,
		`CREATE TABLE t (id INT8 NOT NULL, k new_e NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	}
	remote := []string{
		`CREATE TYPE old_e AS ENUM ('a', 'b')`,
		`CREATE TYPE new_e AS ENUM ('a', 'b', 'c')`,
		`CREATE TABLE t (id INT8 NOT NULL, k old_e NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	}
	ddl := generateDDLFor(t, local, remote)
	joined := strings.Join(ddl, "\n")

	require.Equal(t, 1, countContaining(ddl, "SET DATA TYPE"), "expected exactly one column type change\n%s", joined)
	assert.Contains(t, joined, "USING k::STRING::new_e",
		"enum target must be bridged through text\n%s", joined)
	assert.NotContains(t, joined, "RENAME TO", "labels differ, must not be treated as a rename")
}

func TestColumnTypeChange_ScalarNarrowingKeepsDirectCast(t *testing.T) {
	// A scalar narrowing (INT8 -> INT2) must keep the direct cast; bridging a
	// scalar through text would be wrong.
	local := []string{`CREATE TABLE t (id INT8 NOT NULL, n INT2 NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`}
	remote := []string{`CREATE TABLE t (id INT8 NOT NULL, n INT8 NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`}

	ddl := generateDDLFor(t, local, remote)
	joined := strings.Join(ddl, "\n")

	require.Equal(t, 1, countContaining(ddl, "SET DATA TYPE"), "expected one column type change\n%s", joined)
	assert.Contains(t, joined, "USING n::INT2", "scalar narrowing keeps direct cast\n%s", joined)
	assert.NotContains(t, joined, "::STRING::", "scalar narrowing must not be bridged through text\n%s", joined)
}

// --- #2: safe enum-rename auto-detect --------------------------------------

func TestEnumRename_PureRenameEmitsSingleAlterType(t *testing.T) {
	// Identical label set + a column that switched old -> new => a safe rename.
	local := []string{
		`CREATE TYPE new_e AS ENUM ('a', 'b')`,
		`CREATE TABLE t (id INT8 NOT NULL, k new_e NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	}
	remote := []string{
		`CREATE TYPE old_e AS ENUM ('a', 'b')`,
		`CREATE TABLE t (id INT8 NOT NULL, k old_e NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	}

	ddl := generateDDLFor(t, local, remote)
	joined := strings.Join(ddl, "\n")

	// Exactly one statement, and it is the rename.
	require.Len(t, ddl, 1, "pure rename should emit exactly one statement, got:\n%s", joined)
	assert.Contains(t, ddl[0], "ALTER TYPE old_e RENAME TO new_e")

	// No create/drop of the renamed type, and no column cast.
	assert.NotContains(t, joined, "CREATE TYPE")
	assert.NotContains(t, joined, "DROP TYPE")
	assert.NotContains(t, joined, "SET DATA TYPE")
	assert.NotContains(t, joined, "::STRING::")
}

func TestEnumRename_PureRenamePreservesOtherColumnChanges(t *testing.T) {
	// A renamed-enum column that ALSO changes nullability must still get the
	// nullability DDL — only the (now-redundant) type change is suppressed.
	local := []string{
		`CREATE TYPE new_e AS ENUM ('a', 'b')`,
		`CREATE TABLE t (id INT8 NOT NULL, k new_e NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	}
	remote := []string{
		`CREATE TYPE old_e AS ENUM ('a', 'b')`,
		`CREATE TABLE t (id INT8 NOT NULL, k old_e NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	}

	ddl := generateDDLFor(t, local, remote)
	joined := strings.Join(ddl, "\n")

	assert.Equal(t, 1, countContaining(ddl, "RENAME TO"), "expected the rename\n%s", joined)
	assert.Contains(t, joined, "DROP NOT NULL", "nullability change must be preserved\n%s", joined)
	assert.NotContains(t, joined, "SET DATA TYPE", "type change is redundant after rename\n%s", joined)
}

func TestEnumRename_SupersetLabelsNotARename(t *testing.T) {
	// A superset label set is not a pure rename (it differs), so detection
	// returns nothing and the change flows through the #3 text-bridge path.
	renames := detectEnumRenames(
		NewSchema(parseStatements(
			`CREATE TYPE new_e AS ENUM ('a', 'b', 'c')`,
			`CREATE TABLE t (id INT8 NOT NULL, k new_e NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
		)...),
		NewSchema(parseStatements(
			`CREATE TYPE old_e AS ENUM ('a', 'b')`,
			`CREATE TABLE t (id INT8 NOT NULL, k old_e NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
		)...),
	)
	assert.Empty(t, renames, "superset labels must not be detected as a rename")
}

func TestEnumRename_FalsePositiveGuard_UnrelatedSameLabels(t *testing.T) {
	// Two unrelated enums with identical labels, neither referenced by a column
	// that switched between them. Must NOT be treated as a rename: fall back to
	// the safe drop+create.
	local := []string{
		`CREATE TYPE shade AS ENUM ('x', 'y')`,
		`CREATE TABLE t (id INT8 NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	}
	remote := []string{
		`CREATE TYPE color AS ENUM ('x', 'y')`,
		`CREATE TABLE t (id INT8 NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	}

	local0 := NewSchema(parseStatements(local...)...)
	remote0 := NewSchema(parseStatements(remote...)...)
	assert.Empty(t, detectEnumRenames(local0, remote0), "unrelated same-label enums must not pair")

	ddl := generateDDLFor(t, local, remote)
	joined := strings.Join(ddl, "\n")
	assert.NotContains(t, joined, "RENAME TO")
	assert.Equal(t, 1, countContaining(ddl, "CREATE TYPE"), "expected a CREATE for the added enum\n%s", joined)
	assert.Equal(t, 1, countContaining(ddl, "DROP TYPE"), "expected a DROP for the removed enum\n%s", joined)
}

func TestEnumRename_AmbiguousLabelSetNotARename(t *testing.T) {
	// Two old enums and two new enums all share one label set. Even though
	// columns switch between them, the label set is not one-to-one, so the
	// pairing is ambiguous and we refuse to rename (safe drop+create).
	local := NewSchema(parseStatements(
		`CREATE TYPE n1 AS ENUM ('a', 'b')`,
		`CREATE TYPE n2 AS ENUM ('a', 'b')`,
		`CREATE TABLE t (id INT8 NOT NULL, p n1 NOT NULL, q n2 NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	)...)
	remote := NewSchema(parseStatements(
		`CREATE TYPE o1 AS ENUM ('a', 'b')`,
		`CREATE TYPE o2 AS ENUM ('a', 'b')`,
		`CREATE TABLE t (id INT8 NOT NULL, p o1 NOT NULL, q o2 NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	)...)

	assert.Empty(t, detectEnumRenames(local, remote),
		"ambiguous same-label group must not be treated as renames")
}

func TestDetectEnumRenames_ReturnsPair(t *testing.T) {
	// Mirrors the real content_generation_target -> content_generation_target_type case.
	local := NewSchema(parseStatements(
		`CREATE TYPE content_generation_target_type AS ENUM ('a', 'b')`,
		`CREATE TABLE t (id INT8 NOT NULL, k content_generation_target_type NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	)...)
	remote := NewSchema(parseStatements(
		`CREATE TYPE content_generation_target AS ENUM ('a', 'b')`,
		`CREATE TABLE t (id INT8 NOT NULL, k content_generation_target NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id))`,
	)...)

	renames := detectEnumRenames(local, remote)
	require.Len(t, renames, 1)
	assert.Equal(t, "public.content_generation_target", renames[0].oldName())
	assert.Equal(t, "public.content_generation_target_type", renames[0].newName())
}
