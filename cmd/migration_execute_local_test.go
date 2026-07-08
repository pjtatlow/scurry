package cmd

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/schema"
)

// canonicalSnapshot renders the given DDL into schema.sql's canonical form (the same
// normalized, constraint-based representation dumpProductionSchema writes), so that
// comparisons against introspected definition schemas don't spuriously differ.
func canonicalSnapshot(t *testing.T, ctx context.Context, ddl ...string) string {
	t.Helper()
	shadow, err := db.GetShadowDB(ctx, ddl...)
	require.NoError(t, err)
	defer shadow.Close()
	sch, err := schema.LoadFromDatabase(ctx, shadow)
	require.NoError(t, err)
	stmts, _, err := schema.Compare(sch, schema.NewSchema()).GenerateMigrations(true)
	require.NoError(t, err)
	return strings.Join(stmts, ";\n\n\n") + ";\n"
}

// newLocalTestFS creates an in-memory filesystem seeded with an (empty) migrations
// directory + schema.sql snapshot and an empty definitions directory. It returns the
// filesystem and the definitions directory path.
func newLocalTestFS(t *testing.T, snapshot string) (afero.Fs, string) {
	t.Helper()
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll(flags.MigrationDir, 0755))
	defDir := "/definitions"
	require.NoError(t, fs.MkdirAll(defDir, 0755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(flags.MigrationDir, "schema.sql"), []byte(snapshot), 0644))
	return fs, defDir
}

func writeDef(t *testing.T, fs afero.Fs, defDir, name, content string) {
	t.Helper()
	full := filepath.Join(defDir, name)
	require.NoError(t, fs.MkdirAll(filepath.Dir(full), 0755))
	require.NoError(t, afero.WriteFile(fs, full, []byte(content), 0644))
}

func writeMigrationDir(t *testing.T, fs afero.Fs, dir, content string) {
	t.Helper()
	full := filepath.Join(flags.MigrationDir, dir, "migration.sql")
	require.NoError(t, fs.MkdirAll(filepath.Dir(full), 0755))
	require.NoError(t, afero.WriteFile(fs, full, []byte(content), 0644))
}

func tableExists(t *testing.T, ctx context.Context, client *db.Client, table string) bool {
	t.Helper()
	var n int
	err := client.GetDB().QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table).Scan(&n)
	require.NoError(t, err)
	return n > 0
}

func readMigrationFile(t *testing.T, fs afero.Fs, dir string) string {
	t.Helper()
	b, err := afero.ReadFile(fs, filepath.Join(flags.MigrationDir, dir, "migration.sql"))
	require.NoError(t, err)
	return string(b)
}

// TestMigrationLocalDiffPath covers the common case: author a migration from the
// definitions-vs-snapshot diff, apply it, advance the snapshot, and converge.
func TestMigrationLocalDiffPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fs, defDir := newLocalTestFS(t, "")
	writeDef(t, fs, defDir, "tables/users.sql", "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);")

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	opts := MigrationLocalOptions{
		Fs:             fs,
		DefinitionDirs: []string{defDir},
		DbClient:       client,
		Name:           "add_users",
		Force:          true,
	}

	result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
	require.NoError(t, err)

	assert.True(t, result.Authored, "should author a migration")
	assert.True(t, result.Applied, "should apply the migration")
	assert.True(t, result.Converged, "should converge")
	assert.NotEmpty(t, result.MigrationDir)
	assert.True(t, tableExists(t, ctx, client, "users"), "users table should exist in the DB")

	// schema.sql advanced.
	snapshot, err := afero.ReadFile(fs, filepath.Join(flags.MigrationDir, "schema.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(snapshot), "users")

	// Migration recorded as applied.
	applied, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, applied, 1)
	assert.Equal(t, db.MigrationStatusSucceeded, applied[0].Status)
}

// TestMigrationLocalSuppliedSQL covers the --migration-sql path and its validation.
func TestMigrationLocalSuppliedSQL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name            string
		supplied        string
		migName         string
		wantErr         bool
		wantErrContains string
		check           func(t *testing.T, ctx context.Context, fs afero.Fs, client *db.Client, result *MigrationLocalResult)
	}{
		{
			name:     "no header defaults to sync and writes body verbatim",
			supplied: "-- a hand-written backfill\nCREATE TABLE items (id INT PRIMARY KEY, status TEXT);",
			migName:  "add_items",
			check: func(t *testing.T, ctx context.Context, fs afero.Fs, client *db.Client, result *MigrationLocalResult) {
				assert.True(t, result.Applied)
				assert.True(t, tableExists(t, ctx, client, "items"))
				content := readMigrationFile(t, fs, result.MigrationDir)
				assert.Contains(t, content, "-- scurry:mode=sync")
				// The verbatim comment is preserved (createMigration would have dropped it).
				assert.Contains(t, content, "-- a hand-written backfill")
				applied, err := client.GetAppliedMigrations(ctx)
				require.NoError(t, err)
				require.Len(t, applied, 1)
				assert.False(t, applied[0].Async)
			},
		},
		{
			name:            "user-authored scurry header is rejected",
			supplied:        "-- scurry:mode=async\nCREATE TABLE gizmos (id INT PRIMARY KEY);",
			migName:         "add_gizmos",
			wantErr:         true,
			wantErrContains: "do not include a '-- scurry:' header",
		},
		{
			name:            "malformed scurry header is rejected",
			supplied:        "-- scurry:mode=bogus\nCREATE TABLE x (id INT PRIMARY KEY);",
			migName:         "bad",
			wantErr:         true,
			wantErrContains: "do not include a '-- scurry:' header",
		},
		{
			name:            "empty body errors",
			supplied:        "   \n",
			migName:         "empty",
			wantErr:         true,
			wantErrContains: "empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs, defDir := newLocalTestFS(t, "")

			client, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			opts := MigrationLocalOptions{
				Fs:             fs,
				DefinitionDirs: []string{defDir},
				DbClient:       client,
				SuppliedSQL:    tt.supplied,
				UseSuppliedSQL: true,
				Name:           tt.migName,
				Force:          true,
			}

			result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, ctx, fs, client, result)
			}
		})
	}
}

// TestMigrationLocalSuppliedAsyncClassification verifies that a custom data backfill
// (which the differ can't express) is classified async from table sizes, without the
// user authoring any header.
func TestMigrationLocalSuppliedAsyncClassification(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	bigDDL := "CREATE TABLE big (id INT PRIMARY KEY, val INT)"
	fs, defDir := newLocalTestFS(t, canonicalSnapshot(t, ctx, bigDDL))

	// Mark big as a large table so a backfill against it classifies async.
	require.NoError(t, migrationpkg.SaveTableSizes(fs, flags.MigrationDir, &migrationpkg.TableSizes{
		Threshold: 1,
		Tables:    map[string]migrationpkg.TableInfo{"public.big": {Rows: 100}},
	}))

	writeDef(t, fs, defDir, "tables/big.sql", bigDDL+";")

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()
	// The dev DB already has the base table.
	_, err = client.ExecContext(ctx, bigDDL)
	require.NoError(t, err)

	opts := MigrationLocalOptions{
		Fs:             fs,
		DefinitionDirs: []string{defDir},
		DbClient:       client,
		SuppliedSQL:    "UPDATE big SET val = 0 WHERE val IS NULL;",
		UseSuppliedSQL: true,
		Name:           "backfill_big_val",
		Force:          true,
	}

	result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
	require.NoError(t, err)
	assert.True(t, result.Applied)
	assert.True(t, result.Converged, "a data-only backfill leaves the schema in sync")

	applied, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, applied, 1)
	assert.True(t, applied[0].Async, "a backfill UPDATE on a large table should be recorded async")
}

// TestMigrationLocalCatchUp applies a pending on-disk migration before authoring, and
// when the definitions add nothing new the run is a no-op author that still converges.
func TestMigrationLocalCatchUp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Snapshot and definitions both describe the widgets table; the on-disk migration
	// creates it. Nothing new to author, but catch-up must run the migration.
	fs, defDir := newLocalTestFS(t, canonicalSnapshot(t, ctx, "CREATE TABLE widgets (id INT PRIMARY KEY)"))
	writeDef(t, fs, defDir, "tables/widgets.sql", "CREATE TABLE widgets (id INT PRIMARY KEY);")
	writeMigrationDir(t, fs, "20250101000000_create_widgets", "CREATE TABLE widgets (id INT PRIMARY KEY);")

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	opts := MigrationLocalOptions{
		Fs:             fs,
		DefinitionDirs: []string{defDir},
		DbClient:       client,
		Force:          true,
	}

	result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
	require.NoError(t, err)

	assert.Equal(t, 1, result.CaughtUp, "one pending migration should be caught up")
	assert.False(t, result.Authored, "nothing new to author")
	assert.True(t, result.Converged)
	assert.True(t, tableExists(t, ctx, client, "widgets"))

	applied, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, applied, 1)
	assert.Equal(t, "20250101000000_create_widgets", applied[0].Name)
}

// TestMigrationLocalBaseline adopts an untracked, populated database, then authors a
// new migration on top of it.
func TestMigrationLocalBaseline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	widgetsDDL := "CREATE TABLE widgets (id INT PRIMARY KEY)"
	fs, defDir := newLocalTestFS(t, canonicalSnapshot(t, ctx, widgetsDDL))
	writeMigrationDir(t, fs, "20250101000000_create_widgets", widgetsDDL+";")
	// Definitions add a new table on top of the baselined schema.
	writeDef(t, fs, defDir, "tables/widgets.sql", widgetsDDL+";")
	writeDef(t, fs, defDir, "tables/gadgets.sql", "CREATE TABLE gadgets (id INT PRIMARY KEY);")

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Simulate an existing, untracked database that already has the widgets table.
	_, err = client.ExecContext(ctx, widgetsDDL)
	require.NoError(t, err)

	opts := MigrationLocalOptions{
		Fs:             fs,
		DefinitionDirs: []string{defDir},
		DbClient:       client,
		Name:           "add_gadgets",
		Force:          true,
	}

	result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
	require.NoError(t, err)

	assert.True(t, result.Baselined, "should baseline the untracked database")
	assert.True(t, result.Authored)
	assert.True(t, result.Applied)
	assert.True(t, result.Converged)
	assert.True(t, tableExists(t, ctx, client, "gadgets"))

	// The pre-existing migration is recorded (not re-run), plus the new one.
	applied, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	names := make(map[string]bool)
	for _, m := range applied {
		names[m.Name] = true
	}
	assert.True(t, names["20250101000000_create_widgets"], "baselined migration should be recorded")
	assert.True(t, result.MigrationDir != "" && names[result.MigrationDir], "new migration should be recorded")
}

// TestMigrationLocalDryRun makes no changes to disk or the database.
func TestMigrationLocalDryRun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fs, defDir := newLocalTestFS(t, "")
	writeDef(t, fs, defDir, "tables/users.sql", "CREATE TABLE users (id INT PRIMARY KEY);")

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	opts := MigrationLocalOptions{
		Fs:             fs,
		DefinitionDirs: []string{defDir},
		DbClient:       client,
		Name:           "add_users",
		Force:          true,
		DryRun:         true,
	}

	result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
	require.NoError(t, err)

	assert.False(t, result.Authored)
	assert.False(t, result.Applied)
	assert.False(t, tableExists(t, ctx, client, "users"), "dry-run must not touch the DB")

	// schema.sql unchanged (still empty).
	snapshot, err := afero.ReadFile(fs, filepath.Join(flags.MigrationDir, "schema.sql"))
	require.NoError(t, err)
	assert.Empty(t, strings.TrimSpace(string(snapshot)))

	// No migration directory created.
	entries, err := afero.ReadDir(fs, flags.MigrationDir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.False(t, e.IsDir(), "no migration directory should be created in dry-run")
	}
}

// TestMigrationLocalRequiresName fails without a name in non-interactive mode.
func TestMigrationLocalRequiresName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fs, defDir := newLocalTestFS(t, "")
	writeDef(t, fs, defDir, "tables/users.sql", "CREATE TABLE users (id INT PRIMARY KEY);")

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	opts := MigrationLocalOptions{
		Fs:             fs,
		DefinitionDirs: []string{defDir},
		DbClient:       client,
		Force:          true, // non-interactive, no name
	}

	_, err = executeMigrationLocal(ctx, opts, &ErrorContext{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name required")
}

// TestMigrationLocalStrictDrift returns an error under --strict when the migrations do
// not reproduce the declared schema (T vs S drift), and only warns otherwise.
func TestMigrationLocalStrictDrift(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// The supplied migration creates the wrong shape, so the snapshot won't match the
	// definitions after it is applied — a T-vs-S drift.
	setup := func(t *testing.T) (MigrationLocalOptions, *db.Client) {
		fs, defDir := newLocalTestFS(t, "")
		writeDef(t, fs, defDir, "tables/users.sql", "CREATE TABLE users (id INT PRIMARY KEY, email TEXT);")
		client, err := db.GetShadowDB(ctx)
		require.NoError(t, err)
		return MigrationLocalOptions{
			Fs:             fs,
			DefinitionDirs: []string{defDir},
			DbClient:       client,
			SuppliedSQL:    "CREATE TABLE users (id INT PRIMARY KEY);", // missing email column
			UseSuppliedSQL: true,
			Name:           "add_users_partial",
			Force:          true,
		}, client
	}

	t.Run("strict errors on drift", func(t *testing.T) {
		t.Parallel()
		opts, client := setup(t)
		defer client.Close()
		opts.Strict = true

		result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
		require.Error(t, err)
		assert.ErrorIs(t, err, errMigrationLocalNotConverged)
		assert.True(t, result.SchemaDrift, "definitions vs snapshot should drift")
		assert.False(t, result.Converged)
	})

	t.Run("non-strict warns only", func(t *testing.T) {
		t.Parallel()
		opts, client := setup(t)
		defer client.Close()
		opts.Strict = false

		result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
		require.NoError(t, err)
		assert.True(t, result.SchemaDrift)
		assert.False(t, result.Converged)
	})
}

// TestMigrationLocalAutoCompleteReconciled covers the decided semantics: when applying
// the new migration fails because the change already exists in the DB but the reconcile
// shows the DB already matches, the migration is recorded rather than erroring.
func TestMigrationLocalAutoCompleteReconciled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fs, defDir := newLocalTestFS(t, "")
	writeDef(t, fs, defDir, "tables/users.sql", "CREATE TABLE users (id INT PRIMARY KEY);")

	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Pre-create the users table so the authored CREATE TABLE will fail to apply, but
	// the database already matches the (post-dump) snapshot.
	_, err = client.ExecContext(ctx, "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err)

	opts := MigrationLocalOptions{
		Fs:             fs,
		DefinitionDirs: []string{defDir},
		DbClient:       client,
		Name:           "add_users",
		Force:          true,
	}

	result, err := executeMigrationLocal(ctx, opts, &ErrorContext{})
	require.NoError(t, err, "apply failure should be benign when the DB already matches")

	assert.True(t, result.Authored)
	assert.True(t, result.Applied, "migration should be recorded as applied")
	assert.False(t, result.DatabaseDrift, "the DB already matches the migrations")
	assert.True(t, result.Converged)

	applied, err := client.GetAppliedMigrations(ctx)
	require.NoError(t, err)
	require.Len(t, applied, 1)
}
