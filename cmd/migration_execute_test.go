package cmd

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
)

func TestLoadMigrationsForExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		files      map[string]string // path -> content relative to migration dir
		wantCount  int
		wantNames  []string
		wantModes  []string
		wantDepsOn [][]string
	}{
		{
			name:      "empty directory",
			files:     nil,
			wantCount: 0,
		},
		{
			name: "single sync migration",
			files: map[string]string{
				"20250101000000_create_users/migration.sql": "CREATE TABLE users (id INT PRIMARY KEY);",
			},
			wantCount: 1,
			wantNames: []string{"20250101000000_create_users"},
			wantModes: []string{"sync"},
		},
		{
			name: "async migration with header",
			files: map[string]string{
				"20250101000000_add_index/migration.sql": "-- scurry:mode=async\nCREATE INDEX idx ON users (name);",
			},
			wantCount: 1,
			wantNames: []string{"20250101000000_add_index"},
			wantModes: []string{"async"},
		},
		{
			name: "async migration with depends_on",
			files: map[string]string{
				"20250101000000_create_users/migration.sql": "CREATE TABLE users (id INT PRIMARY KEY);",
				"20250102000000_add_index/migration.sql":    "-- scurry:mode=async,depends_on=20250101000000_create_users\nCREATE INDEX idx ON users (id);",
			},
			wantCount:  2,
			wantNames:  []string{"20250101000000_create_users", "20250102000000_add_index"},
			wantModes:  []string{"sync", "async"},
			wantDepsOn: [][]string{nil, {"20250101000000_create_users"}},
		},
		{
			name: "sorted by timestamp",
			files: map[string]string{
				"20250103000000_third/migration.sql":  "CREATE TABLE c (id INT PRIMARY KEY);",
				"20250101000000_first/migration.sql":  "CREATE TABLE a (id INT PRIMARY KEY);",
				"20250102000000_second/migration.sql": "CREATE TABLE b (id INT PRIMARY KEY);",
			},
			wantCount: 3,
			wantNames: []string{"20250101000000_first", "20250102000000_second", "20250103000000_third"},
		},
		{
			name: "skips non-directory files",
			files: map[string]string{
				"20250101000000_create_users/migration.sql": "CREATE TABLE users (id INT PRIMARY KEY);",
				"schema.sql": "-- schema file, not a migration",
			},
			wantCount: 1,
			wantNames: []string{"20250101000000_create_users"},
		},
		{
			name: "skips directories without migration.sql",
			files: map[string]string{
				"20250101000000_empty/.gitkeep":      "",
				"20250102000000_valid/migration.sql": "CREATE TABLE t (id INT PRIMARY KEY);",
			},
			wantCount: 1,
			wantNames: []string{"20250102000000_valid"},
		},
		{
			name: "header stripped from SQL",
			files: map[string]string{
				"20250101000000_async/migration.sql": "-- scurry:mode=async\nCREATE INDEX idx ON t (col);",
			},
			wantCount: 1,
			wantNames: []string{"20250101000000_async"},
			wantModes: []string{"async"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs := afero.NewMemMapFs()

			err := fs.MkdirAll(flags.MigrationDir, 0755)
			require.NoError(t, err)

			for path, content := range tt.files {
				fullPath := filepath.Join(flags.MigrationDir, path)
				err := fs.MkdirAll(filepath.Dir(fullPath), 0755)
				require.NoError(t, err)
				err = afero.WriteFile(fs, fullPath, []byte(content), 0644)
				require.NoError(t, err)
			}

			migrations, err := loadMigrations(fs)
			require.NoError(t, err)
			assert.Len(t, migrations, tt.wantCount)

			if tt.wantNames != nil {
				for i, name := range tt.wantNames {
					assert.Equal(t, name, migrations[i].Name)
				}
			}

			if tt.wantModes != nil {
				for i, mode := range tt.wantModes {
					assert.Equal(t, mode, migrations[i].Mode)
				}
			}

			if tt.wantDepsOn != nil {
				for i, deps := range tt.wantDepsOn {
					assert.Equal(t, deps, migrations[i].DependsOn)
				}
			}

			// Verify headers are stripped from SQL
			for _, m := range migrations {
				assert.NotContains(t, m.SQL, "-- scurry:")
			}
		})
	}
}

func TestFilterUnappliedMigrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		allMigrations    []db.Migration
		applied          []db.AppliedMigration
		wantUnapplied    []string
		wantWarningCount int
	}{
		{
			name: "all new migrations",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "aaa"},
				{Name: "20250102_b", Checksum: "bbb"},
			},
			applied:       nil,
			wantUnapplied: []string{"20250101_a", "20250102_b"},
		},
		{
			name: "all already applied",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "aaa"},
			},
			applied: []db.AppliedMigration{
				{Name: "20250101_a", Checksum: "aaa"},
			},
			wantUnapplied: nil,
		},
		{
			name: "checksum mismatch produces warning",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "new_checksum"},
			},
			applied: []db.AppliedMigration{
				{Name: "20250101_a", Checksum: "old_checksum"},
			},
			wantUnapplied:    nil,
			wantWarningCount: 1,
		},
		{
			name: "empty stored checksum does not warn",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "aaa"},
			},
			applied: []db.AppliedMigration{
				{Name: "20250101_a", Checksum: ""},
			},
			wantUnapplied:    nil,
			wantWarningCount: 0,
		},
		{
			name: "mix of applied and unapplied",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "aaa"},
				{Name: "20250102_b", Checksum: "bbb"},
				{Name: "20250103_c", Checksum: "ccc"},
			},
			applied: []db.AppliedMigration{
				{Name: "20250101_a", Checksum: "aaa"},
			},
			wantUnapplied: []string{"20250102_b", "20250103_c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			unapplied, warnings, err := filterUnappliedMigrations(tt.allMigrations, tt.applied)
			require.NoError(t, err)

			var names []string
			for _, m := range unapplied {
				names = append(names, m.Name)
			}
			assert.Equal(t, tt.wantUnapplied, names)
			assert.Len(t, warnings, tt.wantWarningCount)
		})
	}
}

func TestRunMigrationList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableExists := func(t *testing.T, client *db.Client, name string) bool {
		var n int
		err := client.GetDB().QueryRowContext(ctx,
			`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, name).Scan(&n)
		require.NoError(t, err)
		return n > 0
	}

	tests := []struct {
		name         string
		setup        func(t *testing.T, client *db.Client)
		migrations   []db.Migration
		wantExecuted int
		wantSkipped  int
		wantErr      bool
		verify       func(t *testing.T, client *db.Client)
	}{
		{
			name: "runs all migrations in order",
			migrations: []db.Migration{
				{Name: "001_a", SQL: "CREATE TABLE rml_a (id INT PRIMARY KEY);", Checksum: "a"},
				{Name: "002_b", SQL: "CREATE TABLE rml_b (id INT PRIMARY KEY);", Checksum: "b"},
			},
			wantExecuted: 2,
			verify: func(t *testing.T, client *db.Client) {
				assert.True(t, tableExists(t, client, "rml_a"))
				assert.True(t, tableExists(t, client, "rml_b"))
				applied, err := client.GetAppliedMigrations(ctx)
				require.NoError(t, err)
				assert.Len(t, applied, 2)
			},
		},
		{
			name: "stops at the first failing migration",
			migrations: []db.Migration{
				{Name: "001_ok", SQL: "CREATE TABLE rml_ok (id INT PRIMARY KEY);", Checksum: "ok"},
				{Name: "002_bad", SQL: "ALTER TABLE rml_missing ADD COLUMN x STRING;", Checksum: "bad"},
				{Name: "003_never", SQL: "CREATE TABLE rml_never (id INT PRIMARY KEY);", Checksum: "never"},
			},
			wantExecuted: 1,
			wantErr:      true,
			verify: func(t *testing.T, client *db.Client) {
				assert.True(t, tableExists(t, client, "rml_ok"))
				assert.False(t, tableExists(t, client, "rml_never"), "execution must stop after the failure")
				failed, err := client.GetFailedMigration(ctx)
				require.NoError(t, err)
				require.NotNil(t, failed)
				assert.Equal(t, "002_bad", failed.Name)
				assert.Equal(t, db.MigrationStatusFailed, failed.Status)
			},
		},
		{
			name: "records squash migration without executing it",
			migrations: []db.Migration{
				{Name: "001_squash", SQL: "CREATE TABLE rml_squash (id INT PRIMARY KEY);", Checksum: "sq", Squash: true},
			},
			wantExecuted: 1,
			verify: func(t *testing.T, client *db.Client) {
				assert.False(t, tableExists(t, client, "rml_squash"), "squash migration must not execute its SQL")
				applied, err := client.GetAppliedMigrations(ctx)
				require.NoError(t, err)
				require.Len(t, applied, 1)
				assert.Equal(t, db.MigrationStatusSucceeded, applied[0].Status)
			},
		},
		{
			name: "skips migration with an unmet dependency",
			migrations: []db.Migration{
				{Name: "001_dep", SQL: "CREATE TABLE rml_dep (id INT PRIMARY KEY);", Checksum: "dep", DependsOn: []string{"999_missing"}},
			},
			wantExecuted: 0,
			wantSkipped:  1,
			verify: func(t *testing.T, client *db.Client) {
				assert.False(t, tableExists(t, client, "rml_dep"))
				applied, err := client.GetAppliedMigrations(ctx)
				require.NoError(t, err)
				assert.Empty(t, applied)
			},
		},
		{
			name: "skips async migration when one is already running",
			setup: func(t *testing.T, client *db.Client) {
				require.NoError(t, client.StartMigration(ctx, "000_running_async", "x", true))
			},
			migrations: []db.Migration{
				{Name: "001_async", SQL: "CREATE TABLE rml_async (id INT PRIMARY KEY);", Checksum: "as", Mode: db.MigrationModeAsync},
			},
			wantExecuted: 0,
			wantSkipped:  1,
			verify: func(t *testing.T, client *db.Client) {
				assert.False(t, tableExists(t, client, "rml_async"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			require.NoError(t, client.InitMigrationHistory(ctx))
			if tt.setup != nil {
				tt.setup(t, client)
			}

			executed, skipped, err := runMigrationList(ctx, client, tt.migrations)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantExecuted, executed)
			assert.Equal(t, tt.wantSkipped, skipped)
			if tt.verify != nil {
				tt.verify(t, client)
			}
		})
	}
}

func TestMarkAllMigrationsComplete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const name = "20250101000000_test"
	const checksum = "test_checksum"

	tests := []struct {
		name       string
		setup      func(t *testing.T, client *db.Client)
		wantStatus string
	}{
		{
			name:       "never applied is recorded as succeeded",
			setup:      func(t *testing.T, client *db.Client) {},
			wantStatus: db.MigrationStatusSucceeded,
		},
		{
			name: "pending is completed",
			setup: func(t *testing.T, client *db.Client) {
				require.NoError(t, client.StartMigration(ctx, name, checksum, false))
			},
			wantStatus: db.MigrationStatusSucceeded,
		},
		{
			name: "failed is recovered",
			setup: func(t *testing.T, client *db.Client) {
				require.NoError(t, client.StartMigration(ctx, name, checksum, false))
				require.NoError(t, client.FailMigration(ctx, name, "bad stmt", "boom"))
			},
			wantStatus: db.MigrationStatusRecovered,
		},
		{
			name: "already succeeded is untouched",
			setup: func(t *testing.T, client *db.Client) {
				require.NoError(t, client.RecordMigration(ctx, name, checksum, false))
			},
			wantStatus: db.MigrationStatusSucceeded,
		},
		{
			name: "already recovered is untouched",
			setup: func(t *testing.T, client *db.Client) {
				require.NoError(t, client.StartMigration(ctx, name, checksum, false))
				require.NoError(t, client.FailMigration(ctx, name, "bad stmt", "boom"))
				require.NoError(t, client.RecoverMigration(ctx, name))
			},
			wantStatus: db.MigrationStatusRecovered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := db.GetShadowDB(ctx)
			require.NoError(t, err)
			defer client.Close()

			require.NoError(t, client.InitMigrationHistory(ctx))
			tt.setup(t, client)

			migrations := []db.Migration{{Name: name, SQL: "CREATE TABLE t (id INT PRIMARY KEY)", Checksum: checksum}}
			require.NoError(t, markAllMigrationsComplete(ctx, client, migrations))

			applied, err := client.GetAppliedMigrations(ctx)
			require.NoError(t, err)
			require.Len(t, applied, 1)
			assert.Equal(t, tt.wantStatus, applied[0].Status)
		})
	}
}
