package data

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
)

func TestLoad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name      string
		setupSQL  []string
		insertSQL []string
		opts      LoadOptions
		wantRows  map[string]int // table -> expected count after load
	}{
		{
			name: "basic load",
			setupSQL: []string{
				"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			},
			insertSQL: []string{
				"INSERT INTO public.users VALUES (1, 'Alice'), (2, 'Bob')",
			},
			opts:     LoadOptions{},
			wantRows: map[string]int{"public.users": 2},
		},
		{
			name: "load with FK tables",
			setupSQL: []string{
				"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
				"CREATE TABLE public.posts (id INT8 PRIMARY KEY, user_id INT8 REFERENCES public.users(id), title STRING)",
			},
			insertSQL: []string{
				"INSERT INTO public.users VALUES (1, 'Alice')",
				"INSERT INTO public.posts VALUES (1, 1, 'Hello')",
			},
			opts:     LoadOptions{},
			wantRows: map[string]int{"public.users": 1, "public.posts": 1},
		},
		{
			name: "load with truncate first",
			setupSQL: []string{
				"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			},
			insertSQL: []string{
				"INSERT INTO public.users VALUES (1, 'Alice')",
			},
			opts:     LoadOptions{TruncateFirst: true},
			wantRows: map[string]int{"public.users": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create source DB with data
			srcClient, err := db.GetShadowDB(ctx, tt.setupSQL...)
			require.NoError(t, err)
			defer srcClient.Close()

			for _, sql := range tt.insertSQL {
				_, err := srcClient.GetDB().ExecContext(ctx, sql)
				require.NoError(t, err)
			}

			// Dump from source
			dumpFile, err := Dump(ctx, srcClient, 100)
			require.NoError(t, err)

			// Create target DB with same schema
			targetClient, err := db.GetShadowDB(ctx, tt.setupSQL...)
			require.NoError(t, err)
			defer targetClient.Close()

			// If truncate first, pre-populate with data that should be removed
			if tt.opts.TruncateFirst {
				_, err := targetClient.GetDB().ExecContext(ctx, "INSERT INTO public.users VALUES (99, 'PreExisting')")
				require.NoError(t, err)
			}

			// Load into target
			result, err := Load(ctx, targetClient, dumpFile, tt.opts)
			require.NoError(t, err)

			// Verify row counts
			for tableName, expectedCount := range tt.wantRows {
				parts := strings.SplitN(tableName, ".", 2)
				var count int
				err := targetClient.GetDB().QueryRowContext(ctx,
					"SELECT count(*) FROM \""+parts[0]+"\".\""+parts[1]+"\"",
				).Scan(&count)
				require.NoError(t, err)
				assert.Equal(t, expectedCount, count, "row count mismatch for %s", tableName)
			}

			assert.Greater(t, result.TablesLoaded, 0)
		})
	}
}

func TestLoadDryRun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	setupSQL := []string{
		"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
	}

	srcClient, err := db.GetShadowDB(ctx, setupSQL...)
	require.NoError(t, err)
	defer srcClient.Close()

	_, err = srcClient.GetDB().ExecContext(ctx, "INSERT INTO public.users VALUES (1, 'Alice')")
	require.NoError(t, err)

	dumpFile, err := Dump(ctx, srcClient, 100)
	require.NoError(t, err)

	targetClient, err := db.GetShadowDB(ctx, setupSQL...)
	require.NoError(t, err)
	defer targetClient.Close()

	result, err := Load(ctx, targetClient, dumpFile, LoadOptions{DryRun: true})
	require.NoError(t, err)

	assert.Equal(t, 1, result.TablesLoaded)
	assert.Equal(t, 1, result.RowsInserted)

	// Verify no data was actually loaded
	var count int
	err = targetClient.GetDB().QueryRowContext(ctx, "SELECT count(*) FROM public.users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestLoadCreateSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Create source DB with schema and data
	setupSQL := []string{
		"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
	}

	srcClient, err := db.GetShadowDB(ctx, setupSQL...)
	require.NoError(t, err)
	defer srcClient.Close()

	_, err = srcClient.GetDB().ExecContext(ctx, "INSERT INTO public.users VALUES (1, 'Alice')")
	require.NoError(t, err)

	dumpFile, err := Dump(ctx, srcClient, 100)
	require.NoError(t, err)

	// Create empty target DB (no schema)
	targetClient, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	defer targetClient.Close()

	// Load with --create-schema
	result, err := Load(ctx, targetClient, dumpFile, LoadOptions{CreateSchema: true})
	require.NoError(t, err)

	assert.Equal(t, 1, result.TablesLoaded)

	// Verify data was loaded
	var count int
	err = targetClient.GetDB().QueryRowContext(ctx, "SELECT count(*) FROM public.users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestLoadCompatibilityError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Source has extra table not in target
	srcClient, err := db.GetShadowDB(ctx,
		"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
		"CREATE TABLE public.posts (id INT8 PRIMARY KEY, user_id INT8 REFERENCES public.users(id))",
	)
	require.NoError(t, err)
	defer srcClient.Close()

	_, err = srcClient.GetDB().ExecContext(ctx, "INSERT INTO public.users VALUES (1, 'Alice')")
	require.NoError(t, err)
	_, err = srcClient.GetDB().ExecContext(ctx, "INSERT INTO public.posts VALUES (1, 1)")
	require.NoError(t, err)

	dumpFile, err := Dump(ctx, srcClient, 100)
	require.NoError(t, err)

	// Target only has users (missing posts)
	targetClient, err := db.GetShadowDB(ctx,
		"CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
	)
	require.NoError(t, err)
	defer targetClient.Close()

	_, err = Load(ctx, targetClient, dumpFile, LoadOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compatibility")
}

func TestLoadSelfRefTable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	setupSQL := []string{
		"CREATE TABLE public.categories (id INT8 PRIMARY KEY, name STRING NOT NULL, parent_id INT8 REFERENCES public.categories(id))",
	}

	// Create source with tree data
	srcClient, err := db.GetShadowDB(ctx, setupSQL...)
	require.NoError(t, err)
	defer srcClient.Close()

	_, err = srcClient.GetDB().ExecContext(ctx, "INSERT INTO public.categories VALUES (1, 'Root', NULL)")
	require.NoError(t, err)
	_, err = srcClient.GetDB().ExecContext(ctx, "INSERT INTO public.categories VALUES (2, 'Child', 1)")
	require.NoError(t, err)
	_, err = srcClient.GetDB().ExecContext(ctx, "INSERT INTO public.categories VALUES (3, 'Grandchild', 2)")
	require.NoError(t, err)

	dumpFile, err := Dump(ctx, srcClient, 100)
	require.NoError(t, err)

	// Load into fresh target
	targetClient, err := db.GetShadowDB(ctx, setupSQL...)
	require.NoError(t, err)
	defer targetClient.Close()

	result, err := Load(ctx, targetClient, dumpFile, LoadOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, result.TablesLoaded)

	// Verify tree structure
	var parentID *int
	err = targetClient.GetDB().QueryRowContext(ctx, "SELECT parent_id FROM public.categories WHERE id = 2").Scan(&parentID)
	require.NoError(t, err)
	require.NotNil(t, parentID)
	assert.Equal(t, 1, *parentID)

	err = targetClient.GetDB().QueryRowContext(ctx, "SELECT parent_id FROM public.categories WHERE id = 3").Scan(&parentID)
	require.NoError(t, err)
	require.NotNil(t, parentID)
	assert.Equal(t, 2, *parentID)
}

func TestCompatibilityError(t *testing.T) {
	t.Parallel()

	err := &CompatibilityError{
		Issues: []CompatibilityIssue{
			{Table: "public.users", Severity: "error", Description: "Table missing"},
			{Table: "public.users", Column: "name", Severity: "warning", Description: "Type differs"},
		},
	}

	msg := err.Error()
	assert.Contains(t, msg, "Table missing")
	assert.Contains(t, msg, "Type differs")
	assert.Contains(t, msg, "[error]")
	assert.Contains(t, msg, "[warning]")
}
