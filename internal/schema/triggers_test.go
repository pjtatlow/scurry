package schema

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
)

const (
	triggerTableSQL = "CREATE TABLE t (id INT8 NOT NULL, touched BOOL NOT NULL DEFAULT false, n INT8, CONSTRAINT t_pkey PRIMARY KEY (id))"
	triggerFuncSQL  = "CREATE FUNCTION touch() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$ BEGIN NEW.touched := true; RETURN NEW; END $$"
	triggerSQL      = "CREATE TRIGGER trg_touch BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION touch()"
)

func diffTypes(diffs []Difference) []DiffType {
	types := make([]DiffType, 0, len(diffs))
	for _, d := range diffs {
		types = append(types, d.Type)
	}
	slices.Sort(types)
	return types
}

func indexContaining(t *testing.T, ddl []string, substr string) int {
	t.Helper()
	for i, s := range ddl {
		if strings.Contains(s, substr) {
			return i
		}
	}
	t.Fatalf("no statement contains %q in:\n%s", substr, strings.Join(ddl, "\n"))
	return -1
}

func TestCompareTriggers(t *testing.T) {
	tests := []struct {
		name          string
		local         []string
		remote        []string
		wantDiffTypes []DiffType
	}{
		{
			name:   "no differences",
			local:  []string{triggerTableSQL, triggerFuncSQL, triggerSQL},
			remote: []string{triggerTableSQL, triggerFuncSQL, triggerSQL},
		},
		{
			name:          "trigger added",
			local:         []string{triggerTableSQL, triggerFuncSQL, triggerSQL},
			remote:        []string{triggerTableSQL, triggerFuncSQL},
			wantDiffTypes: []DiffType{DiffTypeTriggerAdded},
		},
		{
			name:          "trigger removed",
			local:         []string{triggerTableSQL, triggerFuncSQL},
			remote:        []string{triggerTableSQL, triggerFuncSQL, triggerSQL},
			wantDiffTypes: []DiffType{DiffTypeTriggerRemoved},
		},
		{
			name:          "trigger modified",
			local:         []string{triggerTableSQL, triggerFuncSQL, "CREATE TRIGGER trg_touch BEFORE INSERT OR UPDATE ON t FOR EACH ROW EXECUTE FUNCTION touch()"},
			remote:        []string{triggerTableSQL, triggerFuncSQL, triggerSQL},
			wantDiffTypes: []DiffType{DiffTypeTriggerModified},
		},
		{
			name:   "catalog-qualified names from the database compare equal to local names",
			local:  []string{triggerTableSQL, triggerFuncSQL, triggerSQL},
			remote: []string{triggerTableSQL, triggerFuncSQL, "CREATE TRIGGER trg_touch BEFORE INSERT ON _shadow_abc.public.t FOR EACH ROW EXECUTE FUNCTION _shadow_abc.public.touch()"},
		},
		{
			name: "same trigger name on different tables are distinct",
			local: []string{
				triggerTableSQL, triggerFuncSQL, triggerSQL,
				"CREATE TABLE u (id INT8 NOT NULL, touched BOOL NOT NULL DEFAULT false, CONSTRAINT u_pkey PRIMARY KEY (id))",
				"CREATE TRIGGER trg_touch BEFORE INSERT ON u FOR EACH ROW EXECUTE FUNCTION touch()",
			},
			remote:        []string{triggerTableSQL, triggerFuncSQL, triggerSQL},
			wantDiffTypes: []DiffType{DiffTypeTableAdded, DiffTypeTriggerAdded},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := NewSchema(parseStatements(tt.local...)...)
			remote := NewSchema(parseStatements(tt.remote...)...)
			diffs := Compare(local, remote).Differences
			assert.ElementsMatch(t, tt.wantDiffTypes, diffTypes(diffs))
		})
	}
}

func TestModifiedTriggerFunctionRebuildsTriggers(t *testing.T) {
	local := NewSchema(parseStatements(
		triggerTableSQL,
		"CREATE FUNCTION touch() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$ BEGIN NEW.touched := true; NEW.n := 1; RETURN NEW; END $$",
		triggerSQL,
	)...)
	remote := NewSchema(parseStatements(triggerTableSQL, triggerFuncSQL, triggerSQL)...)

	diffs := Compare(local, remote).Differences
	require.Len(t, diffs, 1)
	assert.Equal(t, DiffTypeRoutineModified, diffs[0].Type)

	stmts := diffs[0].MigrationStatements
	require.Len(t, stmts, 5)
	_, isCommit := stmts[0].(*tree.CommitTransaction)
	_, isDrop := stmts[1].(*tree.DropTrigger)
	replace, isReplace := stmts[2].(*tree.CreateRoutine)
	_, isCreate := stmts[3].(*tree.CreateTrigger)
	_, isBegin := stmts[4].(*tree.BeginTransaction)
	assert.True(t, isCommit && isBegin, "trigger statements must run outside the migration transaction")
	assert.True(t, isDrop, "first statement should drop the trigger: %s", stmts[1])
	assert.True(t, isReplace && replace.Replace, "second statement should replace the function: %s", stmts[2])
	assert.True(t, isCreate, "third statement should re-create the trigger: %s", stmts[3])
}

func TestTriggerSwitchedToModifiedFunctionDropsOldTrigger(t *testing.T) {
	local := NewSchema(parseStatements(
		triggerTableSQL,
		"CREATE FUNCTION touch() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$ BEGIN NEW.touched := true; NEW.n := 1; RETURN NEW; END $$",
		"CREATE FUNCTION other() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$ BEGIN RETURN NEW; END $$",
		triggerSQL,
	)...)
	remote := NewSchema(parseStatements(
		triggerTableSQL,
		triggerFuncSQL,
		"CREATE FUNCTION other() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$ BEGIN RETURN NEW; END $$",
		"CREATE TRIGGER trg_touch BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION other()",
	)...)

	diffs := Compare(local, remote).Differences
	require.Len(t, diffs, 1)
	ddl, _, err := Compare(local, remote).GenerateMigrations(false)
	require.NoError(t, err)
	assert.Less(t, indexContaining(t, ddl, "DROP TRIGGER"), indexContaining(t, ddl, "CREATE TRIGGER"))
	assert.Less(t, indexContaining(t, ddl, "DROP TRIGGER"), indexContaining(t, ddl, "CREATE OR REPLACE FUNCTION"))
}

func TestTriggerMigrationOrdering(t *testing.T) {
	t.Run("new trigger runs after its table and function", func(t *testing.T) {
		local := NewSchema(parseStatements(triggerTableSQL, triggerFuncSQL, triggerSQL)...)
		ddl, _, err := Compare(local, NewSchema()).GenerateMigrations(false)
		require.NoError(t, err)
		trigger := indexContaining(t, ddl, "CREATE TRIGGER")
		assert.Greater(t, trigger, indexContaining(t, ddl, "CREATE TABLE"))
		assert.Greater(t, trigger, indexContaining(t, ddl, "CREATE FUNCTION"))
	})

	t.Run("removed trigger drops before its function and table", func(t *testing.T) {
		remote := NewSchema(parseStatements(triggerTableSQL, triggerFuncSQL, triggerSQL)...)
		ddl, _, err := Compare(NewSchema(), remote).GenerateMigrations(false)
		require.NoError(t, err)
		trigger := indexContaining(t, ddl, "DROP TRIGGER")
		assert.Less(t, trigger, indexContaining(t, ddl, "DROP FUNCTION"))
		assert.Less(t, trigger, indexContaining(t, ddl, "DROP TABLE"))
	})
}

// applyDefinitions pushes definition statements into a fresh shadow database the
// way LoadFromDirectories does and loads the standardized schema back out
func applyDefinitions(t *testing.T, ctx context.Context, ddl ...string) (*Schema, *db.Client) {
	t.Helper()
	client, err := db.GetShadowDB(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	statements, _, err := Compare(NewSchema(parseStatements(ddl...)...), NewSchema()).GenerateMigrations(false)
	require.NoError(t, err)
	require.NoError(t, client.ExecuteBulkDDL(ctx, statements...), "definitions must apply cleanly:\n%s", strings.Join(statements, "\n"))
	s, err := LoadFromDatabase(ctx, client)
	require.NoError(t, err)
	return s, client
}

func TestLoadFromDatabase_Triggers(t *testing.T) {
	ctx := context.Background()

	first, _ := applyDefinitions(t, ctx, triggerTableSQL, triggerFuncSQL, triggerSQL)
	require.Len(t, first.Triggers, 1)
	assert.Equal(t, "trg_touch", first.Triggers[0].Name)
	assert.Equal(t, "public", first.Triggers[0].Schema)
	assert.Equal(t, "public.t.trg_touch", getTriggerKey(first.Triggers[0].Ast))
	assert.False(t, first.Triggers[0].Ast.TableName.HasExplicitCatalog())
	assert.Equal(t, 2, first.Triggers[0].Ast.FuncName.NumParts)

	second, _ := applyDefinitions(t, ctx, triggerTableSQL, triggerFuncSQL, triggerSQL)
	assert.False(t, Compare(first, second).HasChanges(), "the same schema loaded from two databases must not differ")
}

func TestApply_TriggerFunctionModified(t *testing.T) {
	ctx := context.Background()

	remoteSchema, remoteClient := applyDefinitions(t, ctx, triggerTableSQL, triggerFuncSQL, triggerSQL)
	localSchema, _ := applyDefinitions(t, ctx,
		triggerTableSQL,
		"CREATE FUNCTION touch() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$ BEGIN NEW.touched := true; NEW.n := 42; RETURN NEW; END $$",
		triggerSQL,
	)

	ddl, _, err := Compare(localSchema, remoteSchema).GenerateMigrations(false)
	require.NoError(t, err)
	require.NoError(t, remoteClient.ExecuteBulkDDL(ctx, ddl...), "replacing a trigger function must apply cleanly:\n%s", strings.Join(ddl, "\n"))

	_, err = remoteClient.ExecContext(ctx, "INSERT INTO t (id) VALUES (1)")
	require.NoError(t, err)
	var touched bool
	var n int64
	require.NoError(t, remoteClient.GetDB().QueryRowContext(ctx, "SELECT touched, n FROM t WHERE id = 1").Scan(&touched, &n))
	assert.True(t, touched)
	assert.Equal(t, int64(42), n, "the re-created trigger should run the new function body")

	applied, err := LoadFromDatabase(ctx, remoteClient)
	require.NoError(t, err)
	assert.False(t, Compare(localSchema, applied).HasChanges())
}

func TestApply_TriggerAndFunctionRemoved(t *testing.T) {
	ctx := context.Background()

	remoteSchema, remoteClient := applyDefinitions(t, ctx, triggerTableSQL, triggerFuncSQL, triggerSQL)
	localSchema, _ := applyDefinitions(t, ctx, triggerTableSQL)

	ddl, _, err := Compare(localSchema, remoteSchema).GenerateMigrations(false)
	require.NoError(t, err)
	require.NoError(t, remoteClient.ExecuteBulkDDL(ctx, ddl...), "dropping a trigger with its function must apply cleanly:\n%s", strings.Join(ddl, "\n"))

	applied, err := LoadFromDatabase(ctx, remoteClient)
	require.NoError(t, err)
	assert.Empty(t, applied.Triggers)
	assert.Empty(t, applied.Routines)
}
