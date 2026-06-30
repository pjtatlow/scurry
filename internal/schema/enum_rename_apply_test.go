package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
)

// loadFromDDL applies DDL to a fresh shadow database and loads the standardized
// schema back out. The returned client stays open (closed at test end) so
// callers can seed and inspect data.
func loadFromDDL(t *testing.T, ctx context.Context, ddl ...string) (*Schema, *db.Client) {
	t.Helper()
	client, err := db.GetShadowDB(ctx, ddl...)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	s, err := LoadFromDatabase(ctx, client)
	require.NoError(t, err)
	return s, client
}

func showCreateTable(t *testing.T, ctx context.Context, client *db.Client, table string) string {
	t.Helper()
	var name, create string
	require.NoError(t, client.GetDB().QueryRowContext(ctx, "SHOW CREATE TABLE "+table).Scan(&name, &create))
	return create
}

// TestApply_EnumRename_PreservesData proves that a detected pure enum rename
// applies against a live CockroachDB and preserves existing rows — the case
// that previously failed with `pq: invalid cast: <oldEnum> -> <newEnum>`.
func TestApply_EnumRename_PreservesData(t *testing.T) {
	ctx := context.Background()

	// Remote (current DB): the old enum, a NOT NULL column of that enum, a row.
	_, remoteClient := loadFromDDL(t, ctx,
		`CREATE TYPE assessment_kind AS ENUM ('refund', 'exchange')`,
		`CREATE TABLE returns (id INT8 NOT NULL, kind assessment_kind NOT NULL, CONSTRAINT returns_pkey PRIMARY KEY (id))`,
	)
	_, err := remoteClient.ExecContext(ctx, `INSERT INTO returns (id, kind) VALUES (1, 'exchange')`)
	require.NoError(t, err)
	remoteSchema, err := LoadFromDatabase(ctx, remoteClient)
	require.NoError(t, err)

	// Local (desired): identical labels, new name. Same column, new type.
	localSchema, _ := loadFromDDL(t, ctx,
		`CREATE TYPE return_type_enum AS ENUM ('refund', 'exchange')`,
		`CREATE TABLE returns (id INT8 NOT NULL, kind return_type_enum NOT NULL, CONSTRAINT returns_pkey PRIMARY KEY (id))`,
	)

	// The migration must be the single safe ALTER TYPE ... RENAME.
	result := Compare(localSchema, remoteSchema)
	ddl, _, err := result.GenerateMigrations(false)
	require.NoError(t, err)
	require.Len(t, ddl, 1, "expected exactly one statement, got:\n%s", strings.Join(ddl, "\n"))
	assert.Contains(t, ddl[0], "RENAME TO return_type_enum")

	require.NoError(t, remoteClient.ExecuteBulkDDL(ctx, ddl...), "rename must apply cleanly")

	// Row preserved under the new type.
	var kind string
	require.NoError(t, remoteClient.GetDB().QueryRowContext(ctx, `SELECT kind FROM returns WHERE id = 1`).Scan(&kind))
	assert.Equal(t, "exchange", kind)

	create := showCreateTable(t, ctx, remoteClient, "returns")
	assert.Contains(t, create, "return_type_enum", "column should now use the renamed type")
	assert.NotContains(t, create, "assessment_kind", "old type name should be gone")
}

// TestApply_EnumRetarget_TextBridge proves the #3 text-bridge cast applies
// against a live DB: a column retargeted to a different, superset-labeled enum
// (not a rename) is rewritten via `col::STRING::newType`, which CRDB accepts
// where a direct enum->enum cast would be rejected.
func TestApply_EnumRetarget_TextBridge(t *testing.T) {
	ctx := context.Background()

	_, remoteClient := loadFromDDL(t, ctx,
		`CREATE TYPE content_generation_target AS ENUM ('email', 'sms')`,
		`CREATE TABLE jobs (id INT8 NOT NULL, target content_generation_target NOT NULL, CONSTRAINT jobs_pkey PRIMARY KEY (id))`,
	)
	_, err := remoteClient.ExecContext(ctx, `INSERT INTO jobs (id, target) VALUES (1, 'email')`)
	require.NoError(t, err)
	remoteSchema, err := LoadFromDatabase(ctx, remoteClient)
	require.NoError(t, err)

	// Local: a differently-named, superset enum (adds 'push'); old type kept so
	// the test isolates the bridge cast from unrelated DROP TYPE ordering.
	localSchema, _ := loadFromDDL(t, ctx,
		`CREATE TYPE content_generation_target AS ENUM ('email', 'sms')`,
		`CREATE TYPE content_generation_target_v2 AS ENUM ('email', 'sms', 'push')`,
		`CREATE TABLE jobs (id INT8 NOT NULL, target content_generation_target_v2 NOT NULL, CONSTRAINT jobs_pkey PRIMARY KEY (id))`,
	)

	result := Compare(localSchema, remoteSchema)
	ddl, _, err := result.GenerateMigrations(false)
	require.NoError(t, err)
	joined := strings.Join(ddl, "\n")
	assert.Contains(t, joined, "::STRING::", "retarget must bridge through text\n%s", joined)
	assert.NotContains(t, joined, "RENAME TO", "superset labels are not a rename")

	require.NoError(t, remoteClient.ExecuteBulkDDL(ctx, ddl...), "text-bridge migration must apply cleanly")

	var target string
	require.NoError(t, remoteClient.GetDB().QueryRowContext(ctx, `SELECT target FROM jobs WHERE id = 1`).Scan(&target))
	assert.Equal(t, "email", target)

	create := showCreateTable(t, ctx, remoteClient, "jobs")
	assert.Contains(t, create, "content_generation_target_v2", "column should now use the new enum")
}
