package migration

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		want    *Header
		wantErr bool
	}{
		{
			name: "sync mode",
			sql:  "-- scurry:mode=sync\nCREATE TABLE t (id INT);",
			want: &Header{Mode: ModeSync},
		},
		{
			name: "async mode",
			sql:  "-- scurry:mode=async\nCREATE INDEX idx ON t (col);",
			want: &Header{Mode: ModeAsync},
		},
		{
			name: "async with single dependency",
			sql:  "-- scurry:mode=async,depends_on=20251115211817_foo\nALTER TABLE t ADD COLUMN x INT;",
			want: &Header{Mode: ModeAsync, DependsOn: []string{"20251115211817_foo"}},
		},
		{
			name: "async with multiple dependencies",
			sql:  "-- scurry:mode=async,depends_on=20251115211817_foo;20251116125806_bar\nALTER TABLE t ADD COLUMN x INT;",
			want: &Header{Mode: ModeAsync, DependsOn: []string{"20251115211817_foo", "20251116125806_bar"}},
		},
		{
			name: "no header",
			sql:  "CREATE TABLE t (id INT);",
			want: nil,
		},
		{
			name: "empty string",
			sql:  "",
			want: nil,
		},
		{
			name:    "invalid mode",
			sql:     "-- scurry:mode=fast\nCREATE TABLE t (id INT);",
			wantErr: true,
		},
		{
			name:    "missing mode",
			sql:     "-- scurry:depends_on=foo\nCREATE TABLE t (id INT);",
			wantErr: true,
		},
		{
			name:    "invalid field format",
			sql:     "-- scurry:badfield\nCREATE TABLE t (id INT);",
			wantErr: true,
		},
		{
			name:    "unknown field",
			sql:     "-- scurry:mode=sync,unknown=value\nCREATE TABLE t (id INT);",
			wantErr: true,
		},
		{
			name:    "empty depends_on value",
			sql:     "-- scurry:mode=async,depends_on=\nCREATE TABLE t (id INT);",
			wantErr: true,
		},
		{
			name: "header only no trailing newline",
			sql:  "-- scurry:mode=sync",
			want: &Header{Mode: ModeSync},
		},
		{
			name: "sync with squash",
			sql:  "-- scurry:mode=sync,squash=true\nCREATE TABLE t (id INT);",
			want: &Header{Mode: ModeSync, Squash: true},
		},
		{
			name:    "squash with invalid value",
			sql:     "-- scurry:mode=sync,squash=false\nCREATE TABLE t (id INT);",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseHeader(tt.sql)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFormatHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header *Header
		want   string
	}{
		{
			name:   "sync mode",
			header: &Header{Mode: ModeSync},
			want:   "-- scurry:mode=sync",
		},
		{
			name:   "async mode no deps",
			header: &Header{Mode: ModeAsync},
			want:   "-- scurry:mode=async",
		},
		{
			name:   "async with single dep",
			header: &Header{Mode: ModeAsync, DependsOn: []string{"20251115211817_foo"}},
			want:   "-- scurry:mode=async,depends_on=20251115211817_foo",
		},
		{
			name:   "async with multiple deps",
			header: &Header{Mode: ModeAsync, DependsOn: []string{"20251115211817_foo", "20251116125806_bar"}},
			want:   "-- scurry:mode=async,depends_on=20251115211817_foo;20251116125806_bar",
		},
		{
			name:   "sync with squash",
			header: &Header{Mode: ModeSync, Squash: true},
			want:   "-- scurry:mode=sync,squash=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := FormatHeader(tt.header)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStripHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "with sync header",
			sql:  "-- scurry:mode=sync\nCREATE TABLE t (id INT);",
			want: "CREATE TABLE t (id INT);",
		},
		{
			name: "with async header and deps",
			sql:  "-- scurry:mode=async,depends_on=foo;bar\nCREATE INDEX idx ON t (col);",
			want: "CREATE INDEX idx ON t (col);",
		},
		{
			name: "no header",
			sql:  "CREATE TABLE t (id INT);",
			want: "CREATE TABLE t (id INT);",
		},
		{
			name: "empty string",
			sql:  "",
			want: "",
		},
		{
			name: "header only no trailing content",
			sql:  "-- scurry:mode=sync",
			want: "",
		},
		{
			name: "regular SQL comment not a header",
			sql:  "-- this is a comment\nCREATE TABLE t (id INT);",
			want: "-- this is a comment\nCREATE TABLE t (id INT);",
		},
		{
			name: "multiline SQL after header",
			sql:  "-- scurry:mode=sync\nCREATE TABLE t (\n  id INT\n);",
			want: "CREATE TABLE t (\n  id INT\n);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := StripHeader(tt.sql)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPrependHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sql    string
		header *Header
		want   string
	}{
		{
			name:   "add header to headerless SQL",
			sql:    "CREATE TABLE t (id INT);",
			header: &Header{Mode: ModeSync},
			want:   "-- scurry:mode=sync\nCREATE TABLE t (id INT);",
		},
		{
			name:   "replace existing header",
			sql:    "-- scurry:mode=sync\nCREATE TABLE t (id INT);",
			header: &Header{Mode: ModeAsync, DependsOn: []string{"foo"}},
			want:   "-- scurry:mode=async,depends_on=foo\nCREATE TABLE t (id INT);",
		},
		{
			name:   "add async header with deps",
			sql:    "ALTER TABLE t ADD COLUMN x INT;",
			header: &Header{Mode: ModeAsync, DependsOn: []string{"a", "b"}},
			want:   "-- scurry:mode=async,depends_on=a;b\nALTER TABLE t ADD COLUMN x INT;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := PrependHeader(tt.sql, tt.header)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFormatThenParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header *Header
	}{
		{
			name:   "sync round-trip",
			header: &Header{Mode: ModeSync},
		},
		{
			name:   "async with deps round-trip",
			header: &Header{Mode: ModeAsync, DependsOn: []string{"20251115211817_foo", "20251116125806_bar"}},
		},
		{
			name:   "sync squash round-trip",
			header: &Header{Mode: ModeSync, Squash: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			formatted := FormatHeader(tt.header) + "\nSELECT 1;"
			parsed, err := ParseHeader(formatted)
			require.NoError(t, err)
			assert.Equal(t, tt.header.Mode, parsed.Mode)
			assert.Equal(t, tt.header.Squash, parsed.Squash)
			if len(tt.header.DependsOn) > 0 {
				assert.Equal(t, tt.header.DependsOn, parsed.DependsOn)
			}
		})
	}
}

func TestHeaderSignature(t *testing.T) {
	t.Parallel()

	body := "CREATE TABLE users (id INT PRIMARY KEY);\n"

	t.Run("sign then verify round-trips through format/parse", func(t *testing.T) {
		t.Parallel()
		h := &Header{Mode: ModeAsync, DependsOn: []string{"20250101000000_a"}}
		require.NoError(t, SignHeader(h, body))
		require.NotEmpty(t, h.Sig)

		parsed, err := ParseHeader(FormatHeader(h) + "\n" + body)
		require.NoError(t, err)
		assert.Equal(t, h.Sig, parsed.Sig)
		sig, err := ComputeSig(parsed, body)
		require.NoError(t, err)
		assert.Equal(t, h.Sig, sig, "recomputed signature matches")
	})

	t.Run("editing the body breaks the signature", func(t *testing.T) {
		t.Parallel()
		h := &Header{Mode: ModeSync}
		require.NoError(t, SignHeader(h, body))
		sig, err := ComputeSig(h, body+"ALTER TABLE users ADD COLUMN x INT;\n")
		require.NoError(t, err)
		assert.NotEqual(t, h.Sig, sig)
	})

	t.Run("flipping the mode breaks the signature", func(t *testing.T) {
		t.Parallel()
		h := &Header{Mode: ModeSync}
		require.NoError(t, SignHeader(h, body))
		// An agent flips mode to async but keeps the old signature.
		forged := &Header{Mode: ModeAsync, Sig: h.Sig}
		sig, err := ComputeSig(forged, body)
		require.NoError(t, err)
		assert.NotEqual(t, forged.Sig, sig)
	})

	t.Run("signature is independent of dependency order", func(t *testing.T) {
		t.Parallel()
		a := &Header{Mode: ModeSync, DependsOn: []string{"x", "y"}}
		b := &Header{Mode: ModeSync, DependsOn: []string{"y", "x"}}
		aSig, err := ComputeSig(a, body)
		require.NoError(t, err)
		bSig, err := ComputeSig(b, body)
		require.NoError(t, err)
		assert.Equal(t, aSig, bSig)
	})

	t.Run("signature is independent of SQL formatting", func(t *testing.T) {
		t.Parallel()
		h := &Header{Mode: ModeSync}
		unformatted := "create table users(id int primary key);\r\n"
		formatted := "CREATE TABLE users (\n  id INT PRIMARY KEY\n);\n"

		unformattedSig, err := ComputeSig(h, unformatted)
		require.NoError(t, err)
		formattedSig, err := ComputeSig(h, formatted)
		require.NoError(t, err)
		assert.Equal(t, unformattedSig, formattedSig)
	})

	t.Run("semantic differences still change the signature", func(t *testing.T) {
		t.Parallel()
		h := &Header{Mode: ModeSync}
		aSig, err := ComputeSig(h, "INSERT INTO messages VALUES ('hello world');")
		require.NoError(t, err)
		bSig, err := ComputeSig(h, "INSERT INTO messages VALUES ('helloworld');")
		require.NoError(t, err)
		assert.NotEqual(t, aSig, bSig)
	})

	t.Run("invalid SQL cannot be signed", func(t *testing.T) {
		t.Parallel()
		h := &Header{Mode: ModeSync}
		err := SignHeader(h, "CREATE TABLE")
		require.Error(t, err)
		assert.Empty(t, h.Sig)
	})
}

func parseStatements(t *testing.T, sql string) []tree.Statement {
	t.Helper()
	parsed, err := parser.Parse(sql)
	require.NoError(t, err)
	stmts := make([]tree.Statement, len(parsed))
	for i, p := range parsed {
		stmts[i] = p.AST
	}
	return stmts
}

func TestFindDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		newSQL     string
		migrations []MigrationInfo
		want       []string
	}{
		{
			name:       "no existing migrations",
			newSQL:     "ALTER TABLE users ADD COLUMN email STRING;",
			migrations: nil,
			want:       nil,
		},
		{
			name:   "single dep: new ALTERs table that existing creates",
			newSQL: "ALTER TABLE users ADD COLUMN email STRING;",
			migrations: []MigrationInfo{
				{Name: "20250101000000_create_users", SQL: "CREATE TABLE users (id INT PRIMARY KEY);"},
			},
			want: []string{"20250101000000_create_users"},
		},
		{
			name:   "no overlap: new creates posts, existing creates users",
			newSQL: "CREATE TABLE posts (id INT PRIMARY KEY);",
			migrations: []MigrationInfo{
				{Name: "20250101000000_create_users", SQL: "CREATE TABLE users (id INT PRIMARY KEY);"},
			},
			want: nil,
		},
		{
			name:   "shared dependency: both touch users",
			newSQL: "ALTER TABLE users ADD COLUMN age INT;",
			migrations: []MigrationInfo{
				{Name: "20250101000000_create_users", SQL: "CREATE TABLE users (id INT PRIMARY KEY);"},
				{Name: "20250102000000_add_email", SQL: "ALTER TABLE users ADD COLUMN email STRING;"},
			},
			want: []string{"20250102000000_add_email"},
		},
		{
			name:   "multiple deps: new touches users and posts",
			newSQL: "ALTER TABLE users ADD COLUMN post_count INT; ALTER TABLE posts ADD COLUMN author_id INT;",
			migrations: []MigrationInfo{
				{Name: "20250101000000_create_users", SQL: "CREATE TABLE users (id INT PRIMARY KEY);"},
				{Name: "20250102000000_create_posts", SQL: "CREATE TABLE posts (id INT PRIMARY KEY);"},
			},
			want: []string{"20250102000000_create_posts", "20250101000000_create_users"},
		},
		{
			name:   "most recent wins: two migrations touch users",
			newSQL: "ALTER TABLE users ADD COLUMN age INT;",
			migrations: []MigrationInfo{
				{Name: "20250101000000_create_users", SQL: "CREATE TABLE users (id INT PRIMARY KEY);"},
				{Name: "20250102000000_add_email", SQL: "ALTER TABLE users ADD COLUMN email STRING;"},
				{Name: "20250103000000_create_posts", SQL: "CREATE TABLE posts (id INT PRIMARY KEY);"},
			},
			want: []string{"20250102000000_add_email"},
		},
		{
			name:   "unparseable SQL is gracefully skipped",
			newSQL: "ALTER TABLE users ADD COLUMN email STRING;",
			migrations: []MigrationInfo{
				{Name: "20250101000000_bad_sql", SQL: "THIS IS NOT VALID SQL !!!"},
				{Name: "20250102000000_create_users", SQL: "CREATE TABLE users (id INT PRIMARY KEY);"},
			},
			want: []string{"20250102000000_create_users"},
		},
		{
			name:   "migration with header is parsed correctly",
			newSQL: "ALTER TABLE users ADD COLUMN email STRING;",
			migrations: []MigrationInfo{
				{Name: "20250101000000_create_users", SQL: "-- scurry:mode=sync\nCREATE TABLE users (id INT PRIMARY KEY);"},
			},
			want: []string{"20250101000000_create_users"},
		},
		{
			name:   "index on table counts as overlap",
			newSQL: "CREATE INDEX idx_users_email ON users (email);",
			migrations: []MigrationInfo{
				{Name: "20250101000000_create_users", SQL: "CREATE TABLE users (id INT PRIMARY KEY, email STRING);"},
			},
			want: []string{"20250101000000_create_users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			newStmts := parseStatements(t, tt.newSQL)
			got := FindDependencies(newStmts, tt.migrations)
			assert.Equal(t, tt.want, got)
		})
	}
}
