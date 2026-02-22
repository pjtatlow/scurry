package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sql       string
		want      *Header
		wantErr   bool
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
			name: "header only no trailing newline",
			sql:  "-- scurry:mode=sync",
			want: &Header{Mode: ModeSync},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			formatted := FormatHeader(tt.header) + "\nSELECT 1;"
			parsed, err := ParseHeader(formatted)
			require.NoError(t, err)
			assert.Equal(t, tt.header.Mode, parsed.Mode)
			if len(tt.header.DependsOn) > 0 {
				assert.Equal(t, tt.header.DependsOn, parsed.DependsOn)
			}
		})
	}
}
