package migration

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadTableSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		content    string
		writeFile  bool
		want       *TableSizes
		wantErr    bool
	}{
		{
			name:      "valid file",
			writeFile: true,
			content: `threshold: 100000
tables:
  public.posts:
    rows: 15000000
    size_bytes: 2300000000
  public.users:
    rows: 500000
    size_bytes: 150000000
`,
			want: &TableSizes{
				Threshold: 100000,
				Tables: map[string]TableInfo{
					"public.posts": {Rows: 15000000, SizeBytes: 2300000000},
					"public.users": {Rows: 500000, SizeBytes: 150000000},
				},
			},
		},
		{
			name: "missing file",
			want: nil,
		},
		{
			name:      "invalid yaml",
			writeFile: true,
			content:   "::invalid[yaml",
			wantErr:   true,
		},
		{
			name:      "empty file",
			writeFile: true,
			content:   "",
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs := afero.NewMemMapFs()
			err := fs.MkdirAll("/migrations", 0755)
			require.NoError(t, err)

			if tt.writeFile {
				err = afero.WriteFile(fs, "/migrations/table_sizes.yaml", []byte(tt.content), 0644)
				require.NoError(t, err)
			}

			got, err := LoadTableSizes(fs, "/migrations")
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSaveTableSizes(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/migrations", 0755)
	require.NoError(t, err)

	ts := &TableSizes{
		Threshold: 50000,
		Tables: map[string]TableInfo{
			"public.users": {Rows: 100000, SizeBytes: 500000},
		},
	}

	err = SaveTableSizes(fs, "/migrations", ts)
	require.NoError(t, err)

	// Load it back and verify
	loaded, err := LoadTableSizes(fs, "/migrations")
	require.NoError(t, err)
	assert.Equal(t, ts.Threshold, loaded.Threshold)
	assert.Equal(t, ts.Tables, loaded.Tables)
}

func TestIsLargeTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ts        *TableSizes
		tableName string
		want      bool
	}{
		{
			name:      "nil receiver",
			ts:        nil,
			tableName: "public.users",
			want:      false,
		},
		{
			name: "table not found",
			ts: &TableSizes{
				Threshold: 100000,
				Tables:    map[string]TableInfo{},
			},
			tableName: "public.users",
			want:      false,
		},
		{
			name: "below threshold",
			ts: &TableSizes{
				Threshold: 100000,
				Tables: map[string]TableInfo{
					"public.users": {Rows: 99999},
				},
			},
			tableName: "public.users",
			want:      false,
		},
		{
			name: "at threshold",
			ts: &TableSizes{
				Threshold: 100000,
				Tables: map[string]TableInfo{
					"public.users": {Rows: 100000},
				},
			},
			tableName: "public.users",
			want:      true,
		},
		{
			name: "above threshold",
			ts: &TableSizes{
				Threshold: 100000,
				Tables: map[string]TableInfo{
					"public.posts": {Rows: 15000000},
				},
			},
			tableName: "public.posts",
			want:      true,
		},
		{
			name: "zero threshold uses default",
			ts: &TableSizes{
				Threshold: 0,
				Tables: map[string]TableInfo{
					"public.posts": {Rows: 100000},
				},
			},
			tableName: "public.posts",
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.ts.IsLargeTable(tt.tableName)
			assert.Equal(t, tt.want, got)
		})
	}
}
