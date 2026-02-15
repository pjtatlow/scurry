package data

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDumpFileRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dump DumpFile
	}{
		{
			name: "basic dump with data",
			dump: DumpFile{
				Version:   1,
				CreatedAt: time.Date(2026, 2, 14, 10, 30, 0, 0, time.UTC),
				Tables:    []string{"public.users", "public.posts"},
				SchemaSQL: "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL);\nCREATE TABLE public.posts (id INT8 PRIMARY KEY, user_id INT8 REFERENCES users(id));",
				TableData: []TableDump{
					{
						QualifiedName: "public.users",
						RowCount:      2,
						Statements: []string{
							"INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES\n(1, 'Alice'),\n(2, 'Bob');",
						},
					},
					{
						QualifiedName: "public.posts",
						RowCount:      1,
						Statements: []string{
							"INSERT INTO \"public\".\"posts\" (\"id\", \"user_id\") VALUES\n(1, 1);",
						},
					},
				},
				Sequences: []SequenceValue{
					{QualifiedName: "public.users_id_seq", Value: 2},
				},
			},
		},
		{
			name: "empty tables",
			dump: DumpFile{
				Version:   1,
				CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				Tables:    []string{"public.empty"},
				SchemaSQL: "CREATE TABLE public.empty (id INT8 PRIMARY KEY);",
				TableData: nil,
			},
		},
		{
			name: "special characters in strings",
			dump: DumpFile{
				Version:   1,
				CreatedAt: time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC),
				Tables:    []string{"public.data"},
				SchemaSQL: "CREATE TABLE public.data (id INT8 PRIMARY KEY, val STRING);",
				TableData: []TableDump{
					{
						QualifiedName: "public.data",
						RowCount:      2,
						Statements: []string{
							"INSERT INTO \"public\".\"data\" (\"id\", \"val\") VALUES\n(1, 'it''s a test'),\n(2, NULL);",
						},
					},
				},
			},
		},
		{
			name: "multiple batches",
			dump: DumpFile{
				Version:   1,
				CreatedAt: time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC),
				Tables:    []string{"public.items"},
				SchemaSQL: "CREATE TABLE public.items (id INT8 PRIMARY KEY);",
				TableData: []TableDump{
					{
						QualifiedName: "public.items",
						RowCount:      3,
						Statements: []string{
							"INSERT INTO \"public\".\"items\" (\"id\") VALUES\n(1),\n(2);",
							"INSERT INTO \"public\".\"items\" (\"id\") VALUES\n(3);",
						},
					},
				},
			},
		},
		{
			name: "no tables",
			dump: DumpFile{
				Version:   1,
				CreatedAt: time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC),
				Tables:    nil,
				SchemaSQL: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Write
			var buf strings.Builder
			err := tt.dump.Write(&buf)
			require.NoError(t, err)

			written := buf.String()

			// Parse back
			parsed, err := ParseDumpFile(strings.NewReader(written))
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.dump.Version, parsed.Version)
			assert.Equal(t, tt.dump.CreatedAt.UTC(), parsed.CreatedAt.UTC())
			assert.Equal(t, tt.dump.SchemaSQL, parsed.SchemaSQL)

			// Tables list
			if tt.dump.Tables == nil {
				// When tables is nil, we write empty string, which parses as nil
				assert.Empty(t, parsed.Tables)
			} else {
				assert.Equal(t, tt.dump.Tables, parsed.Tables)
			}

			// Table data
			assert.Equal(t, len(tt.dump.TableData), len(parsed.TableData))
			for i := range tt.dump.TableData {
				assert.Equal(t, tt.dump.TableData[i].QualifiedName, parsed.TableData[i].QualifiedName)
				assert.Equal(t, tt.dump.TableData[i].RowCount, parsed.TableData[i].RowCount)
				assert.Equal(t, tt.dump.TableData[i].Statements, parsed.TableData[i].Statements)
			}

			// Sequences
			assert.Equal(t, len(tt.dump.Sequences), len(parsed.Sequences))
			for i := range tt.dump.Sequences {
				assert.Equal(t, tt.dump.Sequences[i].QualifiedName, parsed.Sequences[i].QualifiedName)
				assert.Equal(t, tt.dump.Sequences[i].Value, parsed.Sequences[i].Value)
			}
		})
	}
}

func TestDumpFileGzipRoundTrip(t *testing.T) {
	t.Parallel()

	dump := DumpFile{
		Version:   1,
		CreatedAt: time.Date(2026, 2, 14, 10, 30, 0, 0, time.UTC),
		Tables:    []string{"public.users", "public.posts"},
		SchemaSQL: "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL);\nCREATE TABLE public.posts (id INT8 PRIMARY KEY, user_id INT8 REFERENCES users(id));",
		TableData: []TableDump{
			{
				QualifiedName: "public.users",
				RowCount:      2,
				Statements: []string{
					"INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES\n(1, 'Alice'),\n(2, 'Bob');",
				},
			},
			{
				QualifiedName: "public.posts",
				RowCount:      1,
				Statements: []string{
					"INSERT INTO \"public\".\"posts\" (\"id\", \"user_id\") VALUES\n(1, 1);",
				},
			},
		},
		Sequences: []SequenceValue{
			{QualifiedName: "public.users_id_seq", Value: 2},
		},
	}

	// Write through gzip
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	err := dump.Write(gzw)
	require.NoError(t, err)
	require.NoError(t, gzw.Close())

	// Verify it's actually compressed (not valid plain text)
	assert.NotContains(t, buf.String(), "scurry:data-dump")

	// Read back through gzip
	gzr, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	defer gzr.Close()

	parsed, err := ParseDumpFile(gzr)
	require.NoError(t, err)

	assert.Equal(t, dump.Version, parsed.Version)
	assert.Equal(t, dump.CreatedAt.UTC(), parsed.CreatedAt.UTC())
	assert.Equal(t, dump.Tables, parsed.Tables)
	assert.Equal(t, dump.SchemaSQL, parsed.SchemaSQL)
	assert.Equal(t, len(dump.TableData), len(parsed.TableData))
	for i := range dump.TableData {
		assert.Equal(t, dump.TableData[i].QualifiedName, parsed.TableData[i].QualifiedName)
		assert.Equal(t, dump.TableData[i].RowCount, parsed.TableData[i].RowCount)
		assert.Equal(t, dump.TableData[i].Statements, parsed.TableData[i].Statements)
	}
	assert.Equal(t, dump.Sequences, parsed.Sequences)
}

func TestParseDumpFileErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "empty file",
			input:   "",
			wantErr: "empty dump file",
		},
		{
			name:    "missing header marker",
			input:   "-- not a scurry dump\n",
			wantErr: "missing header marker",
		},
		{
			name:    "invalid version",
			input:   "-- scurry:data-dump\n-- version: abc\n",
			wantErr: "invalid version",
		},
		{
			name:    "missing schema section",
			input:   "-- scurry:data-dump\n-- version: 1\n-- created_at: 2026-02-14T10:30:00Z\n-- tables: public.users\n",
			wantErr: "missing schema section",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseDumpFile(strings.NewReader(tt.input))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
