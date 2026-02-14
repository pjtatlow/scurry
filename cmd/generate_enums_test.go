package cmd

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoGenerateEnums(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		files         map[string]string
		expectedCount int
		expectedFiles map[string]string
		wantErr       bool
	}{
		{
			name: "single enum type",
			files: map[string]string{
				"definitions/types.sql": "CREATE TYPE user_status AS ENUM ('active', 'inactive', 'pending_review');",
			},
			expectedCount: 1,
			expectedFiles: map[string]string{
				"output/user-status.ts": `export enum UserStatus {
  Active = "active",
  Inactive = "inactive",
  PendingReview = "pending_review",
}
`,
			},
		},
		{
			name: "multiple enums across files",
			files: map[string]string{
				"definitions/types.sql":  "CREATE TYPE color AS ENUM ('red', 'green', 'blue');",
				"definitions/status.sql": "CREATE TYPE order_status AS ENUM ('pending', 'shipped', 'delivered');",
			},
			expectedCount: 2,
			expectedFiles: map[string]string{
				"output/color.ts": `export enum Color {
  Red = "red",
  Green = "green",
  Blue = "blue",
}
`,
				"output/order-status.ts": `export enum OrderStatus {
  Pending = "pending",
  Shipped = "shipped",
  Delivered = "delivered",
}
`,
			},
		},
		{
			name: "skips non-enum types and tables",
			files: map[string]string{
				"definitions/schema.sql": `CREATE TYPE user_status AS ENUM ('active', 'inactive');
CREATE TABLE users (id UUID PRIMARY KEY, status user_status);`,
			},
			expectedCount: 1,
			expectedFiles: map[string]string{
				"output/user-status.ts": `export enum UserStatus {
  Active = "active",
  Inactive = "inactive",
}
`,
			},
		},
		{
			name: "skips non-sql files",
			files: map[string]string{
				"definitions/types.sql": "CREATE TYPE color AS ENUM ('red', 'blue');",
				"definitions/readme.md": "# Schema definitions",
			},
			expectedCount: 1,
			expectedFiles: map[string]string{
				"output/color.ts": `export enum Color {
  Red = "red",
  Blue = "blue",
}
`,
			},
		},
		{
			name: "no enum types found",
			files: map[string]string{
				"definitions/tables.sql": "CREATE TABLE users (id UUID PRIMARY KEY);",
			},
			expectedCount: 0,
			expectedFiles: map[string]string{},
		},
		{
			name: "invalid SQL",
			files: map[string]string{
				"definitions/bad.sql": "NOT VALID SQL AT ALL",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewMemMapFs()
			for path, content := range tt.files {
				require.NoError(t, afero.WriteFile(fs, path, []byte(content), 0644))
			}

			count, err := doGenerateEnums(fs, "definitions", "output", "ts")
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)

			for path, expectedContent := range tt.expectedFiles {
				content, err := afero.ReadFile(fs, path)
				require.NoError(t, err, "expected file %s to exist", path)
				assert.Equal(t, expectedContent, string(content))
			}
		})
	}
}
