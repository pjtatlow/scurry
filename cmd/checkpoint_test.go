package cmd

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/flags"
)

func TestComputeMigrationsHash(t *testing.T) {
	tests := []struct {
		name       string
		migrations []migration
		wantHash   string // only set for cases where we know the exact hash
	}{
		{
			name:       "empty migrations",
			migrations: []migration{},
			wantHash:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", // SHA256 of empty string
		},
		{
			name: "single migration",
			migrations: []migration{
				{name: "001_init", sql: "CREATE TABLE users (id INT PRIMARY KEY);"},
			},
		},
		{
			name: "multiple migrations concatenated",
			migrations: []migration{
				{name: "001_init", sql: "CREATE TABLE users (id INT);"},
				{name: "002_add_col", sql: "ALTER TABLE users ADD COLUMN name TEXT;"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := computeMigrationsHash(tt.migrations)
			assert.Len(t, hash, 64) // SHA-256 produces 64 hex characters

			// Verify determinism
			hash2 := computeMigrationsHash(tt.migrations)
			assert.Equal(t, hash, hash2)

			if tt.wantHash != "" {
				assert.Equal(t, tt.wantHash, hash)
			}
		})
	}
}

func TestComputeMigrationsHashDifferentContent(t *testing.T) {
	// Verify different content produces different hashes
	migrations1 := []migration{{name: "001", sql: "CREATE TABLE a (id INT);"}}
	migrations2 := []migration{{name: "001", sql: "CREATE TABLE b (id INT);"}}

	hash1 := computeMigrationsHash(migrations1)
	hash2 := computeMigrationsHash(migrations2)

	assert.NotEqual(t, hash1, hash2, "different content should produce different hashes")
}

func TestComputeMigrationsHashStripsHeaders(t *testing.T) {
	tests := []struct {
		name string
		sql1 string
		sql2 string
	}{
		{
			name: "header vs no header",
			sql1: "CREATE TABLE users (id INT PRIMARY KEY);",
			sql2: "-- scurry:mode=sync\nCREATE TABLE users (id INT PRIMARY KEY);",
		},
		{
			name: "different headers same SQL",
			sql1: "-- scurry:mode=sync\nCREATE TABLE users (id INT PRIMARY KEY);",
			sql2: "-- scurry:mode=async,depends_on=foo\nCREATE TABLE users (id INT PRIMARY KEY);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1 := computeMigrationsHash([]migration{{name: "001", sql: tt.sql1}})
			hash2 := computeMigrationsHash([]migration{{name: "001", sql: tt.sql2}})
			assert.Equal(t, hash1, hash2, "header changes should not affect migrations hash")
		})
	}
}

func TestComputeChecksumStripsHeaders(t *testing.T) {
	tests := []struct {
		name string
		sql1 string
		sql2 string
	}{
		{
			name: "header vs no header",
			sql1: "ALTER TABLE users ADD COLUMN name TEXT;",
			sql2: "-- scurry:mode=async\nALTER TABLE users ADD COLUMN name TEXT;",
		},
		{
			name: "different headers same SQL",
			sql1: "-- scurry:mode=sync\nALTER TABLE users ADD COLUMN name TEXT;",
			sql2: "-- scurry:mode=async,depends_on=foo;bar\nALTER TABLE users ADD COLUMN name TEXT;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checksum1 := computeChecksum(tt.sql1)
			checksum2 := computeChecksum(tt.sql2)
			assert.Equal(t, checksum1, checksum2, "header changes should not affect checksum")
		})
	}
}

func TestComputeContentHash(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		wantHash string // only set for cases where we know the exact hash
	}{
		{
			name:     "empty content",
			content:  "",
			wantHash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:    "simple content",
			content: "CREATE TABLE users (id INT);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := computeContentHash(tt.content)
			assert.Len(t, hash, 64) // SHA-256 produces 64 hex characters

			// Verify determinism
			hash2 := computeContentHash(tt.content)
			assert.Equal(t, hash, hash2)

			if tt.wantHash != "" {
				assert.Equal(t, tt.wantHash, hash)
			}
		})
	}
}

func TestFormatCheckpointHeader(t *testing.T) {
	tests := []struct {
		name           string
		migrationsHash string
		checkpointHash string
		want           string
	}{
		{
			name:           "valid hashes",
			migrationsHash: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
			checkpointHash: "f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2",
			want:           "-- scurry:migrations=a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2,checkpoint=f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatCheckpointHeader(tt.migrationsHash, tt.checkpointHash)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseCheckpointHeader(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		wantErr     bool
		wantMigHash string
		wantCpHash  string
	}{
		{
			name:        "valid header",
			line:        "-- scurry:migrations=a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2,checkpoint=f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2",
			wantErr:     false,
			wantMigHash: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
			wantCpHash:  "f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2",
		},
		{
			name:    "missing prefix",
			line:    "migrations=abc,checkpoint=def",
			wantErr: true,
		},
		{
			name:    "invalid hash length",
			line:    "-- scurry:migrations=abc,checkpoint=def",
			wantErr: true,
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: true,
		},
		{
			name:    "wrong prefix",
			line:    "-- other:migrations=a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2,checkpoint=f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2",
			wantErr: true,
		},
		{
			name:    "missing checkpoint",
			line:    "-- scurry:migrations=a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header, err := parseCheckpointHeader(tt.line)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantMigHash, header.MigrationsHash)
			assert.Equal(t, tt.wantCpHash, header.CheckpointHash)
		})
	}
}

func TestLoadCheckpoint(t *testing.T) {
	tests := []struct {
		name              string
		checkpointContent string
		wantErr           bool
		wantNil           bool
	}{
		{
			name:    "no checkpoint file",
			wantNil: true,
		},
		{
			name:              "valid checkpoint",
			checkpointContent: "-- scurry:migrations=a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2,checkpoint=f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2\nCREATE TABLE users (id INT);",
			wantErr:           false,
		},
		{
			name:              "invalid header",
			checkpointContent: "invalid header\nCREATE TABLE users (id INT);",
			wantErr:           true,
		},
		{
			name:              "valid checkpoint with empty schema",
			checkpointContent: "-- scurry:migrations=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855,checkpoint=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
			wantErr:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs := afero.NewMemMapFs()

			migDir := filepath.Join(flags.MigrationDir, "20240101000000_test")
			err := fs.MkdirAll(migDir, 0755)
			require.NoError(t, err)

			if tt.checkpointContent != "" {
				checkpointPath := filepath.Join(migDir, checkpointFileName)
				err = afero.WriteFile(fs, checkpointPath, []byte(tt.checkpointContent), 0644)
				require.NoError(t, err)
			}

			checkpoint, err := loadCheckpoint(fs, migDir)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, checkpoint)
				return
			}

			assert.NotNil(t, checkpoint)
			assert.Equal(t, "20240101000000_test", checkpoint.MigrationName)
		})
	}
}

func TestValidateCheckpoint(t *testing.T) {
	tests := []struct {
		name       string
		checkpoint *Checkpoint
		wantErr    bool
	}{
		{
			name: "valid checkpoint",
			checkpoint: func() *Checkpoint {
				content := "CREATE TABLE users (id INT);"
				hash := computeContentHash(content)
				return &Checkpoint{
					Header: CheckpointHeader{
						MigrationsHash: "abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1",
						CheckpointHash: hash,
					},
					SchemaContent: content,
				}
			}(),
			wantErr: false,
		},
		{
			name: "invalid content hash",
			checkpoint: &Checkpoint{
				Header: CheckpointHeader{
					MigrationsHash: "abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1",
					CheckpointHash: "wronghash1234567890123456789012345678901234567890123456789012345",
				},
				SchemaContent: "CREATE TABLE users (id INT);",
			},
			wantErr: true,
		},
		{
			name: "empty content with matching hash",
			checkpoint: func() *Checkpoint {
				content := ""
				hash := computeContentHash(content)
				return &Checkpoint{
					Header: CheckpointHeader{
						MigrationsHash: "abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1",
						CheckpointHash: hash,
					},
					SchemaContent: content,
				}
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCheckpoint(tt.checkpoint)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFindLatestValidCheckpoint(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	// Create migrations directory
	err := fs.MkdirAll(flags.MigrationDir, 0755)
	require.NoError(t, err)

	// Create some migrations
	migrations := []migration{
		{name: "20240101000000_init", sql: "CREATE TABLE users (id INT PRIMARY KEY);"},
		{name: "20240102000000_add_col", sql: "ALTER TABLE users ADD COLUMN name TEXT;"},
	}

	for _, mig := range migrations {
		migDir := filepath.Join(flags.MigrationDir, mig.name)
		err := fs.MkdirAll(migDir, 0755)
		require.NoError(t, err)

		err = afero.WriteFile(fs, filepath.Join(migDir, "migration.sql"), []byte(mig.sql), 0644)
		require.NoError(t, err)
	}

	// Create valid checkpoint for first migration only
	migrationsHash := computeMigrationsHash(migrations[:1])
	schemaContent := "CREATE TABLE users (id INT8 NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id ASC));\n"
	contentHash := computeContentHash(schemaContent)
	checkpointContent := formatCheckpointHeader(migrationsHash, contentHash) + "\n" + schemaContent

	checkpointPath := filepath.Join(flags.MigrationDir, migrations[0].name, checkpointFileName)
	err = afero.WriteFile(fs, checkpointPath, []byte(checkpointContent), 0644)
	require.NoError(t, err)

	// Find latest valid checkpoint
	result, index, err := findLatestValidCheckpoint(fs, migrations)
	require.NoError(t, err)

	// Should find the checkpoint at index 0
	assert.NotNil(t, result)
	assert.Equal(t, 0, index)
	assert.Equal(t, "20240101000000_init", result.MigrationName)
}

func TestFindLatestValidCheckpointWithInvalidMigrationsHash(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	// Create migrations directory
	err := fs.MkdirAll(flags.MigrationDir, 0755)
	require.NoError(t, err)

	// Create migrations
	migrations := []migration{
		{name: "20240101000000_init", sql: "CREATE TABLE users (id INT PRIMARY KEY);"},
		{name: "20240102000000_add_col", sql: "ALTER TABLE users ADD COLUMN name TEXT;"},
	}

	for _, mig := range migrations {
		migDir := filepath.Join(flags.MigrationDir, mig.name)
		err := fs.MkdirAll(migDir, 0755)
		require.NoError(t, err)

		err = afero.WriteFile(fs, filepath.Join(migDir, "migration.sql"), []byte(mig.sql), 0644)
		require.NoError(t, err)
	}

	// Create checkpoint with WRONG migrations hash (simulates out-of-order insertion)
	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"
	schemaContent := "CREATE TABLE users (id INT8 NOT NULL);\n"
	contentHash := computeContentHash(schemaContent)
	checkpointContent := formatCheckpointHeader(wrongHash, contentHash) + "\n" + schemaContent

	checkpointPath := filepath.Join(flags.MigrationDir, migrations[0].name, checkpointFileName)
	err = afero.WriteFile(fs, checkpointPath, []byte(checkpointContent), 0644)
	require.NoError(t, err)

	// Find latest valid checkpoint - should return nil since hash doesn't match
	result, index, err := findLatestValidCheckpoint(fs, migrations)
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, -1, index)
}

func TestWriteCheckpoint(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	migDir := filepath.Join(flags.MigrationDir, "20240101000000_test")
	err := fs.MkdirAll(migDir, 0755)
	require.NoError(t, err)

	content := "-- scurry:migrations=abc,checkpoint=def\nCREATE TABLE test;"

	err = writeCheckpoint(fs, migDir, content)
	require.NoError(t, err)

	// Verify file was created
	checkpointPath := filepath.Join(migDir, checkpointFileName)
	exists, err := afero.Exists(fs, checkpointPath)
	require.NoError(t, err)
	assert.True(t, exists)

	// Verify content
	readContent, err := afero.ReadFile(fs, checkpointPath)
	require.NoError(t, err)
	assert.Equal(t, content, string(readContent))
}

func TestCreateCheckpointForMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs := afero.NewMemMapFs()

	// Setup
	err := fs.MkdirAll(flags.MigrationDir, 0755)
	require.NoError(t, err)

	migDir := filepath.Join(flags.MigrationDir, "20240101000000_test")
	err = fs.MkdirAll(migDir, 0755)
	require.NoError(t, err)

	migrations := []migration{
		{name: "20240101000000_test", sql: "CREATE TABLE users (id INT PRIMARY KEY);"},
	}

	// Apply migration to get schema
	resultSchema, err := applyMigrationsToCleanDatabase(ctx, migrations, false)
	require.NoError(t, err)

	// Create checkpoint
	err = createCheckpointForMigration(fs, migrations, resultSchema, migDir)
	require.NoError(t, err)

	// Verify checkpoint was created
	checkpointPath := filepath.Join(migDir, checkpointFileName)
	exists, err := afero.Exists(fs, checkpointPath)
	require.NoError(t, err)
	assert.True(t, exists)

	// Load and validate
	checkpoint, err := loadCheckpoint(fs, migDir)
	require.NoError(t, err)
	require.NotNil(t, checkpoint)

	err = validateCheckpoint(checkpoint)
	require.NoError(t, err)

	// Verify migrations hash is correct
	expectedMigHash := computeMigrationsHash(migrations)
	assert.Equal(t, expectedMigHash, checkpoint.Header.MigrationsHash)
}

func TestRoundTripCheckpoint(t *testing.T) {
	// Test that we can create a checkpoint and then load+validate it
	t.Parallel()
	ctx := context.Background()
	fs := afero.NewMemMapFs()

	// Setup migrations
	err := fs.MkdirAll(flags.MigrationDir, 0755)
	require.NoError(t, err)

	migrations := []migration{
		{name: "20240101000000_users", sql: "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);"},
		{name: "20240102000000_posts", sql: "CREATE TABLE posts (id INT PRIMARY KEY, user_id INT REFERENCES users(id));"},
	}

	for _, mig := range migrations {
		migDir := filepath.Join(flags.MigrationDir, mig.name)
		err := fs.MkdirAll(migDir, 0755)
		require.NoError(t, err)
		err = afero.WriteFile(fs, filepath.Join(migDir, "migration.sql"), []byte(mig.sql), 0644)
		require.NoError(t, err)
	}

	// Apply migrations to get schema after second migration
	resultSchema, err := applyMigrationsToCleanDatabase(ctx, migrations, false)
	require.NoError(t, err)

	// Create checkpoint for second migration
	migDir := filepath.Join(flags.MigrationDir, migrations[1].name)
	err = createCheckpointForMigration(fs, migrations, resultSchema, migDir)
	require.NoError(t, err)

	// Now find the valid checkpoint
	checkpoint, index, err := findLatestValidCheckpoint(fs, migrations)
	require.NoError(t, err)
	assert.NotNil(t, checkpoint)
	assert.Equal(t, 1, index)
	assert.Equal(t, migrations[1].name, checkpoint.MigrationName)

	// Verify content hash is valid
	err = validateCheckpoint(checkpoint)
	require.NoError(t, err)
}
