package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CheckpointCache provides a shared remote cache for checkpoint data backed by CockroachDB.
// All public methods are nil-safe (no-op on nil receiver).
type CheckpointCache struct {
	client *Client
}

// NewCheckpointCache connects to the cache database at cacheURL with a 5s timeout.
// Returns nil, nil if cacheURL is empty.
func NewCheckpointCache(ctx context.Context, cacheURL string) (*CheckpointCache, error) {
	if cacheURL == "" {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client, err := Connect(ctx, cacheURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to checkpoint cache: %w", err)
	}

	return &CheckpointCache{client: client}, nil
}

// Close closes the underlying database connection.
func (c *CheckpointCache) Close() {
	if c == nil {
		return
	}
	c.client.Close()
}

// InitTable creates the _scurry_.checkpoints table if it doesn't exist.
func (c *CheckpointCache) InitTable(ctx context.Context) error {
	if c == nil {
		return nil
	}

	_, err := c.client.db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS _scurry_`)
	if err != nil {
		return fmt.Errorf("failed to create _scurry_ schema: %w", err)
	}

	_, err = c.client.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS _scurry_.checkpoints (
			migrations_hash STRING PRIMARY KEY,
			schema_sql STRING NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create checkpoints table: %w", err)
	}

	return nil
}

// Get retrieves cached schema SQL by migrations hash.
// Returns empty string and found=false on cache miss.
func (c *CheckpointCache) Get(ctx context.Context, migrationsHash string) (schemaSQL string, found bool, err error) {
	if c == nil {
		return "", false, nil
	}

	err = c.client.db.QueryRowContext(ctx,
		`SELECT schema_sql FROM _scurry_.checkpoints WHERE migrations_hash = $1`,
		migrationsHash,
	).Scan(&schemaSQL)

	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to get cached checkpoint: %w", err)
	}

	return schemaSQL, true, nil
}

// Put stores schema SQL in the cache, keyed by migrations hash. Uses UPSERT for concurrent writes.
func (c *CheckpointCache) Put(ctx context.Context, migrationsHash, schemaSQL string) error {
	if c == nil {
		return nil
	}

	_, err := c.client.db.ExecContext(ctx,
		`UPSERT INTO _scurry_.checkpoints (migrations_hash, schema_sql) VALUES ($1, $2)`,
		migrationsHash, schemaSQL,
	)
	if err != nil {
		return fmt.Errorf("failed to store cached checkpoint: %w", err)
	}

	return nil
}
