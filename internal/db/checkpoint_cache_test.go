package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, cache *CheckpointCache)
	}{
		{
			name: "nil receiver safety - all methods are no-ops",
			fn: func(t *testing.T, _ *CheckpointCache) {
				var nilCache *CheckpointCache
				ctx := context.Background()

				// Close should not panic
				nilCache.Close()

				// InitTable should return nil
				err := nilCache.InitTable(ctx)
				assert.NoError(t, err)

				// Get should return zero values
				sql, found, err := nilCache.Get(ctx, "somehash")
				assert.NoError(t, err)
				assert.False(t, found)
				assert.Empty(t, sql)

				// Put should return nil
				err = nilCache.Put(ctx, "somehash", "some sql")
				assert.NoError(t, err)
			},
		},
		{
			name: "Put then Get round-trip",
			fn: func(t *testing.T, cache *CheckpointCache) {
				ctx := context.Background()

				hash := "aaaa000000000000000000000000000000000000000000000000000000000001"
				schemaSQL := "CREATE TABLE users (id INT8 NOT NULL);CREATE TABLE posts (id INT8 NOT NULL);"

				err := cache.Put(ctx, hash, schemaSQL)
				require.NoError(t, err)

				got, found, err := cache.Get(ctx, hash)
				require.NoError(t, err)
				assert.True(t, found)
				assert.Equal(t, schemaSQL, got)
			},
		},
		{
			name: "Get miss returns empty string and found=false",
			fn: func(t *testing.T, cache *CheckpointCache) {
				ctx := context.Background()

				got, found, err := cache.Get(ctx, "nonexistent_hash_00000000000000000000000000000000000000000000")
				require.NoError(t, err)
				assert.False(t, found)
				assert.Empty(t, got)
			},
		},
		{
			name: "Put same key twice (upsert) succeeds",
			fn: func(t *testing.T, cache *CheckpointCache) {
				ctx := context.Background()

				hash := "bbbb000000000000000000000000000000000000000000000000000000000002"

				err := cache.Put(ctx, hash, "CREATE TABLE v1 (id INT8);")
				require.NoError(t, err)

				err = cache.Put(ctx, hash, "CREATE TABLE v2 (id INT8);")
				require.NoError(t, err)

				got, found, err := cache.Get(ctx, hash)
				require.NoError(t, err)
				assert.True(t, found)
				assert.Equal(t, "CREATE TABLE v2 (id INT8);", got)
			},
		},
		{
			name: "InitTable is idempotent",
			fn: func(t *testing.T, cache *CheckpointCache) {
				ctx := context.Background()

				// InitTable was already called during setup; call again
				err := cache.InitTable(ctx)
				require.NoError(t, err)

				// Should still work after double init
				err = cache.InitTable(ctx)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var cache *CheckpointCache
			if tt.name != "nil receiver safety - all methods are no-ops" {
				ctx := context.Background()
				client, err := GetShadowDB(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { client.Close() })

				cache = &CheckpointCache{client: client}
				err = cache.InitTable(ctx)
				require.NoError(t, err)
			}

			tt.fn(t, cache)
		})
	}
}
