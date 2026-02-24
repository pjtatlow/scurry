package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCurrentDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	dbName, err := client.GetCurrentDatabase(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, dbName)
}

func TestDropCurrentDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := GetShadowDB(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Get current database name to verify it exists
	dbName, err := client.GetCurrentDatabase(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, dbName)

	// Drop it
	err = client.DropCurrentDatabase(ctx)
	require.NoError(t, err)
}
