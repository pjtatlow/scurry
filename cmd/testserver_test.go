package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
)

func TestApplyMemoryLimits(t *testing.T) {
	tests := []struct {
		name          string
		storeSize     float64
		cacheSize     string
		maxGoMemory   string
		expectedStore float64
		expectedCache int64
		expectedGoMem int64
		expectedError string
	}{
		{
			name: "no limits set",
		},
		{
			name:          "all limits set",
			storeSize:     0.05,
			cacheSize:     "512MiB",
			maxGoMemory:   "2GiB",
			expectedStore: 0.05,
			expectedCache: 536870912,
			expectedGoMem: 2147483648,
		},
		{
			name:          "smallest store size",
			storeSize:     0.01,
			expectedStore: 0.01,
		},
		{
			name:          "largest store size",
			storeSize:     0.99,
			expectedStore: 0.99,
		},
		{
			name:          "store size below one percent",
			storeSize:     0.005,
			expectedError: `invalid --store-size 0.005: expected a fraction of available memory between 0.01 and 0.99 (e.g. 0.05)`,
		},
		{
			name:          "store size of all memory",
			storeSize:     1,
			expectedError: `invalid --store-size 1: expected a fraction of available memory between 0.01 and 0.99 (e.g. 0.05)`,
		},
		{
			name:          "negative store size",
			storeSize:     -0.5,
			expectedError: `invalid --store-size -0.5: expected a fraction of available memory between 0.01 and 0.99 (e.g. 0.05)`,
		},
		{
			name:          "cache size in bytes",
			cacheSize:     "536870912",
			expectedCache: 536870912,
		},
		{
			name:          "cache size with decimal suffix",
			cacheSize:     "512MB",
			expectedCache: 512000000,
		},
		{
			// CockroachDB reads --cache values below 1 as a percentage, which
			// it rounds down to whole percent. Absolute sizes are exact, so
			// that is all we accept.
			name:          "fractional cache size",
			cacheSize:     "0.02",
			expectedError: `invalid --cache-size "0.02": expected a byte size such as 512MiB or 2GiB`,
		},
		{
			name:          "zero cache size",
			cacheSize:     "0",
			expectedError: `invalid --cache-size "0": expected a byte size such as 512MiB or 2GiB`,
		},
		{
			name:          "unparseable cache size",
			cacheSize:     "big",
			expectedError: `invalid --cache-size "big": expected a byte size such as 512MiB or 2GiB`,
		},
		{
			name:          "negative cache size",
			cacheSize:     "-512MiB",
			expectedError: `invalid --cache-size "-512MiB": expected a byte size such as 512MiB or 2GiB`,
		},
		{
			name:          "go memory in bytes",
			maxGoMemory:   "2147483648",
			expectedGoMem: 2147483648,
		},
		{
			name:          "go memory with fractional suffix",
			maxGoMemory:   "1.5GiB",
			expectedGoMem: 1610612736,
		},
		{
			name:          "unparseable go memory",
			maxGoMemory:   "25%",
			expectedError: `invalid --max-go-memory "25%": expected a byte size such as 512MiB or 2GiB`,
		},
		{
			name:          "zero go memory",
			maxGoMemory:   "0GiB",
			expectedError: `invalid --max-go-memory "0GiB": expected a byte size such as 512MiB or 2GiB`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetMemoryLimits(t)
			db.TestServerStoreSize = tt.storeSize
			cacheSize = tt.cacheSize
			maxGoMemory = tt.maxGoMemory

			err := applyMemoryLimits()

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Equal(t, tt.expectedError, err.Error())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedStore, db.TestServerStoreSize)
			assert.Equal(t, tt.expectedCache, db.TestServerCacheSize)
			assert.Equal(t, tt.expectedGoMem, db.TestServerMaxGoMemory)
		})
	}
}

// TestMemoryLimitsAreOptIn makes sure the memory flags are registered and that
// leaving them off keeps CockroachDB's existing defaults.
func TestMemoryLimitsAreOptIn(t *testing.T) {
	tests := []struct {
		name         string
		defaultValue string
	}{
		{name: "store-size", defaultValue: "0"},
		{name: "cache-size", defaultValue: ""},
		{name: "max-go-memory", defaultValue: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := testserverCmd.Flags().Lookup(tt.name)
			require.NotNil(t, flag, "flag --%s is not registered", tt.name)
			assert.Equal(t, tt.defaultValue, flag.DefValue)
		})
	}

	resetMemoryLimits(t)
	require.NoError(t, applyMemoryLimits())

	assert.Zero(t, db.TestServerStoreSize)
	assert.Zero(t, db.TestServerCacheSize)
	assert.Zero(t, db.TestServerMaxGoMemory)
}

// resetMemoryLimits clears the memory limit flags and the values they are
// parsed into, restoring whatever was there before when the test finishes.
func resetMemoryLimits(t *testing.T) {
	t.Helper()

	previousCacheFlag, previousGoMemFlag := cacheSize, maxGoMemory
	previousStore, previousCache, previousGoMem := db.TestServerStoreSize, db.TestServerCacheSize, db.TestServerMaxGoMemory
	t.Cleanup(func() {
		cacheSize, maxGoMemory = previousCacheFlag, previousGoMemFlag
		db.TestServerStoreSize, db.TestServerCacheSize, db.TestServerMaxGoMemory = previousStore, previousCache, previousGoMem
	})

	cacheSize, maxGoMemory = "", ""
	db.TestServerStoreSize, db.TestServerCacheSize, db.TestServerMaxGoMemory = 0, 0, 0
}
