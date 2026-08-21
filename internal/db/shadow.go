package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/cockroachdb/cockroachdb-parser/pkg/util/uuid"
)

var (
	// Shared test server instance
	sharedDbServer  testserver.TestServer
	shadowServerMu  sync.Mutex
	shadowServerURL *url.URL

	CrdbVersion string

	// Optional host and port for test server
	TestServerHost     string
	TestServerPort     int
	TestServerHTTPPort int

	// Optional limits on how much memory the CockroachDB process may use.
	// The zero value of each means "leave CockroachDB's default alone".
	TestServerStoreSize   float64 // fraction of available memory
	TestServerCacheSize   int64   // bytes
	TestServerMaxGoMemory int64   // bytes

	logOutput bytes.Buffer
)

// GetShadowDB creates an ephemeral database server.
// The test server is lazily initialized and reused across calls so we only start one cockroach process.
// For each call, a random database created for this set of statements to be executed.
// It can accept some initial statements to be executed before the client is returned.
//
// Returns a client connected to the test database.
// The caller is responsible for closing the client when done.
func GetShadowDB(ctx context.Context, statements ...string) (*Client, error) {

	client, err := getShadowDbClient(ctx)
	if err != nil {
		return nil, err
	}

	// Execute the provided statements
	if len(statements) > 0 {
		if err := client.ExecuteBulkDDL(ctx, statements...); err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to execute statements: %w", err)
		}
	}

	return client, nil
}

func getShadowDbClient(ctx context.Context) (*Client, error) {
	shadowServerMu.Lock()
	defer shadowServerMu.Unlock()

	// Create test server if it doesn't exist
	if sharedDbServer == nil {
		// Ensure crdbVersion is set
		//
		// // Hide log output from cockroachdb testserver package
		if showLogs := os.Getenv("COCKROACH_SHOW_LOGS"); showLogs != "true" {
			log.SetOutput(&logOutput)
		}

		opts := make([]testserver.TestServerOpt, 0)
		if CrdbVersion != "" {
			opts = append(opts, testserver.CustomVersionOpt(CrdbVersion))
		}

		// Add host option if specified
		if TestServerHost != "" {
			opts = append(opts, testserver.ListenAddrHostOpt(TestServerHost))
		}

		// Add SQL port option if specified
		if TestServerPort > 0 {
			opts = append(opts, testserver.AddListenAddrPortOpt(TestServerPort))
		}

		// Add HTTP port option if specified
		if TestServerHTTPPort > 0 {
			opts = append(opts, testserver.AddHttpPortOpt(TestServerHTTPPort))
		}

		// Size the in-memory store if specified. cockroach-go turns this into
		// --store=type=mem,size=<fraction>, which CockroachDB resolves against
		// the memory available to the process (the cgroup limit in a container).
		if TestServerStoreSize > 0 {
			opts = append(opts, testserver.SetStoreMemSizeOpt(TestServerStoreSize))
		}

		// Size the Pebble block cache if specified. cockroach-go describes this
		// as a fraction, but it only formats the value into --cache, and
		// CockroachDB reads a --cache value of 1 or more as a byte count. The
		// block cache is allocated outside the Go heap, so a Go memory limit
		// does not bound it.
		if TestServerCacheSize > 0 {
			opts = append(opts, testserver.CacheSizeOpt(float64(TestServerCacheSize)))
		}

		// Environment for the cockroach process. cockroach-go appends these
		// last, so later entries win: an explicit limit overrides COCKROACH_ENV.
		envVars := make([]string, 0)

		// Parse COCKROACH_ENV variable if set
		if cockroachEnv := os.Getenv("COCKROACH_ENV"); cockroachEnv != "" {
			// Parse as query parameters
			values, err := url.ParseQuery(cockroachEnv)
			if err != nil {
				return nil, fmt.Errorf("failed to parse COCKROACH_ENV: %w", err)
			}

			// Convert to slice of "key=value" strings
			for key, vals := range values {
				for _, val := range vals {
					envVars = append(envVars, fmt.Sprintf("%s=%s", key, val))
				}
			}
		}

		// Bound the Go heap if specified. CockroachDB otherwise sets its own
		// soft limit of 2.25x --max-sql-memory, and cockroach-go exposes no
		// option for either --max-go-memory or --max-sql-memory. CockroachDB
		// skips that calculation when GOMEMLIMIT is set in the environment.
		if TestServerMaxGoMemory > 0 {
			envVars = append(envVars, fmt.Sprintf("GOMEMLIMIT=%d", TestServerMaxGoMemory))
		}

		if len(envVars) > 0 {
			opts = append(opts, testserver.EnvVarOpt(envVars))
		}

		if cockroachLogsDir := os.Getenv("COCKROACH_LOGS_DIR"); cockroachLogsDir != "" {
			opts = append(opts, testserver.CockroachLogsDirOpt(cockroachLogsDir))
		}

		ts, err := testserver.NewTestServer(opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create test server: %w", err)
		}
		sharedDbServer = ts
		shadowServerURL = ts.PGURL()

		configureShadowServer(ctx, shadowServerURL.String())
	}

	// Choose a random database name
	dbName := fmt.Sprintf("_shadow_%s", uuid.NewV4())

	urlClone, _ := url.Parse(shadowServerURL.String())
	urlClone.Path = fmt.Sprintf("/%s", dbName)

	// Connect will make sure the database exists
	client, err := Connect(ctx, urlClone.String())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to test server: %w", err)
	}
	client.isShadow = true
	client.disableAutocommitDDL = true

	return client, nil
}

// configureShadowServer applies cluster-wide settings to the shared shadow
// server right after it starts, before any shadow database client connects.
// A session-level SET is not reliable for this: Client wraps a connection
// pool, so a SET only applies to whichever pooled connection happens to run
// it, while later DDL can run on other connections that still have the
// defaults. Cluster settings apply to every session opened afterwards.
// Errors are ignored because older CockroachDB versions don't have these
// settings (and don't need them).
func configureShadowServer(ctx context.Context, serverURL string) {
	conn, err := sql.Open("postgres", serverURL)
	if err != nil {
		return
	}
	defer conn.Close()

	// CockroachDB 26.1+ creates all tables with schema_locked by default.
	// Shadow databases are ephemeral and don't benefit from it, and the
	// parameter must not leak into schemas read back from the shadow DB
	// (squash migrations, checkpoints, generated DDL).
	_, _ = conn.ExecContext(ctx, "SET CLUSTER SETTING sql.defaults.create_table_with_schema_locked = false")

	// Newer CockroachDB versions restrict access to crdb_internal by default.
	// We need it for InitMigrationHistory's schema introspection.
	_, _ = conn.ExecContext(ctx, "SET CLUSTER SETTING sql.override.allow_unsafe_internals.enabled = true")

	// Cluster settings propagate asynchronously; wait briefly so sessions
	// opened after this pick up the new schema_locked default.
	for range 100 {
		var enabled bool
		err := conn.QueryRowContext(ctx, "SHOW CLUSTER SETTING sql.defaults.create_table_with_schema_locked").Scan(&enabled)
		if err != nil || !enabled {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func StopShadowDbServer() {
	shadowServerMu.Lock()
	defer shadowServerMu.Unlock()

	if sharedDbServer != nil {
		sharedDbServer.Stop()
		sharedDbServer = nil
		shadowServerURL = nil
	}
}
