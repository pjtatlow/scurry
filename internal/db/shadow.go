package db

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"

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

		// Parse COCKROACH_ENV variable if set
		if cockroachEnv := os.Getenv("COCKROACH_ENV"); cockroachEnv != "" {
			// Parse as query parameters
			values, err := url.ParseQuery(cockroachEnv)
			if err != nil {
				return nil, fmt.Errorf("failed to parse COCKROACH_ENV: %w", err)
			}

			// Convert to slice of "key=value" strings
			envVars := make([]string, 0)
			for key, vals := range values {
				for _, val := range vals {
					envVars = append(envVars, fmt.Sprintf("%s=%s", key, val))
				}
			}

			if len(envVars) > 0 {
				opts = append(opts, testserver.EnvVarOpt(envVars))
			}
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

	// Shadow databases are ephemeral and don't benefit from schema_locked.
	// Disable it so tables can be freely modified without unlock overhead.
	_, _ = client.db.ExecContext(ctx, "SET create_table_with_schema_locked = false")

	return client, nil
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
