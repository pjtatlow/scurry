package db

import (
	"context"
	"fmt"
	"net/url"
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
		opts := make([]testserver.TestServerOpt, 0)
		if CrdbVersion != "" {
			opts = append(opts, testserver.CustomVersionOpt(CrdbVersion))
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
