package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Client wraps the database connection
type Client struct {
	db       *sql.DB
	url      string
	isShadow bool
}

// Connect establishes a connection to the CockroachDB database
func Connect(ctx context.Context, dbURL string) (*Client, error) {
	parsedUrl, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	queryParams := parsedUrl.Query()
	queryParams.Add("application_name", "scurry")
	parsedUrl.RawQuery = queryParams.Encode()

	db, err := sql.Open("postgres", parsedUrl.String())
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	dbName := strings.TrimLeft(parsedUrl.Path, "/")
	if dbName != "" {
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", pq.QuoteIdentifier(dbName)))
		if err != nil {
			return nil, fmt.Errorf("failed to create database: %w", err)
		}
	}

	return &Client{db: db, url: dbURL}, nil
}

func (c *Client) ConnectionString() string {
	return c.url
}

// Close closes the database connection
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// SetStatementTimeout sets the session-level statement timeout.
func (c *Client) SetStatementTimeout(ctx context.Context, d time.Duration) error {
	ms := int64(d / time.Millisecond)
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("SET statement_timeout = '%dms'", ms))
	return err
}

// GetDB returns the underlying database connection
func (c *Client) GetDB() *sql.DB {
	return c.db
}

// ExecContext executes a query without returning any rows
func (c *Client) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

// GetCurrentDatabase returns the name of the currently connected database
func (c *Client) GetCurrentDatabase(ctx context.Context) (string, error) {
	var dbName string
	err := c.db.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName)
	if err != nil {
		return "", fmt.Errorf("failed to get current database: %w", err)
	}
	return dbName, nil
}

// DropCurrentDatabase drops the currently connected database.
// This connects to the defaultdb first, then drops the target database.
func (c *Client) DropCurrentDatabase(ctx context.Context) error {
	dbName, err := c.GetCurrentDatabase(ctx)
	if err != nil {
		return err
	}

	// Parse the current URL to get connection info
	parsedUrl, err := url.Parse(c.url)
	if err != nil {
		return fmt.Errorf("failed to parse database URL: %w", err)
	}

	// Connect to defaultdb instead
	parsedUrl.Path = "/defaultdb"
	defaultDbUrl := parsedUrl.String()

	defaultDb, err := sql.Open("postgres", defaultDbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to defaultdb: %w", err)
	}
	defer defaultDb.Close()

	// Drop the database
	_, err = defaultDb.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", pq.QuoteIdentifier(dbName)))
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %w", dbName, err)
	}

	return nil
}
