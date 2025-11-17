package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/lib/pq"
)

// Client wraps the database connection
type Client struct {
	db  *sql.DB
	url string
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

// GetDB returns the underlying database connection
func (c *Client) GetDB() *sql.DB {
	return c.db
}
