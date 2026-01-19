-- Schema for the _scurry_.migrations table
-- This file is embedded in the binary and used to ensure the table schema is up to date

CREATE TABLE _scurry_.migrations (
    name STRING PRIMARY KEY,
    checksum STRING NOT NULL,
    status STRING NOT NULL DEFAULT 'succeeded',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    executed_by STRING NOT NULL DEFAULT current_user(),
    failed_statement STRING,
    error_msg STRING
);
