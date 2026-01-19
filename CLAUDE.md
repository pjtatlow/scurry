# Scurry Development Guidelines

## Testing

- Use **table-driven tests** for all test cases
- Use `GetShadowDB()` for integration tests against real CockroachDB
- Test failure cases, not just happy paths
- Test schema upgrades from old versions to current
- Validate embedded SQL files are parseable in tests

### Testing Interactive CLI Flows

Use `expect` to test interactive prompts (huh forms, confirmations, etc.):

```bash
expect << 'EOF'
set timeout 30
spawn /tmp/scurry migration recover --db-url=... --migrations=...

# For y/n confirmation prompts (huh.Confirm)
expect {
    -re "Yes.*No|y.*Yes" { send "y" }
    timeout { exit 1 }
}

# For selection menus (huh.Select) - look for key hints
expect {
    -re "up.*down|submit" { send "\r" }  # Enter selects first option
    timeout { exit 1 }
}

# For text input (huh.Input/huh.Text)
expect {
    -re "submit" { send "my input\r" }
    timeout { exit 1 }
}

expect eof
EOF
```

Key points:
- Use `-re` for regex matching (ANSI codes interfere with literal matching)
- Match on key hint text like "up.*down", "submit", "Yes.*No" rather than menu content
- Use `sleep 0.3` before `send` if timing issues occur
- Build binary to `/tmp/scurry` first: `go build -o /tmp/scurry .`
- Set up test database state with psql before running expect
- Local CockroachDB available at: `postgresql://root@localhost:26257/?sslmode=disable`
- Use a fresh database name for each test (scurry auto-creates it if it doesn't exist):
  `--db-url=postgresql://root@localhost:26257/my_test_db?sslmode=disable`

## Interactive UI

- Use `huh` for interactive prompts, not `bufio.Reader` or `fmt.Scanln`
- Use `ui.HuhTheme()` for consistent styling
- Use `ui.SqlCode()` for syntax-highlighted SQL display
- Use `ui.Error()`, `ui.Success()`, `ui.Warning()`, `ui.Info()` for styled output
- Check `isatty` before running interactive prompts; fail with clear message if no TTY

## SQL and Schema

- Define schemas in `.sql` files, embed with `//go:embed`, and diff at runtime
- Don't hardcode SQL strings in Go code
- Use `parser.Parse()` from cockroachdb-parser for SQL parsing/splitting
- Use the schema diffing system for internal tables too (like `_scurry_.migrations`)

