# PR Review TODO: pj/qol

## Correctness & Bugs

- [x] `RecoverMigration` and `ResetMigrationForRetry` don't check `RowsAffected` — added status guards and RowsAffected checks
- [x] `ExecuteRemainingStatements` uses string equality to find failed statement — acceptable, already errors with clear message on mismatch
- [x] `ParseHeader` with `depends_on=` (empty value) produces `[""]` instead of error — now returns error + test added
- [x] Sync-before-async reordering could violate implicit dependencies — now preserves timestamp order, skips async inline
- [x] `generateMigrationsTableAlterStatements` iterates a map — now sorts column names for deterministic output
- [x] `MarkAllMigrationsComplete` is dead code — removed

## Test Coverage

- [x] `internal/recovery/` — added tests for TruncateChecksum, TryAgain, MarkSucceeded (recovery_test.go)
- [ ] `cmd/push.go` retry-on-failure path — requires integration test with intentionally failing DDL (deferred)
- [x] `cmd/migration_execute.go` — added tests for loadMigrationsForExecution and filterUnappliedMigrations (migration_execute_test.go)
- [ ] `cmd/migration_recover.go` — interactive-only, would need expect-based test (deferred)
- [ ] `cmd/migration_table_sizes.go` — requires DB + filesystem integration test (deferred)
- [x] `internal/db/client.go` — added tests for GetCurrentDatabase and DropCurrentDatabase (client_test.go)
- [x] `ExecuteRemainingStatements` — added edge cases: not found, remaining fails, last statement (migrations_test.go)
- [x] `CompleteMigration`/`FailMigration` `rowsAffected == 0` race condition paths — added tests (migration_race_test.go)

## Architecture & Design

- [x] `internal/db/migrations.go` is a monolith — split into `migrations.go` (types/queries), `migration_schema.go` (table evolution), `migration_exec.go` (execution)
- [ ] `internal/recovery/` mixes TUI (huh) with domain logic (deferred — larger restructuring)
- [x] `db.Migration.Mode` is bare `string` — added `MigrationModeSync`/`MigrationModeAsync` constants, eliminated raw `"async"` literals
- [ ] Two parallel migration-loading functions with subtly different header-stripping behavior (deferred)

## CLI UX & Usability

- [x] `migration gen` suggests `scurry migration apply` — fixed to say `scurry migration execute`
- [x] `migration execute` uses `fmt.Scanln` instead of `huh` — replaced with `ui.ConfirmPrompt` (has isatty guard)
- [x] "Mark as succeeded" in recovery says "assumes you handled it manually" — updated description to match actual behavior
- [ ] `push` retry path auto-executes regenerated statements without showing user what changed (deferred)
- [ ] No `--force`/non-interactive mode for `migration recover` (deferred)
- [x] `stat-pull` subcommand name is unintuitive — renamed to `table-sizes`
- [x] Skipped migration messages use unstyled `fmt.Printf` — now use `ui.Warning()`
- [x] Header parse errors silently ignored — now logs warning with `ui.Warning()`
- [ ] `migration new` has no way to set mode/depends_on without hand-editing (deferred — feature request)
- [ ] `stat-pull` and `migration execute` don't validate `--db-url` early (deferred)
- [ ] `migration recover` shows full migration SQL unconditionally (deferred)
- [x] `migration_table_sizes.go` uses `os.Exit(1)` instead of returning error to cobra — now returns error properly
