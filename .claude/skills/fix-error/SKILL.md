---
name: fix-error
description: Reproduce, fix, and PR an error from a scurry error dump file
argument-hint: "<path-to-error-file>"
---

You are given a scurry error dump file. Follow this process end-to-end:

## 1. Diagnose the error

Read the error file at `$ARGUMENTS`. Parse the error message and identify:
- Which command failed (push, migrate, etc.)
- The root cause (the `error:` field)
- The local and remote schema context

## 2. Find the relevant code

Trace the error to its source in the codebase. Understand why the current code produces this failure.

## 3. Write a failing test first

Add tests that reproduce the exact scenario from the error dump:
- An **integration test** in the appropriate `cmd/*_test.go` file using the existing table-driven test pattern with `executePush` / shadow DB
- A **unit test** in the appropriate `internal/schema/*_test.go` file testing the comparison/diff logic directly

Run the tests to confirm they fail (or would have failed before the fix).

## 4. Fix the code

Make the minimal change needed to fix the root cause. Keep it focused — don't refactor surrounding code.

## 5. Verify

Run the full test suite (`go test ./...`) and confirm everything passes, including your new tests.

## 6. Create a PR and watch CI

- Create a branch named `fix/<short-description>`
- Commit with a clear message explaining the bug and fix
- Push and create a PR with a summary, test plan, and the `Co-Authored-By` trailer
- Watch CI checks until they all pass. If any fail, fix and force-push.

Return the PR URL when done.
