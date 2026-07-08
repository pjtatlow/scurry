package cmd

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
)

// signedContent returns a properly scurry-signed migration file for the given body.
func signedContent(body string) string {
	h := &migrationpkg.Header{Mode: migrationpkg.ModeSync}
	migrationpkg.SignHeader(h, body)
	return migrationpkg.FormatHeader(h) + "\n" + body
}

func TestCheckMigrationSignature(t *testing.T) {
	t.Parallel()

	body := "CREATE TABLE t (id INT PRIMARY KEY);\n"
	signed := &migrationpkg.Header{Mode: migrationpkg.ModeSync}
	migrationpkg.SignHeader(signed, body)

	tests := []struct {
		name    string
		dir     string
		content string
		want    sigStatus
	}{
		{
			name:    "valid signed migration",
			dir:     "20250101000000_ok",
			content: migrationpkg.FormatHeader(signed) + "\n" + body,
			want:    sigOK,
		},
		{
			name:    "header without signature",
			dir:     "20250101000001_nosig",
			content: "-- scurry:mode=sync\n" + body,
			want:    sigMissing,
		},
		{
			name:    "no header at all",
			dir:     "20250101000002_none",
			content: body,
			want:    sigMissing,
		},
		{
			name:    "body edited after signing",
			dir:     "20250101000003_edited",
			content: migrationpkg.FormatHeader(signed) + "\n" + body + "ALTER TABLE t ADD COLUMN x INT;\n",
			want:    sigInvalid,
		},
		{
			name:    "mode flipped but signature kept",
			dir:     "20250101000004_forge",
			content: "-- scurry:mode=async,sig=" + signed.Sig + "\n" + body,
			want:    sigInvalid,
		},
		{
			name:    "malformed header",
			dir:     "20250101000005_bad",
			content: "-- scurry:mode=bogus\n" + body,
			want:    sigInvalid,
		},
	}

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll(flags.MigrationDir, 0755))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeMigrationDir(t, fs, tt.dir, tt.content)
			status, err := checkMigrationSignature(fs, tt.dir)
			require.NoError(t, err)
			assert.Equal(t, tt.want, status)
		})
	}
}

func TestVerifyAndReportSignatures(t *testing.T) {
	// Not parallel: toggles the validateRequireSig global.
	body := "CREATE TABLE t (id INT PRIMARY KEY);\n"

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll(flags.MigrationDir, 0755))
	writeMigrationDir(t, fs, "20250101000000_ok", signedContent(body))
	writeMigrationDir(t, fs, "20250101000001_nosig", "-- scurry:mode=sync\n"+body)

	migs, err := loadMigrations(fs)
	require.NoError(t, err)

	old := validateRequireSig
	defer func() { validateRequireSig = old }()

	// A missing signature is a warning (nil error) by default.
	validateRequireSig = false
	assert.NoError(t, verifyAndReportSignatures(fs, migs))

	// ...and a hard failure under --require-signatures.
	validateRequireSig = true
	assert.Error(t, verifyAndReportSignatures(fs, migs))

	// An invalid signature fails regardless of the flag.
	validateRequireSig = false
	signed := &migrationpkg.Header{Mode: migrationpkg.ModeSync}
	migrationpkg.SignHeader(signed, body)
	writeMigrationDir(t, fs, "20250101000002_forge", "-- scurry:mode=async,sig="+signed.Sig+"\n"+body)
	migs, err = loadMigrations(fs)
	require.NoError(t, err)
	assert.Error(t, verifyAndReportSignatures(fs, migs))
}

// TestMigrationNewSignsHeader pins the post-form sequence `migration new` runs — classify
// the entered SQL, build a header, and write it via createMigration — and asserts the
// resulting migration carries a valid signature (the header is never hand-authored).
func TestMigrationNewSignsHeader(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll(flags.MigrationDir, 0755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(flags.MigrationDir, "schema.sql"), []byte(""), 0644))

	parsed, err := parser.Parse("CREATE TABLE widget (id INT PRIMARY KEY);")
	require.NoError(t, err)
	var statementStrings []string
	astStmts := make([]tree.Statement, len(parsed))
	for i, p := range parsed {
		statementStrings = append(statementStrings, p.AST.String())
		astStmts[i] = p.AST
	}

	existing, err := loadMigrations(fs)
	require.NoError(t, err)
	header, err := headerForStatements(fs, astStmts, existing, false)
	require.NoError(t, err)
	assert.Equal(t, migrationpkg.ModeSync, header.Mode)

	dir, _, err := createMigration(fs, "add_widget", statementStrings, header)
	require.NoError(t, err)

	status, err := checkMigrationSignature(fs, dir)
	require.NoError(t, err)
	assert.Equal(t, sigOK, status)
}

func TestSignMigrationHeaders(t *testing.T) {
	t.Parallel()

	body := "CREATE TABLE t (id INT PRIMARY KEY);\n"

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll(flags.MigrationDir, 0755))
	// A header-less migration and one with a header but no signature.
	writeMigrationDir(t, fs, "20250101000000_headerless", body)
	writeMigrationDir(t, fs, "20250101000001_nosig", "-- scurry:mode=sync\n"+body)

	migs, err := loadMigrations(fs)
	require.NoError(t, err)
	require.NoError(t, signMigrationHeaders(fs, migs))

	// Both migrations now carry a valid signature.
	for _, m := range migs {
		status, err := checkMigrationSignature(fs, m.Name)
		require.NoError(t, err)
		assert.Equal(t, sigOK, status, m.Name)
	}
}
