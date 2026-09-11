package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

func env(values map[string]string) platformsecrets.LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestExecuteHelpAndVersionDoNotRequireDatabase(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{{"--help"}, {"--version"}} {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if status := execute(context.Background(), args, env(nil), &stdout, &stderr); status != 0 {
			t.Fatalf("execute(%v) = %d, stderr=%s", args, status, stderr.String())
		}
	}
}

// TestHelpDocumentsBothDSNForms is round-2's (2026-09-11) P3 fix: --help
// documented zero environment variables (not even the pre-existing
// MIGRATION_DATABASE_URI), so an operator had no way to discover either DSN
// form from this binary itself.
func TestHelpDocumentsBothDSNForms(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if status := execute(context.Background(), []string{"--help"}, env(nil), &stdout, &stderr); status != 0 {
		t.Fatalf("execute(--help) = %d, stderr=%s", status, stderr.String())
	}
	for _, want := range []string{
		"MIGRATION_DATABASE_URI",
		"DEV_HEALTH_MIGRATION_PG_HOST",
		"DEV_HEALTH_MIGRATION_PG_PASSWORD",
		"mutually exclusive",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("--help output missing %q, got: %s", want, stderr.String())
		}
	}
}

// coordinatorGrants is the only translation between the coordinator posture
// and what the migration actually grants, so a dropped entry here is a
// readiness deadlock: the check would demand a privilege no GRANT ever
// emitted. Both halves are compared element for element against the posture
// itself rather than against a restated list -- CHAOS-3114 added the first
// column-scoped entry, and the pre-3114 code silently returned nil for exactly
// that case.
func TestCoordinatorGrantsTranslateThePostureWithoutDroppingAnything(t *testing.T) {
	t.Parallel()

	posture := postgresstore.CoordinatorPosture()
	tables, columns, sequences := coordinatorGrants()
	if len(tables) != len(posture.RequiredTables) || len(columns) != len(posture.ColumnScoped) || len(sequences) != len(posture.RequiredSequences) {
		t.Fatalf(
			"coordinatorGrants() = %d tables/%d columns/%d sequences, posture declares %d/%d/%d",
			len(tables), len(columns), len(sequences), len(posture.RequiredTables), len(posture.ColumnScoped), len(posture.RequiredSequences),
		)
	}
	for index, sequence := range posture.RequiredSequences {
		if sequences[index] != sequence {
			t.Fatalf("sequence grant %d = %q, want %q", index, sequences[index], sequence)
		}
	}
	for index, table := range posture.RequiredTables {
		want := riverstore.TableGrant{
			TableName:   table.TableName,
			AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate,
			AllowDelete: table.AllowDelete,
		}
		if tables[index] != want {
			t.Fatalf("table grant %d = %#v, want %#v", index, tables[index], want)
		}
	}
	for index, column := range posture.ColumnScoped {
		want := riverstore.ColumnGrant{
			TableName:  column.TableName,
			ColumnName: column.ColumnName,
			Privilege:  column.Privilege,
		}
		if columns[index] != want {
			t.Fatalf("column grant %d = %#v, want %#v", index, columns[index], want)
		}
	}
	// The translated set must survive the migration's own validation, or the
	// command would fail before it ever connects.
	if err := riverstore.ValidateMigrationOptions(riverstore.MigrationOptions{
		Schema:                  "river",
		DomainRole:              "domain_runtime",
		QueueRole:               "queue_runtime",
		CoordinatorRole:         "coordinator_runtime",
		CoordinatorGrants:       tables,
		CoordinatorColumnGrants: columns,
		CoordinatorSequences:    sequences,
	}); err != nil {
		t.Fatalf("the derived coordinator grant set is rejected by the migration itself: %v", err)
	}
}

// TestReportSchemaCheckLogsTheCauseButKeepsTheGenericExitMessage is
// CHAOS-5469's regression test for this call site specifically (codex round
// finding F1 on PR #2399): before this fix, reportSchemaCheck's predecessor
// discarded CheckSchema's error entirely -- an ErrSchemaCheckUnavailable
// (pool/connection failure) and an ErrSchemaNotCurrent (genuine version
// mismatch) both produced the exact same stderr output, with the real cause
// visible nowhere. The generic stderr line must stay byte-identical (an
// operator script parsing it must not break); the underlying cause must now
// appear in the structured log line.
func TestReportSchemaCheckLogsTheCauseButKeepsTheGenericExitMessage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		err       error
		wantCause string
	}{
		{
			name:      "connection_unavailable",
			err:       fmt.Errorf("%w: %w", riverstore.ErrSchemaCheckUnavailable, errors.New("dial tcp 127.0.0.1:1: connect: connection refused")),
			wantCause: "River schema check could not run: dial tcp 127.0.0.1:1: connect: connection refused",
		},
		{
			name:      "version_mismatch",
			err:       riverstore.ErrSchemaNotCurrent,
			wantCause: "River schema is not at the pinned version",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var logs, stdout, stderr bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))

			status := reportSchemaCheck(context.Background(), logger, &stdout, &stderr, 0, testCase.err)

			if status != 1 {
				t.Fatalf("reportSchemaCheck() = %d, want 1", status)
			}
			// The generic exit message is unchanged -- byte-identical to
			// before this fix, so an operator script matching on it never
			// breaks.
			if stderr.String() != "migration check failed: River schema is not current\n" {
				t.Fatalf("stderr = %q, want the unchanged generic exit message", stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty on failure", stdout.String())
			}
			var record map[string]any
			if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
				t.Fatalf("log line is not JSON (%v): %s", err, logs.String())
			}
			if record["check"] != "river_schema" {
				t.Errorf("log check = %v, want \"river_schema\"", record["check"])
			}
			if record["error"] != testCase.wantCause {
				t.Errorf("log error = %v, want %q -- the underlying cause must reach the log even though the stderr message stays generic", record["error"], testCase.wantCause)
			}
		})
	}
}

// TestReportSchemaCheckSucceedsSilently is the mirror image: passing proves
// the failure-path test above isn't vacuously true for every input.
func TestReportSchemaCheckSucceedsSilently(t *testing.T) {
	t.Parallel()

	var logs, stdout, stderr bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	status := reportSchemaCheck(context.Background(), logger, &stdout, &stderr, riverstore.PinnedSchemaVersion, nil)

	if status != 0 {
		t.Fatalf("reportSchemaCheck() = %d, want 0", status)
	}
	if stderr.Len() != 0 || logs.Len() != 0 {
		t.Fatalf("stderr=%q logs=%q, want both empty on success", stderr.String(), logs.String())
	}
	want := fmt.Sprintf("River schema current at pinned version %d\n", riverstore.PinnedSchemaVersion)
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestExecuteRequiresThreeSeparatedRolesBeforeConnecting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values map[string]string
		want   string
	}{
		{name: "migration missing", values: map[string]string{}, want: "MIGRATION_DATABASE_URI is required"},
		{name: "domain role missing", values: map[string]string{"MIGRATION_DATABASE_URI": "postgres://migration:secret@db/app"}, want: "RIVER_DOMAIN_DATABASE_ROLE is required"},
		{name: "queue role missing", values: map[string]string{"MIGRATION_DATABASE_URI": "postgres://migration:secret@db/app", "RIVER_DOMAIN_DATABASE_ROLE": "domain"}, want: "RIVER_QUEUE_DATABASE_ROLE is required"},
		{name: "roles shared", values: map[string]string{"MIGRATION_DATABASE_URI": "postgres://migration:one@db/app", "RIVER_DOMAIN_DATABASE_ROLE": "domain", "RIVER_QUEUE_DATABASE_ROLE": "domain"}, want: "roles must be distinct"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			status := execute(context.Background(), nil, env(test.values), &stdout, &stderr)
			if status != 1 || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("execute() = %d, stderr=%q, want %q", status, stderr.String(), test.want)
			}
			for _, secret := range []string{"one", "two", "three", "postgres://"} {
				if strings.Contains(stderr.String(), secret) {
					t.Fatalf("stderr leaked %q: %s", secret, stderr.String())
				}
			}
		})
	}
}

// TestExecuteEmitsAnInfoRecordOfTheResolvedForm is round 5's (2026-09-11)
// finding #5, reproduced then fixed: a successful resolution returned with
// no observable record of which form (uri|components) or database it
// reached, at all. The DSN itself (and its credentials) must never appear
// in that record.
func TestExecuteEmitsAnInfoRecordOfTheResolvedForm(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	status := execute(context.Background(), []string{"--check"}, env(map[string]string{
		"MIGRATION_DATABASE_URI":     "postgresql://migration:s3cr3t@unreachable.invalid:5432/migrationdb",
		"RIVER_DOMAIN_DATABASE_ROLE": "domain",
		"RIVER_QUEUE_DATABASE_ROLE":  "queue",
	}), &stdout, &stderr)
	if status != 1 {
		t.Fatalf("execute() = %d, want 1 (unreachable database)", status)
	}
	out := stderr.String()
	if !strings.Contains(out, `"msg":"migration database resolved"`) ||
		!strings.Contains(out, `"form":"uri"`) ||
		!strings.Contains(out, `"database":"migrationdb"`) {
		t.Fatalf("expected the resolution Info record, got: %s", out)
	}
	if strings.Contains(out, "s3cr3t") || strings.Contains(out, "postgresql://") {
		t.Fatalf("the resolution Info record leaked the DSN or a credential: %s", out)
	}
}

// TestResolveMigrationDatabaseURIComponentForm pins CHAOS-5560's fix at this
// binary's own entry point: compose.yml's entrypoint falls back to a raw
// shell-interpolated postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/$POSTGRES_DB
// when MIGRATION_DATABASE_URI is unset -- the exact unescaped-password shape
// this ticket exists to fix, one layer further out than internal/platform/
// config's own URIs. The component path here must survive it, using
// DEV_HEALTH_MIGRATION_PG_* names (never POSTGRES_HOST/_PORT/_USER/
// _PASSWORD/_DB, which deploy/docker-compose/compose.go-workers.yml --
// not touched by this PR -- already sets unconditionally for that same
// shell fallback; round-1 (2026-09-11) proved reusing those names made
// this function silently discard a real, working MIGRATION_DATABASE_URI
// override the instant that compose service ran).
func TestResolveMigrationDatabaseURIComponentForm(t *testing.T) {
	t.Parallel()

	t.Run("neither form set -- required error", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(nil), &stderr)
		if ok {
			t.Fatal("expected failure: neither the component host var nor MIGRATION_DATABASE_URI is set")
		}
		if !strings.Contains(stderr.String(), "MIGRATION_DATABASE_URI") {
			t.Fatalf("expected the MIGRATION_DATABASE_URI required-secret message, got: %s", stderr.String())
		}
	})

	t.Run("pre-built URI only -- still works", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		got, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"MIGRATION_DATABASE_URI": "postgresql://postgres:postgres@postgres:5432/postgres",
		}), &stderr)
		if !ok {
			t.Fatalf("expected success, stderr=%s", stderr.String())
		}
		if got.Reveal() != "postgresql://postgres:postgres@postgres:5432/postgres" {
			t.Fatalf("got %q", got.Reveal())
		}
	})

	t.Run("component form only survives a reserved-character password", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		reserved := "p#ss/w@rd"
		got, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"DEV_HEALTH_MIGRATION_PG_HOST":     "postgres",
			"DEV_HEALTH_MIGRATION_PG_USER":     "postgres",
			"DEV_HEALTH_MIGRATION_PG_PASSWORD": reserved,
			"DEV_HEALTH_MIGRATION_PG_DB":       "postgres",
		}), &stderr)
		if !ok {
			t.Fatalf("expected success, stderr=%s", stderr.String())
		}
		role, err := postgresstore.ConnectionUser(got.Reveal())
		if err != nil {
			t.Fatalf("assembled URI does not parse via the real caller (postgresstore.ConnectionUser): %v (%q)", err, got.Reveal())
		}
		if role != "postgres" {
			t.Fatalf("connection user = %q, want postgres", role)
		}
	})

	// Round-1 (2026-09-11) ruling: URI and component forms are mutually
	// exclusive, refused loudly naming both keys -- neither silently wins.
	t.Run("both forms set -- refused naming both keys", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"MIGRATION_DATABASE_URI":       "postgresql://real:real@real-host:5432/real",
			"DEV_HEALTH_MIGRATION_PG_HOST": "postgres",
			"DEV_HEALTH_MIGRATION_PG_USER": "postgres",
		}), &stderr)
		if ok {
			t.Fatal("expected failure when both forms are set")
		}
		if !strings.Contains(stderr.String(), "MIGRATION_DATABASE_URI") ||
			!strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_HOST") ||
			!strings.Contains(stderr.String(), "mutually exclusive") {
			t.Fatalf("expected an error naming both keys, got: %s", stderr.String())
		}
	})

	// The exact overlay scenario named in the round-1 ruling: this binary
	// must behave EXACTLY as it did before this ticket when
	// deploy/docker-compose/compose.go-workers.yml's own POSTGRES_HOST
	// default (or any of its raw shell-fallback vars) is present --
	// POSTGRES_HOST is not a component key this binary reads at all
	// anymore, so it has zero effect on which form wins.
	t.Run("compose overlay's own POSTGRES_HOST default has no effect", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		got, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"MIGRATION_DATABASE_URI": "postgresql://postgres:postgres@postgres:5432/postgres",
			"POSTGRES_HOST":          "postgres",
			"POSTGRES_USER":          "postgres",
			"POSTGRES_PASSWORD":      "postgres",
			"POSTGRES_DB":            "postgres",
		}), &stderr)
		if !ok {
			t.Fatalf("expected success, stderr=%s", stderr.String())
		}
		if got.Reveal() != "postgresql://postgres:postgres@postgres:5432/postgres" {
			t.Fatalf("POSTGRES_HOST must not be read as a component trigger by this binary, got %q", got.Reveal())
		}
	})

	t.Run("bad port with component host set is refused, not silently defaulted", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"DEV_HEALTH_MIGRATION_PG_HOST": "postgres",
			"DEV_HEALTH_MIGRATION_PG_PORT": "not-a-port",
			"DEV_HEALTH_MIGRATION_PG_USER": "postgres",
			"DEV_HEALTH_MIGRATION_PG_DB":   "postgres",
		}), &stderr)
		if ok {
			t.Fatal("expected failure on a non-numeric port")
		}
	})

	// Round-2 (2026-09-11) finding, second half: a non-host component set
	// with the component HOST absent, and no MIGRATION_DATABASE_URI either,
	// used to silently report "MIGRATION_DATABASE_URI is required" -- true,
	// but hiding that the operator had already started configuring
	// components and only forgot the host var.
	t.Run("non-host component set without host or URI names the missing host key", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"DEV_HEALTH_MIGRATION_PG_USER": "postgres",
		}), &stderr)
		if ok {
			t.Fatal("expected failure")
		}
		if !strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_USER") ||
			!strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_HOST") {
			t.Fatalf("expected an error naming the missing host key, got: %s", stderr.String())
		}
	})

	// Both forms set AND a non-host component: the mutual-exclusion check
	// still fires first (checked before the missing-key check), naming the
	// non-host component, not a spurious "missing host" message.
	t.Run("both forms plus a non-host component -- mutual exclusion still wins", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"MIGRATION_DATABASE_URI":       "postgresql://real:real@real-host:5432/real",
			"DEV_HEALTH_MIGRATION_PG_USER": "postgres",
		}), &stderr)
		if ok {
			t.Fatal("expected failure")
		}
		if !strings.Contains(stderr.String(), "MIGRATION_DATABASE_URI") ||
			!strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_USER") ||
			!strings.Contains(stderr.String(), "mutually exclusive") {
			t.Fatalf("expected the mutual-exclusion error, got: %s", stderr.String())
		}
	})

	// Round-3 (2026-09-11) finding: unlike the four internal/platform/config
	// DSNs, migrate's DB key is NOT shared with any sibling connection, so
	// it must still be swept by the detection -- the round-2 fix's blanket
	// DBKey exclusion wrongly covered this binary too.
	t.Run("component DB alone (no host, no URI) names the missing host key", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"DEV_HEALTH_MIGRATION_PG_DB": "postgres",
		}), &stderr)
		if ok {
			t.Fatal("expected failure")
		}
		if !strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_DB") ||
			!strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_HOST") {
			t.Fatalf("expected the missing-host error naming DEV_HEALTH_MIGRATION_PG_DB, got: %s", stderr.String())
		}
	})

	t.Run("component DB plus a raw MIGRATION_DATABASE_URI is refused", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"MIGRATION_DATABASE_URI":     "postgresql://real:real@real-host:5432/real",
			"DEV_HEALTH_MIGRATION_PG_DB": "postgres",
		}), &stderr)
		if ok {
			t.Fatal("expected failure")
		}
		if !strings.Contains(stderr.String(), "MIGRATION_DATABASE_URI") ||
			!strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_DB") ||
			!strings.Contains(stderr.String(), "mutually exclusive") {
			t.Fatalf("expected a mutual-exclusivity error naming DEV_HEALTH_MIGRATION_PG_DB, got: %s", stderr.String())
		}
	})

	// Round-3 finding: PASSWORD_FILE is a real, supported activation path
	// (ResolveDSNFromComponents resolves DEV_HEALTH_MIGRATION_PG_PASSWORD
	// through secrets.Resolve, which supports its own `_FILE` form) that the
	// round-2 detection sweep never checked.
	t.Run("PASSWORD_FILE alone (no host, no URI) names the missing host key", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		passwordFile := dir + "/password"
		if err := os.WriteFile(passwordFile, []byte("s3cret\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"DEV_HEALTH_MIGRATION_PG_PASSWORD_FILE": passwordFile,
		}), &stderr)
		if ok {
			t.Fatal("expected failure")
		}
		if !strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_PASSWORD_FILE") ||
			!strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_HOST") {
			t.Fatalf("expected the missing-host error naming DEV_HEALTH_MIGRATION_PG_PASSWORD_FILE, got: %s", stderr.String())
		}
	})

	t.Run("USER_FILE plus a raw MIGRATION_DATABASE_URI is refused", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		userFile := dir + "/user"
		if err := os.WriteFile(userFile, []byte("migrator\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"MIGRATION_DATABASE_URI":            "postgresql://real:real@real-host:5432/real",
			"DEV_HEALTH_MIGRATION_PG_USER_FILE": userFile,
		}), &stderr)
		if ok {
			t.Fatal("expected failure")
		}
		if !strings.Contains(stderr.String(), "MIGRATION_DATABASE_URI") ||
			!strings.Contains(stderr.String(), "DEV_HEALTH_MIGRATION_PG_USER_FILE") ||
			!strings.Contains(stderr.String(), "mutually exclusive") {
			t.Fatalf("expected a mutual-exclusivity error naming DEV_HEALTH_MIGRATION_PG_USER_FILE, got: %s", stderr.String())
		}
	})
}
