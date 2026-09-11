package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

// TestResolveMigrationDatabaseURIComponentForm pins CHAOS-5560's fix at this
// binary's own entry point: compose.yml's entrypoint falls back to a raw
// shell-interpolated postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/$POSTGRES_DB
// when MIGRATION_DATABASE_URI is unset -- the exact unescaped-password shape
// this ticket exists to fix, one layer further out than internal/platform/
// config's own URIs. The component path here must survive it.
func TestResolveMigrationDatabaseURIComponentForm(t *testing.T) {
	t.Parallel()

	t.Run("no host var -- falls back to the required pre-built URI", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(nil), &stderr)
		if ok {
			t.Fatal("expected failure: neither POSTGRES_HOST nor MIGRATION_DATABASE_URI is set")
		}
		if !strings.Contains(stderr.String(), "MIGRATION_DATABASE_URI") {
			t.Fatalf("expected the MIGRATION_DATABASE_URI required-secret message, got: %s", stderr.String())
		}
	})

	t.Run("pre-built URI still works when host var is unset", func(t *testing.T) {
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

	t.Run("component form survives a reserved-character password", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		reserved := "p#ss/w@rd"
		got, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"POSTGRES_HOST":     "postgres",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": reserved,
			"POSTGRES_DB":       "postgres",
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

	t.Run("component form wins even when MIGRATION_DATABASE_URI is also set", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		got, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"MIGRATION_DATABASE_URI": "postgresql://ignored:ignored@ignored:5432/ignored",
			"POSTGRES_HOST":          "postgres",
			"POSTGRES_USER":          "postgres",
			"POSTGRES_PASSWORD":      "postgres",
			"POSTGRES_DB":            "postgres",
		}), &stderr)
		if !ok {
			t.Fatalf("expected success, stderr=%s", stderr.String())
		}
		if strings.Contains(got.Reveal(), "ignored") {
			t.Fatalf("component form should have superseded MIGRATION_DATABASE_URI, got %q", got.Reveal())
		}
	})

	t.Run("bad port with host set is refused, not silently defaulted", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, ok := resolveMigrationDatabaseURI(env(map[string]string{
			"POSTGRES_HOST": "postgres",
			"POSTGRES_PORT": "not-a-port",
			"POSTGRES_USER": "postgres",
			"POSTGRES_DB":   "postgres",
		}), &stderr)
		if ok {
			t.Fatal("expected failure on a non-numeric port")
		}
	})
}
