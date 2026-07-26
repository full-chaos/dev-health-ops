package main

import (
	"bytes"
	"context"
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
	tables, columns := coordinatorGrants()
	if len(tables) != len(posture.RequiredTables) || len(columns) != len(posture.ColumnScoped) {
		t.Fatalf(
			"coordinatorGrants() = %d tables/%d columns, posture declares %d/%d",
			len(tables), len(columns), len(posture.RequiredTables), len(posture.ColumnScoped),
		)
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
	}); err != nil {
		t.Fatalf("the derived coordinator grant set is rejected by the migration itself: %v", err)
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
