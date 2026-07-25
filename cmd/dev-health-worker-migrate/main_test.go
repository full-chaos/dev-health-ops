package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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
