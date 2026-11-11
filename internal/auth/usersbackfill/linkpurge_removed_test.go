package usersbackfill

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The legacy link-token purge verb is gone with go-api's link-token code: the
// auth group has no such verb, and no verb connects for it.
func TestAuthVerbsHaveNoLegacyLinkPurge(t *testing.T) {
	for _, child := range Command().Children {
		if child.Name == "legacy-link-purge" {
			t.Errorf("the auth group still has %q: %s", child.Name, child.Summary)
		}
	}
	original := connectFn
	t.Cleanup(func() { connectFn = original })
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		t.Error("a verb connected for legacy-link-purge")
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	lookup := func(key string) (string, bool) {
		if key == "AUTH_USERS_OWNED" {
			return "true", true
		}
		return "", false
	}
	var stdout, stderr bytes.Buffer
	env := cli.Env{Args: []string{"auth", "legacy-link-purge", "-dry-run"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}
	if code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, env); code != cli.ExitUsage {
		t.Errorf("dho auth legacy-link-purge exit %d, want %d (usage); stderr: %s", code, cli.ExitUsage, stderr.String())
	}
}
