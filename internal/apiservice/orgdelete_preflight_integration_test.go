//go:build integration

package apiservice

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// With a ClickHouse login that may purge (here the container's admin over a
// database with an org_id table) go-api logs orgdelete_clickhouse_preflight=ok
// once at start, with no reason and no DSN part.
func TestStartLogsAnOkOrgDeletionPreflight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouseIsolated(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, "CREATE TABLE orgdel_start_probe (org_id String) ENGINE = MergeTree ORDER BY tuple()"); err != nil {
		t.Fatal(err)
	}
	logs := startLog(t, config.Config{APIAddress: "127.0.0.1:0", ClickHouseURI: secrets.NewValue(instance.URI)})
	lines := preflightLines(logs)
	if len(lines) != 1 || !strings.Contains(lines[0], `"`+orgDeletePreflightKey+`":"ok"`) || strings.Contains(lines[0], `"reason"`) {
		t.Fatalf("preflight lines %q, want one with %s ok and no reason\n%s", lines, orgDeletePreflightKey, logs)
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	for _, secret := range []string{password, parsed.User.Username(), parsed.Host} {
		if secret != "" && strings.Contains(logs, secret) {
			t.Errorf("the start log carries %q", secret)
		}
	}
}
