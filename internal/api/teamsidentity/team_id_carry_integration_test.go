//go:build integration

package teamsidentity

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The admin import carries the bare team before it writes: the bare Linear
// team goes inactive and the import lands on the prefixed id.
func TestAdminImportCarriesTheBareTeamFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES ('ENG', generateUUIDv4(), 'Eng', [], ['m@example.com'], [], [], 1, '2026-09-01 00:00:00', 'org-1', 'linear', 'ENG')`); err != nil {
		t.Fatal(err)
	}
	rec := postImportBody(t, Store{Conn: conn}, `{"teams":[{"provider_type":"linear","provider_team_id":"ENG","name":"Eng"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("import = %d %s", rec.Code, rec.Body.String())
	}
	for id, want := range map[string]string{"ENG": "0|m@example.com", "linear:ENG": "1|m@example.com"} {
		var got string
		if err := conn.QueryRow(ctx, `SELECT concat(toString(is_active), '|', arrayStringConcat(manual_members, ',')) FROM teams FINAL WHERE org_id = 'org-1' AND id = ?`, id).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		if got != want {
			t.Errorf("team %s = %q, want %q", id, got, want)
		}
	}
}
