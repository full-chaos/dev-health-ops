// Package opfixture builds contract-2 operational_* rows in a real ClickHouse
// whose schema is the migrated head, for tests of the readers that must select
// the current row of a key by revision rather than by FINAL.
package opfixture

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Start returns a ClickHouse migrated to production's ordering contract 2 and a
// native connection to it. The process environment names contract 2 for the
// migration; it is restored when the test ends.
func Start(ctx context.Context, t *testing.T) (*containers.Instance, driver.Conn) {
	t.Helper()
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return instance, conn
}

// Incident is one stored version of an operational_incidents key.
type Incident struct {
	Org, ID, ServiceID, Status, Title string
	Revision                          int
	Deleted                           bool
	StartedAt                         time.Time
}

func ts(at time.Time) string {
	return fmt.Sprintf("toDateTime64('%s', 6, 'UTC')", at.UTC().Format("2006-01-02 15:04:05.000000"))
}

// InsertIncident stores one version. The conflict key and ingest revision follow
// the revision, so a higher revision is a newer version of the same key.
func InsertIncident(ctx context.Context, t *testing.T, conn driver.Conn, v Incident) {
	t.Helper()
	deleted := 0
	if v.Deleted {
		deleted = 1
	}
	statement := fmt.Sprintf(`INSERT INTO operational_incidents
(org_id, provider, provider_instance_id, source_entity_type, external_id, source_version_at,
 source_revision, source_conflict_key, ingest_revision, ordering_contract, id, observed_at, last_synced,
 normalized_status, title, started_at, service_id, is_deleted)
VALUES ('%s','pagerduty','acme','incident','%s', %s, %d, 'k%d', %d, 2, '%s', %s, %s, '%s', '%s', %s, '%s', %d)`,
		v.Org, v.ID, ts(v.StartedAt), v.Revision, v.Revision, v.Revision, v.ID, ts(v.StartedAt), ts(v.StartedAt),
		v.Status, v.Title, ts(v.StartedAt), v.ServiceID, deleted)
	if err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("insert incident %s r%d: %v", v.ID, v.Revision, err)
	}
}

// Mapping is one stored version of an operational_service_repository_mappings key.
type Mapping struct {
	Org, ID, ServiceID, RepoID string
	Revision                   int
	Active                     bool
	At                         time.Time
}

// InsertMapping stores one version of a service-to-repository mapping.
func InsertMapping(ctx context.Context, t *testing.T, conn driver.Conn, v Mapping) {
	t.Helper()
	active := 0
	if v.Active {
		active = 1
	}
	statement := fmt.Sprintf(`INSERT INTO operational_service_repository_mappings
(org_id, provider, provider_instance_id, source_entity_type, external_id, source_version_at,
 source_revision, source_conflict_key, ingest_revision, ordering_contract, id, observed_at, last_synced,
 service_id, repo_id, is_active)
VALUES ('%s','pagerduty','acme','service_repository_mapping','%s', %s, %d, 'k%d', %d, 2, '%s', %s, %s, '%s', '%s', %d)`,
		v.Org, v.ID, ts(v.At), v.Revision, v.Revision, v.Revision, v.ID, ts(v.At), ts(v.At), v.ServiceID, v.RepoID, active)
	if err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("insert mapping %s r%d: %v", v.ID, v.Revision, err)
	}
}
