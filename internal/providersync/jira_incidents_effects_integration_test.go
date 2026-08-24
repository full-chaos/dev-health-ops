//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const jiraIncidentsDDL = `
CREATE TABLE operational_incidents (
  org_id String, provider LowCardinality(String), provider_instance_id String,
  source_entity_type String, external_id String, source_version_at DateTime64(6, 'UTC'),
  source_revision UInt128, source_conflict_key String, ingest_revision UInt128,
  ordering_contract UInt8, id String, source_id Nullable(UUID), source_url Nullable(String),
  source_event_at Nullable(DateTime64(6, 'UTC')), source_event_id Nullable(String),
  observed_at DateTime64(6, 'UTC'), last_synced DateTime64(6, 'UTC'),
  raw_status Nullable(String), raw_severity Nullable(String), raw_priority Nullable(String),
  normalized_status Nullable(String), normalized_severity Nullable(String),
  normalized_priority Nullable(String), relationship_provenance Nullable(String),
  relationship_confidence Nullable(Float64), service_id Nullable(String),
  service_external_id Nullable(String), escalation_policy_id Nullable(String), title String,
  description Nullable(String), started_at Nullable(DateTime64(6, 'UTC')),
  resolved_at Nullable(DateTime64(6, 'UTC')), is_deleted Bool,
  deleted_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree(source_revision) ORDER BY (org_id, id)`

func TestJiraIncidentReadbackIsExactAndTenantScoped(t *testing.T) {
	ctx, sink, readback := newJiraIncidentIntegrationSink(t)
	revoked := false
	sink.Entitlement = incidentEntitlementFunc(func(context.Context, string) error {
		if revoked {
			return ErrIncidentEntitlementDisabled
		}
		return nil
	})
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	row := jiraIncidentReadbackFixture(t)
	otherClaim, otherRow := claim, row
	otherClaim.OrgID, otherRow.OrgID = "other-org", "other-org"
	otherRow.ID, otherRow.SourceConflictKey = "", ""
	otherRow.SourceRevision, otherRow.IngestRevision = nil, nil
	otherRow.OrderingContract = 0
	if err := fillJiraIncidentOrdering(&otherRow); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, otherClaim, jiraIncidentEffect(t, otherRow)); err != nil {
		t.Fatal(err)
	}
	inspection, err := readback.InspectEffect(ctx, claim, jiraIncidentEffect(t, row))
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign-only inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, jiraIncidentEffect(t, row)); err != nil {
		t.Fatal(err)
	}
	revoked = true
	inspection, err = readback.InspectEffect(ctx, claim, jiraIncidentEffect(t, row))
	if err != nil || inspection != EffectExact {
		t.Fatalf("inspection=%s error=%v", inspection, err)
	}
}

func newJiraIncidentIntegrationSink(
	t *testing.T,
) (context.Context, JiraIncidentClickHouseEffects, JiraIncidentClickHouseReadback) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, jiraIncidentsDDL); err != nil {
		t.Fatal(err)
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	return ctx, JiraIncidentClickHouseEffects{
		Writer: conn, Lease: lease, Entitlement: allowIncidentEntitlement,
	}, JiraIncidentClickHouseReadback{Conn: conn, Lease: lease}
}

func jiraIncidentEffect(t *testing.T, row jiraIncidentRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(
		"operational_incidents", EffectReadbackRequired, []jiraIncidentRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
