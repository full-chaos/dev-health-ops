//go:build integration

package operationaledges

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL; v2.49 left the Go zero value. Every reader here keeps that zero.
func TestOperationalReadersScanNullColumnsAsTheZeroValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const orgID = "6f0f7a51-2b7e-4c1e-9a43-1d1f5f0c9a01"
	repoID := uuid.MustParse("0b9d3a5e-7f61-4c2a-8d6e-3c1b2a4f5e60")
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	for _, seed := range []struct {
		statement string
		args      []any
	}{
		{`INSERT INTO operational_service_repository_mappings (org_id, id, service_id, repo_id, provider, relationship_provenance, mapping_kind, rule_id, source_url, is_active, observed_at, last_synced, source_version_at, ordering_contract)
		 VALUES (?, 'map-1', 'svc-1', ?, 'pagerduty', NULL, NULL, NULL, NULL, 1, ?, ?, ?, 2)`, []any{orgID, repoID, now, now, now}},
		{`INSERT INTO operational_incidents (org_id, id, title, service_id, escalation_policy_id, source_url, started_at, is_deleted, observed_at, last_synced, source_version_at, ordering_contract)
		 VALUES (?, 'inc-1', 'down', NULL, NULL, NULL, ?, 0, ?, ?, ?, 2)`, []any{orgID, now, now, now, now}},
		{`INSERT INTO operational_services (org_id, id, name, owning_team_id, escalation_policy_id, is_deleted, observed_at, last_synced, source_version_at, ordering_contract)
		 VALUES (?, 'svc-1', 'api', NULL, NULL, 0, ?, ?, ?, 2)`, []any{orgID, now, now, now}},
		{`INSERT INTO operational_alerts (org_id, id, title, incident_id, source_url, is_deleted, observed_at, last_synced, source_version_at, ordering_contract)
		 VALUES (?, 'alert-1', 'cpu', 'inc-1', NULL, 0, ?, ?, ?, 2)`, []any{orgID, now, now, now}},
		{`INSERT INTO operational_incident_timeline_events (org_id, id, incident_id, event_type, actor_id, body, source_url, observed_at, last_synced, source_version_at, ordering_contract)
		 VALUES (?, 'event-1', 'inc-1', 'ack', NULL, NULL, NULL, ?, ?, ?, 2)`, []any{orgID, now, now, now}},
		{`INSERT INTO operational_incident_notes (org_id, id, incident_id, body, author_user_id, source_url, observed_at, last_synced, source_version_at, ordering_contract)
		 VALUES (?, 'note-1', 'inc-1', 'looking', NULL, NULL, ?, ?, ?, 2)`, []any{orgID, now, now, now}},
		{`INSERT INTO operational_incident_responders (org_id, id, incident_id, user_id, source_url, observed_at, last_synced, source_version_at, ordering_contract)
		 VALUES (?, 'resp-1', 'inc-1', NULL, NULL, ?, ?, ?, 2)`, []any{orgID, now, now, now}},
		{`INSERT INTO deployments (org_id, repo_id, deployment_id, environment, deployed_at)
		 VALUES (?, ?, 'dep-1', NULL, NULL)`, []any{orgID, repoID}},
		{`INSERT INTO work_items (org_id, work_item_id, title, description)
		 VALUES (?, 'gh:acme/web#7', 'Fix cart totals', NULL)`, []any{orgID}},
	} {
		if err := conn.Exec(ctx, seed.statement, seed.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, seed.statement)
		}
	}
	t.Run("ReadServiceRepositoryMappings", func(t *testing.T) {
		rows, err := ReadServiceRepositoryMappings(ctx, conn, orgID, now.Add(time.Hour), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].RelationshipProvenance != "" || rows[0].MappingKind != "" || rows[0].RuleID != "" || rows[0].SourceURL != "" {
			t.Fatalf("rows = %+v, want one row with empty provenance, kind, rule, url", rows)
		}
	})
	t.Run("ReadIncidents", func(t *testing.T) {
		rows, err := ReadIncidents(ctx, conn, orgID, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].ServiceID != "" || rows[0].EscalationPolicyID != "" || rows[0].SourceURL != "" {
			t.Fatalf("rows = %+v, want one row with empty service, policy, url", rows)
		}
	})
	t.Run("ReadServices", func(t *testing.T) {
		rows, err := ReadServices(ctx, conn, orgID)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].OwningTeamID != "" || rows[0].EscalationPolicyID != "" {
			t.Fatalf("rows = %+v, want one row with empty team, policy", rows)
		}
	})
	for name, read := range map[string]func(context.Context, driver.Conn, string, []string) ([]directRow, error){
		"ReadAlerts": ReadAlerts, "ReadTimelineEvents": ReadTimelineEvents, "ReadNotes": ReadNotes, "ReadResponders": ReadResponders,
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := read(ctx, conn, orgID, []string{"inc-1"})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].SourceURL != "" || rows[0].PersonID != "" || (name == "ReadTimelineEvents" && rows[0].Body != "") {
				t.Fatalf("rows = %+v, want one row with empty url, person, body", rows)
			}
		})
	}
	t.Run("ReadDeployments", func(t *testing.T) {
		rows, err := ReadDeployments(ctx, conn, orgID, nil, []uuid.UUID{repoID}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Environment != "" || !rows[0].DeployedAt.IsZero() {
			t.Fatalf("rows = %+v, want one row with empty environment and a zero deployed_at", rows)
		}
	})
	t.Run("ReadWorkItemText", func(t *testing.T) {
		rows, err := ReadWorkItemText(ctx, conn, orgID)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Description != "" {
			t.Fatalf("rows = %+v, want one row with an empty description", rows)
		}
	})
}
