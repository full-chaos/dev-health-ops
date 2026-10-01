//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

func jiraIncidentVersion(t *testing.T, title string, updated string) jiraIncidentRow {
	t.Helper()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	var issue jiraIncidentPayload
	issue.ID, issue.Key = "10001", "JSM-1"
	issue.Fields.Summary = title
	issue.Fields.Created = "2026-07-22T10:00:00Z"
	issue.Fields.Updated = updated
	issue.Fields.Status.Name = "Investigating"
	issue.Fields.Status.StatusCategory.Key = "indeterminate"
	row, err := normalizeJiraIncident(claim, "cloud-123", "https://acme.atlassian.net", issue,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func keyVersions(ctx context.Context, t *testing.T, conn driver.Conn, table, org, id string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+table+" WHERE org_id = ? AND id = ?", org, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// Under contract 2 FINAL keeps every revision of a key, so a readback of the
// newest version written must select the current row by revision. Two versions
// of one incident are written through the production sink; the readback of the
// newer one is exact and the readback of the older one is a conflict (a newer
// version is stored).
func TestJiraIncidentReadbackSelectsTheCurrentVersionOnContract2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := JiraIncidentClickHouseEffects{Writer: conn, Lease: lease, Entitlement: allowIncidentEntitlement}
	readback := JiraIncidentClickHouseReadback{Conn: conn, Lease: lease}
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"

	v1 := jiraIncidentVersion(t, "API down", "2026-07-22T10:05:00Z")
	v2 := jiraIncidentVersion(t, "API down, mitigated", "2026-07-22T11:05:00Z")
	if v1.ID != v2.ID || v1.SourceRevision.Cmp(v2.SourceRevision) >= 0 {
		t.Fatalf("the fixture must be two versions of one key: ids %q %q revisions %v %v", v1.ID, v2.ID, v1.SourceRevision, v2.SourceRevision)
	}
	for _, row := range []jiraIncidentRow{v1, v2} {
		if err := sink.WriteEffect(ctx, claim, jiraIncidentEffect(t, row)); err != nil {
			t.Fatal(err)
		}
	}
	if got := keyVersions(ctx, t, conn, "operational_incidents", v1.OrgID, v1.ID); got != 2 {
		t.Fatalf("%d stored versions of the key, want 2", got)
	}
	if got, err := readback.InspectEffect(ctx, claim, jiraIncidentEffect(t, v2)); err != nil || got != EffectExact {
		t.Fatalf("readback of the newest version = %s, %v; want exact", got, err)
	}
	if got, err := readback.InspectEffect(ctx, claim, jiraIncidentEffect(t, v1)); err != nil || got != EffectConflict {
		t.Fatalf("readback of the superseded version = %s, %v; want a conflict", got, err)
	}
}

func gitLabIncidentVersion(t *testing.T, title, updated, project string) CompleteRouteBatch {
	t.Helper()
	page := []map[string]any{{
		"id": 9001, "iid": 7, "issue_type": "incident", "state": "opened", "title": title,
		"created_at": "2026-07-20T10:00:00Z", "updated_at": updated, "severity": "sev1",
	}}
	return buildGitLabIncidentOracleBatch(t, map[string]any{
		"project_id": 123, "repo_full_name": project, "provider_instance_id": "https://gitlab.example",
		"pages": [][]map[string]any{page}, "normalized_at": "2026-08-03T12:00:00.123456Z",
	})
}

// The same property for the three GitLab destinations (services, mappings,
// incidents): every readback of the newest stored version is exact.
func TestGitLabIncidentReadbacksSelectTheCurrentVersionOnContract2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabIncidentsClickHouseEffects{Conn: conn, Lease: lease}
	claim := nativeTestClaim("gitlab", "incidents")

	first := gitLabIncidentVersion(t, "API unavailable", "2026-07-21T11:00:00Z", "Acme/API")
	second := gitLabIncidentVersion(t, "API unavailable, mitigated", "2026-07-22T11:00:00Z", "Acme/API")
	for _, batch := range []CompleteRouteBatch{first, second} {
		for _, effect := range batch.Effects {
			if err := sink.WriteEffect(ctx, claim, effect); err != nil {
				t.Fatalf("%s: %v", effect.Destination, err)
			}
		}
	}
	versioned := 0
	for _, effect := range second.Effects {
		var row struct {
			OrgID string `json:"org_id"`
			ID    string `json:"id"`
		}
		if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
			t.Fatal(err)
		}
		if keyVersions(ctx, t, conn, effect.Destination, row.OrgID, row.ID) > 1 {
			versioned++
		}
		if got, err := sink.InspectEffect(ctx, claim, effect); err != nil || got != EffectExact {
			t.Errorf("%s: readback of the newest version = %s, %v; want exact", effect.Destination, got, err)
		}
	}
	if versioned == 0 {
		t.Fatal("no destination stored two versions of a key: the fixture does not exercise the current-row read")
	}
}
