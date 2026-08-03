package providersync

import (
	"math/big"
	"testing"
	"time"
)

func jiraIncidentReadbackFixture(t *testing.T) jiraIncidentRow {
	t.Helper()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	var issue jiraIncidentPayload
	issue.ID, issue.Key = "10001", "JSM-1"
	issue.Fields.Summary = "API down"
	issue.Fields.Created = "2026-07-22T10:00:00Z"
	issue.Fields.Updated = "2026-07-22T10:05:00Z"
	issue.Fields.Status.Name = "Investigating"
	issue.Fields.Status.StatusCategory.Key = "indeterminate"
	row, err := normalizeJiraIncident(
		claim, "cloud-123", "https://acme.atlassian.net", issue,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestJiraIncidentReadbackClassifiesVersionAndContent(t *testing.T) {
	t.Parallel()
	expected := jiraIncidentReadbackFixture(t)
	older := expected
	older.SourceRevision = new(big.Int).Sub(expected.SourceRevision, big.NewInt(1))
	newer := expected
	newer.SourceRevision = new(big.Int).Add(expected.SourceRevision, big.NewInt(1))
	different := expected
	different.Title = "Different incident"

	for name, test := range map[string]struct {
		actual jiraIncidentRow
		found  bool
		want   EffectInspection
	}{
		"missing":            {jiraIncidentRow{}, false, EffectAbsent},
		"older":              {older, true, EffectAbsent},
		"newer":              {newer, true, EffectConflict},
		"same revision diff": {different, true, EffectConflict},
		"exact":              {expected, true, EffectExact},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := compareJiraIncidentVersion(expected, test.actual, test.found); got != test.want {
				t.Fatalf("inspection=%s want=%s", got, test.want)
			}
		})
	}
}

func TestJiraIncidentScopeRejectsCrossTenantRows(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	row := jiraIncidentReadbackFixture(t)
	if err := validateJiraIncidentScope(claim, []jiraIncidentRow{row}); err != nil {
		t.Fatal(err)
	}
	row.OrgID = "other-org"
	row.ID, row.SourceConflictKey = "", ""
	row.SourceRevision, row.IngestRevision = nil, nil
	row.OrderingContract = 0
	if err := fillJiraIncidentOrdering(&row); err != nil {
		t.Fatal(err)
	}
	if err := validateJiraIncidentScope(claim, []jiraIncidentRow{row}); err == nil {
		t.Fatal("cross-tenant Jira incident row was accepted")
	}
}
