package providersync

import (
	"testing"
	"time"
)

func TestJiraTeamID(t *testing.T) {
	// Unlike GitLab's "gl:" prefix, Jira's team unit IS the raw project key --
	// no provider prefix (team_discovery.discover_jira's provider_team_id).
	if got := jiraTeamID(" OPS "); got != "OPS" {
		t.Fatalf("got %q", got)
	}
}

func TestJiraProjectID(t *testing.T) {
	if got := jiraProjectID("org-1", "OPS"); got != "org-1:jira:OPS" {
		t.Fatalf("got %q", got)
	}
}

func TestNormalizeJiraTeamRow(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	description := "Ops team project"
	row, ok := normalizeJiraTeamRow("org-1", jiraTeamCatalogProjectSearchEntry{
		Key: "OPS", Name: "Ops Project", Description: &description,
	}, now)
	if !ok {
		t.Fatal("expected ok")
	}
	if row.ID != "OPS" || row.OrgID != "org-1" || row.Provider != "jira" ||
		row.Name != "Ops Project" || row.Description == nil || *row.Description != description ||
		row.NativeTeamKey == nil || *row.NativeTeamKey != "OPS" || row.ParentTeamID != nil ||
		row.IsActive != 1 || len(row.ProjectKeys) != 1 || row.ProjectKeys[0] != "OPS" {
		t.Fatalf("row=%+v", row)
	}

	if _, ok := normalizeJiraTeamRow("org-1", jiraTeamCatalogProjectSearchEntry{Key: "", Name: "x"}, now); ok {
		t.Fatal("missing key must be rejected")
	}
	if _, ok := normalizeJiraTeamRow("org-1", jiraTeamCatalogProjectSearchEntry{Key: "X", Name: ""}, now); ok {
		t.Fatal("missing name must be rejected")
	}
}

func TestJiraTeamCatalogMembershipFacets(t *testing.T) {
	// No alias configured (the same simplification GitHub/GitLab's native
	// collectors already make): the no-email identity IS the provider-
	// qualified id, "jira:accountid:<id>" (Jira carries no username).
	facets := jiraTeamCatalogMembershipFacets("account-1", nil)
	if len(facets) != 1 || facets[0] != "jira:accountid:account-1" {
		t.Fatalf("got %v", facets)
	}
	email := "Ops.Lead@Example.com"
	facets = jiraTeamCatalogMembershipFacets("account-1", &email)
	if len(facets) != 2 || facets[0] != "jira:accountid:account-1" || facets[1] != "ops.lead@example.com" {
		t.Fatalf("got %v", facets)
	}
	if got := jiraTeamCatalogMembershipFacets("", nil); got != nil {
		t.Fatalf("empty account id should yield no facets, got %v", got)
	}
}

func TestJiraMemberID(t *testing.T) {
	// member_id is lowercased; the qualified facet (raw_provider_user_id) is
	// NOT -- Python computes the two independently.
	if got := jiraMemberID("Account-1"); got != "jira:account-1" {
		t.Fatalf("got %q", got)
	}
}

func TestNormalizeJiraMembershipRow(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	row, ok := normalizeJiraMembershipRow("org-1", "OPS", jiraTeamCatalogUserPayload{
		AccountID: "Account-1", EmailAddress: "ops@example.com", DisplayName: "Ops Lead",
	}, now)
	if !ok {
		t.Fatal("expected ok")
	}
	if row.OrgID != "org-1" || row.Provider != "jira" || row.TeamID != "OPS" ||
		row.MemberID != "jira:account-1" || row.RawProviderUserID == nil ||
		*row.RawProviderUserID != "jira:accountid:Account-1" ||
		row.RawEmail == nil || *row.RawEmail != "ops@example.com" ||
		row.Source != "native" || row.IsPrimary != 1 ||
		row.Specificity != jiraTeamCatalogNativeSpecificity || row.Priority != jiraTeamCatalogNativePriority {
		t.Fatalf("row=%+v", row)
	}

	if _, ok := normalizeJiraMembershipRow("org-1", "OPS", jiraTeamCatalogUserPayload{}, now); ok {
		t.Fatal("a lead with no accountId/email/displayName must be rejected")
	}
}

func TestNormalizeJiraOwnershipRow(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	row := normalizeJiraOwnershipRow("org-1", "OPS", "OPS", now)
	if row.OrgID != "org-1" || row.Provider != "jira" || row.TeamID != "OPS" ||
		row.ProjectID != "org-1:jira:OPS" || row.ProjectKey == nil || *row.ProjectKey != "OPS" ||
		row.Source != "native" || row.IsPrimary != 1 ||
		row.Specificity != jiraTeamCatalogNativeSpecificity || row.Priority != jiraTeamCatalogNativePriority {
		t.Fatalf("row=%+v", row)
	}
}

func TestNormalizeJiraProjectRow(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	row := normalizeJiraProjectRow("org-1", "OPS", "Ops Project", now)
	if row.ID != "org-1:jira:OPS" || row.OrgID != "org-1" || row.Provider != "jira" ||
		row.ProjectKey == nil || *row.ProjectKey != "OPS" || row.Name != "Ops Project" ||
		row.IsActive != 1 || row.State != "" || row.URL != "" ||
		len(row.TeamIDs) != 0 || len(row.TeamKeys) != 0 ||
		row.LeadID != nil || row.LeadName != nil || row.LeadEmail != nil {
		t.Fatalf("row=%+v -- ProjectRecord's Python defaults (state/url empty, team_ids/team_keys empty, lead_* nil) must be preserved exactly", row)
	}
}

func TestJiraTeamCatalogSkippable400Detail(t *testing.T) {
	if got := jiraTeamCatalogSkippable400Detail([]byte(`{"errorMessages":["The board does not support sprints"]}`)); got != "The board does not support sprints" {
		t.Fatalf("got %q", got)
	}
	// A bare 400 with no body, no errorMessages key, or an empty array is
	// NOT skippable (team_autoimport_jira._skippable_jira_400_detail).
	for _, body := range []string{"", "{}", `{"errorMessages":[]}`, "not json"} {
		if got := jiraTeamCatalogSkippable400Detail([]byte(body)); got != "" {
			t.Fatalf("body=%q got %q, want empty (not skippable)", body, got)
		}
	}
}

func TestValidateJiraOwnershipRowAcceptsBothSources(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	claim := Claim{Unit: Unit{OrgID: "org-1", Provider: "jira"}}

	native := normalizeJiraOwnershipRow("org-1", "OPS", "OPS", now)
	if err := validateJiraOwnershipRow(claim, native); err != nil {
		t.Fatalf("native row should validate: %v", err)
	}

	key := "SUP"
	legacy := jiraTeamCatalogOwnershipRow{
		OrgID: "org-1", Provider: "jira", TeamID: "ops-team-id",
		ProjectID: "org-1:jira:SUP", ProjectKey: &key, Source: jiraTeamCatalogLegacySource,
		IsPrimary: 1, Specificity: jiraTeamCatalogLegacySpecificity, Priority: jiraTeamCatalogLegacyPriority,
		ValidFrom: now, UpdatedAt: now,
	}
	if err := validateJiraOwnershipRow(claim, legacy); err != nil {
		t.Fatalf("jira_legacy row should validate: %v", err)
	}

	// A native-source row carrying the legacy specificity/priority pair must
	// be rejected -- the two sources' numbers must never cross.
	mismatched := native
	mismatched.Specificity = jiraTeamCatalogLegacySpecificity
	if err := validateJiraOwnershipRow(claim, mismatched); err == nil {
		t.Fatal("a native row with legacy specificity must be rejected")
	}
}

func TestJiraRosterFromMemberships(t *testing.T) {
	rows := []jiraTeamCatalogMembershipRow{
		{TeamID: "OPS", IdentityFacets: []string{"jira:accountid:account-1", "ops@example.com"}},
	}
	roster := jiraRosterFromMemberships(rows)
	if got := roster["OPS"]; len(got) != 2 || got[0] != "jira:accountid:account-1" || got[1] != "ops@example.com" {
		t.Fatalf("roster=%v", roster)
	}
	if got := roster["UNKNOWN"]; got != nil {
		t.Fatalf("unknown team should have no roster entry, got %v", got)
	}
}

func TestDedupeJiraProjectCatalogRowsAndOwnershipRows(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	project := normalizeJiraProjectRow("org-1", "OPS", "Ops Project", now)
	deduped := dedupeJiraProjectCatalogRows([]jiraTeamCatalogProjectRow{project, project})
	if len(deduped) != 1 {
		t.Fatalf("projects=%+v", deduped)
	}

	ownership := normalizeJiraOwnershipRow("org-1", "OPS", "OPS", now)
	dedupedOwnership := dedupeJiraOwnershipRows([]jiraTeamCatalogOwnershipRow{ownership, ownership})
	if len(dedupedOwnership) != 1 {
		t.Fatalf("ownership=%+v", dedupedOwnership)
	}
}
