//go:build integration

package chquery

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestTeamRepoDonorsExcludeStaleForeignAndNonOwnershipSources(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	at := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	repoA := "11111111-1111-4111-8111-111111111111"
	repoB := "22222222-2222-4222-8222-222222222222"
	repoC := "33333333-3333-4333-8333-333333333333"
	for _, org := range []string{orgAlpha, orgBeta} {
		mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('team',generateUUIDv4(),'Team','linear',1,?,?)`, at, org)
	}
	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('inactive',generateUUIDv4(),'Inactive','linear',0,?,?)`, at, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES (?,'acme/good','github',?),(?,'acme/other','github',?),(?,'acme/good','gitlab',?)`, repoA, orgAlpha, repoB, orgAlpha, repoC, orgAlpha)
	ownership := func(org, team, name, source string, from time.Time, to any, updated time.Time) {
		mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github',?,?,'exact',?,?,?,?)`, org, team, name, source, from, to, updated)
	}
	ownership(orgAlpha, "team", "ACME/GOOD", "provider_access", at.Add(-time.Hour), nil, at)
	// A retracted same-key row must replace its older live physical row.
	ownership(orgAlpha, "team", "acme/other", "inferred", at.Add(-time.Hour), nil, at.Add(-time.Minute))
	ownership(orgAlpha, "team", "acme/other", "inferred", at.Add(-time.Hour), at.Add(-time.Second), at)
	ownership(orgAlpha, "team", "acme/other", "provider_access", at.Add(time.Hour), nil, at)
	ownership(orgAlpha, "team", "acme/other", "manual", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "team", "acme/not-imported", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "inactive", "acme/other", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgBeta, "team", "acme/other", "provider_access", at.Add(-time.Hour), nil, at)
	attr := func(org, issue, team, source string, primary uint8, computed time.Time) {
		mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),?,'linear',?,?,?,'high','provider shaped attribution',?)`, org, issue, team, source, primary, computed)
	}
	ids := []string{"positive", "latest-ineligible", "secondary", "inactive", "missing-team"}
	attr(orgAlpha, "positive", "team", "native_team", 1, at)
	attr(orgAlpha, "latest-ineligible", "team", "native_team", 1, at.Add(-time.Minute))
	attr(orgAlpha, "latest-ineligible", "team", "unassigned", 1, at)
	attr(orgAlpha, "secondary", "team", "native_team", 0, at)
	attr(orgAlpha, "inactive", "inactive", "native_team", 1, at)
	attr(orgAlpha, "missing-team", "absent", "native_team", 1, at)
	for _, source := range []string{"assignee_membership", "author_membership", "linked_issue", "manual_fallback", "unassigned"} {
		ids = append(ids, source)
		attr(orgAlpha, source, "team", source, 1, at)
	}
	// Foreign primary evidence collides with a requested ID but has no local
	// primary. A tenant fence on only ownership cannot protect this join.
	ids = append(ids, "foreign-only")
	attr(orgBeta, "foreign-only", "team", "native_team", 1, at)
	donors, err := reader.FetchTeamRepoDonors(ctx, ids, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(donors))
	for _, d := range donors {
		got = append(got, fmt.Sprintf("%s/%s/%s", d.WorkItemID, d.TeamID, d.RepoID))
	}
	want := []string{"positive/team/" + repoA}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ownership donor positives/negatives: got %v want %v", got, want)
	}
}
