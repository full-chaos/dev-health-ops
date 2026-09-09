//go:build integration

package chquery

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestTeamRepoDonorsExcludeStaleForeignAndNonOwnershipSources seeds one
// positive and a negative for EVERY clause the donor query relies on, so that
// deleting any single clause changes this test's result. A fixture set that
// only exercises the happy path lets a whole predicate be removed silently.
func TestTeamRepoDonorsExcludeStaleForeignAndNonOwnershipSources(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	// Retraction is expressed as a SECOND physical row under the same
	// ReplacingMergeTree key. A background merge would collapse it and make the
	// query's FINAL a silent no-op -- the test would then pass whether or not
	// FINAL is there. Stopping merges keeps both rows on disk so FINAL is the
	// only thing choosing between them.
	mustExec(t, ctx, conn, `SYSTEM STOP MERGES team_repo_ownership`)
	at := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	repoA := "11111111-1111-4111-8111-111111111111"
	repoB := "22222222-2222-4222-8222-222222222222"
	repoC := "33333333-3333-4333-8333-333333333333"
	repoForeign := "44444444-4444-4444-8444-444444444444"
	repoZero := "00000000-0000-0000-0000-000000000000"
	repoByID := "55555555-5555-4555-8555-555555555555"
	for _, org := range []string{orgAlpha, orgBeta} {
		mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('team',generateUUIDv4(),'Team','linear',1,?,?)`, at, org)
	}
	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('inactive',generateUUIDv4(),'Inactive','linear',0,?,?)`, at, orgAlpha)
	// An ACTIVE team in ANOTHER org whose id is the one 'missing-team' points
	// at. Only the teams-side org fence keeps this cross-tenant team out.
	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('absent',generateUUIDv4(),'Foreign','linear',1,?,?)`, at, orgBeta)
	// An active team whose id is the EMPTY STRING, so the empty/NULL team_id
	// clause is the only thing excluding an attribution that names no team.
	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('',generateUUIDv4(),'Blank','linear',1,?,?)`, at, orgAlpha)
	// An active local team referenced ONLY by a foreign-org attribution below.
	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('shadow',generateUUIDv4(),'Shadow','linear',1,?,?)`, at, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES (?,'acme/good','github',?),(?,'acme/other','github',?),(?,'acme/good','gitlab',?)`, repoA, orgAlpha, repoB, orgAlpha, repoC, orgAlpha)
	// A same-named repo in ANOTHER org, and a zero-UUID repo row in THIS org.
	// Both are reachable by name from an eligible ownership row below; only the
	// repos-side org fence and the zero-UUID guard keep them out.
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES (?,'acme/foreign-repo','github',?)`, repoForeign, orgBeta)
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES (?,'acme/zero','github',?)`, repoZero, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES (?,'acme/id-only','github',?)`, repoByID, orgAlpha)
	ownership := func(org, team, name, source string, from time.Time, to any, updated time.Time) {
		mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github',?,?,'exact',?,?,?,?)`, org, team, name, source, from, to, updated)
	}
	ownership(orgAlpha, "team", "ACME/GOOD", "provider_access", at.Add(-time.Hour), nil, at)
	// A second ELIGIBLE ownership source for the same repository. Two rows join
	// to one identical donor tuple; only DISTINCT collapses them.
	// Deliberately a THIRD casing: no ownership row for this repo matches the
	// repos-table name exactly, so dropping lower() loses the positive.
	ownership(orgAlpha, "team", "Acme/Good", "native", at.Add(-time.Hour), nil, at)
	// A retracted same-key row must replace its older live physical row.
	ownership(orgAlpha, "team", "acme/other", "inferred", at.Add(-time.Hour), nil, at.Add(-time.Minute))
	ownership(orgAlpha, "team", "acme/other", "inferred", at.Add(-time.Hour), at.Add(-time.Second), at)
	ownership(orgAlpha, "team", "acme/other", "provider_access", at.Add(time.Hour), nil, at)
	ownership(orgAlpha, "team", "acme/other", "manual", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "team", "acme/not-imported", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "team", "acme/foreign-repo", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "team", "acme/zero", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "inactive", "acme/other", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgBeta, "team", "acme/other", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "absent", "acme/other", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "", "acme/other", "provider_access", at.Add(-time.Hour), nil, at)
	ownership(orgAlpha, "shadow", "acme/other", "provider_access", at.Add(-time.Hour), nil, at)
	// An ID-BEARING ownership row whose stored repo_full_name is stale and names
	// a DIFFERENT repo. The name arm must apply only when repo_id IS NULL;
	// without that clause this row would additionally donate repoB. It points at
	// its own repo, NOT at repoA, so it cannot mask the case-insensitive name
	// match that is repoA's only route in.
	mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github','team',?,'acme/other','exact','provider_access',?,NULL,?)`, orgAlpha, repoByID, at.Add(-time.Hour), at)
	attr := func(org, issue, team, source string, primary uint8, computed time.Time) {
		mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),?,'linear',?,?,?,'high','provider shaped attribution',?)`, org, issue, team, source, primary, computed)
	}
	ids := []string{"positive", "latest-ineligible", "secondary", "inactive", "missing-team", "empty-team"}
	attr(orgAlpha, "positive", "team", "native_team", 1, at)
	// A NEWER row for the same work item in ANOTHER org. The latest-generation
	// subquery must be org-fenced too: without that fence this foreign row wins
	// max(computed_at) and hides orgAlpha's own eligible attribution.
	attr(orgBeta, "positive", "team", "unassigned", 1, at.Add(time.Minute))
	attr(orgAlpha, "latest-ineligible", "team", "native_team", 1, at.Add(-time.Minute))
	attr(orgAlpha, "latest-ineligible", "team", "unassigned", 1, at)
	attr(orgAlpha, "secondary", "team", "native_team", 0, at)
	attr(orgAlpha, "inactive", "inactive", "native_team", 1, at)
	attr(orgAlpha, "missing-team", "absent", "native_team", 1, at)
	attr(orgAlpha, "empty-team", "", "native_team", 1, at)
	ids = append(ids, "null-team")
	mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),'null-team','linear',NULL,'native_team',1,'high','provider shaped attribution',?)`, orgAlpha, at)
	for _, source := range []string{"assignee_membership", "author_membership", "linked_issue", "manual_fallback", "unassigned"} {
		ids = append(ids, source)
		attr(orgAlpha, source, "team", source, 1, at)
	}
	// Foreign primary evidence collides with a requested ID but has no local
	// primary. A tenant fence on only ownership cannot protect this join.
	ids = append(ids, "foreign-only")
	attr(orgBeta, "foreign-only", "team", "native_team", 1, at)
	// A work item with an eligible primary attribution in BOTH orgs at the SAME
	// computed_at. The latest-generation filter cannot separate them, so the
	// outer org fence is the only clause excluding the foreign team's repo.
	ids = append(ids, "cross-tenant-tie")
	attr(orgAlpha, "cross-tenant-tie", "team", "native_team", 1, at)
	attr(orgBeta, "cross-tenant-tie", "shadow", "native_team", 1, at)
	donors, err := reader.FetchTeamRepoDonors(ctx, ids, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(donors))
	for _, d := range donors {
		got = append(got, fmt.Sprintf("%s/%s/%s", d.WorkItemID, d.TeamID, d.RepoID))
	}
	want := []string{
		"cross-tenant-tie/team/" + repoA, "cross-tenant-tie/team/" + repoByID,
		"positive/team/" + repoA, "positive/team/" + repoByID,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ownership donor positives/negatives: got %v want %v", got, want)
	}
}
