//go:build integration

package integrationsvenue

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ids are the seeded rows' ids, fixed per run so requests can name them.
type ids struct {
	orgA, orgB, orgC                                 uuid.UUID
	adminA, memberA, adminB, adminC, adminNoOrg      uuid.UUID
	superNoOrg                                       uuid.UUID
	credGitHub, credGitHubB, credJira, credGitLab    uuid.UUID
	intGitHub, intJira, intLinear, intPagerDuty      uuid.UUID
	intEmpty, intB, intC, intGitLab, intUpper        uuid.UUID
	srcCapped, srcUpper, srcSpaced, srcFalsy, srcGit uuid.UUID
	srcEnabledMarked, srcOther                       uuid.UUID
	srcB, srcC1, srcC2, srcCEnabled                  uuid.UUID
	dsCommits, dsPrs, dsJiraItems, dsUnavailable     uuid.UUID
	cfgA1, cfgA2, cfgA3, cfgC1, cfgCPlanner          uuid.UUID
	orgD, adminD                                     uuid.UUID
	intPairs, intBadPairs, intCustom                 uuid.UUID
	srcTurkish, srcSep, srcPairsMeta, dsPairs        uuid.UUID
}

func newIDs() ids {
	var v ids
	for _, target := range []*uuid.UUID{
		&v.orgA, &v.orgB, &v.orgC, &v.adminA, &v.memberA, &v.adminB, &v.adminC, &v.adminNoOrg, &v.superNoOrg,
		&v.credGitHub, &v.credGitHubB, &v.credJira, &v.credGitLab, &v.intGitHub, &v.intJira, &v.intLinear,
		&v.intPagerDuty, &v.intEmpty, &v.intB, &v.intC, &v.intGitLab, &v.intUpper, &v.srcCapped, &v.srcUpper,
		&v.srcSpaced, &v.srcFalsy, &v.srcGit, &v.srcEnabledMarked, &v.srcOther, &v.srcB, &v.srcC1, &v.srcC2,
		&v.srcCEnabled, &v.dsCommits, &v.dsPrs, &v.dsJiraItems, &v.dsUnavailable, &v.cfgA1, &v.cfgA2, &v.cfgA3,
		&v.cfgC1, &v.cfgCPlanner, &v.orgD, &v.adminD, &v.intPairs, &v.intBadPairs, &v.intCustom, &v.srcTurkish, &v.srcSep,
		&v.srcPairsMeta, &v.dsPairs,
	} {
		*target = uuid.New()
	}
	return v
}

// seed writes the venue's rows. Timestamps carry microseconds where a
// rendering depends on them; JSON columns are given as text so their
// spelling (key order, duplicate keys, float forms) is what is stored.
func seed(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v ids) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	// A: community (max_repos 3); B: enterprise (no limit); C: community.
	for _, org := range []struct {
		id   uuid.UUID
		slug string
		tier string
	}{{v.orgA, "integ-a", "community"}, {v.orgB, "integ-b", "enterprise"}, {v.orgC, "integ-c", "community"}, {v.orgD, "integ-d", "community"}} {
		exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at)
VALUES ($1, $2, $2, '{}', $3, true, now(), now())`, org.id, org.slug, org.tier)
	}
	for _, user := range []struct {
		id    uuid.UUID
		email string
		super bool
	}{
		{v.adminA, "admin-a@example.com", false}, {v.memberA, "member-a@example.com", false},
		{v.adminB, "admin-b@example.com", false}, {v.adminC, "admin-c@example.com", false},
		{v.adminNoOrg, "admin-noorg@example.com", false}, {v.superNoOrg, "super-noorg@example.com", true},
		{v.adminD, "admin-d@example.com", false},
	} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, user.id, user.email, user.super)
	}
	for _, member := range []struct {
		org, user uuid.UUID
		role      string
	}{{v.orgA, v.adminA, "admin"}, {v.orgA, v.memberA, "member"}, {v.orgB, v.adminB, "admin"}, {v.orgC, v.adminC, "admin"}, {v.orgD, v.adminD, "admin"}} {
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, now(), now())`,
			uuid.New(), member.user, member.org, member.role)
	}

	credential := func(id, org uuid.UUID, provider string) {
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, now(), now())`, id, org.String(), provider, "cred-"+provider)
	}
	credential(v.credGitHub, v.orgA, "github")
	credential(v.credGitHubB, v.orgB, "github")
	credential(v.credJira, v.orgA, "jira")
	credential(v.credGitLab, v.orgA, "gitlab")

	integration := func(id, org uuid.UUID, provider string, credential any, name, config string, active bool, cron, zone any, created string) {
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, schedule_cron, timezone, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::json, $7, $8, $9, $10::timestamptz, $10::timestamptz + interval '1 hour 2 microseconds')`,
			id, org.String(), provider, credential, name, config, active, cron, zone, created)
	}
	integration(v.intGitHub, v.orgA, "github", v.credGitHub, "seed-a-github",
		`{"owner": "acme", "n": 1, "f": 1.0, "e": 1e2, "big": 12345678901234567890, "tiny": 1e-7, "uni": "café 😀", "dup": 1, "dup": 2, "nested": {"b": 1, "a": [1.5, "x", null, true]}}`,
		true, "0 * * * *", "UTC", "2026-03-01 00:00:00.123456+00")
	integration(v.intJira, v.orgA, "jira", nil, "seed-a-jira", `{}`, false, nil, nil, "2026-03-02 00:00:00.5+00")
	integration(v.intLinear, v.orgA, "linear", nil, "seed-a-linear", `null`, true, nil, "Europe/Paris", "2026-03-03 00:00:00+00")
	integration(v.intPagerDuty, v.orgA, "pagerduty", nil, "seed-a-pagerduty", `[]`, true, "", "", "2026-03-04 00:00:00.000001+00")
	integration(v.intEmpty, v.orgA, "gitlab", v.credGitLab, "seed-a-gitlab-empty", `{"group": "g"}`, true, nil, nil, "2026-03-05 00:00:00.999999+00")
	integration(v.intUpper, v.orgA, "JIRA", nil, "seed-a-upper-jira", `{"k": "v"}`, true, nil, nil, "2026-03-06 00:00:00+00")
	integration(v.intGitLab, v.orgA, "gitlab", nil, "seed-a-gitlab-datasets", `{}`, true, nil, nil, "2026-03-07 00:00:00+00")
	integration(v.intB, v.orgB, "jira", nil, "seed-b-jira", `{"b": true}`, true, nil, nil, "2026-03-08 00:00:00+00")
	integration(v.intC, v.orgC, "jira", nil, "seed-c-jira", `{}`, true, nil, nil, "2026-03-09 00:00:00+00")
	integration(v.intCustom, v.orgA, "custom", nil, "seed-a-custom", `{}`, true, nil, nil, "2026-03-10 00:00:00+00")
	// dict(value) reads a list of pairs; a pair whose key is not a string
	// cannot be rendered (the response model is dict[str, Any]).
	integration(v.intPairs, v.orgD, "github", nil, "seed-d-pairs", `[["k", "v"], ["n", 1], "ab"]`, true, nil, nil, "2026-03-11 00:00:00+00")
	integration(v.intBadPairs, v.orgD, "github", nil, "seed-d-bad-pairs", `[[1, "v"]]`, true, nil, nil, "2026-03-12 00:00:00+00")

	source := func(id, org, integrationID uuid.UUID, provider, externalID, name, fullName, metadata string, enabled bool, syncedAt any, success any, syncError any) {
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name,
metadata, is_enabled, discovered_at, last_seen_at, last_sync_at, last_sync_success, last_sync_error)
VALUES ($1, $2, $3, $4, 'repository', $5, $6, $7, $8::json, $9, '2026-02-01 00:00:00.000123+00', '2026-02-02 00:00:00+00', $10::timestamptz, $11, $12)`,
			id, org.String(), integrationID, provider, externalID, name, fullName, metadata, enabled, syncedAt, success, syncError)
	}
	// Jira sources on the org-A jira integration: every enable-path shape.
	source(v.srcCapped, v.orgA, v.intJira, "jira", "P1", "P1", "Proj One", `{"capped_by_repo_limit": true, "keep": [1, 2.50], "z": null}`, false, nil, nil, nil)
	source(v.srcUpper, v.orgA, v.intJira, "JIRA", "P2", "P2", "Proj Two", `{"superseded_by_scope_change": true, "capped_by_repo_limit": 1}`, false, nil, nil, nil)
	source(v.srcSpaced, v.orgA, v.intJira, " jira ", "P3", "P3", "Proj Three", `{}`, false, "2026-02-03 04:05:06.789012+00", false, "boom")
	source(v.srcFalsy, v.orgA, v.intJira, "jira", "P4", "P4", "Proj Four", `{"capped_by_repo_limit": false, "superseded_by_scope_change": "", "z": 1}`, false, nil, nil, nil)
	source(v.srcEnabledMarked, v.orgA, v.intJira, "jira", "P5", "P5", "Proj Five", `{"superseded_by_scope_change": true, "k": 2}`, true, "2026-02-04 00:00:00+00", true, nil)
	source(v.srcTurkish, v.orgA, v.intJira, "J\u0130RA", "P6", "P6", "Proj Six", `{}`, false, nil, nil, nil)
	source(v.srcSep, v.orgA, v.intJira, "\x1fjira\x1c", "P7", "P7", "Proj Seven", `{"capped_by_repo_limit": true}`, false, nil, nil, nil)
	source(v.srcPairsMeta, v.orgD, v.intPairs, "github", "d/repo", "repo", "d/repo", `[["m", 1]]`, false, nil, nil, nil)
	source(v.srcGit, v.orgA, v.intGitHub, "github", "acme/repo", "repo", "acme/repo", `{"private": false}`, false, nil, nil, nil)
	source(v.srcOther, v.orgA, v.intGitHub, "github", "acme/other", "other", "acme/other", `{}`, true, nil, nil, nil)
	source(v.srcB, v.orgB, v.intB, "jira", "B1", "B1", "B Proj", `{"capped_by_repo_limit": true, "b": 1}`, false, nil, nil, nil)
	source(v.srcC1, v.orgC, v.intC, "jira", "C1", "C1", "C Proj One", `{"capped_by_repo_limit": true}`, false, nil, nil, nil)
	source(v.srcC2, v.orgC, v.intC, "jira", "C2", "C2", "C Proj Two", `{}`, false, nil, nil, nil)
	source(v.srcCEnabled, v.orgC, v.intC, "jira", "C0", "C0", "C Proj Zero", `{}`, true, nil, nil, nil)

	dataset := func(id, org, integrationID uuid.UUID, key string, enabled bool, options string, reason, since, seen any) {
		exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options, unavailable_reason, unavailable_since, unavailable_last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6::json, $7, $8::timestamptz, $9::timestamptz)`, id, org.String(), integrationID, key, enabled, options, reason, since, seen)
	}
	dataset(v.dsPairs, v.orgD, v.intPairs, "commits", true, `[["o", true]]`, nil, nil, nil)
	dataset(v.dsCommits, v.orgA, v.intGitHub, "commits", true, `{"depth": 30}`, nil, nil, nil)
	dataset(v.dsPrs, v.orgA, v.intGitHub, "prs", false, `{}`, "provider_dataset_unavailable", "2026-04-01 00:00:00.000001+00", "2026-04-02 00:00:00+00")
	dataset(v.dsJiraItems, v.orgA, v.intJira, "work-items", true, `{}`, nil, nil, nil)
	dataset(v.dsUnavailable, v.orgA, v.intJira, "incidents", false, `{"a": 1.0}`, nil, nil, nil)

	// The repo limit's inputs: org A holds three active plain sync configs
	// (at its community limit of 3); org C one, plus a planner-managed
	// integration whose enabled sources count.
	config := func(id, org uuid.UUID, name string, plannerManaged bool, integrationID any) {
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, created_at, updated_at)
VALUES ($1, $2, $3, 'github', '[]'::json, '{}'::json, true, $4, $5, now(), now())`, id, org.String(), name, plannerManaged, integrationID)
	}
	config(v.cfgA1, v.orgA, "a1", false, nil)
	config(v.cfgA2, v.orgA, "a2", false, nil)
	config(v.cfgA3, v.orgA, "a3", false, nil)
	config(v.cfgC1, v.orgC, "c1", false, nil)
	config(v.cfgCPlanner, v.orgC, "c-planner", true, v.intC)
}
