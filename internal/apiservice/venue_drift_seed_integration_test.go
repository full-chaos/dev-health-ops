//go:build integration

package apiservice

import (
	"context"
	"fmt"
	"strings"
	"testing"

	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// seedDriftReview inserts the rows the team drift review requests read into
// BOTH planes' ClickHouse databases (the routes only READ what import and
// provider sync write, so the two planes must start from identical state):
// pending/decided/foreign-org drift changes of every shape, provider
// observations, and the manual membership / member fallback an identity
// change conflicts with. Payload JSON is what Python writes: canonical
// (sorted keys, compact) with datetimes rendered by default=str -- aware
// ("... 00:00:00+00:00") in a new_value built from a membership dataclass,
// naive in an old_value built from a ClickHouse read.
func seedDriftReview(t *testing.T, ctx context.Context, venue *venueoracle.Venue, orgA, orgB string) {
	t.Helper()
	q := func(s string) string {
		return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", `\'`) + "'"
	}
	ts := func(s string) string { return "toDateTime64(" + q(s) + ", 6, 'UTC')" }
	nullable := func(s string) string {
		if s == "" {
			return "NULL"
		}
		return q(s)
	}
	type change struct {
		org, id, entityType, entityID, provider, nativeKey, changeType, field, oldJSON, newJSON, status, firstSeen string
	}
	membershipNew := `{"is_primary":0,"member_id":"alice","org_id":"` + orgA + `","priority":0,"provider":"github","raw_email":"alice@example.com","raw_provider_user_id":"alice-gh","source":"native","specificity":0,"team_id":"design","updated_at":"2026-09-01 10:00:00+00:00","valid_from":"2026-09-01 00:00:00+00:00","valid_to":null}`
	membershipOld := `{"field":"team_memberships","manual_membership":{"is_primary":0,"member_id":"alice","org_id":"` + orgA + `","priority":0,"provider":"github","raw_email":"alice@example.com","raw_provider_user_id":"alice-gh","source":"manual","specificity":0,"team_id":"eng-old","updated_at":"2026-08-01 00:00:00","valid_from":"2026-08-01 00:00:00","valid_to":null}}`
	fallbackOld := `{"field":"manual_attribution_fallbacks.member","manual_fallback":{"created_at":"2026-08-01 00:00:00","created_by":"admin","org_id":"` + orgA + `","priority":50,"provider":"github","reason":"manual","scope_id":"bob@example.com","scope_type":"member","team_id":"eng-old","team_name":"Eng Old","updated_at":"2026-08-01 00:00:00","valid_from":"2026-08-01 00:00:00","valid_to":null}}`
	membershipNewBob := `{"is_primary":1,"member_id":"bob","org_id":"` + orgA + `","priority":5,"provider":"github","raw_email":"bob@example.com","raw_provider_user_id":null,"source":"native","specificity":2,"team_id":"design","updated_at":"2026-09-01 10:00:00+00:00","valid_from":"2026-09-01 00:00:00+00:00","valid_to":null}`
	changes := []change{
		{orgA, "c-name-qa", "team", "qa", "jira", "qa", "field_changed", "name", `"QA"`, `"QA Observed"`, "pending", "2026-09-01 10:00:00.123456"},
		{orgA, "c-members-qa", "team", "qa", "jira", "qa", "field_changed", "members", `["a"]`, `["b","c"]`, "pending", "2026-09-01 11:00:00"},
		{orgA, "c-desc-qa", "team", "qa", "jira", "qa", "field_changed", "description", `null`, `"obs desc"`, "pending", "2026-09-01 12:00:00"},
		{orgA, "c-badcol-qa", "team", "qa", "jira", "qa", "field_changed", "extra_field", `1`, `2`, "pending", "2026-09-01 13:00:00"},
		{orgA, "c-active-qa", "team", "qa", "jira", "qa", "field_changed", "is_active", `1`, `0`, "pending", "2026-09-01 14:00:00"},
		{orgA, "c-nofield-qa", "team", "qa", "jira", "qa", "team_removed", "", `null`, `null`, "pending", "2026-09-01 15:00:00"},
		{orgA, "c-projkeys-dr", "team", "dr-team", "jira", "dr-team", "field_changed", "project_keys", `[]`, `["DR1","DR2"]`, "pending", "2026-09-01 09:00:00"},
		{orgA, "c-repo-dr", "team", "dr-team", "jira", "dr-team", "field_changed", "repo_patterns", `[]`, `["dr/repo"]`, "pending", "2026-09-01 09:00:00"},
		{orgA, "c-members-dr", "team", "dr-team", "jira", "dr-team", "field_changed", "members", `[]`, `["x"]`, "pending", "2026-09-01 08:00:00"},
		{orgA, "c-noobs", "team", "nobs", "jira", "nobs", "field_changed", "name", `"A"`, `"B"`, "pending", "2026-09-01 07:00:00"},
		{orgA, "c-ident-mem", "identity", "design", "github", "design", "membership_changed", "team_memberships", membershipOld, membershipNew, "pending", "2026-09-01 06:00:00"},
		{orgA, "c-ident-fb", "identity", "design", "github", "design", "membership_changed", "manual_attribution_fallbacks.member", fallbackOld, membershipNewBob, "pending", "2026-09-01 05:00:00"},
		{orgA, "c-ident-bad", "identity", "design", "github", "design", "membership_changed", "team_memberships", `null`, `"not-a-dict"`, "pending", "2026-09-01 04:30:00"},
		{orgA, "c-already", "team", "qa", "jira", "qa", "field_changed", "name", `"x"`, `"y"`, "approved", "2026-09-01 04:00:00"},
		{orgA, "c-dismissed", "team", "qa", "jira", "qa", "field_changed", "name", `"x"`, `"z"`, "dismissed", "2026-09-01 03:00:00"},
		{orgA, "c-badjson", "team", "qa", "jira", "qa", "field_changed", "description", `not json`, `{bad`, "pending", "2026-09-01 02:00:00"},
		{orgB, "c-other-org", "team", "qa", "jira", "qa", "field_changed", "name", `"o"`, `"p"`, "pending", "2026-09-01 01:00:00"},
	}
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatal(err)
		}
		exec := func(statement string) {
			t.Helper()
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("seed %s: %v\n%s", database, err, statement)
			}
		}
		for _, c := range changes {
			exec(fmt.Sprintf(`INSERT INTO team_drift_changes (org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field,
				old_value_json, new_value_json, status, first_seen_at, last_seen_at, decided_at, decided_by, updated_at) VALUES
				(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, %s)`,
				q(c.org), q(c.id), q(c.entityType), q(c.entityID), q(c.provider), q(c.nativeKey), q(c.changeType), nullable(c.field),
				q(c.oldJSON), q(c.newJSON), q(c.status), ts(c.firstSeen), ts("2026-09-02 00:00:00"), ts("2026-09-02 00:00:00")))
		}
		obs := func(team, key, name, desc, members, projects, repos string) {
			exec(fmt.Sprintf(`INSERT INTO team_provider_observations (org_id, provider, native_team_key, team_id, name, description, members_json,
				project_keys_json, repo_patterns_json, is_active, parent_team_id, discovered_at, updated_at) VALUES
				(%s, 'jira', %s, %s, %s, %s, %s, %s, %s, 1, NULL, %s, %s)`,
				q(orgA), q(key), q(team), nullable(name), nullable(desc), q(members), q(projects), q(repos),
				ts("2026-09-01 00:00:00"), ts("2026-09-01 00:00:00")))
		}
		obs("qa", "qa", "QA Observed", "obs desc", `["b","c"]`, `["PK1"]`, `["r1"]`)
		// A double-encoded members column: _json_list decodes it a second time.
		obs("dr-team", "dr-team", "DR Observed", "", `"[\"x\",\"y\"]"`, `["DR1","DR2"]`, `["dr/repo"]`)
		exec(fmt.Sprintf(`INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source,
			is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES
			(%s, 'github', 'eng-old', 'alice', 'alice-gh', 'alice@example.com', [], 'manual', 0, 0, 0, %s, NULL, %s)`,
			q(orgA), ts("2026-08-01 00:00:00"), ts("2026-08-01 00:00:00")))
		exec(fmt.Sprintf(`INSERT INTO manual_attribution_fallbacks (org_id, provider, scope_type, scope_id, team_id, team_name, reason, priority,
			valid_from, valid_to, created_by, created_at, updated_at) VALUES
			(%s, 'github', 'member', 'bob@example.com', 'eng-old', 'Eng Old', 'manual', 50, %s, NULL, 'admin', %s, %s)`,
			q(orgA), ts("2026-08-01 00:00:00"), ts("2026-08-01 00:00:00"), ts("2026-08-01 00:00:00")))
		_ = conn.Close()
	}
}
