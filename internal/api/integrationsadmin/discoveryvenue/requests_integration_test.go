//go:build integration

package discoveryvenue

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// seed writes the orgs (A enterprise: no repo limit; B community: max_repos 3),
// users, the Jira credentials (encrypted by the Python plane's encrypt_value,
// the base URL the fake Jira) and the integrations, planner-managed parents and
// existing sources each discovery path starts from.
func seed(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, v ids, jiraURL string) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES
($1, 'disc-a', 'disc-a', '{}', 'enterprise', true, $3, $3), ($2, 'disc-b', 'disc-b', '{}', 'community', true, $3, $3)`, v.orgA, v.orgB, at)
	for _, user := range []struct {
		id, org uuid.UUID
		email   string
		role    string
	}{{v.adminA, v.orgA, "disc-admin-a@example.com", "admin"}, {v.memberA, v.orgA, "disc-member-a@example.com", "member"},
		{v.adminB, v.orgB, "disc-admin-b@example.com", "admin"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, $3, $3)`, user.id, user.email, at)
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			uuid.New(), user.id, user.org, user.role, at)
	}
	exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'disc-noorg@example.com', true, true, false, 0, $2, $2)`, v.adminNoOrg, at)

	var calls []venueoracle.PythonCall
	for _, token := range []string{"good-token", "bad-token"} {
		encoded, _ := json.Marshal(map[string]any{"email": "venue@example.com", "api_token": token, "base_url": jiraURL})
		calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}})
	}
	var ciphertexts []string
	for _, raw := range venue.CallPython(t, calls...) {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			t.Fatal(err)
		}
		ciphertexts = append(ciphertexts, text)
	}
	credential := func(id, org uuid.UUID, name, ciphertext string) {
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'jira', $3, true, $4, '{}'::json, $5, $5)`, id, org.String(), name, ciphertext, at)
	}
	credential(v.credGood, v.orgA, "jira good", ciphertexts[0])
	credential(v.credBad, v.orgA, "jira bad", ciphertexts[1])
	credential(v.credGoodB, v.orgB, "jira good b", ciphertexts[0])

	integration := func(id, org uuid.UUID, provider string, credential any, name, config string) {
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::json, true, $7, $7)`, id, org.String(), provider, credential, name, config, at)
	}
	planner := func(id, org, integrationID uuid.UUID, options string) {
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1, $2, $3, 'jira', '["work-items"]'::json, $4::json, true, true, $5, $6, $6)`, id, org.String(), "planner-"+id.String()[:8], options, integrationID, at)
	}
	source := func(id, org, integrationID uuid.UUID, provider, externalID, name string, metadata string, enabled bool, discovered string) {
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, $4, 'project', $5, $6, $5, $7::json, $8, $9::timestamptz, $9::timestamptz)`, id, org.String(), integrationID, provider, externalID, name, metadata, enabled, discovered)
	}

	// Unbounded planner-managed Jira integration: every project is tagged.
	integration(v.intJira, v.orgA, "jira", v.credGood, "disc-jira", `{"note": "plain"}`)
	planner(v.cfgJira, v.orgA, v.intJira, `{}`)
	// Bounded by its planner config's project_key: rediscovery supersedes the
	// stale tagged source and reconfirms the one that was superseded before.
	integration(v.intScoped, v.orgA, "jira", v.credGood, "disc-jira-scoped", `{"project_key": "ignored-by-planner"}`)
	planner(v.cfgScoped, v.orgA, v.intScoped, `{"project_key": "ACM"}`)
	source(v.srcAcm, v.orgA, v.intScoped, "jira", "ACM", "Old ACM name", `{"superseded_by_scope_change": true, "keep": [1, 2.50], "planner_managed_sync_config_id": "`+v.cfgScoped.String()+`"}`, false, "2026-02-01 00:00:00.000123+00")
	source(v.srcOld, v.orgA, v.intScoped, "jira", "OLDKEY", "Old", `{"planner_managed_sync_config_id": "`+v.cfgScoped.String()+`"}`, true, "2026-02-02 00:00:00+00")
	// No planner parent: the integration's own config scopes the discovery,
	// and a case-variant duplicate pair folds into one row.
	integration(v.intConfigScoped, v.orgA, "jira", v.credGood, "disc-jira-config-scoped", `{"project_key": "aerogear"}`)
	source(v.srcDupLower, v.orgA, v.intConfigScoped, "jira", "aerogear", "lower", `{}`, true, "2026-02-03 00:00:00+00")
	source(v.srcDupUpper, v.orgA, v.intConfigScoped, "JIRA", "AEROGEAR", "upper", `{"x": 1}`, false, "2026-02-04 00:00:00+00")
	integration(v.intBad, v.orgA, "jira", v.credBad, "disc-jira-bad", `{}`)
	integration(v.intNoCred, v.orgA, "jira", nil, "disc-jira-nocred", `{}`)
	integration(v.intLinear, v.orgA, "linear", nil, "disc-linear", `{}`)
	integration(v.intEmpty, v.orgA, "jira", v.credGood, "disc-jira-empty", `{"project_key": "NOSUCHPROJECT"}`)
	// Community org at max_repos 3: the discovered projects are capped.
	integration(v.intB, v.orgB, "jira", v.credGoodB, "disc-jira-b", `{}`)
	planner(v.cfgB, v.orgB, v.intB, `{}`)
}

func discoverRequests(venue *venueoracle.Venue, v ids) []venueoracle.Request {
	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	var out []venueoracle.Request
	post := func(name, id, token string) {
		out = append(out, venueoracle.Request{Name: name, Method: "POST", Path: discoverPath + id + "/discover", Headers: bearer(token)})
	}
	for _, who := range []string{"", "memberA", "adminNoOrg"} {
		out = append(out, venueoracle.Request{Name: "guard as " + who, Method: "POST", Path: discoverPath + v.intJira.String() + "/discover", Headers: func() map[string]string {
			if who == "" {
				return map[string]string{}
			}
			return bearer(who)
		}()})
	}
	out = append(out, venueoracle.Request{Name: "guard bad scheme", Method: "POST", Path: discoverPath + v.intJira.String() + "/discover",
		Headers: map[string]string{"Authorization": "Basic abc"}})
	post("unknown integration", uuid.NewString(), "adminA")
	post("not a uuid", "zzz", "adminA")
	post("another org's integration", v.intB.String(), "adminA")
	post("empty scope: no such project", v.intEmpty.String(), "adminA")
	post("linear has no discovery", v.intLinear.String(), "adminA")
	post("no credential", v.intNoCred.String(), "adminA")
	post("provider refuses the credential", v.intBad.String(), "adminA")
	post("unbounded planner-managed: first discovery", v.intJira.String(), "adminA")
	post("unbounded planner-managed: rediscovery", v.intJira.String(), "adminA")
	post("path spelling is echoed (upper-case uuid)", strings.ToUpper(v.intJira.String()), "adminA")
	post("bounded by the planner config: supersede and reconfirm", v.intScoped.String(), "adminA")
	post("bounded by the planner config: rediscovery", v.intScoped.String(), "adminA")
	post("scoped by the integration config, duplicates fold", v.intConfigScoped.String(), "adminA")
	post("scoped by the integration config, again", v.intConfigScoped.String(), "adminA")
	post("community org over its repo limit", v.intB.String(), "adminB")
	post("community org over its repo limit, again", v.intB.String(), "adminB")
	return out
}
