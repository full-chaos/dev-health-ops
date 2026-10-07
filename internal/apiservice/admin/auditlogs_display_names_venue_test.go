//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestAuditLogDisplayNamesVenueOracle runs the real, migrated Go API against
// Postgres. It is Go-only because the frozen Python audit response predates
// the approved additive fields; TestGovernanceRoutesVenueOracle still checks
// every unchanged audit response field against that frozen API contract.
func TestAuditLogDisplayNamesVenueOracle(t *testing.T) {
	ctx := context.Background()
	id := func(name string) uuid.UUID {
		return uuid.MustParse(venueoracle.StableUUID("CHAOS-8116/" + name))
	}

	orgID, otherOrgID := id("org"), id("other-org")
	adminID, superID, actorID, blankActorID := id("admin"), id("super"), id("actor"), id("blank-actor")
	memberID, blankMemberID, otherMemberID := id("member"), id("blank-member"), id("other-member")
	providerID, sourceID, blankSourceID, tokenID := id("provider"), id("source"), id("blank-source"), id("token")
	logIDs := map[string]uuid.UUID{}
	for _, action := range []string{
		"membership", "user", "session", "organization", "sso-provider", "ingest-source",
		"ingest-source-without-display-name", "ingest-token", "missing", "cross-org-user", "malformed", "user-without-display-name", "actor-without-display-name",
	} {
		logIDs[action] = id("audit-" + action)
	}

	const jwtKey = "venue-oracle-audit-display-names-key-32-bytes!"
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   repoRoot(t),
		GoOnly: true,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, query, args...); err != nil {
					t.Fatalf("seed: %v\\n%s", err, query)
				}
			}
			organization := func(org uuid.UUID, slug, name string) {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, 'enterprise', 'stripe', true, now(), now())`, org, slug, name)
			}
			organization(orgID, "audit-display", "Audit Display Organization")
			organization(otherOrgID, "audit-display-other", "Other Organization")
			exec(`INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, features_override, created_at, updated_at)
VALUES ($1, $2, 'enterprise', true, 'saas', 'stripe', '{}'::json, now(), now())`, id("license"), orgID)

			user := func(userID uuid.UUID, email, fullName string, superuser bool) {
				exec(`INSERT INTO users (id, email, full_name, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, true, $4, 0, now(), now())`, userID, email, fullName, superuser)
			}
			user(adminID, "audit-admin@example.com", "Audit Administrator", false)
			user(superID, "audit-super@example.com", "Audit Superuser", true)
			user(actorID, "audit-actor@example.com", "Audit Actor", false)
			user(blankActorID, "actor-email-only@example.com", "", false)
			user(memberID, "audit-member@example.com", "Audited Member", false)
			user(blankMemberID, "member-email-only@example.com", "", false)
			user(otherMemberID, "other-member@example.com", "Other Organization Member", false)
			membership := func(membershipID, userID, membershipOrg uuid.UUID, role string) {
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, membershipID, membershipOrg, userID, role)
			}
			membership(id("admin-membership"), adminID, orgID, "admin")
			membership(id("actor-membership"), actorID, orgID, "member")
			membership(id("member-membership"), memberID, orgID, "member")
			membership(id("blank-member-membership"), blankMemberID, orgID, "member")
			membership(id("other-member-membership"), otherMemberID, otherOrgID, "member")
			exec(`INSERT INTO org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, 'invite-email-is-not-a-display-name@example.com', 'member', 'audit-display-invite-token-hash', $3,
 'pending', now() + interval '1 day', now(), now())`, id("invite"), orgID, actorID)

			exec(`INSERT INTO sso_providers (id, org_id, name, protocol, status, is_default, allow_idp_initiated,
auto_provision_users, default_role, config, encrypted_secrets, allowed_domains, created_at, updated_at)
VALUES ($1, $2, 'Audit Identity', 'oidc', 'active', false, true, true, 'member', '{}'::json, NULL, '[]'::json,
now(), now())`, providerID, orgID)
			exec(`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, display_name, mode, enabled, created_at, updated_at)
VALUES ($1, $2, 'github', 'example/audit', 'operational', 'Audit Ingest Source', 'customer_push', true, now(), now()),
($3, $2, 'gitlab', 'instance-is-not-a-display-name', 'operational', '', 'customer_push', true, now(), now())`,
				sourceID, orgID, blankSourceID)
			exec(`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
VALUES ($1, $2, $3, 'Audit Ingest Token', 'test-token-hash', 'fcpush_audit', '["ingest:status"]'::jsonb, now())`,
				tokenID, orgID, sourceID)

			audit := func(action, resourceType, resourceID string, userID *uuid.UUID, second int) {
				exec(`INSERT INTO audit_logs (id, org_id, user_id, action, resource_type, resource_id, description, changes, request_metadata, status, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $4, '{}'::json, '{}'::json, 'success',
 '2026-10-04T21:00:00Z'::timestamptz + $7 * interval '1 second')`,
					logIDs[action], orgID, userID, action, resourceType, resourceID, second)
			}
			audit("membership", "membership", id("invite").String(), &actorID, 1)
			audit("user", "user", memberID.String(), &actorID, 2)
			audit("session", "session", memberID.String(), &actorID, 3)
			audit("organization", "organization", orgID.String(), &actorID, 4)
			audit("sso-provider", "sso_provider", providerID.String(), &actorID, 5)
			audit("ingest-source", "ingest_source", sourceID.String(), &actorID, 6)
			audit("ingest-source-without-display-name", "ingest_source", blankSourceID.String(), &actorID, 7)
			audit("ingest-token", "ingest_token", tokenID.String(), &actorID, 8)
			audit("missing", "sso_provider", id("deleted-provider").String(), &actorID, 9)
			audit("cross-org-user", "user", otherMemberID.String(), &actorID, 10)
			audit("malformed", "user", "not-a-uuid", &actorID, 11)
			audit("user-without-display-name", "user", blankMemberID.String(), &actorID, 12)
			audit("actor-without-display-name", "historical_extension", "opaque-id", &blankActorID, 13)

			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "audit-admin@example.com", "org_id": orgID.String(), "role": "admin"},
				"super": {"user_id": superID.String(), "email": "audit-super@example.com", "is_superuser": true},
			}
		},
	})
	base, _ := startGoServer(t, ctx, venue, jwtKey)
	auth := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}

	expected := map[string]auditDisplayExpectation{
		"membership":                         {actor: "Audit Actor", resource: nil},
		"user":                               {actor: "Audit Actor", resource: "Audited Member"},
		"session":                            {actor: "Audit Actor", resource: "Audited Member"},
		"organization":                       {actor: "Audit Actor", resource: "Audit Display Organization"},
		"sso-provider":                       {actor: "Audit Actor", resource: "Audit Identity"},
		"ingest-source":                      {actor: "Audit Actor", resource: "Audit Ingest Source"},
		"ingest-source-without-display-name": {actor: "Audit Actor", resource: nil},
		"ingest-token":                       {actor: "Audit Actor", resource: "Audit Ingest Token"},
		"missing":                            {actor: "Audit Actor", resource: nil},
		"cross-org-user":                     {actor: "Audit Actor", resource: nil},
		"malformed":                          {actor: "Audit Actor", resource: nil},
		"user-without-display-name":          {actor: "Audit Actor", resource: nil},
		"actor-without-display-name":         {actor: nil, resource: nil},
	}

	listResponse := venueoracle.Do(t, base, venueoracle.Request{
		Name: "audit list resolved display names", Method: http.MethodGet, Path: "/api/v1/admin/audit-logs", Headers: auth,
	})
	list := auditLogPage(t, listResponse)
	if list.Total != len(expected) || len(list.Items) != len(expected) {
		t.Fatalf("audit list cardinality = total %d, items %d; want %d", list.Total, len(list.Items), len(expected))
	}
	assertAuditDisplayNames(t, list.Items, expected)

	detail := auditLogObjectResponse(t, venueoracle.Do(t, base, venueoracle.Request{
		Name: "audit detail resolved display names", Method: http.MethodGet,
		Path: "/api/v1/admin/audit-logs/" + logIDs["user"].String(), Headers: auth,
	}))
	assertAuditDisplayName(t, detail, expected["user"])

	history := auditLogList(t, venueoracle.Do(t, base, venueoracle.Request{
		Name: "audit resource history resolved display names", Method: http.MethodGet,
		Path: "/api/v1/admin/audit-logs/resource/ingest_source/" + sourceID.String(), Headers: auth,
	}))
	if len(history) != 1 {
		t.Fatalf("audit resource history items = %d; want 1", len(history))
	}
	assertAuditDisplayName(t, history[0], expected["ingest-source"])

	activity := auditLogList(t, venueoracle.Do(t, base, venueoracle.Request{
		Name: "audit user activity resolved display names", Method: http.MethodGet,
		Path: "/api/v1/admin/audit-logs/user/" + actorID.String(), Headers: auth,
	}))
	if len(activity) != len(expected)-1 {
		t.Fatalf("audit user activity items = %d; want %d", len(activity), len(expected)-1)
	}
	assertAuditDisplayNames(t, activity, withoutAuditAction(expected, "actor-without-display-name"))

	platform := auditLogPage(t, venueoracle.Do(t, base, venueoracle.Request{
		Name: "platform audit list resolved display names", Method: http.MethodGet, Path: "/api/v1/admin/platform/audit-logs",
		Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["super"]},
	}))
	if platform.Total != len(expected) || len(platform.Items) != len(expected) {
		t.Fatalf("platform audit list cardinality = total %d, items %d; want %d", platform.Total, len(platform.Items), len(expected))
	}
	assertAuditDisplayNames(t, platform.Items, expected)

	venueoracle.WriteGoOnlyProof(t, "real migrated Postgres and all audit read routes resolve only authoritative audited-org display names; missing, malformed, cross-org, and e-mail-only records stay null")
}

type auditDisplayExpectation struct {
	actor, resource any
}

type auditLogPageResponse struct {
	Items []map[string]any `json:"items"`
	Total int              `json:"total"`
}

func auditLogPage(t *testing.T, response venueoracle.Response) auditLogPageResponse {
	t.Helper()
	if response.Status != http.StatusOK {
		t.Fatalf("audit list status = %d; want 200: %s", response.Status, response.Body)
	}
	var page auditLogPageResponse
	if err := json.Unmarshal([]byte(response.Body), &page); err != nil {
		t.Fatalf("decode audit list: %v", err)
	}
	return page
}

func auditLogList(t *testing.T, response venueoracle.Response) []map[string]any {
	t.Helper()
	if response.Status != http.StatusOK {
		t.Fatalf("audit list route status = %d; want 200: %s", response.Status, response.Body)
	}
	var items []map[string]any
	if err := json.Unmarshal([]byte(response.Body), &items); err != nil {
		t.Fatalf("decode audit list route: %v", err)
	}
	return items
}

func auditLogObjectResponse(t *testing.T, response venueoracle.Response) map[string]any {
	t.Helper()
	if response.Status != http.StatusOK {
		t.Fatalf("audit detail status = %d; want 200: %s", response.Status, response.Body)
	}
	var item map[string]any
	if err := json.Unmarshal([]byte(response.Body), &item); err != nil {
		t.Fatalf("decode audit detail: %v", err)
	}
	return item
}

func assertAuditDisplayNames(t *testing.T, items []map[string]any, expected map[string]auditDisplayExpectation) {
	t.Helper()
	seen := map[string]bool{}
	for _, item := range items {
		action, ok := item["action"].(string)
		if !ok {
			t.Fatalf("audit item action = %#v; want string", item["action"])
		}
		want, ok := expected[action]
		if !ok {
			t.Fatalf("unexpected audit action %q", action)
		}
		if seen[action] {
			t.Fatalf("duplicate audit action %q", action)
		}
		seen[action] = true
		assertAuditDisplayName(t, item, want)
	}
	if len(seen) != len(expected) {
		t.Fatalf("audit actions seen = %d; want %d", len(seen), len(expected))
	}
}

func assertAuditDisplayName(t *testing.T, item map[string]any, expected auditDisplayExpectation) {
	t.Helper()
	for field, want := range map[string]any{"actor_display_name": expected.actor, "resource_display_name": expected.resource} {
		got, present := item[field]
		if !present {
			t.Errorf("%s absent from %#v", field, item)
			continue
		}
		if got != want {
			t.Errorf("%s = %#v; want %#v for action %q", field, got, want, item["action"])
		}
	}
}

func withoutAuditAction(expected map[string]auditDisplayExpectation, action string) map[string]auditDisplayExpectation {
	without := make(map[string]auditDisplayExpectation, len(expected)-1)
	for name, value := range expected {
		if name != action {
			without[name] = value
		}
	}
	return without
}
