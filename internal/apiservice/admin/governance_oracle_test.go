//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// redactDeep blanks every string value stored under one of keys at any depth
// of a JSON body, leaving key order and every other value alone. It is used
// only on responses that carry rows a write created on each plane (random
// ids, wall-clock timestamps).
func redactDeep(t *testing.T, body string, keys ...string) string {
	t.Helper()
	value, err := pyjson.DecodeString(body)
	if err != nil {
		return body
	}
	var walk func(pyjson.Value) pyjson.Value
	walk = func(node pyjson.Value) pyjson.Value {
		switch typed := node.(type) {
		case *pyjson.Object:
			for _, key := range typed.Keys() {
				item, _ := typed.Get(key)
				redacted := false
				for _, want := range keys {
					if key == want {
						if _, isString := item.(string); isString {
							typed.Set(key, "")
							redacted = true
						}
					}
				}
				if !redacted {
					typed.Set(key, walk(item))
				}
			}
			return typed
		case []pyjson.Value:
			for index := range typed {
				typed[index] = walk(typed[index])
			}
			return typed
		default:
			return node
		}
	}
	encoded, err := pyjson.Marshal(walk(value))
	if err != nil {
		return body
	}
	return string(encoded)
}

// TestGovernanceRoutesVenueOracle is the venue-oracle proof for the
// governance admin routes: audit logs, feature flags and overrides, the IP
// allowlist, and platform stats. The real Python api and the Go api answer
// the same requests against two copies of one database; every response is
// compared byte for byte, and the rows the writes touched are compared after.
func TestGovernanceRoutesVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-governance-flow-32-bytes!!"

	orgEnterprise := uuid.New() // org_licenses tier enterprise
	orgCommunity := uuid.New()  // org_licenses tier community
	orgOverride := uuid.New()   // community license with features_override audit_log
	orgTierOnly := uuid.New()   // no license row, organizations.tier enterprise
	orgBogus := uuid.New()      // org_licenses tier outside LicenseTier
	orgAnyNoLicense := uuid.New()
	superID, superOrgID := uuid.New(), uuid.New()
	adminEnt, adminComm, adminOvr, adminTier, adminBogus, memberEnt := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	adminNoLicense := uuid.New()

	auditIDs := make([]uuid.UUID, 8)
	for i := range auditIDs {
		auditIDs[i] = uuid.New()
	}
	badShapeAuditID := uuid.New()
	otherOrgAuditID := uuid.New()
	ipRows := map[string]uuid.UUID{}
	for _, name := range []string{"v4", "cidr", "v6", "expired", "inactive"} {
		ipRows[name] = uuid.New()
	}
	overrideSeedID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			org := func(id uuid.UUID, slug, tier string) {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, $3, 'stripe', true, now(), now())`, id, slug, tier)
			}
			license := func(orgID uuid.UUID, tier, overrides string) {
				exec(`INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, features_override, created_at, updated_at)
VALUES ($1, $2, $3, true, 'saas', 'stripe', $4::json, now(), now())`, uuid.New(), orgID, tier, overrides)
			}
			org(orgEnterprise, "gov-ent", "enterprise")
			license(orgEnterprise, "enterprise", "{}")
			org(orgCommunity, "gov-comm", "community")
			license(orgCommunity, "community", "{}")
			org(orgOverride, "gov-ovr", "community")
			license(orgOverride, "community", `{"audit_log": true, "ip_allowlist": 0}`)
			org(orgTierOnly, "gov-tier", "enterprise")
			org(orgBogus, "gov-bogus", "enterprise")
			license(orgBogus, "platinum", "{}")
			org(orgAnyNoLicense, "gov-none", "community")
			user := func(id uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, email, super)
			}
			user(superID, "gov-super@example.com", true)
			user(superOrgID, "gov-superorg@example.com", true)
			user(adminEnt, "gov-adminent@example.com", false)
			user(adminComm, "gov-admincomm@example.com", false)
			user(adminOvr, "gov-adminovr@example.com", false)
			user(adminTier, "gov-admintier@example.com", false)
			user(adminBogus, "gov-adminbogus@example.com", false)
			user(memberEnt, "gov-memberent@example.com", false)
			user(adminNoLicense, "gov-adminnone@example.com", false)

			audit := func(id, org uuid.UUID, user *uuid.UUID, action, rtype, rid string, changes, meta any, status string, errMsg any, at string) {
				exec(`INSERT INTO audit_logs (id, org_id, user_id, action, resource_type, resource_id, description, changes, request_metadata, status, error_message, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::json, $9::json, $10, $11, $12::timestamptz)`,
					id, org, user, action, rtype, rid, "described "+action, changes, meta, status, errMsg, at)
			}
			adminEntRef := adminEnt
			audit(auditIDs[0], orgEnterprise, &adminEntRef, "create", "team", "team-1",
				`{"created": {"zeta": 1, "alpha": [1.5, 2.0, 1e21, 12345678901234567890], "name": "Café 東京", "nested": {"b": null, "a": true}}}`,
				`{"ip_address": "10.0.0.1", "user_agent": "venue"}`, "success", nil, "2026-01-01T10:00:00.000001+00:00")
			audit(auditIDs[1], orgEnterprise, &adminEntRef, "update", "team", "team-1",
				`{"before": {"x": 1}, "after": {"x": 2}}`, `null`, "success", nil, "2026-01-01T11:00:00+00:00")
			audit(auditIDs[2], orgEnterprise, nil, "delete", "user", "user-9",
				nil, nil, "failure", "boom", "2026-01-02T09:30:00+00:00")
			audit(auditIDs[3], orgEnterprise, &adminEntRef, "login", "session", adminEnt.String(),
				`{}`, `{}`, "success", nil, "2026-01-03T00:00:00+00:00")
			audit(auditIDs[4], orgEnterprise, &adminEntRef, "create", "team", "team-2",
				`{"created": {"k": "v"}}`, `{"request_id": "r-1"}`, "success", nil, "2026-01-04T12:00:00+00:00")
			audit(auditIDs[5], orgEnterprise, &adminEntRef, "update", "team", "team-1",
				`{"after": {"x": 3}}`, `{}`, "failure", "denied", "2026-01-05T12:00:00+00:00")
			audit(auditIDs[6], orgCommunity, nil, "create", "team", "team-1", `{}`, `{}`, "success", nil, "2026-01-06T12:00:00+00:00")
			audit(auditIDs[7], orgOverride, nil, "create", "team", "team-o", `{"o": 1}`, `{}`, "success", nil, "2026-01-07T12:00:00+00:00")
			audit(badShapeAuditID, orgTierOnly, nil, "create", "team", "bad", `[]`, `{}`, "success", nil, "2026-01-08T12:00:00+00:00")
			audit(otherOrgAuditID, orgTierOnly, nil, "create", "team", "other", `{}`, `{}`, "success", nil, "2026-01-09T12:00:00+00:00")

			ip := func(name, rng string, desc any, active bool, expires any, createdAt string) {
				exec(`INSERT INTO org_ip_allowlist (id, org_id, ip_range, description, is_active, created_by_id, created_at, updated_at, expires_at)
VALUES ($1, $2, $3, $4, $5, NULL, $6::timestamptz, $6::timestamptz, $7::timestamptz)`,
					ipRows[name], orgEnterprise, rng, desc, active, createdAt, expires)
			}
			ip("v4", "192.168.1.5", "office", true, nil, "2026-02-01T00:00:00+00:00")
			ip("cidr", "10.0.0.0/8", nil, true, "2099-01-01T00:00:00+00:00", "2026-02-02T00:00:00+00:00")
			ip("v6", "2001:db8::/32", "lab", true, nil, "2026-02-03T00:00:00+00:00")
			ip("expired", "172.16.0.0/12", "old", true, "2020-01-01T00:00:00+00:00", "2026-02-04T00:00:00+00:00")
			ip("inactive", "203.0.113.7", "off", false, nil, "2026-02-05T00:00:00+00:00")

			exec(`UPDATE feature_flags SET created_at = '2020-01-01T00:00:00+00:00', updated_at = '2020-01-01T00:00:00+00:00'`)
			exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, (SELECT id FROM feature_flags ORDER BY key LIMIT 1), true, NULL, '{"b": 1, "a": 2}'::json, 'seed', NULL, '2026-03-01T00:00:00+00:00', '2026-03-01T00:00:00+00:00')`,
				overrideSeedID, orgEnterprise)

			admin2 := func(id, org uuid.UUID, email string) map[string]any {
				return map[string]any{"user_id": id.String(), "email": email, "org_id": org.String(), "role": "admin"}
			}
			return map[string]map[string]any{
				"super":    {"user_id": superID.String(), "email": "gov-super@example.com", "is_superuser": true},
				"superOrg": {"user_id": superOrgID.String(), "email": "gov-superorg@example.com", "org_id": orgEnterprise.String(), "role": "owner", "is_superuser": true},
				"ent":      admin2(adminEnt, orgEnterprise, "gov-adminent@example.com"),
				"comm":     admin2(adminComm, orgCommunity, "gov-admincomm@example.com"),
				"ovr":      admin2(adminOvr, orgOverride, "gov-adminovr@example.com"),
				"tier":     admin2(adminTier, orgTierOnly, "gov-admintier@example.com"),
				"bogus":    admin2(adminBogus, orgBogus, "gov-adminbogus@example.com"),
				"none":     admin2(adminNoLicense, orgAnyNoLicense, "gov-adminnone@example.com"),
				"member":   {"user_id": memberEnt.String(), "email": "gov-memberent@example.com", "org_id": orgEnterprise.String(), "role": "member"},
			}
		},
	})

	flagIDs := strings.Split(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB),
		`SELECT id::text FROM feature_flags ORDER BY key LIMIT 14`), " | ")
	if len(flagIDs) < 14 {
		t.Fatalf("seeded only %d feature flags, need 14", len(flagIDs))
	}

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	jsonAuthWithUser := func(name, userID string) map[string]string {
		h := jsonAuth(name)
		h["X-User-Id"] = userID
		return h
	}
	const admin = "/api/v1/admin"
	get := func(name, path, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: admin + path, Headers: auth(token)}
	}
	send := func(name, method, path, token, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: method, Path: admin + path, Headers: jsonAuth(token), Body: venueoracle.B64(body)}
	}

	requests := []venueoracle.Request{
		// ---- platform stats -------------------------------------------------
		get("stats superuser", "/platform/stats", "super"),
		get("stats non-superuser refused", "/platform/stats", "ent"),
		venueoracle.Request{Name: "stats unauthenticated", Method: "GET", Path: admin + "/platform/stats"},

		// ---- audit logs (reads, before any write) ---------------------------
		get("audit list", "/audit-logs", "ent"),
		get("audit list paged", "/audit-logs?limit=2&offset=1", "ent"),
		get("audit list limit zero", "/audit-logs?limit=0", "ent"),
		get("audit list limit over", "/audit-logs?limit=501", "ent"),
		get("audit list offset negative", "/audit-logs?offset=-1", "ent"),
		get("audit list limit text", "/audit-logs?limit=abc", "ent"),
		get("audit list duplicate limit", "/audit-logs?limit=1&limit=3", "ent"),
		get("audit list padded limit", "/audit-logs?limit=%202%20", "ent"),
		get("audit list huge offset", "/audit-logs?offset=99999999999999999999", "ent"),
		get("audit list action", "/audit-logs?action=update", "ent"),
		get("audit list empty action", "/audit-logs?action=", "ent"),
		get("audit list resource type and id", "/audit-logs?resource_type=team&resource_id=team-1", "ent"),
		get("audit list status", "/audit-logs?status=failure", "ent"),
		get("audit list user", "/audit-logs?user_id="+adminEnt.String(), "ent"),
		get("audit list user malformed", "/audit-logs?user_id=not-a-uuid", "ent"),
		get("audit list user urn form", "/audit-logs?user_id=urn:uuid:"+adminEnt.String(), "ent"),
		get("audit list start_date z", "/audit-logs?start_date=2026-01-02T00:00:00Z", "ent"),
		get("audit list start_date naive", "/audit-logs?start_date=2026-01-02T00:00:00", "ent"),
		get("audit list start_date date", "/audit-logs?start_date=2026-01-03", "ent"),
		get("audit list start_date offset", "/audit-logs?start_date=2026-01-03T05:00:00%2B05:00", "ent"),
		get("audit list start_date epoch", "/audit-logs?start_date=1767225600", "ent"),
		get("audit list end_date", "/audit-logs?end_date=2026-01-03T00:00:00Z", "ent"),
		get("audit list both dates", "/audit-logs?start_date=2026-01-01T10:30:00Z&end_date=2026-01-04T12:00:00Z", "ent"),
		get("audit list bad date", "/audit-logs?start_date=yesterday", "ent"),
		get("audit list bad date and limit", "/audit-logs?end_date=nope&limit=0&offset=-2", "ent"),
		get("audit list community refused", "/audit-logs", "comm"),
		get("audit list feature override", "/audit-logs", "ovr"),
		get("audit list tier only", "/audit-logs", "tier"),
		get("audit list bogus license tier", "/audit-logs", "bogus"),
		get("audit list no license community org", "/audit-logs", "none"),
		get("audit list member refused", "/audit-logs", "member"),
		get("audit list superuser without org", "/audit-logs", "super"),
		get("audit list superuser with org", "/audit-logs", "superOrg"),
		venueoracle.Request{Name: "audit list org header of another org", Method: "GET", Path: admin + "/audit-logs",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["ent"], "X-Org-Id": orgCommunity.String()}},
		venueoracle.Request{Name: "audit list superuser org header", Method: "GET", Path: admin + "/audit-logs",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["super"], "X-Org-Id": orgEnterprise.String()}},
		venueoracle.Request{Name: "audit list unauthenticated", Method: "GET", Path: admin + "/audit-logs"},
		get("audit list community with 422", "/audit-logs?limit=0", "comm"),
		get("audit get", "/audit-logs/"+auditIDs[0].String(), "ent"),
		get("audit get empty json", "/audit-logs/"+auditIDs[3].String(), "ent"),
		get("audit get null json", "/audit-logs/"+auditIDs[2].String(), "ent"),
		get("audit get unknown", "/audit-logs/"+uuid.New().String(), "ent"),
		get("audit get other org", "/audit-logs/"+auditIDs[6].String(), "ent"),
		get("audit get malformed id", "/audit-logs/not-a-uuid", "ent"),
		get("audit get bad shape", "/audit-logs/"+badShapeAuditID.String(), "tier"),
		get("audit list bad shape", "/audit-logs", "tier"),
		get("audit get community refused", "/audit-logs/"+auditIDs[6].String(), "comm"),
		get("audit resource history", "/audit-logs/resource/team/team-1", "ent"),
		get("audit resource history limit", "/audit-logs/resource/team/team-1?limit=1", "ent"),
		get("audit resource history limit zero", "/audit-logs/resource/team/team-1?limit=0", "ent"),
		get("audit resource history none", "/audit-logs/resource/team/none", "ent"),
		get("audit resource history community", "/audit-logs/resource/team/team-1", "comm"),
		get("audit user activity", "/audit-logs/user/"+adminEnt.String(), "ent"),
		get("audit user activity limit", "/audit-logs/user/"+adminEnt.String()+"?limit=2", "ent"),
		get("audit user activity malformed", "/audit-logs/user/not-a-uuid", "ent"),
		get("audit user activity unknown", "/audit-logs/user/"+uuid.New().String(), "ent"),
		get("platform audit list", "/platform/audit-logs", "super"),
		get("platform audit filtered", "/platform/audit-logs?status=failure&limit=1", "super"),
		get("platform audit user malformed", "/platform/audit-logs?user_id=zzz", "super"),
		get("platform audit dates", "/platform/audit-logs?start_date=2026-01-03T00:00:00Z&end_date=2026-01-06T12:00:00Z", "super"),
		get("platform audit bad limit", "/platform/audit-logs?limit=501", "super"),
		get("platform audit admin refused", "/platform/audit-logs", "ent"),

		// ---- feature flags and overrides (reads) ----------------------------
		get("flags list", "/feature-flags", "super"),
		get("flags list non-superuser", "/feature-flags", "ent"),
		get("overrides list", "/orgs/"+orgEnterprise.String()+"/feature-overrides", "super"),
		get("overrides list empty org", "/orgs/"+orgCommunity.String()+"/feature-overrides", "super"),
		get("overrides list unknown org", "/orgs/"+uuid.New().String()+"/feature-overrides", "super"),
		get("overrides list malformed org", "/orgs/not-a-uuid/feature-overrides", "super"),
		get("overrides list non-superuser", "/orgs/"+orgEnterprise.String()+"/feature-overrides", "ent"),

		// ---- IP allowlist (reads) ------------------------------------------
		get("ip list", "/ip-allowlist", "ent"),
		get("ip list active only", "/ip-allowlist?active_only=true", "ent"),
		get("ip list active only mixed case", "/ip-allowlist?active_only=YeS", "ent"),
		get("ip list active only invalid", "/ip-allowlist?active_only=maybe", "ent"),
		get("ip list paged", "/ip-allowlist?limit=2&offset=1", "ent"),
		get("ip list limit over", "/ip-allowlist?limit=501", "ent"),
		get("ip list limit zero", "/ip-allowlist?limit=0", "ent"),
		get("ip list duplicate active_only", "/ip-allowlist?active_only=true&active_only=false", "ent"),
		get("ip list community refused", "/ip-allowlist", "comm"),
		get("ip list override only for audit", "/ip-allowlist", "ovr"),
		get("ip list tier only", "/ip-allowlist", "tier"),
		get("ip list superuser without org", "/ip-allowlist", "super"),
		get("ip list member refused", "/ip-allowlist", "member"),
		get("ip get", "/ip-allowlist/"+ipRows["cidr"].String(), "ent"),
		get("ip get null description", "/ip-allowlist/"+ipRows["cidr"].String(), "ent"),
		get("ip get unknown", "/ip-allowlist/"+uuid.New().String(), "ent"),
		get("ip get malformed", "/ip-allowlist/xyz", "ent"),
		get("ip get community", "/ip-allowlist/"+ipRows["cidr"].String(), "comm"),
	}

	checkAddresses := []string{
		"192.168.1.5", "192.168.1.6", "10.9.8.7", "11.0.0.1", "2001:db8::1", "2001:db9::1", "172.16.0.1",
		"203.0.113.7", "not-an-ip", "", "10.0.0.1/8", " 10.9.8.7", "10.9.8.7 ", "010.9.8.7", "::ffff:10.9.8.7",
		"2001:DB8::1", "2001:db8::1%eth0", "10.9.8.7%eth0", "1.2.3", "300.1.1.1", "١٢٣.1.1.1",
	}
	for index, address := range checkAddresses {
		requests = append(requests, send(fmt.Sprintf("ip check %02d %q", index, address), "POST", "/ip-allowlist/check", "ent",
			fmt.Sprintf(`{"ip_address":%q}`, address)))
	}
	requests = append(requests,
		send("ip check no entries allows anything", "POST", "/ip-allowlist/check", "tier", `{"ip_address":"8.8.8.8"}`),
		send("ip check missing field", "POST", "/ip-allowlist/check", "ent", `{}`),
		send("ip check wrong type", "POST", "/ip-allowlist/check", "ent", `{"ip_address":5}`),
		send("ip check body not object", "POST", "/ip-allowlist/check", "ent", `[]`),
		send("ip check invalid json", "POST", "/ip-allowlist/check", "ent", `{`),
		send("ip post on entry path is 405", "POST", "/ip-allowlist/"+ipRows["cidr"].String(), "ent", `{}`),
		send("ip put on entry path is 405", "PUT", "/ip-allowlist/"+ipRows["cidr"].String(), "ent", `{}`),
		get("ip get check path is a uuid error", "/ip-allowlist/check", "ent"),
		get("ip get check path community", "/ip-allowlist/check", "comm"),
		send("ip check community refused", "POST", "/ip-allowlist/check", "comm", `{"ip_address":"8.8.8.8"}`),
		send("ip check 422 beats license", "POST", "/ip-allowlist/check", "comm", `{}`),
	)

	// ---- IP allowlist writes: parse edge cases in a fresh org -------------
	candidates := []string{
		"203.0.113.9", "198.51.100.0/24", "198.51.100.77/24", "198.51.101.0/255.255.255.0", "198.51.102.0/0.0.0.255",
		"198.51.103.0/255.0.255.0", "198.51.104.0/33", "198.51.105.0/-1", "198.51.106.0/", "/24", "198.51.107.0/24/24",
		"198.51.108.0/+24", "198.51.109.0/024", "198.51.110.0/2 4", "198.51.111.0/٢٤", "2001:db8:1::/48", "2001:db8:2::5/64",
		"2001:db8:3::/129", "2001:db8:4::/255.255.0.0", "fe80::1%eth0", "fe80::1%eth0/64", "fe80::1%", "fe80::1%a%b",
		"::ffff:198.51.100.1", "::ffff:198.51.100.1/120", "::1", "::", "1::", "1:2:3:4:5:6:7::", "1:2:3:4:5:6:7:8:9",
		"1:2:3:4:5:6:7:8", "12345::1", "g::1", "1.2.3", "01.2.3.4", "1.2.3.4.5", "256.1.1.1", "0.0.0.0/0", "::/0",
		"not-an-ip", " 203.0.113.10", "203.0.113.11 ", "203.0.113.12\n", "203.0.113.13/24 ", "203.0.113.14/8/",
	}
	for index, candidate := range candidates {
		requests = append(requests, send(fmt.Sprintf("W ip create candidate %02d %q", index, candidate), "POST", "/ip-allowlist", "ent",
			fmt.Sprintf(`{"ip_range":%q}`, candidate)))
	}
	requests = append(requests,
		// creates
		send("W ip create with everything", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.1","description":"d","expires_at":"2099-12-31T23:59:59Z"}`),
		send("W ip create naive expiry", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.2","expires_at":"2099-12-31T23:59:59"}`),
		send("W ip create offset expiry", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.3","expires_at":"2099-12-31T23:59:59.5+05:30"}`),
		send("W ip create epoch expiry", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.4","expires_at":4102444800}`),
		send("W ip create date expiry", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.5","expires_at":"2099-12-31"}`),
		send("W ip create bad expiry", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.6","expires_at":"soon"}`),
		send("W ip create null description and expiry", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.7","description":null,"expires_at":null}`),
		send("W ip create empty description", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.8","description":""}`),
		send("W ip create wrong description type", "POST", "/ip-allowlist", "ent", `{"ip_range":"100.64.0.9","description":5}`),
		send("W ip create missing range", "POST", "/ip-allowlist", "ent", `{}`),
		send("W ip create null range", "POST", "/ip-allowlist", "ent", `{"ip_range":null}`),
		send("W ip create duplicate range", "POST", "/ip-allowlist", "ent", `{"ip_range":"192.168.1.5"}`),
		send("W ip create community refused", "POST", "/ip-allowlist", "comm", `{"ip_range":"100.64.0.10"}`),
		send("W ip create body 422 beats license", "POST", "/ip-allowlist", "comm", `{}`),
		send("W ip create invalid range beats nothing", "POST", "/ip-allowlist", "comm", `{"ip_range":"bad"}`),
		venueoracle.Request{Name: "W ip create with existing X-User-Id", Method: "POST", Path: admin + "/ip-allowlist",
			Headers: jsonAuthWithUser("ent", adminEnt.String()), Body: venueoracle.B64(`{"ip_range":"100.64.0.11"}`)},
		venueoracle.Request{Name: "W ip create with malformed X-User-Id", Method: "POST", Path: admin + "/ip-allowlist",
			Headers: jsonAuthWithUser("ent", "not-a-uuid"), Body: venueoracle.B64(`{"ip_range":"100.64.0.12"}`)},
		venueoracle.Request{Name: "W ip create with 31-char X-User-Id", Method: "POST", Path: admin + "/ip-allowlist",
			Headers: jsonAuthWithUser("ent", strings.Repeat("a", 31)), Body: venueoracle.B64(`{"ip_range":"100.64.0.13"}`)},
		venueoracle.Request{Name: "W ip create with non-hex 32-char X-User-Id", Method: "POST", Path: admin + "/ip-allowlist",
			Headers: jsonAuthWithUser("ent", strings.Repeat("z", 32)), Body: venueoracle.B64(`{"ip_range":"100.64.0.14"}`)},
		venueoracle.Request{Name: "W ip create with unknown X-User-Id", Method: "POST", Path: admin + "/ip-allowlist",
			Headers: jsonAuthWithUser("ent", uuid.New().String()), Body: venueoracle.B64(`{"ip_range":"100.64.0.15"}`)},
		venueoracle.Request{Name: "W ip create with braced X-User-Id", Method: "POST", Path: admin + "/ip-allowlist",
			Headers: jsonAuthWithUser("ent", "{"+adminEnt.String()+"}"), Body: venueoracle.B64(`{"ip_range":"100.64.0.16"}`)},
		// updates
		send("W ip patch description", "PATCH", "/ip-allowlist/"+ipRows["v4"].String(), "ent", `{"description":"changed"}`),
		send("W ip patch range", "PATCH", "/ip-allowlist/"+ipRows["v4"].String(), "ent", `{"ip_range":"192.168.1.55"}`),
		send("W ip patch range invalid", "PATCH", "/ip-allowlist/"+ipRows["v4"].String(), "ent", `{"ip_range":"nope","description":"never"}`),
		send("W ip patch range duplicate", "PATCH", "/ip-allowlist/"+ipRows["v4"].String(), "ent", `{"ip_range":"10.0.0.0/8"}`),
		send("W ip patch deactivate", "PATCH", "/ip-allowlist/"+ipRows["v6"].String(), "ent", `{"is_active":false}`),
		send("W ip patch reactivate and expiry", "PATCH", "/ip-allowlist/"+ipRows["inactive"].String(), "ent", `{"is_active":true,"expires_at":"2098-01-01T00:00:00+02:00"}`),
		send("W ip patch expiry naive", "PATCH", "/ip-allowlist/"+ipRows["expired"].String(), "ent", `{"expires_at":"2099-01-01T00:00:00"}`),
		send("W ip patch empty body", "PATCH", "/ip-allowlist/"+ipRows["cidr"].String(), "ent", `{}`),
		send("W ip patch nulls", "PATCH", "/ip-allowlist/"+ipRows["cidr"].String(), "ent", `{"ip_range":null,"description":null,"is_active":null,"expires_at":null}`),
		send("W ip patch bad bool", "PATCH", "/ip-allowlist/"+ipRows["cidr"].String(), "ent", `{"is_active":"perhaps"}`),
		send("W ip patch unknown entry", "PATCH", "/ip-allowlist/"+uuid.New().String(), "ent", `{"ip_range":"nope"}`),
		send("W ip patch malformed id", "PATCH", "/ip-allowlist/xyz", "ent", `{}`),
		send("W ip patch community refused", "PATCH", "/ip-allowlist/"+ipRows["cidr"].String(), "comm", `{}`),
		// list after writes (ids and timestamps of created rows redacted)
		get("W ip list after writes", "/ip-allowlist?limit=500", "ent"),
		get("W ip list active after writes", "/ip-allowlist?active_only=true&limit=500", "ent"),
		send("W ip check after writes", "POST", "/ip-allowlist/check", "ent", `{"ip_address":"192.168.1.55"}`),
		send("W ip check after v6 deactivated", "POST", "/ip-allowlist/check", "ent", `{"ip_address":"2001:db8::1"}`),
		// deletes
		venueoracle.Request{Name: "W ip delete", Method: "DELETE", Path: admin + "/ip-allowlist/" + ipRows["inactive"].String(), Headers: auth("ent")},
		venueoracle.Request{Name: "W ip delete again", Method: "DELETE", Path: admin + "/ip-allowlist/" + ipRows["inactive"].String(), Headers: auth("ent")},
		venueoracle.Request{Name: "W ip delete malformed", Method: "DELETE", Path: admin + "/ip-allowlist/xyz", Headers: auth("ent")},
		venueoracle.Request{Name: "W ip delete community", Method: "DELETE", Path: admin + "/ip-allowlist/" + ipRows["cidr"].String(), Headers: auth("comm")},
	)

	// ---- feature overrides and flags (writes) ------------------------------
	ov := func(org uuid.UUID) string { return "/orgs/" + org.String() + "/feature-overrides" }
	requests = append(requests,
		send("W override create defaults", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q}`, flagIDs[1])),
		send("W override create full", "POST", ov(orgEnterprise), "super", fmt.Sprintf(
			`{"feature_id":%q,"is_enabled":false,"expires_at":"2099-01-01T00:00:00+02:00","config":{"z":1,"a":[1,2.5],"s":"Café"},"reason":"why"}`, flagIDs[2])),
		send("W override create naive expiry", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q,"expires_at":"2099-01-01T00:00:00"}`, flagIDs[3])),
		send("W override create epoch expiry", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q,"expires_at":4102444800}`, flagIDs[4])),
		send("W override create nulls", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q,"expires_at":null,"config":null,"reason":null}`, flagIDs[5])),
		send("W override create empty config", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q,"config":{}}`, flagIDs[6])),
		send("W override create is_enabled null", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q,"is_enabled":null}`, flagIDs[7])),
		send("W override create is_enabled string", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q,"is_enabled":"yes"}`, flagIDs[7])),
		send("W override create config list", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q,"config":[]}`, flagIDs[7])),
		send("W override create bad expiry", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q,"expires_at":"x"}`, flagIDs[7])),
		send("W override create duplicate", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q}`, flagIDs[1])),
		send("W override create seeded duplicate", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q}`, flagIDs[0])),
		send("W override create unknown flag", "POST", ov(orgEnterprise), "super", fmt.Sprintf(`{"feature_id":%q}`, uuid.New())),
		send("W override create malformed flag id", "POST", ov(orgEnterprise), "super", `{"feature_id":"nope"}`),
		send("W override create missing feature", "POST", ov(orgEnterprise), "super", `{}`),
		send("W override create unknown org", "POST", ov(uuid.New()), "super", fmt.Sprintf(`{"feature_id":%q}`, flagIDs[8])),
		send("W override create malformed org", "POST", "/orgs/nope/feature-overrides", "super", fmt.Sprintf(`{"feature_id":%q}`, flagIDs[8])),
		send("W override create non-superuser", "POST", ov(orgEnterprise), "ent", fmt.Sprintf(`{"feature_id":%q}`, flagIDs[9])),
		get("W overrides list after creates", "/orgs/"+orgEnterprise.String()+"/feature-overrides", "super"),
		send("W override patch enable and reason", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{"is_enabled":false,"reason":"changed"}`),
		send("W override patch config", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{"config":{"k":[1,{"x":null}]}}`),
		send("W override patch same config", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{"config":{"k":[1,{"x":null}]}}`),
		send("W override patch expiry", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{"expires_at":"2050-06-01T12:00:00Z"}`),
		send("W override patch same expiry offset", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{"expires_at":"2050-06-01T14:00:00+02:00"}`),
		send("W override patch empty", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{}`),
		send("W override patch nulls", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{"is_enabled":null,"expires_at":null,"config":null,"reason":null}`),
		send("W override patch empty reason", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{"reason":""}`),
		send("W override patch bad config", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "super", `{"config":5}`),
		send("W override patch wrong org", "PATCH", ov(orgCommunity)+"/"+overrideSeedID.String(), "super", `{}`),
		send("W override patch unknown", "PATCH", ov(orgEnterprise)+"/"+uuid.New().String(), "super", `{}`),
		send("W override patch malformed id", "PATCH", ov(orgEnterprise)+"/nope", "super", `{}`),
		send("W override patch non-superuser", "PATCH", ov(orgEnterprise)+"/"+overrideSeedID.String(), "ent", `{}`),
		send("W flag patch enabled", "PATCH", "/feature-flags/"+flagIDs[10], "super", `{"is_enabled":false}`),
		send("W flag patch all", "PATCH", "/feature-flags/"+flagIDs[11], "super", `{"is_enabled":true,"is_beta":true,"is_deprecated":true}`),
		send("W flag patch same values", "PATCH", "/feature-flags/"+flagIDs[11], "super", `{"is_beta":true}`),
		send("W flag patch empty", "PATCH", "/feature-flags/"+flagIDs[12], "super", `{}`),
		send("W flag patch nulls", "PATCH", "/feature-flags/"+flagIDs[12], "super", `{"is_enabled":null,"is_beta":null,"is_deprecated":null}`),
		send("W flag patch bad bool", "PATCH", "/feature-flags/"+flagIDs[12], "super", `{"is_beta":"perhaps"}`),
		send("W flag patch unknown", "PATCH", "/feature-flags/"+uuid.New().String(), "super", `{}`),
		send("W flag patch malformed", "PATCH", "/feature-flags/nope", "super", `{}`),
		send("W flag patch non-superuser", "PATCH", "/feature-flags/"+flagIDs[12], "ent", `{}`),
		get("W flags list after patches", "/feature-flags", "super"),
		venueoracle.Request{Name: "W override delete", Method: "DELETE", Path: admin + ov(orgEnterprise) + "/" + overrideSeedID.String(), Headers: auth("super")},
		venueoracle.Request{Name: "W override delete again", Method: "DELETE", Path: admin + ov(orgEnterprise) + "/" + overrideSeedID.String(), Headers: auth("super")},
		venueoracle.Request{Name: "W override delete malformed", Method: "DELETE", Path: admin + ov(orgEnterprise) + "/nope", Headers: auth("super")},
		venueoracle.Request{Name: "W override delete non-superuser", Method: "DELETE", Path: admin + ov(orgEnterprise) + "/" + overrideSeedID.String(), Headers: auth("ent")},
	)

	python := venue.ServePython(t, requests)
	goBase, goPool := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		// Ids are redacted from the comparison, so a created row's id is
		// checked here instead: the Go response must carry a valid UUID that
		// names the row it inserted.
		Inspect: func(request venueoracle.Request, response venueoracle.Response) {
			table := ""
			switch {
			case request.Method == "POST" && strings.HasSuffix(request.Path, "/ip-allowlist"):
				table = "org_ip_allowlist"
			case request.Method == "POST" && strings.HasSuffix(request.Path, "/feature-overrides"):
				table = "org_feature_overrides"
			}
			if table == "" || response.Status != 201 {
				return
			}
			value, err := pyjson.DecodeString(response.Body)
			object, isObject := value.(*pyjson.Object)
			if err != nil || !isObject {
				t.Errorf("%s: 201 body is not an object: %s", request.Name, response.Body)
				return
			}
			raw, _ := object.Get("id")
			id, isString := raw.(string)
			parsed, parseErr := uuid.Parse(id)
			if !isString || parseErr != nil {
				t.Errorf("%s: created id %v is not a UUID", request.Name, raw)
				return
			}
			var rows int
			if err := goPool.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE id = $1", parsed).Scan(&rows); err != nil || rows != 1 {
				t.Errorf("%s: created id %s names %d rows in %s (err %v), want 1", request.Name, id, rows, table, err)
			}
		},
		Normalize: func(request venueoracle.Request, body string) string {
			if strings.HasPrefix(request.Name, "W ") {
				body = redactDeep(t, body, "id", "created_at", "updated_at")
			}
			return body
		},
	})
	t.Log(receipt)

	compare := func(name, query string) {
		t.Helper()
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		if source == "" {
			t.Errorf("%s: the query matched no rows on the Python plane; the comparison proves nothing", name)
		}
		if source != goRows {
			t.Errorf("%s differs after the writes:\n python: %s\n go:     %s", name, source, goRows)
		}
	}
	compare("org_ip_allowlist rows", `SELECT ip_range, description, is_active, created_by_id::text, expires_at::text, (updated_at > '2026-06-01')::text
FROM org_ip_allowlist ORDER BY ip_range`)
	compare("org_feature_overrides rows", `SELECT feature_id::text, is_enabled, expires_at::text, config::text, reason, created_by::text, updated_by::text
FROM org_feature_overrides ORDER BY feature_id::text, org_id::text`)
	compare("feature_flags rows", `SELECT key, is_enabled, is_beta, is_deprecated, (updated_at > '2020-01-02')::text FROM feature_flags ORDER BY key`)
}
