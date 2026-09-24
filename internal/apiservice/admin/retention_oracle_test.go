//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

var dbFailureText = regexp.MustCompile(`"error":"[^"]*sentinel retention delete failure[^"]*"`)

// TestRetentionRoutesVenueOracle is the venue-oracle proof for the retention
// policy admin routes, including the execute route that deletes audit_logs
// rows. The real Python api and the real Go api answer the same requests
// against two copies of one database; responses are compared byte for byte
// and the rows the writes touched are compared after.
func TestRetentionRoutesVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-retention-flow-32-bytes!"

	orgEnterprise, orgCommunity, orgOverride, orgTierOnly, orgBogus := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	superID := uuid.New()
	adminEnt, adminComm, adminOvr, adminTier, adminBogus, memberEnt := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	pAudit, pInactive, pMetrics, pTier, pHuge := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	orgFail, adminFail, pFail := uuid.New(), uuid.New(), uuid.New()

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
			org(orgEnterprise, "ret-ent", "enterprise")
			license(orgEnterprise, "enterprise", "{}")
			org(orgCommunity, "ret-comm", "community")
			license(orgCommunity, "community", "{}")
			org(orgOverride, "ret-ovr", "community")
			license(orgOverride, "community", `{"custom_retention": true}`)
			org(orgTierOnly, "ret-tier", "enterprise")
			org(orgBogus, "ret-bogus", "enterprise")
			license(orgBogus, "platinum", "{}")
			user := func(id uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, email, super)
			}
			user(superID, "ret-super@example.com", true)
			user(adminEnt, "ret-adminent@example.com", false)
			user(adminComm, "ret-admincomm@example.com", false)
			user(adminOvr, "ret-adminovr@example.com", false)
			user(adminTier, "ret-admintier@example.com", false)
			user(adminBogus, "ret-adminbogus@example.com", false)
			user(memberEnt, "ret-memberent@example.com", false)
			// An org whose audit_logs deletes fail inside the database, so
			// execute reaches its catch-all: the error text is the driver's
			// own and is compared only for the sentinel it carries.
			org(orgFail, "ret-fail", "enterprise")
			license(orgFail, "enterprise", "{}")
			user(adminFail, "ret-adminfail@example.com", false)
			exec(`CREATE FUNCTION retention_delete_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'sentinel retention delete failure'; END $$`)
			exec(`CREATE TRIGGER retention_delete_guard BEFORE DELETE ON audit_logs FOR EACH ROW
WHEN (OLD.org_id = '` + orgFail.String() + `') EXECUTE FUNCTION retention_delete_guard()`)

			policy := func(id, org uuid.UUID, rtype string, days int, desc any, active bool, createdAt string) {
				exec(`INSERT INTO org_retention_policies (id, org_id, resource_type, retention_days, description, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz, $7::timestamptz)`, id, org, rtype, days, desc, active, createdAt)
			}
			policy(pAudit, orgEnterprise, "audit_logs", 30, "keep a month", true, "2026-02-01T00:00:00+00:00")
			policy(pInactive, orgEnterprise, "sync_logs", 10, nil, false, "2026-02-02T00:00:00+00:00")
			policy(pMetrics, orgEnterprise, "metrics_daily", 45, "not implemented", true, "2026-02-03T00:00:00+00:00")
			policy(pTier, orgTierOnly, "audit_logs", 5, nil, true, "2026-02-04T00:00:00+00:00")
			policy(pHuge, orgOverride, "audit_logs", 2000000000, nil, true, "2026-02-05T00:00:00+00:00")
			policy(pFail, orgFail, "audit_logs", 30, nil, true, "2026-02-06T00:00:00+00:00")
			audit := func(org uuid.UUID, action, ageDays string) {
				exec(`INSERT INTO audit_logs (id, org_id, action, resource_type, resource_id, changes, request_metadata, status, created_at)
VALUES ($1, $2, $3, 'team', 'r', '{}'::json, '{}'::json, 'success', now() - ($4 || ' days')::interval)`, uuid.New(), org, action, ageDays)
			}
			audit(orgFail, "fail-old", "400")
			// Rows of the audit trail around the 30-day cutoff, plus other orgs'.
			for _, age := range []string{"400", "90", "31", "29", "1", "0"} {
				audit(orgEnterprise, "ent-"+age, age)
			}
			audit(orgCommunity, "comm-old", "400")
			audit(orgTierOnly, "tier-old", "10")
			audit(orgTierOnly, "tier-new", "1")
			admin2 := func(id, org uuid.UUID, email string) map[string]any {
				return map[string]any{"user_id": id.String(), "email": email, "org_id": org.String(), "role": "admin"}
			}
			return map[string]map[string]any{
				"super":  {"user_id": superID.String(), "email": "ret-super@example.com", "is_superuser": true},
				"ent":    admin2(adminEnt, orgEnterprise, "ret-adminent@example.com"),
				"comm":   admin2(adminComm, orgCommunity, "ret-admincomm@example.com"),
				"ovr":    admin2(adminOvr, orgOverride, "ret-adminovr@example.com"),
				"tier":   admin2(adminTier, orgTierOnly, "ret-admintier@example.com"),
				"bogus":  admin2(adminBogus, orgBogus, "ret-adminbogus@example.com"),
				"member": {"user_id": memberEnt.String(), "email": "ret-memberent@example.com", "org_id": orgEnterprise.String(), "role": "member"},
				"fail":   admin2(adminFail, orgFail, "ret-adminfail@example.com"),
			}
		},
	})

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	withUser := func(name, userID string) map[string]string {
		h := jsonAuth(name)
		h["X-User-Id"] = userID
		return h
	}
	const admin = "/api/v1/admin/retention-policies"
	get := func(name, path, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: admin + path, Headers: auth(token)}
	}
	send := func(name, method, path, token, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: method, Path: admin + path, Headers: jsonAuth(token), Body: venueoracle.B64(body)}
	}
	noBody := func(name, method, path, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: method, Path: admin + path, Headers: auth(token)}
	}

	requests := []venueoracle.Request{
		// ---- reads --------------------------------------------------------
		get("list", "", "ent"),
		get("list paged", "?limit=2&offset=1", "ent"),
		get("list active only", "?active_only=true", "ent"),
		get("list active only invalid", "?active_only=maybe", "ent"),
		get("list limit zero", "?limit=0", "ent"),
		get("list limit over", "?limit=501", "ent"),
		get("list offset negative", "?offset=-1", "ent"),
		get("list duplicate limit", "?limit=1&limit=3", "ent"),
		get("list community refused", "", "comm"),
		get("list override grants", "", "ovr"),
		get("list tier only", "", "tier"),
		get("list bogus license tier", "", "bogus"),
		get("list superuser without org", "", "super"),
		get("list member refused", "", "member"),
		{Name: "list unauthenticated", Method: "GET", Path: admin},
		get("list 422 beats licence", "?limit=0", "comm"),
		get("resource types", "/resource-types", "ent"),
		get("resource types community", "/resource-types", "comm"),
		get("resource types superuser without org", "/resource-types", "super"),
		get("resource types member", "/resource-types", "member"),
		noBody("resource types post is 405", "POST", "/resource-types", "ent"),
		send("resource types patch is a uuid error", "PATCH", "/resource-types", "ent", `{}`),
		noBody("resource types delete is a uuid error", "DELETE", "/resource-types", "ent"),
		noBody("resource types put is 405", "PUT", "/resource-types", "ent"),
		get("get", "/"+pAudit.String(), "ent"),
		get("get null description", "/"+pInactive.String(), "ent"),
		get("get unknown", "/"+uuid.New().String(), "ent"),
		get("get malformed", "/nope", "ent"),
		get("get other org", "/"+pTier.String(), "ent"),
		get("get community", "/"+pAudit.String(), "comm"),
		noBody("policy id put is 405", "PUT", "/"+pAudit.String(), "ent"),
		noBody("execute get is 405", "GET", "/"+pAudit.String()+"/execute", "ent"),

		// ---- execute: dry runs and errors ---------------------------------
		noBody("execute no body is a dry run", "POST", "/"+pAudit.String()+"/execute", "ent"),
		send("execute empty object", "POST", "/"+pAudit.String()+"/execute", "ent", `{}`),
		send("execute dry_run true", "POST", "/"+pAudit.String()+"/execute", "ent", `{"dry_run":true}`),
		send("execute dry_run yes", "POST", "/"+pAudit.String()+"/execute", "ent", `{"dry_run":"yes"}`),
		send("execute dry_run null", "POST", "/"+pAudit.String()+"/execute", "ent", `{"dry_run":null}`),
		send("execute dry_run invalid", "POST", "/"+pAudit.String()+"/execute", "ent", `{"dry_run":"perhaps"}`),
		send("execute body null", "POST", "/"+pAudit.String()+"/execute", "ent", `null`),
		send("execute body list", "POST", "/"+pAudit.String()+"/execute", "ent", `[]`),
		send("execute invalid json", "POST", "/"+pAudit.String()+"/execute", "ent", `{`),
		venueoracle.Request{Name: "execute non-json content type", Method: "POST", Path: admin + "/" + pAudit.String() + "/execute",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["ent"], "Content-Type": "text/plain"}, Body: venueoracle.B64("dry_run")},
		send("execute unknown policy", "POST", "/"+uuid.New().String()+"/execute", "ent", `{}`),
		send("execute other org policy", "POST", "/"+pTier.String()+"/execute", "ent", `{}`),
		send("execute inactive", "POST", "/"+pInactive.String()+"/execute", "ent", `{"dry_run":false}`),
		send("execute not implemented type", "POST", "/"+pMetrics.String()+"/execute", "ent", `{"dry_run":false}`),
		send("execute malformed id", "POST", "/nope/execute", "ent", `{}`),
		send("execute community refused", "POST", "/"+pAudit.String()+"/execute", "comm", `{}`),
		send("execute 422 beats licence", "POST", "/"+pAudit.String()+"/execute", "comm", `{"dry_run":null}`),
		send("execute database failure dry run counts", "POST", "/"+pFail.String()+"/execute", "fail", `{}`),
		send("execute database failure on delete", "POST", "/"+pFail.String()+"/execute", "fail", `{"dry_run":false}`),
		send("execute member refused", "POST", "/"+pAudit.String()+"/execute", "member", `{}`),
		send("execute huge retention days overflows", "POST", "/"+pHuge.String()+"/execute", "ovr", `{}`),
		send("execute huge retention days real", "POST", "/"+pHuge.String()+"/execute", "ovr", `{"dry_run":false}`),
		noBody("execute unauthenticated dry", "POST", "/"+pAudit.String()+"/execute", "member"),

		// ---- create --------------------------------------------------------
		send("W create default days", "POST", "", "ent", `{"resource_type":"work_items"}`),
		send("W create full", "POST", "", "ent", `{"resource_type":"git_commits","retention_days":7,"description":"seven"}`),
		send("W create string days", "POST", "", "ent", `{"resource_type":"work_items_x","retention_days":"7"}`),
	}
	requests = append(requests,
		send("W create invalid type", "POST", "", "ent", `{"resource_type":"nope"}`),
		send("W create days 7.0", "POST", "", "ent", `{"resource_type":"nope2","retention_days":7.0}`),
		send("W create days zero", "POST", "", "ent", `{"resource_type":"work_items","retention_days":0}`),
		send("W create days negative", "POST", "", "ent", `{"resource_type":"work_items","retention_days":-5}`),
		send("W create days fractional", "POST", "", "ent", `{"resource_type":"work_items","retention_days":7.5}`),
		send("W create days null", "POST", "", "ent", `{"resource_type":"work_items","retention_days":null}`),
		send("W create days text", "POST", "", "ent", `{"resource_type":"work_items","retention_days":"abc"}`),
		// A valid type the org has no policy for yet, so each case reaches the
		// integer checks and the 32-bit storage limit rather than the
		// duplicate check (the override org holds only an audit_logs policy).
		send("W create days past int32", "POST", "", "ovr", `{"resource_type":"work_items","retention_days":2147483648}`),
		send("W create days beyond int64", "POST", "", "ovr", `{"resource_type":"work_items","retention_days":100000000000000000000000000000}`),
		send("W create days float beyond int64", "POST", "", "ovr", `{"resource_type":"work_items","retention_days":1e19}`),
		send("W create days infinite float", "POST", "", "ovr", `{"resource_type":"work_items","retention_days":1e400}`),
		send("W create days boolean false", "POST", "", "ovr", `{"resource_type":"work_items","retention_days":false}`),
		send("W create days boolean true", "POST", "", "ovr", `{"resource_type":"work_items","retention_days":true}`),
		send("W create duplicate type", "POST", "", "ent", `{"resource_type":"audit_logs"}`),
		send("W create missing type", "POST", "", "ent", `{}`),
		send("W create null type", "POST", "", "ent", `{"resource_type":null}`),
		send("W create description null", "POST", "", "tier", `{"resource_type":"metrics_daily","description":null}`),
		send("W create description empty", "POST", "", "tier", `{"resource_type":"work_items","description":""}`),
		send("W create description wrong type", "POST", "", "tier", `{"resource_type":"git_commits","description":5}`),
		send("W create community refused", "POST", "", "comm", `{"resource_type":"audit_logs"}`),
		send("W create 422 beats licence", "POST", "", "comm", `{}`),
		send("W create invalid type on community", "POST", "", "comm", `{"resource_type":"nope"}`),
		send("W create member refused", "POST", "", "member", `{"resource_type":"audit_logs"}`),
		send("W create superuser without org", "POST", "", "super", `{"resource_type":"audit_logs"}`),
		venueoracle.Request{Name: "W create with existing X-User-Id", Method: "POST", Path: admin, Headers: withUser("tier", adminTier.String()),
			Body: venueoracle.B64(`{"resource_type":"sync_logs"}`)},
		venueoracle.Request{Name: "W create with malformed X-User-Id", Method: "POST", Path: admin, Headers: withUser("tier", "not-a-uuid"),
			Body: venueoracle.B64(`{"resource_type":"nope"}`)},
		venueoracle.Request{Name: "W create with non-hex 32-char X-User-Id", Method: "POST", Path: admin, Headers: withUser("tier", strings.Repeat("z", 32)),
			Body: venueoracle.B64(`{"resource_type":"work_items"}`)},
		venueoracle.Request{Name: "W create with unknown X-User-Id", Method: "POST", Path: admin, Headers: withUser("tier", uuid.New().String()),
			Body: venueoracle.B64(`{"resource_type":"git_commits"}`)},
		get("W list after creates", "?limit=500", "ent"),
		get("W list tier org after creates", "?limit=500", "tier"),

		// ---- update ---------------------------------------------------------
		send("W patch days", "PATCH", "/"+pAudit.String(), "ent", `{"retention_days":60}`),
		send("W patch description", "PATCH", "/"+pAudit.String(), "ent", `{"description":"changed"}`),
		send("W patch empty description", "PATCH", "/"+pAudit.String(), "ent", `{"description":""}`),
		send("W patch deactivate", "PATCH", "/"+pMetrics.String(), "ent", `{"is_active":false}`),
		send("W patch reactivate and days", "PATCH", "/"+pInactive.String(), "ent", `{"is_active":true,"retention_days":"12"}`),
		send("W patch empty body", "PATCH", "/"+pAudit.String(), "ent", `{}`),
		send("W patch nulls", "PATCH", "/"+pAudit.String(), "ent", `{"retention_days":null,"description":null,"is_active":null}`),
		send("W patch days zero", "PATCH", "/"+pAudit.String(), "ent", `{"retention_days":0}`),
		send("W patch days fractional", "PATCH", "/"+pAudit.String(), "ent", `{"retention_days":1.5}`),
		send("W patch days past int32", "PATCH", "/"+pAudit.String(), "ent", `{"retention_days":2147483648}`),
		send("W patch days beyond int64", "PATCH", "/"+pAudit.String(), "ent", `{"retention_days":100000000000000000000000000000}`),
		send("W patch days float beyond int64", "PATCH", "/"+pAudit.String(), "ent", `{"retention_days":-1e19}`),
		send("W patch days boolean false", "PATCH", "/"+pInactive.String(), "ent", `{"retention_days":false}`),
		send("W patch days boolean", "PATCH", "/"+pInactive.String(), "ent", `{"retention_days":true}`),
		send("W patch bad bool", "PATCH", "/"+pAudit.String(), "ent", `{"is_active":"perhaps"}`),
		send("W patch unknown", "PATCH", "/"+uuid.New().String(), "ent", `{}`),
		send("W patch malformed id", "PATCH", "/nope", "ent", `{}`),
		send("W patch other org", "PATCH", "/"+pTier.String(), "ent", `{}`),
		send("W patch community refused", "PATCH", "/"+pAudit.String(), "comm", `{}`),
		send("W patch 422 beats licence", "PATCH", "/"+pAudit.String(), "comm", `{"retention_days":0}`),

		// ---- execute: real runs ---------------------------------------------
		send("W execute dry run after patches", "POST", "/"+pAudit.String()+"/execute", "ent", `{}`),
		send("W execute real", "POST", "/"+pAudit.String()+"/execute", "ent", `{"dry_run":false}`),
		send("W execute real again", "POST", "/"+pAudit.String()+"/execute", "ent", `{"dry_run":false}`),
		send("W execute real tier org", "POST", "/"+pTier.String()+"/execute", "tier", `{"dry_run":false}`),
		get("W get after execute", "/"+pAudit.String(), "ent"),
		get("W list after execute", "?limit=500", "ent"),

		// ---- delete ---------------------------------------------------------
		noBody("W delete", "DELETE", "/"+pInactive.String(), "ent"),
		noBody("W delete again", "DELETE", "/"+pInactive.String(), "ent"),
		noBody("W delete malformed", "DELETE", "/nope", "ent"),
		noBody("W delete other org", "DELETE", "/"+pTier.String(), "ent"),
		noBody("W delete community", "DELETE", "/"+pAudit.String(), "comm"),
	)
	python := venue.ServePython(t, requests)
	goBase, goPool := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Inspect: func(request venueoracle.Request, response venueoracle.Response) {
			if request.Name == "execute database failure on delete" && !strings.Contains(response.Body, "sentinel retention delete failure") {
				t.Errorf("%s: the error text does not carry the database failure: %s", request.Name, response.Body)
			}
			if request.Method != "POST" || !strings.HasSuffix(request.Path, "/retention-policies") || response.Status != 201 {
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
			if err := goPool.QueryRow(ctx, "SELECT count(*) FROM org_retention_policies WHERE id = $1", parsed).Scan(&rows); err != nil || rows != 1 {
				t.Errorf("%s: created id %s names %d rows (err %v), want 1", request.Name, id, rows, err)
			}
		},
		Normalize: func(request venueoracle.Request, body string) string {
			if request.Name == "execute database failure on delete" {
				// The error text is each driver's own message; the sentinel
				// the trigger raised must be in it (checked in Inspect).
				body = dbFailureText.ReplaceAllString(body, `"error":"<database error>"`)
			}
			if strings.HasPrefix(request.Name, "W ") {
				body = redactDeep(t, body, "id", "created_at", "updated_at", "last_run_at", "next_run_at")
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
	compare("org_retention_policies rows", fmt.Sprintf(`SELECT org_id::text, resource_type, retention_days, description, is_active, last_run_deleted_count,
	(last_run_at IS NOT NULL)::text, (next_run_at IS NOT NULL AND next_run_at - last_run_at BETWEEN interval '23 hours 59 minutes' AND interval '24 hours 1 minute')::text,
	created_by_id::text, (updated_at > '2026-06-01')::text
FROM org_retention_policies ORDER BY org_id::text, resource_type`))
	compare("audit_logs rows", `SELECT org_id::text, action FROM audit_logs ORDER BY org_id::text, action`)
}
