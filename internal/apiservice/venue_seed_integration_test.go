//go:build integration

package apiservice

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

type venueFixture struct {
	orgA, orgB, orgFree, orgNoLicense                          uuid.UUID
	member, admin, owner, superuser, outsider, inactive, stale uuid.UUID
	impersonator                                               uuid.UUID
}

// tokenSpecs are AuthService.create_access_token keyword arguments, one
// per caller.
func (f venueFixture) tokenSpecs() map[string]map[string]any {
	spec := func(user uuid.UUID, org uuid.UUID, role string, extra map[string]any) map[string]any {
		out := map[string]any{"user_id": user.String(), "email": user.String()[:8] + "@example.com", "org_id": org.String(), "role": role}
		for key, value := range extra {
			out[key] = value
		}
		return out
	}
	return map[string]map[string]any{
		"member":        spec(f.member, f.orgA, "member", nil),
		"admin":         spec(f.admin, f.orgA, "admin", nil),
		"owner":         spec(f.owner, f.orgA, "owner", nil),
		"superuser":     spec(f.superuser, f.orgB, "member", map[string]any{"is_superuser": true}),
		"outsider":      spec(f.outsider, f.orgB, "member", nil),
		"inactive":      spec(f.inactive, f.orgA, "admin", nil),
		"stale":         spec(f.stale, f.orgA, "admin", map[string]any{"token_version": 0}),
		"no_org":        {"user_id": f.member.String(), "email": "m@example.com", "org_id": "", "role": "member"},
		"bad_org_claim": {"user_id": f.admin.String(), "email": "a@example.com", "org_id": "not-a-uuid", "role": "admin"},
		"admin_free":    spec(f.admin, f.orgFree, "admin", nil),
		"impersonator":  spec(f.impersonator, f.orgB, "member", map[string]any{"is_superuser": true}),
	}
}

func venueSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) venueFixture {
	t.Helper()
	f := venueFixture{}
	for _, id := range []*uuid.UUID{&f.orgA, &f.orgB, &f.orgFree, &f.orgNoLicense, &f.member, &f.admin, &f.owner, &f.superuser, &f.outsider, &f.inactive, &f.stale, &f.impersonator} {
		*id = uuid.New()
	}
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, description, tier) VALUES
			($1,'org-a','Org A','first',  'team'),
			($2,'org-b','Org B',NULL,     'enterprise'),
			($3,'org-free','Org Free',NULL,DEFAULT),
			($4,'org-none','Org None','x','community')`, []any{f.orgA, f.orgB, f.orgFree, f.orgNoLicense}},
		// An inactive org no request names: the report's active_organizations
		// must differ from its total.
		{`INSERT INTO organizations (id, slug, name, tier, is_active) VALUES (gen_random_uuid(), 'org-off', 'Org Off', 'community', false)`, nil},
		{`INSERT INTO users (id, email, is_superuser, is_active, token_version) VALUES
			($1,'member@x',false,true,0),($2,'admin@x',false,true,0),($3,'owner@x',false,true,0),
			($4,'super@x',true,true,0),($5,'outsider@x',false,true,0),($6,'inactive@x',false,false,0),
			($7,'stale@x',false,true,3)`, []any{f.member, f.admin, f.owner, f.superuser, f.outsider, f.inactive, f.stale}},
		{`INSERT INTO memberships (id, user_id, org_id, role) VALUES
			(gen_random_uuid(),$1,$5,'member'),(gen_random_uuid(),$2,$5,'admin'),(gen_random_uuid(),$3,$5,'owner'),
			(gen_random_uuid(),$4,$6,'viewer'),(gen_random_uuid(),$2,$7,'admin'),(gen_random_uuid(),$8,$5,'admin')`,
			[]any{f.member, f.admin, f.owner, f.outsider, f.orgA, f.orgB, f.orgFree, f.stale}},
		{`INSERT INTO org_licenses (id, org_id, tier, licensed_users, licensed_repos, expires_at, is_valid,
			features_override, limits_override, created_at, updated_at) VALUES
			(gen_random_uuid(),$1,'enterprise',25,NULL,'2027-01-02T03:04:05.123456Z',true,
			 '{"sso_saml": false, "git_sync": true, "custom_key": 1, "falsy": 0, "empty": ""}',
			 '{"max_users": 50, "extra_float": 1.5, "flag": true, "text": "x", "none": null, "big": 1e16}', now(), now()),
			(gen_random_uuid(),$2,'bogus',NULL,NULL,NULL,false,'[1]','{}', now(), now())`, []any{f.orgA, f.orgB}},
		{`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, created_at, updated_at)
			SELECT gen_random_uuid(), $1, id, v.enabled, v.expires, now(), now()
			FROM feature_flags JOIN (VALUES ('api_access', false, NULL::timestamptz),
				('work_graph', true, now() - interval '1 day'),
				('ask_dev', true, now() + interval '1 day')) AS v(key, enabled, expires) USING (key)`, []any{f.orgA}},
		// agent_context_runtime on for orgB, so one acr entitlement answer is true.
		{`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, created_at, updated_at)
			SELECT gen_random_uuid(), $1, id, true, NULL, now(), now() FROM feature_flags WHERE key = 'agent_context_runtime'`, []any{f.orgB}},
		{`INSERT INTO users (id, email, is_superuser) VALUES ($1, 'imp@x', true)`, []any{f.impersonator}},
		{`INSERT INTO impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, expires_at)
			VALUES (gen_random_uuid(), $1, $2, $3, 'member', now() + interval '1 hour')`, []any{f.impersonator, f.member, f.orgA}},
		// Sync configurations for the report's totals: active and synced within
		// a day (two, so no count coincides with another), active but stale,
		// inactive but recent, and never synced.
		{`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, last_sync_at, created_at, updated_at) VALUES
			(gen_random_uuid(), $1, 'fresh', 'github', '[]', '{}', true, now() - interval '1 hour', now(), now()),
			(gen_random_uuid(), $2, 'fresh too', 'gitlab', '[]', '{}', true, now() - interval '2 hours', now(), now()),
			(gen_random_uuid(), $1, 'stale', 'github', '[]', '{}', true, now() - interval '3 days', now(), now()),
			(gen_random_uuid(), $2, 'off', 'gitlab', '[]', '{}', false, now() - interval '1 hour', now(), now()),
			(gen_random_uuid(), $2, 'never', 'jira', '[]', '{}', true, NULL, now(), now())`, []any{f.orgA.String(), f.orgB.String()}},
		{`INSERT INTO worker_instances (instance_id, worker_group, queues, state, started_at, heartbeat_at, expires_at) VALUES
			(gen_random_uuid(), 'ops', '{default}', 'accepting', now(), now(), now() + interval '1 hour'),
			(gen_random_uuid(), 'sync', '{default}', 'draining', now(), now(), now() - interval '1 minute'),
			(gen_random_uuid(), 'other', '{default}', 'accepting', now(), now(), now() + interval '1 hour')`, nil},
		{`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at) VALUES
			(gen_random_uuid(), $1, 'telemetry', 'telemetry_last_report_at', '2026-09-01T10:00:00.123456+00:00', false, NULL, now(), now()),
			(gen_random_uuid(), $2, 'telemetry', 'telemetry_last_report_at', 'garbage', false, NULL, now(), now()),
			(gen_random_uuid(), $2, 'telemetry', 'telemetry_opt_in', ' YES ', false, 'old', now(), now()),
			(gen_random_uuid(), $3, 'telemetry', 'telemetry_last_report_at', '2026-09-01 10:00:00', false, NULL, now(), now()),
			(gen_random_uuid(), $4, 'telemetry', 'telemetry_last_report_at', '2026-09-01T10:00:00Z', false, NULL, now(), now()),
			(gen_random_uuid(), $4, 'telemetry', 'telemetry_opt_in', 'true', false, 'Controls voluntary telemetry reporting.', now() - interval '1 day', now() - interval '1 day')`,
			[]any{f.orgA.String(), f.orgB.String(), f.orgFree.String(), f.orgNoLicense.String()}},
		// acr's service credential: Python's internal acr routes answer only
		// a live svc_acr_ bearer with the entitlements:read scope. The Go
		// routes do not check it (the network boundary is the control), so
		// the venue compares the authorised answers only.
		{`INSERT INTO internal_service_credentials (id, service_name, token_hash, token_prefix, scopes, created_at)
			VALUES (gen_random_uuid(), 'acr', encode(sha256(convert_to($1, 'UTF8')), 'hex'), left($1, 12), '["entitlements:read"]', now())`,
			[]any{venueACRToken}},
		{`INSERT INTO feature_flags (id, key, name, min_tier, is_enabled, created_at, updated_at) VALUES
			(gen_random_uuid(),'stored_only','Stored','team',true,now(),now()),
			(gen_random_uuid(),'bad_tier','Bad','platinum',true,now(),now()),
			(gen_random_uuid(),'disabled_one','Off','community',false,now(),now())`, nil},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement.sql)
		}
	}
	return f
}

// venueACRToken is the fixed acr service token the venue seeds (as its
// sha256) and sends.
const venueACRToken = "svc_acr_venue-fixed-token"

func b64(text string) *string { return venueoracle.B64(text) }

func venueRequests(f venueFixture, tokens map[string]string) []venueoracle.Request {
	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + tokens[name]}
	}
	with := func(headers map[string]string, key, value string) map[string]string {
		out := map[string]string{}
		for k, v := range headers {
			out[k] = v
		}
		out[key] = value
		return out
	}
	json := func(headers map[string]string) map[string]string {
		return with(headers, "Content-Type", "application/json")
	}
	var out []venueoracle.Request
	add := func(name, method, path string, headers map[string]string, body *string) {
		out = append(out, venueoracle.Request{Name: name, Method: method, Path: path, Headers: headers, Body: body})
	}
	me := "/api/v1/orgs/me"
	ent := "/api/v1/licensing/entitlements/"
	// GET /orgs/me
	add("me: anonymous", "GET", me, nil, nil)
	add("me: basic scheme", "GET", me, map[string]string{"Authorization": "Basic x"}, nil)
	add("me: garbage token", "GET", me, map[string]string{"Authorization": "Bearer a.b.c"}, nil)
	for _, caller := range []string{"member", "admin", "superuser", "outsider", "inactive", "stale", "no_org", "admin_free"} {
		add("me: "+caller, "GET", me, bearer(caller), nil)
	}
	add("me: member X-Org-Id stranger", "GET", me, with(bearer("member"), "X-Org-Id", f.orgB.String()), nil)
	add("me: superuser X-Org-Id any", "GET", me, with(bearer("superuser"), "X-Org-Id", f.orgA.String()), nil)
	add("me: HEAD", "HEAD", me, bearer("member"), nil)
	add("me: PUT", "PUT", me, bearer("member"), nil)
	add("me: DELETE anonymous", "DELETE", me, nil, nil)
	// CORS as a browser meets it (starlette 1.7.0): no Origin, an empty one,
	// the allowed one, a foreign one; a simple request, a refused one, an
	// unknown path, and preflights. Every answer carries Vary: Origin; only
	// the allowed Origin is echoed.
	add("cors: member allowed origin", "GET", me, with(bearer("member"), "Origin", "http://localhost:3000"), nil)
	add("cors: member foreign origin", "GET", me, with(bearer("member"), "Origin", "https://evil.example"), nil)
	add("cors: anonymous empty origin", "GET", me, map[string]string{"Origin": ""}, nil)
	add("cors: anonymous allowed origin", "GET", me, map[string]string{"Origin": "http://localhost:3000"}, nil)
	add("cors: unknown path allowed origin", "GET", "/api/v1/nope-cors", map[string]string{"Origin": "http://localhost:3000"}, nil)
	add("cors: unknown path no origin", "GET", "/api/v1/nope-cors", nil, nil)
	add("cors: preflight allowed", "OPTIONS", me, map[string]string{"Origin": "http://localhost:3000",
		"Access-Control-Request-Method": "PATCH", "Access-Control-Request-Headers": "authorization, content-type"}, nil)
	add("cors: preflight foreign", "OPTIONS", me, map[string]string{"Origin": "https://evil.example",
		"Access-Control-Request-Method": "GET"}, nil)
	add("cors: OPTIONS without request method", "OPTIONS", me, map[string]string{"Origin": "http://localhost:3000"}, nil)
	// PATCH /orgs/me: decode before auth, validation after auth, role gate.
	add("patch: anonymous bad json", "PATCH", me, json(nil), b64("{"))
	add("patch: anonymous valid", "PATCH", me, json(nil), b64(`{"name":"x"}`))
	add("patch: member", "PATCH", me, json(bearer("member")), b64(`{"name":"Member Try"}`))
	add("patch: stale admin", "PATCH", me, json(bearer("stale")), b64(`{"name":"Stale"}`))
	add("patch: admin empty body", "PATCH", me, bearer("admin"), nil)
	add("patch: admin null", "PATCH", me, json(bearer("admin")), b64(`null`))
	add("patch: admin list", "PATCH", me, json(bearer("admin")), b64(`[]`))
	add("patch: admin text/plain", "PATCH", me, with(bearer("admin"), "Content-Type", "text/plain"), b64(`{"name":"x"}`))
	add("patch: admin +json type", "PATCH", me, with(bearer("admin"), "Content-Type", "application/merge-patch+json"), b64(`{"description":"merge"}`))
	add("patch: admin empty name", "PATCH", me, json(bearer("admin")), b64(`{"name":""}`))
	add("patch: admin long name", "PATCH", me, json(bearer("admin")), b64(`{"name":"`+string(make([]byte, 0))+repeat("é", 256)+`"}`))
	add("patch: admin name 255", "PATCH", me, json(bearer("admin")), b64(`{"name":"`+repeat("n", 255)+`"}`))
	add("patch: admin wrong types", "PATCH", me, json(bearer("admin")), b64(`{"name":5,"description":["x"]}`))
	add("patch: admin unknown field", "PATCH", me, json(bearer("admin")), b64(`{"tier":"enterprise"}`))
	add("patch: admin null name", "PATCH", me, json(bearer("admin")), b64(`{"name":null,"description":"only desc"}`))
	add("patch: admin both", "PATCH", me, json(bearer("admin")), b64(`{"name":"Org A <renamed> & \"q\"","description":"tab\there é"}`))
	add("patch: owner", "PATCH", me, json(bearer("owner")), b64(`{"description":""}`))
	add("patch: no org", "PATCH", me, json(bearer("no_org")), b64(`{"name":"x"}`))
	add("patch: admin free org", "PATCH", me, json(bearer("admin_free")), b64(`{"name":"Free Renamed"}`))
	utf16Body := func(text string, bigEndian bool) *string {
		raw := []byte{0xff, 0xfe}
		if bigEndian {
			raw = []byte{0xfe, 0xff}
		}
		for _, r := range text {
			if bigEndian {
				raw = append(raw, byte(r>>8), byte(r))
			} else {
				raw = append(raw, byte(r), byte(r>>8))
			}
		}
		encoded := base64.StdEncoding.EncodeToString(raw)
		return &encoded
	}
	rawBody := func(raw []byte) *string {
		encoded := base64.StdEncoding.EncodeToString(raw)
		return &encoded
	}
	add("patch: anonymous UTF-16LE", "PATCH", me, json(nil), utf16Body(`{"name":"x"}`, false))
	add("patch: admin UTF-16BE description", "PATCH", me, json(bearer("admin")), utf16Body(`{"description":"sixteen"}`, true))
	add("patch: admin lone surrogate name", "PATCH", me, json(bearer("admin")), b64(`{"name":"\ud800"}`))
	add("patch: admin surrogate too long", "PATCH", me, json(bearer("admin")), b64(`{"name":"`+repeat(`\ud800`, 256)+`"}`))
	add("patch: anonymous invalid UTF-8", "PATCH", me, json(nil), rawBody([]byte("{\"name\":\"\xff\"}")))
	add("patch: admin text/plain invalid UTF-8", "PATCH", me, with(bearer("admin"), "Content-Type", "text/plain"), rawBody([]byte("\xff")))
	add("me: after writes", "GET", me, bearer("admin"), nil)
	// Entitlements.
	add("ent: anonymous", "GET", ent+f.orgA.String(), nil, nil)
	add("ent: member own", "GET", ent+f.orgA.String(), bearer("member"), nil)
	add("ent: member other", "GET", ent+f.orgB.String(), bearer("member"), nil)
	add("ent: member own uppercase", "GET", ent+upper(f.orgA.String()), bearer("member"), nil)
	add("ent: superuser org A", "GET", ent+f.orgA.String(), bearer("superuser"), nil)
	add("ent: superuser org B (bad license)", "GET", ent+f.orgB.String(), bearer("superuser"), nil)
	add("ent: superuser free org", "GET", ent+f.orgFree.String(), bearer("superuser"), nil)
	add("ent: superuser no license", "GET", ent+f.orgNoLicense.String(), bearer("superuser"), nil)
	add("ent: superuser uppercase", "GET", ent+upper(f.orgA.String()), bearer("superuser"), nil)
	add("ent: superuser braces", "GET", ent+"{"+f.orgA.String()+"}", bearer("superuser"), nil)
	add("ent: superuser not uuid", "GET", ent+"nope", bearer("superuser"), nil)
	add("ent: superuser unknown org", "GET", ent+"00000000-0000-4000-8000-000000000000", bearer("superuser"), nil)
	add("ent: bad org claim", "GET", ent+"not-a-uuid", bearer("bad_org_claim"), nil)
	add("ent: anonymous encoded slash", "GET", ent+"not-a%2Fuuid", nil, nil)
	add("ent: member encoded slash", "GET", ent+"a%2fb", bearer("member"), nil)
	add("ent: acr encoded slash", "GET", "/api/v1/internal/acr/entitlements/not-a%2Fuuid", nil, nil)
	// Internal acr routes, with the credential Python requires.
	acrAuth := map[string]string{"Authorization": "Bearer " + venueACRToken}
	acrEnt := "/api/v1/internal/acr/entitlements/"
	add("acr: health", "GET", "/api/v1/internal/acr/health", acrAuth, nil)
	add("acr: team org", "GET", acrEnt+f.orgA.String(), acrAuth, nil)
	add("acr: enterprise org", "GET", acrEnt+f.orgB.String(), acrAuth, nil)
	add("acr: free org", "GET", acrEnt+f.orgFree.String(), acrAuth, nil)
	add("acr: no license org", "GET", acrEnt+f.orgNoLicense.String(), acrAuth, nil)
	add("acr: uppercase", "GET", acrEnt+upper(f.orgA.String()), acrAuth, nil)
	add("acr: braces", "GET", acrEnt+"{"+f.orgA.String()+"}", acrAuth, nil)
	add("acr: urn", "GET", acrEnt+"urn:uuid:"+f.orgA.String(), acrAuth, nil)
	add("acr: no hyphens", "GET", acrEnt+strings.ReplaceAll(f.orgA.String(), "-", ""), acrAuth, nil)
	add("acr: unknown org", "GET", acrEnt+"00000000-0000-4000-8000-000000000000", acrAuth, nil)
	add("acr: not uuid", "GET", acrEnt+"nope", acrAuth, nil)
	add("acr: POST", "POST", acrEnt+f.orgA.String(), acrAuth, nil)
	add("ent: POST", "POST", ent+f.orgA.String(), bearer("member"), nil)
	add("ent: HEAD", "HEAD", ent+f.orgA.String(), bearer("member"), nil)
	// Telemetry settings and the instance report.
	status, optIn, optOut, report := "/api/v1/telemetry/status", "/api/v1/telemetry/opt-in", "/api/v1/telemetry/opt-out", "/api/v1/telemetry/report"
	add("tel: anonymous", "GET", status, nil, nil)
	add("tel: member own", "GET", status, bearer("member"), nil)
	add("tel: member header member org", "GET", status, with(bearer("admin"), "X-Org-Id", f.orgFree.String()), nil)
	add("tel: member header stranger", "GET", status, with(bearer("member"), "X-Org-Id", f.orgB.String()), nil)
	add("tel: no org", "GET", status, bearer("no_org"), nil)
	add("tel: superuser org B", "GET", status, with(bearer("superuser"), "X-Org-Id", f.orgB.String()), nil)
	add("tel: superuser uppercase org", "GET", status, with(bearer("superuser"), "X-Org-Id", upper(f.orgA.String())), nil)
	add("tel: superuser no-license org", "GET", status, with(bearer("superuser"), "X-Org-Id", f.orgNoLicense.String()), nil)
	add("tel: impersonator own", "GET", status, bearer("impersonator"), nil)
	add("tel: impersonator header target", "GET", status, with(bearer("impersonator"), "X-Org-Id", f.orgA.String()), nil)
	add("tel: impersonator header other", "GET", status, with(bearer("impersonator"), "X-Org-Id", f.orgB.String()), nil)
	add("tel: opt-in member", "POST", optIn, bearer("member"), nil)
	add("tel: opt-in again", "POST", optIn, bearer("member"), nil)
	add("tel: opt-out org B", "POST", optOut, with(bearer("superuser"), "X-Org-Id", f.orgB.String()), nil)
	add("tel: opt-in unchanged", "POST", optIn, with(bearer("superuser"), "X-Org-Id", f.orgNoLicense.String()), nil)
	add("tel: opt-in uppercase org", "POST", optIn, with(bearer("superuser"), "X-Org-Id", upper(f.orgA.String())), nil)
	add("tel: opt-out impersonator", "POST", optOut, bearer("impersonator"), nil)
	add("tel: opt-in stranger", "POST", optIn, with(bearer("member"), "X-Org-Id", f.orgB.String()), nil)
	add("tel: status after", "GET", status, bearer("member"), nil)
	add("tel: GET opt-in", "GET", optIn, bearer("member"), nil)
	add("tel: status HEAD", "HEAD", status, bearer("member"), nil)
	add("report: anonymous", "POST", report, nil, nil)
	add("report: member", "POST", report, bearer("member"), nil)
	add("report: impersonator", "POST", report, bearer("impersonator"), nil)
	add("report: superuser not opted in", "POST", report, with(bearer("superuser"), "X-Org-Id", f.orgFree.String()), nil)
	add("report: superuser opted in", "POST", report, with(bearer("superuser"), "X-Org-Id", f.orgNoLicense.String()), nil)
	add("report: superuser no org", "POST", report, bearer("superuser"), nil)
	add("tel: opt-in by slug", "POST", optIn, with(bearer("superuser"), "X-Org-Id", "org-free"), nil)
	add("report: superuser by slug", "POST", report, with(bearer("superuser"), "X-Org-Id", "org-free"), nil)
	// Product telemetry (public).
	events := "/api/v1/product-telemetry/events"
	event := `{"name":"page_viewed","schemaVersion":"1","eventId":"e1","ts":"2026-09-23T02:00:00.123Z","sessionId":"s","anonymousUserId":"a","payload":{"f":1.5,"i":7,"b":true,"n":null,"s":"é\u2028x","big":123456789012345678901234567890}}`
	for _, body := range []struct{ name, text, contentType string }{
		{"valid anonymous", `{"events":[` + event + `]}`, "application/json"},
		{"valid org hash + names", `{"org_id_hash":"h1","orgIdHash":"h2","events":[` + event + `,{"name":"client_error","schema_version":"2","event_id":"e2","ts":1700000000.5,"session_id":"s2","anonymous_user_id":"a2","orgIdHash":null,"route_pattern":"/x","payload":{}}]}`, "application/json"},
		{"empty org hash", `{"orgIdHash":"","events":[` + event + `]}`, "application/json"},
		{"octet-stream", `{"events":[` + event + `]}`, "application/octet-stream"},
		{"ts forms", `{"events":[` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `"2026-09-23"`, 1) + `,` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `"2026-09-23 02:00+0200"`, 1) + `,` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `"1700000000123"`, 1) + `,` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `-1`, 1) + `]}`, "application/json"},
		{"bad ts forms", `{"events":[` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `"2026-02-30"`, 1) + `,` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `"2026-09-23T25:00"`, 1) + `,` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `true`, 1) + `,` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `"x"`, 1) + `,` + strings.Replace(event, `"2026-09-23T02:00:00.123Z"`, `1e20`, 1) + `]}`, "application/json"},
		{"wrong types", `{"source":"other","orgIdHash":5,"events":[{"name":"nope","schemaVersion":1,"eventId":null,"ts":"2026-09-23","sessionId":"s","anonymousUserId":"a","payload":{"l":[1],"o":{}}},{}, 3]}`, "application/json"},
		{"payload not object", `{"events":[` + strings.Replace(event, `"payload":{`, `"payload":[],"x":{`, 1) + `]}`, "application/json"},
		{"events empty", `{"events":[]}`, "application/json"},
		{"events not list", `{"events":"x","source":null}`, "application/json"},
		{"events missing", `{}`, "application/json"},
		{"too many", `{"events":[` + strings.TrimSuffix(strings.Repeat("1,", 501), ",") + `]}`, "application/json"},
		{"body list", `[]`, "application/json"},
		{"text plain", `{"events":[]}`, "text/plain"},
		{"bad json", `{"events":`, "application/json"},
		{"nan payload", `{"events":[` + strings.Replace(event, `"f":1.5`, `"f":NaN`, 1) + `]}`, "application/json"},
		{"lone surrogate payload", `{"events":[` + strings.Replace(event, `"f":1.5`, `"f":"\ud800"`, 1) + `]}`, "application/json"},
		{"lone surrogate org hash", `{"orgIdHash":"\ud800","events":[` + event + `]}`, "application/json"},
		{"lone surrogate org hash snake", `{"org_id_hash":"a\udfffb","events":[` + event + `]}`, "application/json"},
		{"paired surrogate org hash", `{"orgIdHash":"\ud83d\ude00","events":[` + event + `]}`, "application/json"},
		{"lone surrogate bad name", `{"events":[` + strings.Replace(event, `"page_viewed"`, `"\ud800"`, 1) + `]}`, "application/json"},
	} {
		headers := map[string]string{}
		if body.contentType != "" {
			headers["Content-Type"] = body.contentType
		}
		add("pt: "+body.name, "POST", events, headers, b64(body.text))
	}
	add("pt: GET", "GET", events, nil, nil)
	add("pt: stranger org header", "POST", events, with(json(bearer("member")), "X-Org-Id", f.orgB.String()), b64(`{"events":[`+event+`]}`))
	add("ent: impersonator", "GET", ent+f.orgB.String(), bearer("impersonator"), nil)
	// Probes (the rate limiter and Celery values are normalized: ruled).
	for _, path := range []string{"/health", "/ready", "/health/workers"} {
		add("probe: GET "+path, "GET", path, nil, nil)
		add("probe: HEAD "+path, "HEAD", path, nil, nil)
		add("probe: POST "+path, "POST", path, nil, nil)
		add("probe: GET "+path+" stranger org", "GET", path, with(bearer("member"), "X-Org-Id", f.orgB.String()), nil)
	}
	// Starlette's redirect_slashes: an unmatched path whose trailing slash,
	// toggled, matches a route answers 307 (the harness does not follow
	// redirects). Removing: one or more trailing slashes, any method, the
	// query kept. Adding: no Python route ends in "/", so an unmatched path
	// without one stays 404 on both planes.
	add("slash: GET /health/", "GET", "/health/", nil, nil)
	add("slash: GET /health/workers//", "GET", "/health/workers//", nil, nil)
	add("slash: POST /ready/ with query", "POST", "/ready/?a=1&b=%2F", nil, nil)
	add("slash: GET org route with slash", "GET", "/api/v1/orgs/me/", bearer("admin"), nil)
	add("slash: GET unknown/", "GET", "/api/v1/nothing-here/", nil, nil)
	add("slash: GET unknown (add direction)", "GET", "/api/v1/nothing-here", nil, nil)
	add("slash: GET /health with slash and Host", "GET", "/health/", map[string]string{"Host": "api.example.com:8443"}, nil)
	// Dot segments: Starlette matches the decoded path literally, so an
	// encoded ".." is an {org_id} value. Invalid UTF-8: uvicorn's unquote
	// replaces each maximal ill-formed subpart with one U+FFFD, which the
	// Location carries as %EF%BF%BD.
	add("slash: encoded dot-dot segment", "GET", ent+"%2e%2e/", nil, nil)
	add("slash: encoded dot segment", "GET", ent+"%2E/", nil, nil)
	add("slash: encoded dot-dot without slash", "GET", ent+"%2e%2e", nil, nil)
	add("slash: invalid utf-8 byte", "GET", ent+"%FF/", nil, nil)
	add("slash: truncated utf-8 sequence", "GET", ent+"%E2%82/", nil, nil)
	add("slash: overlong utf-8 pair", "GET", ent+"a%C0%AFb/", nil, nil)

	// Team + identity admin CRUD (CHAOS-6310). Both org-A callers below
	// authenticate to the SAME org, so their writes accumulate in order --
	// Python and Go each process this exact sequence once, against their
	// own isolated ClickHouse database, so the two must reach identical
	// end states.
	teams := "/api/v1/admin/teams"
	identities := "/api/v1/admin/identities"
	add("teams: anonymous", "GET", teams, nil, nil)
	add("teams: non-admin", "GET", teams, bearer("member"), nil)
	add("teams: list empty", "GET", teams, bearer("admin"), nil)
	add("teams: create missing team_id", "POST", teams, json(bearer("admin")), b64(`{"name":"Eng"}`))
	add("teams: create missing name", "POST", teams, json(bearer("admin")), b64(`{"team_id":"eng"}`))
	add("teams: create ok", "POST", teams, json(bearer("admin")),
		b64(`{"team_id":"eng","name":"Engineering","description":"core","repo_patterns":["svc-*"],"project_keys":["ENG"]}`))
	add("teams: create is update on same id", "POST", teams, json(bearer("admin")), b64(`{"team_id":"eng","name":"Engineering Renamed"}`))
	add("teams: get existing", "GET", teams+"/eng", bearer("admin"), nil)
	add("teams: get missing", "GET", teams+"/nope", bearer("admin"), nil)
	// r2 finding #4: GET .../teams/discover without `provider` must be the
	// SAME 422 "Field required" on both planes, not Go's own 404 "Team not
	// found" from treating "discover" as a team_id.
	add("teams: get discover without provider", "GET", teams+"/discover", bearer("admin"), nil)
	add("teams: list after create", "GET", teams, bearer("admin"), nil)
	add("teams: patch description only", "PATCH", teams+"/eng", json(bearer("admin")), b64(`{"description":"patched"}`))
	add("teams: patch missing team", "PATCH", teams+"/nope", json(bearer("admin")), b64(`{"name":"x"}`))
	add("teams: patch wrong type", "PATCH", teams+"/eng", json(bearer("admin")), b64(`{"repo_patterns":"not-a-list"}`))
	add("teams: create second team", "POST", teams, json(bearer("owner")), b64(`{"team_id":"design","name":"Design"}`))
	add("identities: create missing canonical_id", "POST", identities, json(bearer("admin")), b64(`{}`))
	add("identities: create unknown team_id", "POST", identities, json(bearer("admin")), b64(`{"canonical_id":"alice","team_ids":["nope"]}`))
	add("identities: create ok", "POST", identities, json(bearer("admin")),
		b64(`{"canonical_id":"alice","email":"alice@example.com","provider_identities":{"github":["alice-gh"]},"team_ids":["eng"]}`))
	add("identities: create conflicting provider identity", "POST", identities, json(bearer("admin")),
		b64(`{"canonical_id":"bob","provider_identities":{"github":["alice-gh"]}}`))
	add("identities: list", "GET", identities, bearer("admin"), nil)
	add("identities: update moves team and email", "POST", identities, json(bearer("admin")),
		b64(`{"canonical_id":"alice","email":"alice-new@example.com","team_ids":["design"]}`))
	add("teams: get eng after identity left", "GET", teams+"/eng", bearer("admin"), nil)
	add("teams: get design after identity joined", "GET", teams+"/design", bearer("admin"), nil)
	add("teams: delete eng", "DELETE", teams+"/eng", bearer("admin"), nil)
	add("teams: delete already deleted", "DELETE", teams+"/eng", bearer("admin"), nil)
	add("teams: get after delete", "GET", teams+"/eng", bearer("admin"), nil)

	// r1 findings, extended coverage (CHAOS-6310): each of these reproduces
	// one of the 10 defects the codex round's live venue run found, now
	// fixed at the pydantic-model source rather than the reported case.
	add("teams: create qa", "POST", teams, json(bearer("admin")),
		b64(`{"team_id":"qa","name":"QA","repo_patterns":["qa-*"]}`))
	add("teams: create rejects null repo_patterns (create-shaped, no | None)", "POST", teams, json(bearer("admin")),
		b64(`{"team_id":"qa2","name":"QA2","repo_patterns":null}`))
	add("teams: patch null repo_patterns keeps existing (update-shaped, | None)", "PATCH", teams+"/qa", json(bearer("admin")),
		b64(`{"repo_patterns":null}`))
	add("teams: create rejects extra_data not a dict", "POST", teams, json(bearer("admin")),
		b64(`{"team_id":"qa3","name":"QA3","extra_data":"not-a-dict"}`))
	add("teams: patch rejects extra_data null-vs-object mismatch", "PATCH", teams+"/qa", json(bearer("admin")),
		b64(`{"extra_data":"not-a-dict"}`))
	add("teams: create coerces sync_policy string", "POST", teams, json(bearer("admin")),
		b64(`{"team_id":"qa4","name":"QA4","sync_policy":"1"}`))
	// r2 finding #1: sync_policy far past int64 (2**64+1) must be a bounds
	// rejection on both planes, not a wrapped-and-accepted write.
	add("teams: create rejects sync_policy beyond int64", "POST", teams, json(bearer("admin")),
		b64(`{"team_id":"qa5","name":"QA5","sync_policy":18446744073709551617}`))
	add("teams: list rejects active_only=not-a-bool", "GET", teams+"?active_only=not-a-bool", bearer("admin"), nil)
	add("identities: create rejects null provider_identities (create-shaped, no | None)", "POST", identities, json(bearer("admin")),
		b64(`{"canonical_id":"carol","provider_identities":null}`))
	add("identities: create rejects null team_ids (create-shaped, no | None)", "POST", identities, json(bearer("admin")),
		b64(`{"canonical_id":"dave","team_ids":null}`))
	// r1 finding #6, fixed in r2: unknownTeamIDsDetail now uses the shared
	// pythonparity.StrRepr encoder, so the full response BODY -- not just
	// the status -- must match byte for byte here too, including the
	// apostrophe's repr()-style double-quote switch.
	add("identities: create unknown team_id with apostrophe", "POST", identities, json(bearer("admin")),
		b64(`{"canonical_id":"erin","team_ids":["doesn't-exist"]}`))
	// r2 finding #3: provider_identities must preserve the request's own
	// insertion order ("zeta" before "alpha") in BOTH the response body and
	// the identity read back afterward, not an alphabetized order.
	add("identities: create multi-provider preserves insertion order", "POST", identities, json(bearer("admin")),
		b64(`{"canonical_id":"frank","provider_identities":{"zeta":["z-gh"],"alpha":["a-gh"]}}`))
	add("identities: get multi-provider identity preserves stored order", "GET", identities, bearer("admin"), nil)

	// POST /teams/import (CHAOS-6311): status + body compared per request,
	// and the rows it writes (teams, team_provider_observations,
	// team_drift_changes, team_sync_policies) compared as raw text after
	// the whole sequence. The sequence exercises every branch of
	// import_teams: new (imported), existing under on_conflict=skip
	// (skipped, observation only), existing under merge with the default
	// AUTO_APPLY policy (merged), existing under a FLAG_FOR_REVIEW policy
	// (qa4, created above with sync_policy=1: drift-change rows, catalog
	// untouched), and the pydantic 422 shapes.
	imp := teams + "/import"
	team := func(fields string) *string {
		return b64(`{"teams":[{` + fields + `}]}`)
	}
	add("teams import: anonymous", "POST", imp, json(nil), b64(`{"teams":[]}`))
	add("teams import: non-admin", "POST", imp, json(bearer("member")), b64(`{"teams":[]}`))
	add("teams import: missing teams", "POST", imp, json(bearer("admin")), b64(`{}`))
	add("teams import: bad on_conflict", "POST", imp, json(bearer("admin")), b64(`{"teams":[],"on_conflict":"overwrite"}`))
	add("teams import: associations null is a 422", "POST", imp, json(bearer("admin")),
		team(`"provider_type":"jira","provider_team_id":"BAD","name":"Bad","associations":null`))
	add("teams import: member_count fractional is a 422", "POST", imp, json(bearer("admin")),
		team(`"provider_type":"jira","provider_team_id":"BAD","name":"Bad","member_count":5.5`))
	add("teams import: empty list", "POST", imp, json(bearer("admin")), b64(`{"teams":[]}`))
	add("teams import: jira team is imported", "POST", imp, json(bearer("admin")),
		team(`"provider_type":"jira","provider_team_id":"IMP","name":"Imported Jira","description":"","associations":{"project_keys":["IMP"],"provider_org":"https://x.atlassian.net"}`))
	add("teams import: github team with repo patterns", "POST", imp, json(bearer("admin")),
		team(`"provider_type":"github","provider_team_id":"platform","name":"Platform","description":"gh team","member_count":"4","associations":{"repo_patterns":["acme/api","acme/web"],"provider_org":"acme"}`))
	add("teams import: gitlab subgroup path", "POST", imp, json(bearer("admin")),
		team(`"provider_type":"gitlab","provider_team_id":"acme/sub","name":"Sub","associations":{"repo_patterns":["acme/sub/x"],"provider_org":"acme"}`))
	add("teams import: same team again is skipped by default", "POST", imp, json(bearer("admin")),
		team(`"provider_type":"jira","provider_team_id":"IMP","name":"Imported Jira","associations":{"project_keys":["IMP"]}`))
	add("teams import: same team merged with a new name", "POST", imp, json(bearer("admin")),
		b64(`{"teams":[{"provider_type":"jira","provider_team_id":"IMP","name":"Imported Jira Renamed","associations":{"project_keys":["IMP","IMP2"]}}],"on_conflict":"merge"}`))
	add("teams import: existing manual team merged (AUTO_APPLY)", "POST", imp, json(bearer("admin")),
		b64(`{"teams":[{"provider_type":"jira","provider_team_id":"qa","name":"QA Imported","associations":{"project_keys":["QA"]}}],"on_conflict":"merge"}`))
	add("teams import: existing team skipped", "POST", imp, json(bearer("admin")),
		team(`"provider_type":"jira","provider_team_id":"design","name":"Design Imported"`))
	add("teams import: FLAG_FOR_REVIEW team writes drift changes only", "POST", imp, json(bearer("admin")),
		b64(`{"teams":[{"provider_type":"jira","provider_team_id":"qa4","name":"QA4 Imported","description":"drifted","associations":{"project_keys":["QA4","QA4B"]}}],"on_conflict":"merge"}`))
	add("teams import: FLAG_FOR_REVIEW same drift again is idempotent", "POST", imp, json(bearer("admin")),
		b64(`{"teams":[{"provider_type":"jira","provider_team_id":"qa4","name":"QA4 Imported","description":"drifted","associations":{"project_keys":["QA4","QA4B"]}}],"on_conflict":"merge"}`))
	// _list_field (clickhouse_team_drift_projector.py:497): str() of every
	// non-null element, a string is a one-element list, anything else is [].
	for _, c := range []struct{ id, projectKeys string }{
		{"AINT", `[7,"ENG"]`}, {"ANULL", `["a",null]`}, {"ABOOL", `[true]`}, {"AFLOAT", `[1.5]`},
		{"ASTR", `"single"`}, {"ANUM", `5`}, {"ADICT", `{"a":1}`}, {"ANESTED", `[["x"]]`}, {"AEMPTY", `[]`},
		{"ANULLV", `null`}, {"ABOOLS", `true`}, {"AFLOATS", `1.5`}, {"AEMPTYSTR", `""`}, {"AUNI", `"日本ü"`}, {"ADICT2", `{"k":[1],"z":null}`},
	} {
		add("teams import: AUTO_APPLY project_keys "+c.id, "POST", imp, json(bearer("admin")),
			team(`"provider_type":"jira","provider_team_id":"`+c.id+`","name":"`+c.id+`","associations":{"project_keys":`+c.projectKeys+`}`))
	}
	add("teams import: FLAG_FOR_REVIEW numeric association elements", "POST", imp, json(bearer("admin")),
		b64(`{"teams":[{"provider_type":"jira","provider_team_id":"qa4","name":"QA4 Imported","description":"drifted","associations":{"project_keys":[7,"QA4"]}}],"on_conflict":"merge"}`))
	add("teams import: several teams in one request", "POST", imp, json(bearer("admin")),
		b64(`{"teams":[{"provider_type":"linear","provider_team_id":"L1","name":"Lin One","associations":{"project_keys":["L1"]}},{"provider_type":"linear","provider_team_id":"L2","name":"Lin Two"}],"on_conflict":"merge"}`))
	// Team drift review (CHAOS-6312): seeded by seedDriftReview.
	pend := teams + "/pending-changes"
	add("drift: pending anonymous", "GET", pend, nil, nil)
	add("drift: pending non-admin", "GET", pend, bearer("member"), nil)
	add("drift: pending all", "GET", pend, bearer("admin"), nil)
	// The route takes no filter: a team_id query parameter is ignored on both
	// planes, so these three requests must answer the full pending list.
	add("drift: pending filtered to qa", "GET", pend+"?team_id=qa", bearer("admin"), nil)
	add("drift: pending filtered to a team with none", "GET", pend+"?team_id=nope", bearer("admin"), nil)
	add("drift: pending other org sees own only", "GET", pend, bearer("owner"), nil)
	dec := func(verb, team string) string { return teams + "/" + team + "/" + verb + "-changes" }
	add("drift: approve anonymous", "POST", dec("approve", "qa"), json(nil), b64(`{"change_ids":["c-name-qa"]}`))
	add("drift: approve non-admin", "POST", dec("approve", "qa"), json(bearer("member")), b64(`{"change_ids":["c-name-qa"]}`))
	add("drift: approve empty body object", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{}`))
	add("drift: approve change_ids wrong type", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":"c-name-qa"}`))
	add("drift: approve approve_all wrong type", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"approve_all":"maybe"}`))
	add("drift: approve body not an object", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`[1]`))
	add("drift: approve unknown ids", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["nope","c-other-org"]}`))
	add("drift: approve already-approved and dismissed ids", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-already","c-dismissed"]}`))
	add("drift: approve name change", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-name-qa"]}`))
	add("drift: approve name change again is a no-op", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-name-qa"]}`))
	add("drift: get qa after name approval", "GET", teams+"/qa", bearer("admin"), nil)
	add("drift: approve members change", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-members-qa"]}`))
	add("drift: approve description change", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-desc-qa"]}`))
	add("drift: approve unmapped field", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-badcol-qa"]}`))
	add("drift: approve is_active field", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-active-qa"]}`))
	add("drift: approve change without a field", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-nofield-qa"]}`))
	add("drift: approve with no observation", "POST", dec("approve", "nobs"), json(bearer("admin")), b64(`{"change_ids":["c-noobs"]}`))
	add("drift: approve bad-json payload", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"change_ids":["c-badjson"]}`))
	add("drift: approve project_keys on a new team", "POST", dec("approve", "dr-team"), json(bearer("admin")), b64(`{"change_ids":["c-projkeys-dr","c-repo-dr","c-members-dr"]}`))
	add("drift: get dr-team", "GET", teams+"/dr-team", bearer("admin"), nil)
	add("drift: approve identity membership change", "POST", dec("approve", "design"), json(bearer("admin")), b64(`{"change_ids":["c-ident-mem"]}`))
	add("drift: approve identity fallback change", "POST", dec("approve", "design"), json(bearer("admin")), b64(`{"change_ids":["c-ident-fb"]}`))
	add("drift: approve identity malformed payload", "POST", dec("approve", "design"), json(bearer("admin")), b64(`{"change_ids":["c-ident-bad"]}`))
	add("drift: pending after approvals", "GET", pend, bearer("admin"), nil)
	add("drift: dismiss unknown", "POST", dec("dismiss", "qa"), json(bearer("admin")), b64(`{"change_ids":["nope"]}`))
	add("drift: dismiss empty body object", "POST", dec("dismiss", "qa"), json(bearer("admin")), b64(`{}`))
	add("drift: dismiss dismiss_all wrong type", "POST", dec("dismiss", "qa"), json(bearer("admin")), b64(`{"dismiss_all":[]}`))
	add("drift: dismiss all", "POST", dec("dismiss", "qa"), json(bearer("admin")), b64(`{"dismiss_all":true}`))
	add("drift: pending after dismiss_all", "GET", pend, bearer("admin"), nil)
	add("drift: approve_all with nothing pending", "POST", dec("approve", "qa"), json(bearer("admin")), b64(`{"approve_all":true}`))
	add("teams: PUT is not a route (Allow header of the pattern's first route)", "PUT", teams+"/eng", json(bearer("admin")), b64(`{}`))
	add("teams import: POST to another team id with a malformed body is still a 405", "POST", teams+"/eng", json(bearer("admin")), b64(`{not json`))
	add("teams import: POST to another team id is not a route", "POST", teams+"/eng", json(bearer("admin")), b64(`{}`))
	return out
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

func upper(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c >= 'a' && c <= 'f' {
			out[i] = c - 32
		}
	}
	return string(out)
}
