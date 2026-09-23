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
		{`INSERT INTO users (id, email, is_superuser) VALUES ($1, 'imp@x', true)`, []any{f.impersonator}},
		{`INSERT INTO impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, expires_at)
			VALUES (gen_random_uuid(), $1, $2, $3, 'member', now() + interval '1 hour')`, []any{f.impersonator, f.member, f.orgA}},
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
