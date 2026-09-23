//go:build integration

package apiservice

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// customerPushFixture is the customer-push admin venue's seed.
type customerPushFixture struct {
	orgTeam, orgCommunity, orgDisabled, orgLicenseWins     uuid.UUID
	admin, member, communityOwner, disabledAdmin, licAdmin uuid.UUID
	superuser                                              uuid.UUID
	sourceGitHub, sourceGitLab, sourceCustom, sourceOther  uuid.UUID
	tokenBound, tokenOrgWide, tokenRevoked, tokenOther     uuid.UUID
	batchMain, batchOld, batchOtherSource, batchOtherOrg   uuid.UUID
}

func (f customerPushFixture) tokenSpecs() map[string]map[string]any {
	spec := func(user, org uuid.UUID, role string, extra map[string]any) map[string]any {
		out := map[string]any{"user_id": user.String(), "email": user.String()[:8] + "@example.com", "org_id": org.String(), "role": role}
		for key, value := range extra {
			out[key] = value
		}
		return out
	}
	return map[string]map[string]any{
		"admin":           spec(f.admin, f.orgTeam, "admin", nil),
		"member":          spec(f.member, f.orgTeam, "member", nil),
		"community_owner": spec(f.communityOwner, f.orgCommunity, "owner", nil),
		"disabled_admin":  spec(f.disabledAdmin, f.orgDisabled, "admin", nil),
		"license_admin":   spec(f.licAdmin, f.orgLicenseWins, "admin", nil),
		"superuser":       spec(f.superuser, f.orgCommunity, "member", map[string]any{"is_superuser": true}),
		"bad_org_claim":   {"user_id": f.admin.String(), "email": "a@example.com", "org_id": "not-a-uuid", "role": "admin"},
	}
}

func customerPushSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) customerPushFixture {
	t.Helper()
	f := customerPushFixture{}
	for _, id := range []*uuid.UUID{&f.orgTeam, &f.orgCommunity, &f.orgDisabled, &f.orgLicenseWins, &f.admin, &f.member,
		&f.communityOwner, &f.disabledAdmin, &f.licAdmin, &f.superuser, &f.sourceGitHub, &f.sourceGitLab, &f.sourceCustom,
		&f.sourceOther, &f.tokenBound, &f.tokenOrgWide, &f.tokenRevoked, &f.tokenOther, &f.batchMain, &f.batchOld,
		&f.batchOtherSource, &f.batchOtherOrg} {
		*id = uuid.New()
	}
	team, community := f.orgTeam.String(), f.orgCommunity.String()
	statements := []struct {
		sql  string
		args []any
	}{
		// Team tier: enabled by tier. Community: 402 with its tier. An
		// enterprise org whose override disables the feature: 403. An org
		// whose license row (community) wins over its organizations tier
		// (team): 402 community.
		{`INSERT INTO organizations (id, slug, name, tier) VALUES
			($1,'cp-team','CP Team','team'), ($2,'cp-community','CP Community','community'),
			($3,'cp-disabled','CP Disabled','enterprise'), ($4,'cp-license','CP License','team')`,
			[]any{f.orgTeam, f.orgCommunity, f.orgDisabled, f.orgLicenseWins}},
		{`INSERT INTO org_licenses (id, org_id, tier, licensed_users, licensed_repos, is_valid, created_at, updated_at)
			VALUES (gen_random_uuid(), $1, 'community', 5, 5, true, now(), now())`, []any{f.orgLicenseWins}},
		{`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
			SELECT gen_random_uuid(), $1, id, false, now(), now() FROM feature_flags WHERE key = 'customer_push_ingest'`, []any{f.orgDisabled}},
		{`INSERT INTO users (id, email, is_superuser, is_active, token_version) VALUES
			($1,'cp-admin@x',false,true,0), ($2,'cp-member@x',false,true,0), ($3,'cp-owner@x',false,true,0),
			($4,'cp-dis@x',false,true,0), ($5,'cp-lic@x',false,true,0), ($6,'cp-su@x',true,true,0)`,
			[]any{f.admin, f.member, f.communityOwner, f.disabledAdmin, f.licAdmin, f.superuser}},
		{`INSERT INTO memberships (id, user_id, org_id, role) VALUES
			(gen_random_uuid(),$1,$5,'admin'), (gen_random_uuid(),$2,$5,'member'), (gen_random_uuid(),$3,$6,'owner'),
			(gen_random_uuid(),$4,$7,'admin')`, []any{f.admin, f.member, f.communityOwner, f.disabledAdmin, f.orgTeam, f.orgCommunity, f.orgDisabled}},
		{`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, display_name, mode, enabled,
			webhook_mode, matched_integration_source_id, created_at, updated_at) VALUES
			($1,$5,'github','Acme/API','legacy','Acme API','customer_push',true,'disabled',NULL,'2026-09-01T10:00:00.123456Z','2026-09-02T11:00:00Z'),
			($2,$5,'gitlab','grp/proj','operational',NULL,'managed_sync',false,'customer_hosted',$3,'2026-09-03T10:00:00Z','2026-09-03T10:00:00Z'),
			($3,$5,'custom','x é y','legacy','Ünïcode','customer_push',true,'disabled',NULL,'2026-09-02T10:00:00Z','2026-09-02T10:00:00.5Z'),
			($4,$6,'github','other/repo','legacy',NULL,'customer_push',true,'disabled',NULL,'2026-09-01T00:00:00Z','2026-09-01T00:00:00Z')`,
			[]any{f.sourceGitHub, f.sourceGitLab, f.sourceCustom, f.sourceOther, team, community}},
		{`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_by_user_id,
			expires_at, revoked_at, last_used_at, created_at) VALUES
			($1,$5,$7,'bound','h1','fcpush_aaaaa','["ingest:write", "schema:read"]',NULL,'2027-01-01T00:00:00Z',NULL,'2026-09-20T01:02:03.000004Z','2026-09-10T00:00:00Z'),
			($2,$5,NULL,'org wide','h2','fcpush_bbbbb','["schema:read"]',NULL,NULL,NULL,NULL,'2026-09-11T00:00:00Z'),
			($3,$5,$7,'revoked','h3','fcpush_ccccc','[]',NULL,NULL,'2026-09-12T00:00:00Z',NULL,'2026-09-09T00:00:00Z'),
			($4,$6,$8,'other','h4','fcpush_ddddd','["ingest:status"]',NULL,NULL,NULL,NULL,'2026-09-11T00:00:00Z')`,
			[]any{f.tokenBound, f.tokenOrgWide, f.tokenRevoked, f.tokenOther, team, community, f.sourceGitHub, f.sourceOther}},
		{`INSERT INTO external_ingest_batches (ingestion_id, org_id, idempotency_key, payload_hash, source_system, source_instance,
			producer, producer_version, schema_version, window_started_at, window_ended_at, status, attempts, items_received,
			items_accepted, items_rejected, record_counts, error_summary, created_at, updated_at, completed_at) VALUES
			($1,$5,'k1','p1','github','Acme/API','ci','1.2','external-ingest.v1','2026-09-05T00:00:00Z','2026-09-05T01:00:00.25Z','completed',2,3,2,1,
			 '{"work_item": 2, "pull_request": 1}','{"zeta": ["b"], "alpha": {"n": 1}}','2026-09-06T00:00:00.000001Z','2026-09-06T00:05:00Z','2026-09-06T00:06:00Z'),
			($2,$5,'k2','p2','github','Acme/API',NULL,NULL,'external-ingest.v1',NULL,NULL,'accepted',1,1,0,0,NULL,NULL,'2026-09-04T00:00:00Z','2026-09-04T00:00:00Z',NULL),
			($3,$5,'k3','p3','gitlab','grp/proj','ci',NULL,'external-ingest.v1',NULL,NULL,'failed',1,1,0,1,NULL,'{"code": "x"}','2026-09-07T00:00:00Z','2026-09-07T00:00:00Z',NULL),
			($4,$6,'k4','p4','github','other/repo','ci',NULL,'external-ingest.v1',NULL,NULL,'completed',1,1,1,0,NULL,NULL,'2026-09-06T00:00:00Z','2026-09-06T00:00:00Z',NULL)`,
			[]any{f.batchMain, f.batchOld, f.batchOtherSource, f.batchOtherOrg, team, community}},
		{`INSERT INTO external_ingest_rejections (id, org_id, ingestion_id, record_index, record_kind, external_id, code, message, path) VALUES
			(gen_random_uuid(),$2,$1,4,'work_item','w-4','invalid_field','bad "value"','payload.state'),
			(gen_random_uuid(),$2,$1,0,'pull_request',NULL,'missing_field','required',NULL),
			(gen_random_uuid(),$2,$1,2,'work_item','w-2','unknown_kind','nope','kind')`, []any{f.batchMain, team}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement.sql)
		}
	}
	return f
}

func customerPushRequests(f customerPushFixture, tokens map[string]string) []venueoracle.Request {
	bearer := func(name string, extra ...string) map[string]string {
		out := map[string]string{"Authorization": "Bearer " + tokens[name]}
		for index := 0; index+1 < len(extra); index += 2 {
			out[extra[index]] = extra[index+1]
		}
		return out
	}
	var out []venueoracle.Request
	add := func(name, method, path string, headers map[string]string) {
		out = append(out, venueoracle.Request{Name: name, Method: method, Path: path, Headers: headers})
	}
	p := "/api/v1/admin/customer-push"
	github, other := f.sourceGitHub.String(), f.sourceOther.String()
	// Authorization and the access gate, on every read path.
	for _, path := range []string{"/sources", "/sources/" + github, "/sources/" + github + "/tokens", "/tokens",
		"/sources/" + github + "/batches", "/batches/" + f.batchMain.String(), "/schemas", "/schemas/external-ingest.v1"} {
		add("anon "+path, "GET", p+path, nil)
		add("member "+path, "GET", p+path, bearer("member"))
		add("admin "+path, "GET", p+path, bearer("admin"))
		add("community "+path, "GET", p+path, bearer("community_owner"))
		add("disabled "+path, "GET", p+path, bearer("disabled_admin"))
		add("license wins "+path, "GET", p+path, bearer("license_admin"))
	}
	add("bad org claim", "GET", p+"/sources", bearer("bad_org_claim"))
	add("superuser own org", "GET", p+"/sources", bearer("superuser"))
	add("superuser x-org team", "GET", p+"/sources", bearer("superuser", "X-Org-Id", f.orgTeam.String()))
	add("admin x-org stranger", "GET", p+"/sources", bearer("admin", "X-Org-Id", f.orgCommunity.String()))
	// Source ids as uuid.UUID() reads them.
	for name, id := range map[string]string{
		"upper": strings.ToUpper(github), "braces": "{" + github + "}", "no dashes": strings.ReplaceAll(github, "-", ""),
		"urn": "urn:uuid:" + github, "other org": other, "not uuid": "nope", "short": github[:30],
	} {
		add("source id "+name, "GET", p+"/sources/"+id, bearer("admin"))
		add("tokens source id "+name, "GET", p+"/sources/"+id+"/tokens", bearer("admin"))
	}
	add("source gitlab", "GET", p+"/sources/"+f.sourceGitLab.String(), bearer("admin"))
	add("source custom", "GET", p+"/sources/"+f.sourceCustom.String(), bearer("admin"))
	// Batch filters and query validation.
	batches := p + "/sources/" + github + "/batches"
	for name, query := range map[string]string{
		"status": "?status=completed", "empty status": "?status=", "producer": "?producer=ci", "no producer match": "?producer=zz",
		"from": "?from=2026-09-05T00:00:00Z", "to date only": "?to=2026-09-05", "from unix": "?from=1757030400",
		"page": "?limit=1&offset=1", "repeated limit": "?limit=1&limit=2", "limit float zeros": "?limit=2.00",
		"limit spaced": "?limit=%201%20", "limit 0": "?limit=0", "limit 201": "?limit=201", "limit abc": "?limit=abc",
		"offset -1": "?offset=-1", "from bad": "?from=2026-13-01", "to bad": "?to=x", "all bad": "?from=x&to=y&limit=0&offset=-1",
		"huge limit": "?limit=99999999999999999999999", "huge offset": "?offset=99999999999999999999",
		"offset max int64": "?offset=9223372036854775807",
	} {
		add("batches "+name, "GET", batches+query, bearer("admin"))
	}
	add("batches unknown source", "GET", p+"/sources/"+other+"/batches", bearer("admin"))
	add("batches bad query and no auth", "GET", batches+"?limit=0", nil)
	add("batches bad query and gate refused", "GET", batches+"?limit=0", bearer("community_owner"))
	detail := p + "/batches/"
	for name, path := range map[string]string{
		"main": f.batchMain.String(), "main page": f.batchMain.String() + "?rejected_records_limit=1&rejected_records_offset=1",
		"old": f.batchOld.String(), "other source": f.batchOtherSource.String(), "other org": f.batchOtherOrg.String(),
		"not uuid": "nope", "upper": strings.ToUpper(f.batchMain.String()),
		"limit 0": f.batchMain.String() + "?rejected_records_limit=0", "offset bad": f.batchMain.String() + "?rejected_records_offset=x",
		"offset huge": f.batchMain.String() + "?rejected_records_offset=99999999999999999999",
	} {
		add("batch "+name, "GET", detail+path, bearer("admin"))
	}
	// Schemas.
	for name, version := range map[string]string{"v2": "external-ingest.v2", "quote": "it's", "both quotes": `a'b"c`,
		"escaped": "%E2%80%A8x", "tab": "a%09b"} {
		add("schema "+name, "GET", p+"/schemas/"+version, bearer("admin"))
	}
	// Routing edges.
	add("PUT sources", "PUT", p+"/sources", bearer("admin"))
	add("DELETE source", "DELETE", p+"/sources/"+github, bearer("admin"))
	add("HEAD schemas", "HEAD", p+"/schemas", bearer("admin"))
	add("trailing slash", "GET", p+"/sources/", bearer("admin"))
	return out
}

// TestVenueOracleCustomerPushReads is the customer-push admin read
// routes' differential: the real Python api and dho api answer every
// request byte for byte, on two copies of one seeded database.
func TestVenueOracleCustomerPushReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Setenv("EXTERNAL_INGEST_MAX_RECORDS", " 2_500 ")
	var seed customerPushFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{"EXTERNAL_INGEST_MAX_RECORDS= 2_500 "},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = customerPushSeed(t, ctx, admin)
			return seed.tokenSpecs()
		},
	})
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
	}
	base := startVenueAPI(t, ctx, cfg, venue)
	requests := customerPushRequests(seed, venue.Tokens)
	receipt := venueoracle.Diff(t, base, requests, venue.ServePython(t, requests), venueoracle.DiffOptions{})
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}
