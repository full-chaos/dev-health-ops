//go:build integration

package apiservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// customerPushWriteFixture adds, on top of the read seed, the managed
// integrations the ownership check reads and the sources and tokens the
// writes change.
type customerPushWriteFixture struct {
	customerPushFixture
	sourceClash, sourceDisabledPush, tokenToRevoke uuid.UUID
}

func customerPushWriteSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) customerPushWriteFixture {
	t.Helper()
	f := customerPushWriteFixture{customerPushFixture: customerPushSeed(t, ctx, pool)}
	f.sourceClash, f.sourceDisabledPush, f.tokenToRevoke = uuid.New(), uuid.New(), uuid.New()
	team := f.orgTeam.String()
	var (
		githubLegacy, gitlabInactive, linearActive, gheOperational, gitlabCredential = uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
		credentialConfigured, credentialBare                                         = uuid.New(), uuid.New()
	)
	statements := []struct {
		sql  string
		args []any
	}{
		// Managed integrations: an active GitHub (legacy sources), an
		// inactive GitLab, an active Linear with an org-wide placeholder,
		// an active GitHub whose config names a GHE host, and two GitLab
		// integrations whose host comes from a credential (one credential
		// config names a host, one does not).
		{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at) VALUES
			($1, $3, 'GitLab', 'with-host', true, NULL, '{"gitlab_url": "https://gitlab.acme.test:8443"}', now(), now()),
			($2, $3, 'gitlab', 'bare', true, NULL, '{}', now(), now())`,
			[]any{credentialConfigured, credentialBare, team}},
		{`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at) VALUES
			($1, $8, 'GitHub', NULL, 'gh', '{}', true, now(), now()),
			($2, $8, 'gitlab', NULL, 'gl', '{}', false, now(), now()),
			($3, $8, 'linear', NULL, 'lin', '{}', true, now(), now()),
			($4, $8, 'github', NULL, 'ghe', '{"github_instance_url": "https://GHE.acme.test", "github_url": "https://ignored.test"}', true, now(), now()),
			($5, $8, 'gitlab', $6, 'gl-cred', '{}', true, now(), now()),
			(gen_random_uuid(), $8, 'gitlab', $7, 'gl-bare', '{"gitlab_instance_url": 0, "gitlab_url": "  "}', false, now(), now())`,
			[]any{githubLegacy, gitlabInactive, linearActive, gheOperational, gitlabCredential, credentialConfigured, credentialBare, team}},
		{`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
			is_enabled, discovered_at, last_seen_at) VALUES
			(gen_random_uuid(), $6, $1, 'github', 'repository', 'acme/managed', 'managed', 'Acme/Managed', '{}', true, now(), now()),
			(gen_random_uuid(), $6, $1, 'github', 'repository', '12345', 'dormant', 'Acme/Dormant', '{}', false, now(), now()),
			(gen_random_uuid(), $6, $2, 'gitlab', 'project', '77', 'sub', 'Group/Sub', '{"path_with_namespace": "grp/sub"}', true, now(), now()),
			(gen_random_uuid(), $6, $3, 'Linear', 'team', 'linear', 'all', 'All teams', '{}', true, now(), now()),
			(gen_random_uuid(), $6, $4, 'github', 'repository', 'ghe-1', 'ghe', 'ghe/repo', '{}', true, now(), now()),
			(gen_random_uuid(), $6, $5, 'gitlab', 'project', 'gl-1', 'glc', 'glc/repo', '{}', true, now(), now())`,
			[]any{githubLegacy, gitlabInactive, linearActive, gheOperational, gitlabCredential, team}},
		// A disabled customer-push source that a managed source owns (its
		// enable is refused), a disabled push source nothing owns, and a
		// live token to revoke.
		{`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, display_name, mode, enabled,
			webhook_mode, matched_integration_source_id, created_at, updated_at) VALUES
			($1,$3,'github','ACME/managed','legacy',NULL,'customer_push',false,'disabled',NULL,'2026-08-01T00:00:00Z','2026-08-01T00:00:00Z'),
			($2,$3,'jira','OPS','legacy','Ops','disabled',false,'customer_relay',NULL,'2026-08-02T00:00:00Z','2026-08-02T00:00:00Z')`,
			[]any{f.sourceClash, f.sourceDisabledPush, team}},
		{`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_by_user_id,
			expires_at, revoked_at, last_used_at, created_at) VALUES
			($1,$2,NULL,'to revoke','h5','fcpush_eeeee','["ingest:status"]',NULL,NULL,NULL,NULL,'2026-09-13T00:00:00Z')`,
			[]any{f.tokenToRevoke, team}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement.sql)
		}
	}
	return f
}

func customerPushWriteRequests(f customerPushWriteFixture, tokens map[string]string) []venueoracle.Request {
	// Every request names its peer and agent, so the audit rows' request
	// metadata is the same on both planes (TestClient's peer is
	// "testclient").
	headers := func(name string, extra ...string) map[string]string {
		out := map[string]string{"X-Forwarded-For": " 203.0.113.9 , 10.0.0.1", "User-Agent": "venue/1", "X-Request-ID": "req-1"}
		if name != "" {
			out["Authorization"] = "Bearer " + tokens[name]
		}
		for index := 0; index+1 < len(extra); index += 2 {
			out[extra[index]] = extra[index+1]
		}
		return out
	}
	var out []venueoracle.Request
	add := func(name, method, path, body string, header map[string]string) {
		request := venueoracle.Request{Name: name, Method: method, Path: path, Headers: header}
		if body != "\x00" {
			request.Body = venueoracle.B64(body)
			if request.Headers == nil {
				request.Headers = map[string]string{}
			}
			if _, set := request.Headers["Content-Type"]; !set {
				request.Headers["Content-Type"] = "application/json"
			}
		}
		out = append(out, request)
	}
	const none = "\x00"
	p := "/api/v1/admin/customer-push"
	github := f.sourceGitHub.String()

	// Order of checks: decode, then auth, then body validation, then the
	// access gate.
	add("create: bad json anon", "POST", p+"/sources", `{"system":`, headers(""))
	add("create: anon", "POST", p+"/sources", `{"system":"github","instance":"a/b"}`, headers(""))
	add("create: member", "POST", p+"/sources", `{"system":"github","instance":"a/b"}`, headers("member"))
	add("create: member invalid body", "POST", p+"/sources", `{}`, headers("member"))
	add("create: invalid body", "POST", p+"/sources", `{"instance":" ","entity_family":"x","webhook_mode":"y"}`, headers("admin"))
	add("create: invalid body community", "POST", p+"/sources", `{}`, headers("community_owner"))
	add("create: community", "POST", p+"/sources", `{"system":"github","instance":"a/b"}`, headers("community_owner"))
	add("create: disabled org", "POST", p+"/sources", `{"system":"github","instance":"a/b"}`, headers("disabled_admin"))
	add("create: no body", "POST", p+"/sources", none, headers("admin"))
	add("create: form body", "POST", p+"/sources", `system=github`, headers("admin", "Content-Type", "application/x-www-form-urlencoded"))
	// Business rules.
	add("create: bad system", "POST", p+"/sources", `{"system":" Bitbucket ","instance":"a/b"}`, headers("admin"))
	add("create: bad mode", "POST", p+"/sources", `{"system":"github","instance":"a/b","mode":"Customer_Push"}`, headers("admin"))
	add("create: fullchaos hosted", "POST", p+"/sources", `{"system":"github","instance":"a/b","webhook_mode":"fullchaos_hosted"}`, headers("admin"))
	add("create: case variant", "POST", p+"/sources", `{"system":" GITHUB ","instance":"  acme/api "}`, headers("admin"))
	add("create: owned by managed sync", "POST", p+"/sources", `{"system":"github","instance":"ACME/MANAGED","mode":"disabled"}`, headers("admin"))
	add("create: owned (push)", "POST", p+"/sources", `{"system":"github","instance":"acme/managed","entity_family":"operational"}`, headers("admin"))
	add("create: matched dormant", "POST", p+"/sources", `{"system":"github","instance":"acme/dormant","display_name":"Dormant é"}`, headers("admin"))
	add("create: matched inactive gitlab", "POST", p+"/sources", `{"system":"gitlab","instance":"GRP/SUB","webhook_mode":"customer_relay"}`, headers("admin"))
	add("create: linear placeholder", "POST", p+"/sources", `{"system":"linear","instance":"ENG"}`, headers("admin"))
	add("create: custom", "POST", p+"/sources", `{"system":"custom","instance":"anything"}`, headers("admin"))
	add("create: jira no integration", "POST", p+"/sources", `{"system":"jira","instance":"PROJ","mode":"fullchaos_sync"}`, headers("admin"))
	add("create: operational ghe host", "POST", p+"/sources", `{"system":"github","instance":"https://ghe.ACME.test:443/x","entity_family":"operational"}`, headers("admin"))
	add("create: operational other host", "POST", p+"/sources", `{"system":"github","instance":"other.acme.test","entity_family":"operational"}`, headers("admin"))
	add("create: operational invalid host", "POST", p+"/sources", `{"system":"github","instance":"not a host/x","entity_family":"operational"}`, headers("admin"))
	add("create: operational credential host", "POST", p+"/sources", `{"system":"gitlab","instance":"gitlab.acme.test:8443","entity_family":"operational"}`, headers("admin"))
	add("create: operational credential other", "POST", p+"/sources", `{"system":"gitlab","instance":"gitlab.other.test","entity_family":"operational"}`, headers("admin"))
	add("create: operational gitlab.com", "POST", p+"/sources", `{"system":"gitlab","instance":"gitlab.com","entity_family":"operational"}`, headers("admin"))
	add("create: duplicate of created", "POST", p+"/sources", `{"system":"custom","instance":"ANYTHING"}`, headers("admin"))
	add("create: superuser x-org", "POST", p+"/sources", `{"system":"pagerduty","instance":"P1"}`, headers("superuser", "X-Org-Id", f.orgTeam.String()))
	add("list sources after creates", "GET", p+"/sources", none, headers("admin"))

	// Patch.
	add("patch: anon bad json", "PATCH", p+"/sources/"+github, `[`, headers(""))
	add("patch: invalid body", "PATCH", p+"/sources/"+github, `{"enabled":"maybe","mode":1}`, headers("admin"))
	add("patch: not found", "PATCH", p+"/sources/"+f.sourceOther.String(), `{}`, headers("admin"))
	add("patch: bad id", "PATCH", p+"/sources/nope", `{}`, headers("admin"))
	add("patch: no change", "PATCH", p+"/sources/"+github, `{"mode":"customer_push","enabled":true,"display_name":"Acme API","webhook_mode":"disabled"}`, headers("admin"))
	add("patch: bad mode", "PATCH", p+"/sources/"+github, `{"display_name":"x","mode":"nope"}`, headers("admin"))
	add("patch: fullchaos hosted", "PATCH", p+"/sources/"+github, `{"webhook_mode":"fullchaos_hosted"}`, headers("admin"))
	add("patch: rename", "PATCH", p+"/sources/"+strings.ToUpper(github), `{"display_name":"Acme \"API\" ü","webhook_mode":"customer_relay"}`, headers("admin"))
	add("patch: enable owned", "PATCH", p+"/sources/"+f.sourceClash.String(), `{"enabled":"yes"}`, headers("admin"))
	add("patch: enable unowned push", "PATCH", p+"/sources/"+f.sourceDisabledPush.String(), `{"enabled":1,"mode":"customer_push"}`, headers("admin"))
	add("patch: disable", "PATCH", p+"/sources/"+f.sourceGitLab.String(), `{"enabled":false,"display_name":null}`, headers("admin"))
	add("get patched source", "GET", p+"/sources/"+github, none, headers("admin"))

	// Tokens.
	bound := p + "/sources/" + github + "/tokens"
	add("token: anon", "POST", bound, `{"name":"x","scopes":["schema:read"]}`, headers(""))
	add("token: invalid body", "POST", bound, `{"name":"","scopes":["nope"],"expires_at":"soon"}`, headers("admin"))
	add("token: unknown source", "POST", p+"/sources/"+f.sourceOther.String()+"/tokens", `{"name":"x","scopes":["schema:read"]}`, headers("admin"))
	add("token: bound aware expiry", "POST", bound, `{"name":"ci é","scopes":["ingest:write","schema:read","ingest:write"],"expires_at":"2031-01-01T00:00:00.5+05:30"}`, headers("admin"))
	add("token: bound naive expiry", "POST", bound, `{"name":"naive","scopes":["ingest:status"],"expires_at":"2031-06-01T12:00:00"}`, headers("admin"))
	add("token: bound unix expiry", "POST", bound, `{"name":"unix","scopes":["schema:read"],"expires_at":1950000000}`, headers("admin"))
	add("token: org wide write refused", "POST", p+"/tokens", `{"name":"x","scopes":["schema:read","ingest:write"]}`, headers("admin"))
	add("token: org wide", "POST", p+"/tokens", `{"name":"ow","scopes":["schema:read"]}`, headers("admin"))
	add("token: org wide community", "POST", p+"/tokens", `{"name":"ow","scopes":["schema:read"]}`, headers("community_owner"))
	add("list tokens after creates", "GET", p+"/tokens", none, headers("admin"))
	add("list source tokens after creates", "GET", bound, none, headers("admin"))

	// Rotate and revoke.
	tokenPath := func(id, verb string) string { return p + "/tokens/" + id + "/" + verb }
	add("rotate: anon", "POST", tokenPath(f.tokenBound.String(), "rotate"), none, headers(""))
	add("rotate: member", "POST", tokenPath(f.tokenBound.String(), "rotate"), none, headers("member"))
	add("rotate: community", "POST", tokenPath(f.tokenBound.String(), "rotate"), none, headers("community_owner"))
	add("rotate: bad id", "POST", tokenPath("nope", "rotate"), none, headers("admin"))
	add("rotate: other org", "POST", tokenPath(f.tokenOther.String(), "rotate"), none, headers("admin"))
	add("rotate: revoked", "POST", tokenPath(f.tokenRevoked.String(), "rotate"), none, headers("admin"))
	add("rotate: bound with ttl", "POST", tokenPath(f.tokenBound.String(), "rotate"), `{"ignored":1}`, headers("admin"))
	add("rotate: again", "POST", tokenPath(f.tokenBound.String(), "rotate"), none, headers("admin"))
	add("rotate: org wide no ttl", "POST", tokenPath(strings.ToUpper(f.tokenOrgWide.String()), "rotate"), none, headers("admin"))
	add("revoke: live", "POST", tokenPath(f.tokenToRevoke.String(), "revoke"), none, headers("admin"))
	add("revoke: again", "POST", tokenPath(f.tokenToRevoke.String(), "revoke"), none, headers("admin"))
	add("revoke: other org", "POST", tokenPath(f.tokenOther.String(), "revoke"), none, headers("admin"))
	add("revoke: disabled org", "POST", tokenPath(f.tokenToRevoke.String(), "revoke"), none, headers("disabled_admin"))
	add("list tokens after rotation", "GET", p+"/tokens", none, headers("admin"))
	// Routing.
	add("GET rotate", "GET", tokenPath(f.tokenBound.String(), "rotate"), none, headers("admin"))
	add("PUT source", "PUT", p+"/sources/"+github, `{}`, headers("admin"))
	add("DELETE tokens", "DELETE", p+"/tokens", none, headers("admin"))
	return out
}

var (
	venueUUID      = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	venueToken     = regexp.MustCompile(`fcpush_[A-Za-z0-9_-]{43}`)
	venuePrefix    = regexp.MustCompile(`"token_prefix":"fcpush_[A-Za-z0-9_-]{5}"`)
	venueTimestamp = regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)?`)
)

// customerPushNormalizer blanks what each plane generates: ids it did not
// seed, token plaintexts and their display prefixes, and timestamps taken
// from the clock during the run (the window from start to a year after,
// which rotation's carried TTL falls in; request-supplied expiries are
// outside it).
func customerPushNormalizer(seeded map[string]bool, start time.Time) func(string) string {
	return func(body string) string {
		body = venueToken.ReplaceAllString(body, "<token>")
		body = venuePrefix.ReplaceAllStringFunc(body, func(match string) string {
			if strings.Contains(match, "fcpush_aaaaa") || strings.Contains(match, "fcpush_bbbbb") ||
				strings.Contains(match, "fcpush_ccccc") || strings.Contains(match, "fcpush_ddddd") || strings.Contains(match, "fcpush_eeeee") {
				return match
			}
			return `"token_prefix":"<prefix>"`
		})
		body = venueUUID.ReplaceAllStringFunc(body, func(match string) string {
			if seeded[strings.ToLower(match)] {
				return match
			}
			return "<id>"
		})
		return venueTimestamp.ReplaceAllStringFunc(body, func(match string) string {
			for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999"} {
				if at, err := time.Parse(layout, match); err == nil {
					if !at.Before(start.Add(-time.Minute)) && at.Before(start.Add(400*24*time.Hour)) {
						return "<now>"
					}
					return match
				}
			}
			return match
		})
	}
}

// TestVenueOracleCustomerPushWrites is the customer-push admin write
// routes' differential: both planes answer every request the same, then
// the rows they wrote (sources, tokens, audit) compare equal with generated
// values blanked, and on each plane every minted token hashes to its
// stored hash and prefix.
func TestVenueOracleCustomerPushWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	start := time.Now().UTC()
	var seed customerPushWriteFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = customerPushWriteSeed(t, ctx, admin)
			return seed.tokenSpecs()
		},
	})
	seeded := map[string]bool{}
	admin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.SourceDB))
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{`SELECT id::text FROM external_ingest_sources`, `SELECT id::text FROM external_ingest_tokens`,
		`SELECT id::text FROM organizations`, `SELECT id::text FROM users`, `SELECT id::text FROM integration_sources`} {
		rows, err := admin.Query(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			seeded[id] = true
		}
		rows.Close()
	}
	admin.Close()
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
	}
	base := startVenueAPI(t, ctx, cfg, venue)
	requests := customerPushWriteRequests(seed, venue.Tokens)
	python := venue.ServePython(t, requests)
	normalize := customerPushNormalizer(seeded, start)
	var goMinted []string
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, body string) string { return normalize(body) },
		Inspect: func(_ venueoracle.Request, response venueoracle.Response) {
			goMinted = append(goMinted, response.Body)
		},
	})
	var pythonMinted []string
	for _, response := range python {
		pythonMinted = append(pythonMinted, response.Body)
	}

	// Minted tokens: sha256(token) is the stored hash, token[:12] the
	// stored prefix, on each plane's own database.
	minted := 0
	for _, plane := range []struct {
		name, uri string
		bodies    []string
	}{{"python", venue.AdminURI(t, venue.SourceDB), pythonMinted}, {"go", venue.AdminURI(t, venue.GoDB), goMinted}} {
		for _, body := range plane.bodies {
			var created struct{ ID, Token string }
			if json.Unmarshal([]byte(body), &created) != nil || created.Token == "" {
				continue
			}
			sum := sha256.Sum256([]byte(created.Token))
			row := venueoracle.TableRows(t, ctx, plane.uri, fmt.Sprintf(
				`SELECT token_hash = '%s', token_prefix = '%s' FROM external_ingest_tokens WHERE id = '%s'`,
				hex.EncodeToString(sum[:]), created.Token[:12], created.ID))
			if row != "true true" {
				t.Errorf("%s: token %s does not match its stored hash and prefix: %q", plane.name, created.ID, row)
			}
			minted++
		}
	}
	if minted == 0 {
		t.Fatal("no minted token was checked")
	}

	// Stored rows, generated values blanked.
	tables := map[string]string{
		"sources": `SELECT id::text, org_id, system, instance, entity_family, display_name, mode, enabled, webhook_mode,
			webhook_secret_id::text, matched_integration_source_id IS NOT NULL, created_by_user_id::text,
			created_at, updated_at FROM external_ingest_sources ORDER BY org_id, system, instance, entity_family`,
		"tokens": `SELECT id::text, org_id, source_id::text, name, length(token_hash), token_prefix, scopes::text,
			created_by_user_id::text, expires_at, revoked_at, last_used_at, last_used_ip, created_at
			FROM external_ingest_tokens ORDER BY name, created_at, expires_at NULLS FIRST`,
		"audit": `SELECT org_id::text, user_id::text, action, resource_type, resource_id, description, changes::text,
			request_metadata::text, status, error_message, created_at FROM audit_logs
			WHERE action LIKE 'ingest_%' ORDER BY created_at, action`,
	}
	for name, query := range tables {
		pythonRows := normalizeRows(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query), seeded, start)
		goRows := normalizeRows(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query), seeded, start)
		mark := venueoracle.Mark(pythonRows == goRows)
		receipt += fmt.Sprintf("%s rows after writes: %s\n", name, mark)
		if pythonRows != goRows {
			t.Errorf("%s rows differ:\n python %s\n go     %s", name, pythonRows, goRows)
		}
	}
	receipt += fmt.Sprintf("minted tokens checked against stored hash and prefix: %d\n", minted)
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}

var venueGoTime = regexp.MustCompile(`\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)? \+0000 UTC`)

// normalizeRows blanks generated ids and clock times in TableRows output
// (Go's time rendering).
func normalizeRows(rows string, seeded map[string]bool, start time.Time) string {
	rows = venueUUID.ReplaceAllStringFunc(rows, func(match string) string {
		if seeded[strings.ToLower(match)] {
			return match
		}
		return "<id>"
	})
	rows = venuePrefix.ReplaceAllString(rows, "<prefix>")
	rows = regexp.MustCompile(`fcpush_[A-Za-z0-9_-]{5}\b`).ReplaceAllStringFunc(rows, func(match string) string {
		switch match {
		case "fcpush_aaaaa", "fcpush_bbbbb", "fcpush_ccccc", "fcpush_ddddd", "fcpush_eeeee":
			return match
		}
		return "<prefix>"
	})
	return venueGoTime.ReplaceAllStringFunc(rows, func(match string) string {
		at, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", match)
		if err == nil && !at.Before(start.Add(-time.Minute)) && at.Before(start.Add(400*24*time.Hour)) {
			return "<now>"
		}
		return match
	})
}
