//go:build integration

package admin_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const pagerDutyBindingsVenueEncryptionKey = "venue-pd-bindings-fernet-key-32-bytes!"

// TestPagerDutyWebhookBindingsVenueOracle is the venue-oracle proof for the
// five webhook-binding routes: the real Python api and the real Go api answer
// the same requests against two copies of one database, and the rows they
// leave (and every secret they store, opened with Python's decrypt) match.
func TestPagerDutyWebhookBindingsVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-pagerduty-bindings-32-b"

	ids := map[string]uuid.UUID{}
	id := func(name string) uuid.UUID {
		if v, ok := ids[name]; ok {
			return v
		}
		ids[name] = uuid.New()
		return ids[name]
	}
	orgs := map[string]uuid.UUID{}
	for _, slug := range []string{"main", "other", "off"} {
		orgs[slug] = uuid.New()
	}
	adminID, memberID, superID := uuid.New(), uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		JWTKey:    jwtKey,
		PythonEnv: []string{"SETTINGS_ENCRYPTION_KEY=" + pagerDutyBindingsVenueEncryptionKey},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for slug, org := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, org, "pd-bind-"+slug)
			}
			user := func(uid uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, uid, email, super)
			}
			user(adminID, "pd-bind-admin@example.com", false)
			user(memberID, "pd-bind-member@example.com", false)
			user(superID, "pd-bind-super@example.com", true)
			exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, (SELECT id FROM feature_flags WHERE key = 'canonical_incident_ingestion'), false, NULL, NULL, 'kill switch', NULL, now(), now())`,
				uuid.New(), orgs["off"])

			credential := func(name, org string, active bool) {
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, $4, NULL, '{}'::json, now(), now())`, id(name), orgs[org].String(), name, active)
			}
			integration := func(name, org, provider string, credentialName string, active bool) {
				var cred any
				if credentialName != "" {
					cred = id(credentialName)
				}
				exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, credential_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, '{}'::json, $5, $6, now(), now())`, id(name), orgs[org].String(), provider, name, active, cred)
			}
			source := func(name, org, integrationName, provider string, enabled bool) {
				exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, $4, 'service', $5, $5, $5, '{}'::json, $6, now(), now())`, id(name), orgs[org].String(), id(integrationName), provider, name, enabled)
			}
			credential("C1", "main", true)
			credential("C2-inactive", "main", false)
			credential("C3-unlinked", "main", true)
			credential("C-other", "other", true)
			integration("I1", "main", "pagerduty", "C1", true)
			integration("I2", "main", "pagerduty", "C2-inactive", true)
			integration("I-inactive", "main", "pagerduty", "C1", false)
			integration("I-nocred", "main", "pagerduty", "", true)
			integration("I-github", "main", "github", "C1", true)
			integration("I-other", "other", "pagerduty", "C-other", true)
			for _, s := range []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S13", "S14", "S15", "S16", "S17", "S18", "S19", "S20"} {
				source(s, "main", "I1", "pagerduty", true)
			}
			source("S-disabled", "main", "I1", "pagerduty", false)
			source("S-inactive-int", "main", "I-inactive", "pagerduty", true)
			source("S-nocred-int", "main", "I-nocred", "pagerduty", true)
			source("S-badcred", "main", "I2", "pagerduty", true)
			source("S-github-src", "main", "I1", "github", true)
			source("S-github-int", "main", "I-github", "pagerduty", true)
			source("S-other", "other", "I-other", "pagerduty", true)

			binding := func(name, org, sourceName, credentialName, status, subscription string, revoked bool) {
				var cred any
				if credentialName != "" {
					cred = id(credentialName)
				}
				var revokedAt any
				if revoked {
					revokedAt = "2026-01-02T03:04:05+00:00"
				}
				exec(`INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, credential_id, provider_subscription_id, signing_secret_encrypted, signing_secret_key_version, status, created_at, updated_at, revoked_at)
VALUES ($1, $2, $3, $4, $5, 'v1:seed-not-decrypted', 'v1', $6, now(), now(), $7::timestamptz)`, id(name), orgs[org], id(sourceName), cred, subscription, status, revokedAt)
			}
			binding("B-active-S2", "main", "S2", "C1", "active", "sub-a", false)
			binding("B-active-S3", "main", "S3", "C1", "active", "sub-s3", false)
			binding("B-cand-S3", "main", "S3", "C1", "candidate", "sub-s3-next", false)
			binding("B-active-S4", "main", "S4", "C1", "active", "sub-s4", false)
			binding("B-ready-S4", "main", "S4", "C1", "ready", "sub-s4-next", false)
			binding("B-ready-S5", "main", "S5", "C1", "ready", "sub-s5", false)
			binding("B-cand-S6", "main", "S6", "C1", "candidate", "sub-s6", false)
			binding("B-inactive-S7", "main", "S7", "C1", "inactive", "sub-s7", true)
			binding("B-active-S8", "main", "S8", "C1", "active", "sub-s8", false)
			binding("B-ready-S13", "main", "S13", "C1", "ready", "sub-a", false)
			binding("B-ready-S14", "main", "S14", "", "ready", "sub-s14", false)
			binding("B-ready-S15", "main", "S15", "C1", "ready", "sub-s15", false)
			binding("B-active-S16", "main", "S16", "C1", "active", "sub-s16", false)
			binding("B-other", "other", "S-other", "C-other", "active", "sub-other", false)
			// A ready row that already carries a revoked_at (a revoke must keep
			// it), and an active row whose organisation is not its source's
			// (a rotation from the source's organisation must be refused on
			// the organisation alone).
			binding("B-ready-revoked-S17", "main", "S17", "C1", "ready", "sub-s17", true)
			binding("B-crossorg-S18", "other", "S18", "C1", "active", "sub-s18", false)
			// Activation swaps the source's active row whatever its
			// organisation (Python filters the lock by source only): the two
			// planes must do the same on data no route can create.
			binding("B-crossorg-S19", "other", "S19", "C1", "active", "sub-s19", false)
			binding("B-ready-S19", "main", "S19", "C1", "ready", "sub-s19-next", false)
			// The concurrency section's source: one active binding, no candidate.
			binding("B-active-S20", "main", "S20", "C1", "active", "sub-s20", false)

			admin_ := func(slug string) map[string]any {
				return map[string]any{"user_id": adminID.String(), "email": "pd-bind-admin@example.com", "org_id": orgs[slug].String(), "role": "admin"}
			}
			return map[string]map[string]any{
				"admin": admin_("main"), "admin_off": admin_("off"), "admin_other": admin_("other"),
				"admin_badorg": {"user_id": adminID.String(), "email": "pd-bind-admin@example.com", "org_id": "not-a-uuid", "role": "admin"},
				"member":       {"user_id": memberID.String(), "email": "pd-bind-member@example.com", "org_id": orgs["main"].String(), "role": "member"},
				"super":        {"user_id": superID.String(), "email": "pd-bind-super@example.com", "is_superuser": true},
			}
		},
	})

	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	plainAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	const prefix = "/api/v1/admin/integrations/pagerduty/webhook-bindings"
	body := func(source, credential, subscription, secret string) string {
		return fmt.Sprintf(`{"integration_source_id":"%s","credential_id":"%s","provider_subscription_id":"%s","signing_secret":"%s"}`, source, credential, subscription, secret)
	}
	create := func(name, token, payload string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: prefix, Headers: jsonAuth(token), Body: venueoracle.B64(payload)}
	}
	rotate := func(name, token, binding, payload string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: prefix + "/" + binding + "/rotate", Headers: jsonAuth(token), Body: venueoracle.B64(payload)}
	}
	act := func(name, verb, token, binding string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: prefix + "/" + binding + "/" + verb, Headers: plainAuth(token)}
	}
	get := func(name, token, binding string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: prefix + "/" + binding, Headers: plainAuth(token)}
	}
	S := func(n string) string { return id(n).String() }

	requests := []venueoracle.Request{
		// ---- create -------------------------------------------------------------
		create("new: create", "admin", body(S("S1"), S("C1"), "sub-new-1", "secret-one")),
		create("new: create again for the same source (two candidates)", "admin", body(S("S1"), S("C1"), "sub-new-2", "secret-two")),
		create("create subscription id blank", "admin", body(S("S1"), S("C1"), "   ", "s")),
		create("create source unknown", "admin", body(uuid.NewString(), S("C1"), "sub", "s")),
		create("create credential unknown", "admin", body(S("S1"), uuid.NewString(), "sub", "s")),
		create("create credential not linked to the integration", "admin", body(S("S1"), S("C3-unlinked"), "sub", "s")),
		create("create credential inactive", "admin", body(S("S-badcred"), S("C2-inactive"), "sub", "s")),
		create("create source disabled", "admin", body(S("S-disabled"), S("C1"), "sub", "s")),
		create("create integration inactive", "admin", body(S("S-inactive-int"), S("C1"), "sub", "s")),
		create("create integration without a credential", "admin", body(S("S-nocred-int"), S("C1"), "sub", "s")),
		create("create source of another provider", "admin", body(S("S-github-src"), S("C1"), "sub", "s")),
		create("create integration of another provider", "admin", body(S("S-github-int"), S("C1"), "sub", "s")),
		create("create source of another org", "admin", body(S("S-other"), S("C-other"), "sub", "s")),
		create("create feature off refused", "admin_off", body(S("S1"), S("C1"), "sub", "s")),
		create("create invalid body wins over the gate", "admin_off", `{}`),
		create("create bad org claim is the gate's 403", "admin_badorg", body(S("S1"), S("C1"), "sub", "s")),
		create("create fields missing", "admin", `{}`),
		create("create fields empty", "admin", `{"integration_source_id":"","credential_id":"","provider_subscription_id":"","signing_secret":""}`),
		create("create uuids malformed", "admin", `{"integration_source_id":"nope","credential_id":"1234","provider_subscription_id":"s","signing_secret":"s"}`),
		create("new: create uuid forms", "admin", body("{"+S("S1")+"}", "urn:uuid:"+S("C1"), "sub-form", "s")),
		create("create types wrong", "admin", `{"integration_source_id":5,"credential_id":null,"provider_subscription_id":[],"signing_secret":{}}`),
		create("create extra fields", "admin", body(S("S1"), S("C1"), "s", "s")[:len(body(S("S1"), S("C1"), "s", "s"))-1]+`,"z":1,"a":2}`),
		create("create body list", "admin", `[]`),
		create("create member refused", "member", body(S("S1"), S("C1"), "s", "s")),
		create("create superuser without org", "super", body(S("S1"), S("C1"), "s", "s")),
		{Name: "create unauthenticated", Method: "POST", Path: prefix, Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(body(S("S1"), S("C1"), "s", "s"))},
		{Name: "collection get is 405", Method: "GET", Path: prefix, Headers: plainAuth("admin")},
		// ---- rotate ---------------------------------------------------------------
		rotate("new: rotate an active binding", "admin", S("B-active-S2"), body(S("S2"), S("C1"), "sub-rot", "rot-secret")),
		rotate("rotate again: a candidate now exists", "admin", S("B-active-S2"), body(S("S2"), S("C1"), "sub-rot2", "s")),
		rotate("rotate: candidate already exists", "admin", S("B-active-S3"), body(S("S3"), S("C1"), "sub-x", "s")),
		rotate("rotate: ready candidate already exists", "admin", S("B-active-S4"), body(S("S4"), S("C1"), "sub-x", "s")),
		rotate("rotate: binding is not active", "admin", S("B-inactive-S7"), body(S("S7"), S("C1"), "sub-x", "s")),
		rotate("rotate: unknown binding", "admin", uuid.NewString(), body(S("S1"), S("C1"), "sub-x", "s")),
		rotate("rotate: another source than the binding's", "admin", S("B-active-S16"), body(S("S1"), S("C1"), "sub-x", "s")),
		rotate("rotate: another organisation's active binding", "admin", S("B-other"), body(S("S1"), S("C1"), "sub-x", "s")),
		rotate("rotate: active binding of another organisation on this source", "admin", S("B-crossorg-S18"), body(S("S18"), S("C1"), "sub-x", "s")),
		rotate("rotate: blank subscription id", "admin", S("B-active-S16"), body(S("S16"), S("C1"), " ", "s")),
		rotate("rotate: graph failure precedes the lookup", "admin", uuid.NewString(), body(S("S-disabled"), S("C1"), "s", "s")),
		rotate("rotate: path id malformed and body invalid", "admin", "nope", `{}`),
		rotate("rotate: path id malformed", "admin", "nope", body(S("S1"), S("C1"), "s", "s")),
		rotate("rotate: feature off refused", "admin_off", S("B-active-S16"), body(S("S16"), S("C1"), "s", "s")),
		rotate("rotate: member refused", "member", S("B-active-S16"), body(S("S16"), S("C1"), "s", "s")),
		// ---- activate ---------------------------------------------------------------
		act("activate a ready candidate over an active binding", "activate", "admin", S("B-ready-S4")),
		act("activate again: it is active now", "activate", "admin", S("B-ready-S4")),
		act("activate a ready candidate with no active binding", "activate", "admin", S("B-ready-S5")),
		act("activate a candidate that is not ready", "activate", "admin", S("B-cand-S6")),
		act("activate an inactive binding", "activate", "admin", S("B-inactive-S7")),
		act("activate another organisation's binding", "activate", "admin", S("B-other")),
		act("activate unknown binding", "activate", "admin", uuid.NewString()),
		act("activate: subscription id already active elsewhere", "activate", "admin", S("B-ready-S13")),
		act("activate: candidate without a credential", "activate", "admin", S("B-ready-S14")),
		act("activate: path id malformed", "activate", "admin", "nope"),
		act("activate: feature off refused", "activate", "admin_off", S("B-ready-S15")),
		act("activate: member refused", "activate", "member", S("B-ready-S15")),
		act("activate: superuser without org", "activate", "super", S("B-ready-S15")),
		{Name: "activate get is 405", Method: "GET", Path: prefix + "/" + S("B-ready-S15") + "/activate", Headers: plainAuth("admin")},
		// ---- revoke (no feature gate) ----------------------------------------------------
		act("revoke an active binding", "revoke", "admin", S("B-active-S8")),
		act("revoke again: it is inactive now", "revoke", "admin", S("B-active-S8")),
		act("revoke a candidate", "revoke", "admin", S("B-cand-S6")),
		act("revoke an inactive binding", "revoke", "admin", S("B-inactive-S7")),
		act("revoke another organisation's binding", "revoke", "admin", S("B-other")),
		act("revoke keeps an earlier revoked_at", "revoke", "admin", S("B-ready-revoked-S17")),
		act("revoke unknown binding", "revoke", "admin", uuid.NewString()),
		act("revoke is not behind the feature flag", "revoke", "admin_off", S("B-ready-S15")),
		act("revoke: bad org claim is a 400", "revoke", "admin_badorg", S("B-active-S16")),
		act("revoke: path id malformed", "revoke", "admin", "nope"),
		act("revoke: member refused", "revoke", "member", S("B-active-S16")),
		// ---- get ----------------------------------------------------------------------------
		get("get an active binding", "admin", S("B-active-S16")),
		get("get an inactive binding", "admin", S("B-inactive-S7")),
		get("get a binding without a credential", "admin", S("B-ready-S14")),
		get("get another organisation's binding", "admin", S("B-other")),
		get("get the other organisation's binding as its admin", "admin_other", S("B-other")),
		get("get unknown binding", "admin", uuid.NewString()),
		get("get uppercase path id", "admin", strings.ToUpper(S("B-active-S16"))),
		get("get path id without hyphens", "admin", strings.ReplaceAll(S("B-active-S16"), "-", "")),
		get("get path id malformed", "admin", "nope"),
		get("get bad org claim is a 400", "admin_badorg", S("B-active-S16")),
		get("get member refused", "member", S("B-active-S16")),
		get("get superuser without org", "super", S("B-active-S16")),
		{Name: "get unauthenticated", Method: "GET", Path: prefix + "/" + S("B-active-S16")},
		{Name: "binding post is 405", Method: "POST", Path: prefix + "/" + S("B-active-S16"), Headers: plainAuth("admin")},
	}

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(pagerDutyBindingsVenueEncryptionKey), "")
	if err != nil {
		t.Fatalf("build decryptor: %v", err)
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) { deps.Decryptor = decryptor })
	// The rows a create or rotate inserts carry a random id and now()
	// timestamps: blank the ids of the "new:" answers and every well-formed
	// timestamp (a malformed one -- "+00:00", a missing "Z" -- stays visible).
	timestamp := regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d{1,6})?Z`)
	newID := regexp.MustCompile(`"id":"[0-9a-f-]{36}"`)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, text string) string {
			text = timestamp.ReplaceAllString(text, "<ts>")
			if strings.HasPrefix(request.Name, "new:") {
				text = newID.ReplaceAllString(text, `"id":"<id>"`)
			}
			return text
		},
	})
	t.Log(receipt)

	compare := func(name, query string) {
		t.Helper()
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		if source != goRows {
			t.Errorf("%s differs after the writes:\n python: %s\n go:     %s", name, source, goRows)
		}
	}
	// Seeded rows keep their ids; a created row is told apart by its
	// subscription id. Timestamps are compared as relations, not values.
	compare("pagerduty_webhook_bindings rows", `SELECT org_id, integration_source_id, provider_subscription_id, status, signing_secret_key_version,
	(credential_id IS NULL)::text, (rotated_at IS NULL)::text, (revoked_at IS NULL)::text,
	COALESCE((revoked_at = updated_at)::text, '-'), COALESCE((rotated_at = revoked_at)::text, '-'), (updated_at >= created_at)::text
FROM pagerduty_webhook_bindings ORDER BY org_id, integration_source_id, provider_subscription_id, status`)

	// A stored secret is opened with Python's own decrypt.
	opened := func(dbName string) string {
		t.Helper()
		pool, err := pgxpool.New(ctx, venue.AdminURI(t, dbName))
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		rows, err := pool.Query(ctx, `SELECT provider_subscription_id, signing_secret_encrypted FROM pagerduty_webhook_bindings WHERE signing_secret_encrypted <> 'v1:seed-not-decrypted' ORDER BY provider_subscription_id`)
		if err != nil {
			t.Fatal(err)
		}
		type item struct{ subscription, ciphertext string }
		var items []item
		for rows.Next() {
			var i item
			if err := rows.Scan(&i.subscription, &i.ciphertext); err != nil {
				t.Fatal(err)
			}
			items = append(items, i)
		}
		rows.Close()
		calls := make([]venueoracle.PythonCall, len(items))
		for i, it := range items {
			calls[i] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{it.ciphertext}}
		}
		results := venue.CallPython(t, calls...)
		var lines []string
		for i, it := range items {
			var plain string
			if err := json.Unmarshal(results[i], &plain); err != nil {
				t.Fatalf("decode decrypt_value: %v", err)
			}
			lines = append(lines, it.subscription+" => "+plain)
		}
		return strings.Join(lines, "\n")
	}
	pythonSecrets, goSecrets := opened(venue.SourceDB), opened(venue.GoDB)
	if pythonSecrets != goSecrets {
		t.Errorf("stored signing secrets differ:\n python:\n%s\n go:\n%s", pythonSecrets, goSecrets)
	}
	if strings.Count(pythonSecrets, "\n") < 2 {
		t.Errorf("only %q secrets were stored by the Python plane", pythonSecrets)
	}

	// ---- two named divergences from Python (Go-first fixes) -----------------------
	// Sent after the parity run and its row comparisons, so the two planes'
	// tables were still equal when compared.
	goSend := func(request venueoracle.Request) (int, string) {
		t.Helper()
		var reader io.Reader
		if request.Body != nil {
			decoded, err := base64.StdEncoding.DecodeString(*request.Body)
			if err != nil {
				t.Fatal(err)
			}
			reader = strings.NewReader(string(decoded))
		}
		httpRequest, err := http.NewRequestWithContext(ctx, request.Method, goBase+request.Path, reader)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range request.Headers {
			httpRequest.Header.Set(key, value)
		}
		response, err := http.DefaultClient.Do(httpRequest)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(body)
	}
	statusOf := func(dbName, binding string) string {
		t.Helper()
		return venueoracle.TableRows(t, ctx, venue.AdminURI(t, dbName), `SELECT status FROM pagerduty_webhook_bindings WHERE id = '`+binding+`'`)
	}

	// (a) activation swaps only the caller's organisation's rows. Python
	// deactivates the source's active row whatever its organisation; Go
	// leaves another organisation's row alone. (No route can create such a
	// row: a source belongs to one organisation.)
	foreign := act("activate with a foreign-organisation active row on the source", "activate", "admin", S("B-ready-S19"))
	pythonForeign := venue.ServePython(t, []venueoracle.Request{foreign})
	if pythonForeign[0].Status != 200 {
		t.Fatalf("python activate: status %d", pythonForeign[0].Status)
	}
	if got := statusOf(venue.SourceDB, S("B-crossorg-S19")); got != "inactive" {
		t.Errorf("python plane left the foreign row %q; the named divergence expects it to swap it (inactive)", got)
	}
	if status, _ := goSend(foreign); status != 200 {
		t.Fatalf("go activate: status %d", status)
	}
	if got := statusOf(venue.GoDB, S("B-crossorg-S19")); got != "active" {
		t.Errorf("go plane changed another organisation's binding to %q; it must leave it active", got)
	}
	if got := statusOf(venue.GoDB, S("B-ready-S19")); got != "active" {
		t.Errorf("go plane did not activate the caller's ready candidate: %q", got)
	}

	// (b) concurrent rotations of one source create one candidate, not one
	// each. The test holds the active row's lock so every rotation blocks at
	// the point where the unfixed code read the source's rows; when the lock is
	// released, only the first may insert.
	goPool, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	defer goPool.Close()
	holder, err := goPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT 1 FROM pagerduty_webhook_bindings WHERE id = $1 FOR UPDATE`, id("B-active-S20")); err != nil {
		t.Fatal(err)
	}
	const racers = 6
	type answer struct {
		status int
		body   string
	}
	answers := make([]answer, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status, text := goSend(rotate("race", "admin", S("B-active-S20"), body(S("S20"), S("C1"), fmt.Sprintf("sub-race-%d", i), "race-secret")))
			answers[i] = answer{status, text}
		}(i)
	}
	time.Sleep(4 * time.Second) // every racer is blocked behind the held row lock
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	accepted := 0
	for _, a := range answers {
		switch a.status {
		case 200:
			accepted++
		case 400:
			if !strings.Contains(a.body, "a rotation candidate already exists for this source") {
				t.Errorf("unexpected 400 body %q", a.body)
			}
		default:
			t.Errorf("unexpected answer %d %s", a.status, a.body)
		}
	}
	candidates := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB),
		`SELECT count(*) FROM pagerduty_webhook_bindings WHERE integration_source_id = '`+S("S20")+`' AND status IN ('candidate', 'ready')`)
	if accepted != 1 || candidates != "1" {
		t.Errorf("%d concurrent rotations of one source: %d accepted, %s candidate rows; want exactly 1 and 1", racers, accepted, candidates)
	}
}
