//go:build integration

package ownershipvenue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// encryptionKey is SETTINGS_ENCRYPTION_KEY on both planes.
const encryptionKey = "venue-ownership-settings-encryption-key"

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

// payload is what a scenario's credential stores: none (NULL), raw ciphertext
// text (a payload that cannot decrypt), or a value the Python plane encrypts
// with encrypt_value.
type payload struct {
	kind string // "none", "raw" or "json"
	text string
}

var (
	noPayload = payload{kind: "none"}
	garbled   = payload{kind: "raw", text: "gAAAAABnot-a-fernet-token"}
)

func encrypted(text string) payload { return payload{kind: "json", text: text} }

// scenario is one managed integration the ownership check reads: its system,
// the credential it links (provider, config and payload), the integration's
// own config, and whether its source is enabled and its integration active.
type scenario struct {
	name         string
	system       string
	credProvider string // "" = the integration has no credential
	credConfig   string
	cred         payload
	intConfig    string
	sourceOn     bool
	intActive    bool
	// host is the host the credential would resolve to ("" = none): the
	// instance that matches when the credential is read.
	host string
}

func scenarios() []scenario {
	gh := func(name, config string, p payload, host string) scenario {
		return scenario{name: name, system: "github", credProvider: "github", credConfig: config, cred: p,
			intConfig: "{}", sourceOn: true, intActive: true, host: host}
	}
	list := []scenario{
		gh("decrypted url", "{}", encrypted(`{"url":"https://ghe-url.acme.test"}`), "ghe-url.acme.test"),
		gh("decrypted github_url outranks url and the config", `{"url":"https://cfg-loses.acme.test"}`,
			encrypted(`{"github_url":"https://ghe-prov.acme.test","url":"https://ignored.acme.test"}`), "ghe-prov.acme.test"),
		gh("decrypted base_url", "{}", encrypted(`{"base_url":"https://ghe-base.acme.test"}`), "ghe-base.acme.test"),
		gh("garbled payload, config host", `{"base_url":"https://cfg-host.acme.test"}`, garbled, "cfg-host.acme.test"),
		gh("garbled payload, no host", "{}", garbled, ""),
		gh("not json payload, no host", "{}", encrypted("not json {"), ""),
		gh("list payload ignores the config host", `{"url":"https://cfg-list.acme.test"}`, encrypted(`["x"]`), ""),
		gh("string payload ignores the config host", `{"url":"https://cfg-str.acme.test"}`, encrypted(`"just a string"`), ""),
		gh("number payload", "{}", encrypted(`5`), ""),
		gh("null payload uses the config host", `{"url":"https://cfg-null.acme.test"}`, encrypted(`null`), "cfg-null.acme.test"),
		gh("empty object payload, no host", "{}", encrypted(`{}`), ""),
		gh("blank url in the payload", "{}", encrypted(`{"url":"   "}`), ""),
		gh("invalid host in the payload", "{}", encrypted(`{"url":"not a host"}`), ""),
		gh("invalid payload host, valid config host", `{"url":"https://cfg-valid.acme.test"}`, encrypted(`{"url":"not a host"}`), "cfg-valid.acme.test"),
		gh("non-string url in the payload", "{}", encrypted(`{"url":5}`), ""),
		gh("no payload, config host", `{"github_url":"https://cfg-nopayload.acme.test"}`, noPayload, "cfg-nopayload.acme.test"),
		gh("no payload, no host", "{}", noPayload, ""),
		{name: "credential of another provider", system: "github", credProvider: "gitlab", credConfig: "{}",
			cred: encrypted(`{"url":"https://ghe-other.acme.test"}`), intConfig: "{}", sourceOn: true, intActive: true},
		{name: "gitlab decrypted gitlab_url", system: "gitlab", credProvider: "gitlab", credConfig: "{}",
			cred: encrypted(`{"gitlab_url":"https://gl-prov.acme.test"}`), intConfig: "{}", sourceOn: true, intActive: true, host: "gl-prov.acme.test"},
		{name: "gitlab garbled payload", system: "gitlab", credProvider: "gitlab", credConfig: "{}", cred: garbled,
			intConfig: "{}", sourceOn: true, intActive: true},
		{name: "integration config names the host", system: "github", credProvider: "github", credConfig: "{}", cred: garbled,
			intConfig: `{"github_instance_url":"https://cfg-int.acme.test"}`, sourceOn: true, intActive: true, host: "cfg-int.acme.test"},
	}
	// A credential config that is truthy and not a JSON object raises in
	// Python's `.get` (500) once the host is read, whatever the payload holds;
	// a falsy one is `{}`.
	list = append(list,
		gh("config array, no payload", `["bad"]`, noPayload, ""),
		gh("config array with a decrypted host", `["bad"]`, encrypted(`{"url":"https://ghe-cfgarr.acme.test"}`), "ghe-cfgarr.acme.test"),
		gh("config string, garbled payload", `"just text"`, garbled, ""),
		gh("config number, garbled payload", `5`, garbled, ""),
		gh("config true, garbled payload", `true`, garbled, ""),
		gh("config empty array, garbled payload", `[]`, garbled, ""),
		gh("config empty string, garbled payload", `""`, garbled, ""),
		gh("config zero, garbled payload", `0`, garbled, ""),
		gh("config false, garbled payload", `false`, garbled, ""),
		gh("config null, garbled payload", `null`, garbled, ""),
		gh("config array with another provider's credential", `["bad"]`, encrypted(`{"url":"https://x.acme.test"}`), ""),
	)
	list[len(list)-1].credProvider = "gitlab"
	// An unreadable credential only raises for an enabled source under an
	// active integration.
	unreadable := gh("garbled payload, source disabled", "{}", garbled, "")
	unreadable.sourceOn = false
	inactive := gh("garbled payload, integration inactive", "{}", garbled, "")
	inactive.intActive = false
	list = append(list, unreadable, inactive)
	return list
}

// hosts are the instances a scenario is asked about: the host its credential
// resolves to, another host and the default host.
func (s scenario) hosts() []string {
	def := "github.com"
	if s.system == "gitlab" {
		def = "gitlab.com"
	}
	out := []string{"other.acme.test", def}
	if s.host != "" {
		out = append([]string{s.host}, out...)
	}
	return out
}

func tokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

type ids struct {
	pushOrg, batchOrg, pushAdmin uuid.UUID
	cred, integration, source    uuid.UUID
}

var volatile = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?`)

// normalize blanks the ids and timestamps a created row carries: they are
// generated on each plane.
func normalize(_ venueoracle.Request, body string) string {
	return volatile.ReplaceAllString(body, "<volatile>")
}

// TestOwnershipVenueOracle asks POST /api/v1/admin/customer-push/sources and
// POST /api/v1/external-ingest/batches, for operational github and gitlab
// sources, whether a managed integration owns an instance: one organization
// per credential state (an organization's managed sources are all compared, so
// one unreadable credential decides every request in it), each asked about the
// host its credential resolves to, another host and the default host.
func TestOwnershipVenueOracle(t *testing.T) { runOwnership(t, scenarios(), true) }

// TestOwnershipVenueNoKeyOracle runs a credential whose stored payload cannot
// be decrypted because neither api holds SETTINGS_ENCRYPTION_KEY: Python's
// decrypt_value raises RuntimeError, unhandled, before it looks at the token,
// so both operations answer the generic 500 (a batch accept's error envelope
// included).
func TestOwnershipVenueNoKeyOracle(t *testing.T) {
	garbledGitHub := scenario{name: "no key, stored payload", system: "github", credProvider: "github", credConfig: "{}",
		cred: garbled, intConfig: "{}", sourceOn: true, intActive: true}
	noPayloadGitHub := scenario{name: "no key, no stored payload", system: "github", credProvider: "github",
		credConfig: `{"url":"https://cfg-nokey.acme.test"}`, cred: noPayload, intConfig: "{}", sourceOn: true, intActive: true, host: "cfg-nokey.acme.test"}
	runOwnership(t, []scenario{garbledGitHub, noPayloadGitHub}, false)
}

func runOwnership(t *testing.T, cases []scenario, withKey bool) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-ownership-32-bytes!"
	all := make([]ids, len(cases))
	for i := range cases {
		all[i] = ids{pushOrg: uuid.New(), batchOrg: uuid.New(), pushAdmin: uuid.New(),
			cred: uuid.New(), integration: uuid.New(), source: uuid.New()}
	}
	type batchSource struct{ token, host string }
	batchTokens := make([][]batchSource, len(cases))

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: jwtKey,
		PythonEnv: pythonEnv(withKey),
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			var calls []venueoracle.PythonCall
			var callFor []int
			for i, c := range cases {
				if c.cred.kind == "json" {
					calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{c.cred.text}})
					callFor = append(callFor, i)
				}
			}
			ciphertexts := map[int]string{}
			if len(calls) > 0 {
				for k, raw := range v.CallPython(t, calls...) {
					var text string
					if err := json.Unmarshal(raw, &text); err != nil {
						t.Fatal(err)
					}
					ciphertexts[callFor[k]] = text
				}
			}
			tokens := map[string]map[string]any{}
			for i, c := range cases {
				id := all[i]
				for _, org := range []uuid.UUID{id.pushOrg, id.batchOrg} {
					exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'enterprise', 'stripe', true, now(), now())`, org, "own-"+org.String()[:8])
					// The credential, integration and source of the scenario.
					var credential any
					if c.credProvider != "" {
						credentialID := uuid.New()
						var cipher any
						switch c.cred.kind {
						case "raw":
							cipher = c.cred.text
						case "json":
							cipher = ciphertexts[i]
						}
						exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, 'cred', true, $4, $5::json, now(), now())`, credentialID, org.String(), c.credProvider, cipher, c.credConfig)
						credential = credentialID
					}
					integrationID := uuid.New()
					exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'managed', $5::json, $6, now(), now())`, integrationID, org.String(), c.system, credential, c.intConfig, c.intActive)
					exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES (gen_random_uuid(), $1, $2, $3, 'repository', 'acme/managed', 'managed', 'Acme/Managed', '{}'::json, $4, now(), now())`,
						org.String(), integrationID, c.system, c.sourceOn)
				}
				// The push organization's admin.
				email := fmt.Sprintf("own-admin-%d@example.com", i)
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id.pushAdmin, email)
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), id.pushOrg, id.pushAdmin)
				tokens[fmt.Sprintf("admin%d", i)] = map[string]any{"user_id": id.pushAdmin.String(), "email": email, "org_id": id.pushOrg.String(), "role": "admin"}
				// The batch organization's registered sources and their tokens.
				for j, host := range c.hosts() {
					sourceID := uuid.New()
					exec(`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, mode, enabled, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'operational', 'customer_push', true, now(), now())`, sourceID, id.batchOrg.String(), c.system, host)
					token := fmt.Sprintf("fcpush_own%d_%d_%s", i, j, uuid.NewString()[:8])
					exec(`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
VALUES ($1, $2, $3, 'venue', $4, 'fcpush_venue', $5::jsonb, now())`, uuid.New(), id.batchOrg.String(), sourceID, tokenHash(token), `["schema:read","ingest:write","ingest:status"]`)
					batchTokens[i] = append(batchTokens[i], batchSource{token: token, host: host})
				}
			}
			return tokens
		},
	})

	var requests []venueoracle.Request
	for i, c := range cases {
		for _, host := range c.hosts() {
			body := fmt.Sprintf(`{"system":%q,"instance":%q,"entity_family":"operational"}`, c.system, host)
			requests = append(requests, venueoracle.Request{
				Name: fmt.Sprintf("register %s: %s @ %s", c.name, c.system, host), Method: "POST", Path: "/api/v1/admin/customer-push/sources",
				Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens[fmt.Sprintf("admin%d", i)], "Content-Type": "application/json"},
				Body:    venueoracle.B64(body),
			})
		}
		for j, source := range batchTokens[i] {
			body := fmt.Sprintf(`{"schemaVersion":"external-ingest.v1","idempotencyKey":"own-%d-%d","source":{"system":%q,"instance":%q,"entityFamily":"operational"},`+
				`"records":[{"kind":"operational_service.v1","externalId":"svc-1","payload":{"externalId":"svc-1","sourceSystem":%q,"name":"Service"}}]}`,
				i, j, c.system, source.host, c.system)
			requests = append(requests, venueoracle.Request{
				Name: fmt.Sprintf("batch %s: %s @ %s", c.name, c.system, source.host), Method: "POST", Path: "/api/v1/external-ingest/batches",
				Headers: map[string]string{"Authorization": "Bearer " + source.token, "Content-Type": "application/json"},
				Body:    venueoracle.B64(body),
			})
		}
	}
	scrape := venueoracle.Request{Name: "metrics", Method: "GET", Path: "/metrics"}
	python := venue.ServePython(t, append(append([]venueoracle.Request{}, requests...), scrape))
	pythonMetrics := python[len(python)-1]
	if pythonMetrics.Status != 200 {
		t.Fatalf("python /metrics answered %d", pythonMetrics.Status)
	}

	goBase, operator := startGoServer(t, ctx, venue, jwtKey, withKey)
	receipt := venueoracle.Diff(t, goBase, requests, python[:len(requests)], venueoracle.DiffOptions{Normalize: normalize})
	t.Log(receipt)
	if withKey {
		compareDecryptFailed(t, pythonMetrics.Body, operator())
	}
}

// decryptFailedCounts reads devhealth_integration_credential_decrypt_failed_total
// out of a Prometheus text exposition: provider -> value. A zero sample is no
// movement and is left out.
func decryptFailedCounts(t *testing.T, exposition string) map[string]float64 {
	t.Helper()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(exposition))
	if err != nil {
		t.Fatalf("parse exposition: %v", err)
	}
	out := map[string]float64{}
	family := families["devhealth_integration_credential_decrypt_failed_total"]
	if family == nil {
		family = families["devhealth_integration_credential_decrypt_failed"]
	}
	if family == nil {
		return out
	}
	for _, metric := range family.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "provider" {
				if value := metric.GetCounter().GetValue(); value != 0 {
					out[label.GetValue()] = value
				}
			}
		}
	}
	return out
}

// compareDecryptFailed requires the counter to have moved by the same amount
// per provider on both planes: one count for each unreadable stored payload a
// request read (a credential shared by the org's sources is read once per
// request), none for a payload that decrypts, even to JSON null or a list.
func compareDecryptFailed(t *testing.T, python, goExposition string) {
	t.Helper()
	pythonCounts, goCounts := decryptFailedCounts(t, python), decryptFailedCounts(t, goExposition)
	for _, provider := range []string{"github", "gitlab"} {
		if pythonCounts[provider] == 0 {
			t.Errorf("the Python api did not count provider %s (%v); the run does not exercise it", provider, pythonCounts)
		}
	}
	if !reflect.DeepEqual(pythonCounts, goCounts) {
		t.Errorf("devhealth_integration_credential_decrypt_failed_total: DIFF\n python %v\n go     %v", pythonCounts, goCounts)
		return
	}
	t.Logf("devhealth_integration_credential_decrypt_failed_total: SAME %v", goCounts)
}

// startGoServer serves the dho api and returns its base URL and a reader of
// its operator /metrics, the Go api's scrape surface.
func pythonEnv(withKey bool) []string {
	if withKey {
		return []string{"SETTINGS_ENCRYPTION_KEY=" + encryptionKey}
	}
	return nil
}

func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string, withKey bool) (string, func() string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatalf("go pool: %v", err)
	}
	t.Cleanup(pool.Close)
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatalf("authenticator: %v", err)
	}
	guard := policy.NewGuard(auth, logger)
	client, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatalf("valkey: %v", err)
	}
	t.Cleanup(client.Close)
	var decryptor providerfoundation.FernetDecryptor
	if withKey {
		var err error
		decryptor, err = providerfoundation.NewFernetDecryptor(secrets.NewValue(encryptionKey), "")
		if err != nil {
			t.Fatalf("decryptor: %v", err)
		}
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	deps := apiservice.Deps{Pool: pool, Valkey: client, Auth: auth, Guard: guard, Decryptor: decryptor, Verifier: verifier}
	registry := health.NewRegistry(0)
	if err := apiservice.RegisterOperatorMetrics(registry, &deps); err != nil {
		t.Fatalf("operator metrics: %v", err)
	}
	routes := apiservice.Routes(deps, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	operatorServer, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "api"})
	if err != nil {
		t.Fatalf("operator server: %v", err)
	}
	operator := httptest.NewServer(operatorServer.Handler())
	t.Cleanup(operator.Close)
	return ts.URL, func() string {
		response, err := http.Get(operator.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("operator /metrics answered %d: %s", response.StatusCode, body)
		}
		return string(body)
	}
}
