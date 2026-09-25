//go:build integration

package adminllmspendvenue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func text(value string) *string { return &value }

// scenario is one organization's tier, kill switch and LLM settings.
type scenario struct {
	name       string
	tier       string
	killSwitch bool
	provider   *string
	apiKey     *string
	baseURL    *string
}

// scenarios cover the gates and every branch of evaluate_org_llm_status. The
// organizations with active settings answer from ClickHouse (empty here: the
// data scenarios use the rich organization).
func scenarios() []scenario {
	team := func(name string, provider, apiKey, baseURL *string) scenario {
		return scenario{name: name, tier: "team", provider: provider, apiKey: apiKey, baseURL: baseURL}
	}
	return []scenario{
		{name: "community tier is refused", tier: "community", provider: text("openai"), apiKey: text("k")},
		{name: "kill switch is refused", tier: "team", killSwitch: true, provider: text("openai"), apiKey: text("k")},
		team("no settings at all", nil, nil, nil),
		team("provider empty", text(""), text("k"), nil),
		team("provider auto", text("auto"), text("k"), nil),
		team("provider mock", text("mock"), text("k"), nil),
		team("provider none", text("none"), text("k"), nil),
		team("provider whitespace only", text("   "), text("k"), nil),
		team("provider unknown", text("nope"), text("k"), nil),
		team("openai with a key", text("openai"), text("k"), nil),
		team("openai upper case and padded", text(" OpenAI "), text("k"), nil),
		team("openai without a key", text("openai"), nil, nil),
		team("openai with an empty key", text("openai"), text(""), nil),
		team("openai with a base url only", text("openai"), nil, text("https://8.8.8.8/v1")),
		team("anthropic with a key", text("anthropic"), text("k"), nil),
		team("gemini with a key", text("gemini"), text("k"), nil),
		team("qwen with a key", text("qwen"), text("k"), nil),
		team("ollama with nothing", text("ollama"), nil, nil),
		team("ollama with a base url only", text("ollama"), nil, text("https://8.8.8.8")),
		team("lmstudio with a key only", text("lmstudio"), text("k"), nil),
		team("qwen-local with a public url", text("qwen-local"), nil, text("https://8.8.4.4/v1")),
		team("local with a private url", text("local"), nil, text("http://10.0.0.1:11434")),
		team("local with a loopback url", text("local"), nil, text("http://127.0.0.1:11434")),
		team("local with a metadata url", text("local"), nil, text("http://169.254.169.254/latest")),
		team("openai with a key and a private url", text("openai"), text("k"), text("http://192.168.1.5")),
		team("openai with a key and a plain http public url", text("openai"), text("k"), text("http://8.8.8.8")),
		team("openai with a key and userinfo", text("openai"), text("k"), text("https://user@8.8.8.8")),
		team("openai with a key and a bad scheme", text("openai"), text("k"), text("ftp://8.8.8.8")),
		team("openai with a key and a blank url", text("openai"), text("k"), text("   ")),
		team("openai with a key and an empty url", text("openai"), text("k"), text("")),
		team("openai with a key and a broken bracket", text("openai"), text("k"), text("https://[::1")),
	}
}

// The rich organization's ClickHouse rows: what the data requests read.
const (
	richOrg  = "rich"
	otherOrg = "other"
	hugeOrg  = "huge"
)

var (
	// requests over the rich organization, by query string.
	richQueries = []string{
		"", "limit=1", "limit=2", "limit=3", "limit=20", "limit=25", "limit=100", "limit=101", "limit=1000000",
		"limit=1000000000000000000000000000000", "limit=0", "limit=-1", "limit=abc", "limit=", "limit=1.0", "limit=1.5",
		"limit=%201%20", "limit=1_0", "limit=3&limit=1", "limit=0&limit=2",
		"since=2026-01-01", "since=2026-01-01T00:00:00", "since=2026-01-01T00:00:00Z", "since=2026-01-01T05:30:00%2B05:30",
		"since=2026-01-01T00:00:00.123456", "since=2026-01-01%2012:00:00", "since=2020-01-01", "since=2030-01-01",
		"since=1900-01-01", "since=0001-01-01T00:00:00", "since=0001-01-01T00:00:00Z", "since=9999-12-31T23:59:59Z",
		"since=0001-01-01T23:59:59", "since=0001-01-02T00:00:00", "since=9999-12-31T00:00:00", "since=9999-12-30T23:59:59", "since=9999-12-31T23:59:59",
		"since=0001-01-01T00:00:00%2B05:00", "since=0001-01-01T05:00:00%2B05:00", "since=0001-01-01T00:00:00-05:00", "since=9999-12-31T23:59:59-05:00",
		"since=9999-12-31T18:59:59-05:00", "since=0001-01-01", "since=9999-12-31",
		"since=abc", "since=", "since=1700000000", "since=2026-13-01", "since=2026-02-30", "since=2026-01-01T25:00:00",
		"since=2026-01-01&since=2020-01-01", "since=abc&limit=0", "limit=0&since=abc", "since=2026-01-01&limit=2",
	}
)

// windowEdges are the "since" values placed exactly on a row's computed_at.
func edgeQueries(base time.Time) []string {
	at := func(offset time.Duration) string {
		return "since=" + url.QueryEscape(base.Add(offset).Format("2006-01-02T15:04:05Z"))
	}
	return []string{at(-2 * time.Hour), at(-2*time.Hour + time.Second), at(-2*time.Hour - time.Second),
		at(-5 * time.Hour), at(-30 * time.Hour), at(-31 * 24 * time.Hour), at(-29 * 24 * time.Hour)}
}

// seedStatements are the ClickHouse rows both planes read.
func seedStatements(orgs map[string]uuid.UUID, base time.Time) []string {
	stamp := func(offset time.Duration) string { return base.Add(offset).Format("2006-01-02 15:04:05") }
	usage := func(org, run, provider, model, source string, in, out, calls uint64, at string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', 'legacy', %d, %d, %d, '%s')", org, run, provider, model, source, in, out, calls, at)
	}
	rich, other := orgs[richOrg].String(), orgs[otherOrg].String()
	var usageRows []string
	// twenty-five runs an hour apart, two models each, a repeated model row.
	for i := 0; i < 25; i++ {
		run := fmt.Sprintf("run-%02d", i)
		at := stamp(-time.Duration(i+2) * time.Hour)
		usageRows = append(usageRows,
			usage(rich, run, "openai", "gpt-5-mini", "categorize", uint64(100+i), uint64(10+i), uint64(1+i), at),
			usage(rich, run, "openai", "gpt-5-mini", "explain", 5, 1, 1, at),
			usage(rich, run, "anthropic", "claude-x", "categorize", 7, 3, 2, at),
			// another organization's rows under the same run id
			usage(other, run, "openai", "gpt-5-mini", "categorize", 999, 999, 999, at),
		)
	}
	// runs that tie on time (run id descending decides), models that tie (model ascending decides)
	tie := stamp(-90 * time.Hour)
	usageRows = append(usageRows,
		usage(rich, "tie-a", "openai", "m-b", "x", 1, 1, 1, tie),
		usage(rich, "tie-b", "openai", "m-a", "x", 2, 2, 2, tie),
		usage(rich, "tie-b", "openai", "m-c", "x", 3, 3, 3, tie),
		usage(rich, "tie-c", "openai", "m-a", "x", 4, 4, 4, tie),
		// a run whose rows straddle the default window (one old, one recent)
		usage(rich, "straddle", "openai", "gpt-5-mini", "x", 10, 10, 10, stamp(-40*24*time.Hour)),
		usage(rich, "straddle", "openai", "gpt-5-mini", "x", 20, 20, 20, stamp(-3*time.Hour)),
		// a run outside the default window only
		usage(rich, "ancient", "openai", "gpt-5-mini", "x", 30, 30, 30, stamp(-45*24*time.Hour)),
		// legacy rows (empty run id): several providers and models, recent and old
		usage(rich, "", "openai", "gpt-5-mini", "categorize", 11, 1, 1, stamp(-4*time.Hour)),
		usage(rich, "", "openai", "gpt-5-mini", "explain", 12, 2, 2, stamp(-5*time.Hour)),
		usage(rich, "", "anthropic", "claude-x", "categorize", 13, 3, 3, stamp(-4*time.Hour)),
		usage(rich, "", "openai", "a-model", "categorize", 14, 4, 4, stamp(-4*time.Hour)),
		usage(rich, "", "openai", "old-model", "categorize", 15, 5, 5, stamp(-50*24*time.Hour)),
		usage(other, "", "openai", "gpt-5-mini", "categorize", 999, 999, 999, stamp(-4*time.Hour)),
		// a very large count
		usage(rich, "big", "openai", "gpt-5-mini", "x", 9007199254740993, 18446744073709551000, 4611686018427387904, stamp(-1*time.Hour)),
	)
	statements := []string{
		"INSERT INTO llm_token_usage (org_id, run_id, provider, model, source, use_case, input_tokens, output_tokens, calls, computed_at) VALUES\n" +
			strings.Join(usageRows, ",\n"),
	}
	outcome := func(org, run, status, errorsJSON string, i int, at string) string {
		return fmt.Sprintf("('wu-%s-%s-%d', '%s', '%s', '%s', '%s', 'fte_days', 1, '%s', '%s')", org, run, i, org, run, status, strings.ReplaceAll(errorsJSON, "'", "''"), at, at)
	}
	var outcomeRows []string
	i := 0
	add := func(org, run, status, errorsJSON string, offset time.Duration) {
		i++
		outcomeRows = append(outcomeRows, outcome(org, run, status, errorsJSON, i, stamp(offset)))
	}
	add(rich, "run-00", "ok", "[]", -2*time.Hour)
	add(rich, "run-00", "repaired", "[]", -2*time.Hour)
	add(rich, "run-00", "repaired", "[]", -2*time.Hour)
	add(rich, "run-00", "invalid_llm_output", "[\"x\"]", -2*time.Hour)
	add(rich, "run-00", "", "[]", -2*time.Hour)
	add(rich, "run-00", "", "[\"boom\"]", -2*time.Hour)
	add(rich, "run-00", "", "not json", -2*time.Hour)
	add(rich, "run-00", "", "", -2*time.Hour)
	add(rich, "run-00", "", "{}", -2*time.Hour)
	add(rich, "run-00", "", "null", -2*time.Hour)
	add(rich, "run-00", "", "0", -2*time.Hour)
	add(rich, "run-00", "", "NaN", -2*time.Hour)
	add(rich, "run-00", "", " [ ] ", -2*time.Hour)
	add(rich, "run-00", "", "[1] trailing", -2*time.Hour)
	add(rich, "run-00", "", "[[]]", -2*time.Hour)
	add(rich, "run-00", "", "\"[1]\"", -2*time.Hour)
	add(rich, "run-00", "OK", "[]", -2*time.Hour)
	add(rich, "run-00", " ok", "[]", -2*time.Hour)
	add(rich, "run-01", "fallback_reason", "[]", -3*time.Hour)
	add(rich, "run-01", "ok", "[]", -3*time.Hour)
	add(rich, "run-02", "ok", "[]", -4*time.Hour)
	// a failure row older than the window start of some queries, and another organization's
	add(rich, "run-03", "invalid_llm_output", "[]", -50*24*time.Hour)
	add(other, "run-00", "invalid_llm_output", "[]", -2*time.Hour)
	add(rich, "tie-a", "zzz_class", "[]", -90*time.Hour)
	add(rich, "tie-a", "aaa_class", "[]", -90*time.Hour)
	add(rich, "", "invalid_llm_output", "[]", -4*time.Hour)
	statements = append(statements,
		"INSERT INTO work_unit_investments (work_unit_id, org_id, categorization_run_id, categorization_status, categorization_errors_json, effort_metric, effort_value, computed_at, to_ts) VALUES\n"+
			strings.Join(outcomeRows, ",\n"))
	// an organization whose errors json is an integer of more than 4300 digits:
	// json.loads refuses it with a ValueError that is not a JSONDecodeError.
	huge := orgs[hugeOrg].String()
	statements = append(statements,
		usageInsert(huge, "run-huge", stamp(-2*time.Hour)),
		"INSERT INTO work_unit_investments (work_unit_id, org_id, categorization_run_id, categorization_status, categorization_errors_json, effort_metric, effort_value, computed_at, to_ts) VALUES\n"+
			outcome(huge, "run-huge", "", strings.Repeat("9", 4301), 1, stamp(-2*time.Hour)))
	return statements
}

func usageInsert(org, run, at string) string {
	return fmt.Sprintf("INSERT INTO llm_token_usage (org_id, run_id, provider, model, source, use_case, input_tokens, output_tokens, calls, computed_at) VALUES ('%s', '%s', 'openai', 'gpt-5-mini', 'x', 'legacy', 1, 1, 1, '%s')", org, run, at)
}

var sinceField = regexp.MustCompile(`"since":"[^"]*"`)

// TestAdminLLMSpendVenueOracle answers GET /api/v1/admin/llm-settings/spend
// with the real Python api and the real Go api over the same organizations
// and ClickHouse rows, and requires the same status and response text.
func TestAdminLLMSpendVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-spend-32-bytes!"
	cases := scenarios()
	orgs := map[string]uuid.UUID{richOrg: uuid.New(), otherOrg: uuid.New(), hugeOrg: uuid.New()}
	caseOrgs := make([]uuid.UUID, len(cases))
	for i := range cases {
		caseOrgs[i] = uuid.New()
	}
	memberOrg := uuid.New()
	base := time.Now().UTC().Truncate(time.Hour)

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			exec(`UPDATE feature_flags SET created_at = '2020-01-01T00:00:00+00:00', updated_at = '2020-01-01T00:00:00+00:00'`)
			tokens := map[string]map[string]any{}
			org := func(key string, id uuid.UUID, tier string) {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, $3, 'stripe', true, now(), now())`, id, "llm-spend-"+key, tier)
			}
			addUser := func(key string, orgID uuid.UUID, role string) {
				id, email := uuid.New(), key+"@example.com"
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, email)
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), orgID, id, role)
				tokens[key] = map[string]any{"user_id": id.String(), "email": email, "org_id": orgID.String(), "role": role}
			}
			setting := func(orgID uuid.UUID, key, value string) {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES ($1, $2, 'llm', $3, $4, false, '2026-02-01T00:00:00+00:00', '2026-02-01T00:00:00+00:00')`, uuid.New(), orgID.String(), key, value)
			}
			active := func(orgID uuid.UUID) {
				setting(orgID, "provider", "openai")
				setting(orgID, "api_key", "key")
			}
			for key, id := range orgs {
				org(key, id, "team")
				addUser("admin-"+key, id, "admin")
				active(id)
			}
			for i, c := range cases {
				id := caseOrgs[i]
				org(fmt.Sprintf("c%d", i), id, c.tier)
				if c.killSwitch {
					exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, (SELECT id FROM feature_flags WHERE key = 'byo_llm'), false, NULL, NULL, 'kill switch', NULL, now(), now())`, uuid.New(), id)
				}
				addUser(fmt.Sprintf("admin%d", i), id, "admin")
				if c.provider != nil {
					setting(id, "provider", *c.provider)
				}
				if c.apiKey != nil {
					setting(id, "api_key", *c.apiKey)
				}
				if c.baseURL != nil {
					setting(id, "base_url", *c.baseURL)
				}
			}
			org("member", memberOrg, "team")
			addUser("member", memberOrg, "member")
			return tokens
		},
	})

	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatalf("seed clickhouse %s: %v", database, err)
		}
		for _, statement := range seedStatements(orgs, base) {
			if err := conn.Exec(ctx, statement); err != nil {
				_ = conn.Close()
				t.Fatalf("seed clickhouse %s: %v\n%.400s", database, err, statement)
			}
		}
		_ = conn.Close()
	}

	const path = "/api/v1/admin/llm-settings/spend"
	var requests []venueoracle.Request
	auth := func(key string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[key]}
	}
	for i, c := range cases {
		requests = append(requests, venueoracle.Request{Name: c.name, Method: "GET", Path: path, Headers: auth(fmt.Sprintf("admin%d", i))})
	}
	for _, query := range append(append([]string{}, richQueries...), edgeQueries(base)...) {
		full := path
		if query != "" {
			full += "?" + query
		}
		requests = append(requests, venueoracle.Request{Name: "rich " + query, Method: "GET", Path: full, Headers: auth("admin-" + richOrg)})
	}
	requests = append(requests,
		venueoracle.Request{Name: "other organization", Method: "GET", Path: path + "?limit=100", Headers: auth("admin-" + otherOrg)},
		venueoracle.Request{Name: "an integer of more than 4300 digits", Method: "GET", Path: path, Headers: auth("admin-" + hugeOrg)},
		venueoracle.Request{Name: "community tier with a bad limit", Method: "GET", Path: path + "?limit=0", Headers: auth("admin0")},
		venueoracle.Request{Name: "kill switch with a bad since", Method: "GET", Path: path + "?since=abc", Headers: auth("admin1")},
		venueoracle.Request{Name: "a member is not an admin", Method: "GET", Path: path, Headers: auth("member")},
		venueoracle.Request{Name: "member with a bad limit", Method: "GET", Path: path + "?limit=0", Headers: auth("member")},
		venueoracle.Request{Name: "no credentials", Method: "GET", Path: path},
	)
	python := venue.ServePython(t, requests)

	goBase := startGoServer(t, ctx, venue, jwtKey)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		// A window that starts "30 days ago" is read from each plane's own clock.
		Normalize: func(request venueoracle.Request, body string) string {
			if strings.Contains(request.Path, "since=") {
				return body
			}
			return sinceField.ReplaceAllString(body, `"since":"<now-30d>"`)
		},
	})
	t.Log(receipt)
}

func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string) string {
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
	// The api's own ClickHouse login, granted exactly clickhouse.APIPosture.
	clickHouse, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.GoAPIClickHouseURI(t)))
	if err != nil {
		t.Fatalf("go clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	routes := apiservice.Routes(apiservice.Deps{Pool: pool, Valkey: client, ClickHouse: clickHouse, Auth: auth, Guard: guard}, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}
