//go:build integration

package adminllmvenue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// operatorMax is BYO_LLM_MAX_BUDGET_MICRO_USD on both planes.
const operatorMax = "10000000"

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

type reservation struct {
	status   string
	reserved int64
	actual   *int64
}

func actual(value int64) *int64 { return &value }

// scenario is one organization: its settings, licence override, tier, kill
// switch and reservations.
type scenario struct {
	name         string
	tier         string
	license      string // licence tier, "" = none
	overrides    string // limits_override JSON
	killSwitch   bool
	provider     *string
	model        *string
	baseURL      *string
	limit        *string
	reservations []reservation
}

func text(value string) *string { return &value }

func scenarios() []scenario {
	priced := func(name string, model, baseURL *string) scenario {
		return scenario{name: name, tier: "team", provider: text("openai"), model: model, baseURL: baseURL, limit: text("5000000")}
	}
	limited := func(name, limit string, reservations ...reservation) scenario {
		return scenario{name: name, tier: "team", provider: text("openai"), model: text("gpt-5-mini"), limit: text(limit), reservations: reservations}
	}
	return []scenario{
		// gates
		{name: "community tier is refused", tier: "community", provider: text("openai"), model: text("gpt-5-mini")},
		{name: "kill switch is refused", tier: "team", killSwitch: true, provider: text("openai"), model: text("gpt-5-mini")},
		// nothing configured / not priced
		{name: "no settings at all", tier: "team"},
		{name: "unpriced provider, limit set", tier: "team", provider: text("anthropic"), limit: text("1000")},
		{name: "unpriced provider, no limit", tier: "team", provider: text("anthropic")},
		{name: "empty model falls back to the provider default", tier: "team", provider: text("openai"), model: text(""), limit: text("1000")},
		{name: "unknown provider has no default model", tier: "team", provider: text("nope"), limit: text("1000")},
		// priced, no reservations
		{name: "priced, no limit", tier: "team", provider: text("openai"), model: text("gpt-5-mini")},
		limited("limit within the ceiling", "1000"),
		limited("limit above the operator ceiling is capped", "999999999"),
		limited("limit zero is exhausted at once", "0"),
		limited("limit padded and underscored", " 7_000 "),
		limited("limit not an integer", "abc"),
		limited("limit negative", "-5"),
		limited("limit float text", "5.0"),
		limited("limit empty text", ""),
		// reservations
		limited("reserved counts as used", "1000", reservation{"reserved", 300, nil}),
		limited("actual replaces the reserve", "1000", reservation{"succeeded", 300, actual(120)}),
		limited("voided is ignored", "1000", reservation{"voided", 300, nil}, reservation{"succeeded", 50, actual(40)}),
		limited("failed and cancelled count their actual", "1000", reservation{"failed", 100, actual(60)}, reservation{"cancelled", 200, actual(0)}),
		limited("used equals the limit is exhausted", "300", reservation{"succeeded", 500, actual(300)}),
		limited("used above the limit clamps remaining", "300", reservation{"succeeded", 500, actual(900)}),
		limited("usage unavailable hides the usage", "1000", reservation{"usage_unavailable", 500, nil}, reservation{"succeeded", 50, actual(40)}),
		{name: "no limit with reservations still reports used", tier: "team", provider: text("openai"), model: text("gpt-5-mini"),
			reservations: []reservation{{"succeeded", 500, actual(410)}}},
		// price book: model spellings
		priced("model gpt-5-mini dated", text("gpt-5-mini-2025-08-07"), nil),
		priced("model gpt-5-mini upper case", text("GPT-5-MINI"), nil),
		priced("model gpt-5-mini padded", text("  gpt-5-mini  "), nil),
		priced("model gpt-5-nano", text("gpt-5-nano"), nil),
		priced("model gpt-5-nano dated", text("gpt-5-nano-2026-01-01"), nil),
		priced("model gpt-5-nano upper case is unpriced", text("GPT-5-NANO"), nil),
		priced("model gpt-5-nano padded", text(" gpt-5-nano "), nil),
		priced("model gpt-5 is unpriced", text("gpt-5"), nil),
		priced("model scripted without its transport", text("ask-dev-scripted-v1"), nil),
		priced("model scripted look-alike", text("ask-dev-scripted-v1-v2"), text("http://ask-dev-scripted-openai:8001")),
		// price book: endpoints
		priced("official endpoint", text("gpt-5-mini"), text("https://api.openai.com")),
		priced("official endpoint with a path", text("gpt-5-mini"), text("https://api.openai.com/v1")),
		priced("official endpoint upper case host", text("gpt-5-mini"), text("https://API.OPENAI.COM/v1")),
		priced("plain http endpoint", text("gpt-5-mini"), text("http://api.openai.com")),
		priced("look-alike endpoint", text("gpt-5-mini"), text("https://api.openai.com.evil.example")),
		priced("endpoint with userinfo", text("gpt-5-mini"), text("https://user@api.openai.com")),
		priced("endpoint blank", text("gpt-5-mini"), text("   ")),
		priced("endpoint empty", text("gpt-5-mini"), text("")),
		priced("endpoint not a url", text("gpt-5-mini"), text("not a url")),
		priced("endpoint with a broken bracket", text("gpt-5-mini"), text("https://[::1")),
		priced("azure endpoint is unpriced", text("gpt-5-mini"), text("https://example.openai.azure.com")),
		priced("scripted model on its transport", text("ask-dev-scripted-v1"), text("http://ask-dev-scripted-openai:8001")),
		priced("scripted model, wrong port", text("ask-dev-scripted-v1"), text("http://ask-dev-scripted-openai:8002")),
		priced("scripted model, https", text("ask-dev-scripted-v1"), text("https://ask-dev-scripted-openai:8001")),
		priced("scripted model, unparsable port", text("ask-dev-scripted-v1"), text("http://ask-dev-scripted-openai:abc")),
		priced("scripted model, no port", text("ask-dev-scripted-v1"), text("http://ask-dev-scripted-openai")),
		// providers
		{name: "provider upper case", tier: "team", provider: text("OpenAI"), model: text("gpt-5-mini"), limit: text("1000")},
		{name: "provider padded", tier: "team", provider: text(" openai "), model: text("gpt-5-mini"), limit: text("1000")},
		// licence ceilings
		{name: "licence lowers the ceiling", tier: "team", license: "enterprise", overrides: `{"byo_llm_budget_micro_usd": 5000}`,
			provider: text("openai"), model: text("gpt-5-mini"), limit: text("999999")},
		{name: "licence above the operator ceiling", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": 999999999}`,
			provider: text("openai"), model: text("gpt-5-mini"), limit: text("999999")},
		{name: "licence override not an integer", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": "abc"}`,
			provider: text("openai"), model: text("gpt-5-mini"), limit: text("999999")},
		{name: "licence override float", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": 5.0}`,
			provider: text("openai"), model: text("gpt-5-mini"), limit: text("999999")},
		{name: "licence override text", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": " 7_000 "}`,
			provider: text("openai"), model: text("gpt-5-mini"), limit: text("999999")},
		{name: "licence override negative", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": -1}`,
			provider: text("openai"), model: text("gpt-5-mini"), limit: text("999999")},
		{name: "licence override absent key", tier: "team", license: "team", overrides: `{"other": 1}`,
			provider: text("openai"), model: text("gpt-5-mini"), limit: text("999999")},
		{name: "licence override null", tier: "team", license: "team", overrides: `null`,
			provider: text("openai"), model: text("gpt-5-mini"), limit: text("999999")},
	}
}

// TestAdminLLMBudgetVenueOracle answers GET /api/v1/admin/llm-settings/budget
// for one organization per state with the real Python api and the real Go api
// and requires the same status and the same response text.
// budgetNow is the clock of the frozen golden: a moment in the calendar month
// the Python plane's answers were executed in. The reservations are seeded in
// that month's window and the Go plane's clock is set to it, so a replay in any
// later month reads the same window the Python plane read.
var budgetNow = time.Date(2026, 9, 26, 0, 30, 0, 0, time.UTC)

func TestAdminLLMBudgetVenueOracle(t *testing.T) {
	ctx := context.Background()
	golden := venueoracle.OpenGolden(t, goldenSpec("budget", "TestAdminLLMBudgetVenueOracle", "9e319b62f82f312fb891adc3155a0da68083d2df70fcb978acb24dd0bf0d3361"))
	root := golden.PythonRoot(t, repoRoot(t))
	nextID := goldenIDs("budget")
	const jwtKey = "venue-oracle-test-secret-key-for-llm-budget-32-bytes!"
	t.Setenv("BYO_LLM_MAX_BUDGET_MICRO_USD", operatorMax)
	cases := scenarios()
	orgs := make([]uuid.UUID, len(cases))
	admins := make([]uuid.UUID, len(cases))
	for i := range cases {
		orgs[i], admins[i] = nextID(), nextID()
	}
	memberOrg, member := nextID(), nextID()
	windowStart, _ := llmbudget.MonthWindow(budgetNow)

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: jwtKey,
		PythonEnv: []string{"BYO_LLM_MAX_BUDGET_MICRO_USD=" + operatorMax},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			exec(`UPDATE feature_flags SET created_at = '2020-01-01T00:00:00+00:00', updated_at = '2020-01-01T00:00:00+00:00'`)
			tokens := map[string]map[string]any{}
			addUser := func(key string, org, id uuid.UUID, role string) {
				email := key + "@example.com"
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, email)
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, nextID(), org, id, role)
				tokens[key] = map[string]any{"user_id": id.String(), "email": email, "org_id": org.String(), "role": role}
			}
			setting := func(org uuid.UUID, category, key string, value string) {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, false, '2026-02-01T00:00:00+00:00', '2026-02-01T00:00:00+00:00')`, nextID(), org.String(), category, key, value)
			}
			for i, c := range cases {
				org := orgs[i]
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, $3, 'stripe', true, now(), now())`, org, fmt.Sprintf("llm-budget-%d", i), c.tier)
				if c.license != "" {
					overrides := c.overrides
					if overrides == "" {
						overrides = "{}"
					}
					exec(`INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, limits_override, created_at, updated_at)
VALUES ($1, $2, $3, true, 'saas', 'stripe', $4::json, now(), now())`, nextID(), org, c.license, overrides)
				}
				if c.killSwitch {
					exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, (SELECT id FROM feature_flags WHERE key = 'byo_llm'), false, NULL, NULL, 'kill switch', NULL, now(), now())`, nextID(), org)
				}
				addUser(fmt.Sprintf("admin%d", i), org, admins[i], "admin")
				if c.provider != nil {
					setting(org, "llm", "provider", *c.provider)
				}
				if c.model != nil {
					setting(org, "llm", "model", *c.model)
				}
				if c.baseURL != nil {
					setting(org, "llm", "base_url", *c.baseURL)
				}
				if c.limit != nil {
					setting(org, llmbudget.Category, llmbudget.LimitKey, *c.limit)
				}
				for r, res := range c.reservations {
					// ck_byo_llm_budget_reconciliation_state fixes which columns each
					// status carries: reserved has neither actual nor reconciled_at,
					// usage_unavailable a reconciled_at only, voided zeros throughout,
					// the settled statuses an actual and token counts.
					var (
						actualCost, input, cached, output any
						reconciled                        any
					)
					switch res.status {
					case "reserved":
					case "usage_unavailable":
						reconciled = budgetNow
					case "voided":
						actualCost, input, cached, output, reconciled = int64(0), 0, 0, 0, budgetNow
					default:
						actualCost, input, cached, output, reconciled = *res.actual, 10, 0, 5, budgetNow
					}
					exec(`INSERT INTO byo_llm_budget_reservations (id, org_id, window_start, idempotency_key, provider, model, reserved_micro_usd, actual_micro_usd, status, pricing_version, input_tokens, cached_input_tokens, output_tokens, created_at, reconciled_at)
VALUES ($1, $2, $3, $4, 'openai', 'gpt-5-mini', $5, $6, $7, $8, $9, $10, $11, now(), $12)`,
						nextID(), org, windowStart, fmt.Sprintf("key-%d-%d", i, r), res.reserved, actualCost, res.status, llmbudget.PricingVersion, input, cached, output, reconciled)
				}
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'llm-budget-member', 'llm-budget-member', 'team', 'stripe', true, now(), now())`, memberOrg)
			addUser("member", memberOrg, member, "member")
			return tokens
		},
	})

	requests := make([]venueoracle.Request, 0, len(cases)+2)
	for i, c := range cases {
		requests = append(requests, venueoracle.Request{
			Name: c.name, Method: "GET", Path: "/api/v1/admin/llm-settings/budget",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens[fmt.Sprintf("admin%d", i)]},
		})
	}
	requests = append(requests,
		venueoracle.Request{Name: "a member is not an admin", Method: "GET", Path: "/api/v1/admin/llm-settings/budget",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["member"]}},
		venueoracle.Request{Name: "no credentials", Method: "GET", Path: "/api/v1/admin/llm-settings/budget"},
	)
	python := golden.Python(t, venue, requests)

	goBase := startGoServer(t, ctx, venue, jwtKey)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{Golden: golden})
	t.Log(receipt)
	golden.Finish(t)
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
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	routes := apiservice.Routes(apiservice.Deps{Pool: pool, Valkey: client, Auth: auth, Guard: guard, Now: func() time.Time { return budgetNow }}, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}
