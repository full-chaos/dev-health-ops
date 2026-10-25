//go:build integration

package investment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// ledgerStub stands in for the org's provider: it reports full usage, or fails
// before dispatch. No test reaches a real provider.
type ledgerStub struct {
	model  string
	refuse bool
	calls  int
}

type notDispatchedError struct{}

func (notDispatchedError) Error() string               { return "stub: not dispatched" }
func (notDispatchedError) ProviderNotDispatched() bool { return true }

func (stub *ledgerStub) Complete(context.Context, categorize.CompletionRequest) (categorize.CompletionResult, error) {
	stub.calls++
	if stub.refuse {
		return categorize.CompletionResult{}, notDispatchedError{}
	}
	in, out, cached := 10, 5, 0
	return categorize.CompletionResult{Text: "{}", InputTokens: &in, OutputTokens: &out, CachedInputTokens: &cached, Model: stub.model}, nil
}
func (stub *ledgerStub) Close() error  { return nil }
func (stub *ledgerStub) Model() string { return stub.model }

// stubbedBYO is the production wiring over pool with stub provider
// constructors: the text provider is newProvider's stub and the clef client
// runs on the package's fake network (clefbyo_test.go), so no real provider is
// ever built and no request leaves (D6279).
func stubbedBYO(pool *pgxpool.Pool, cipher providerfoundation.FernetDecryptor, network *clefBYO, newProvider func(categorize.ProviderKind, string, string, string) (categorize.Provider, error)) *orgCategorizeBYO {
	return newOrgCategorizeBYO(pool, cipher, testLogger(), newProvider, network.build().newClef)
}

// otherPurposeKeyRows counts the other purposes' api_key rows the domain role
// reads through the definer function for org.
func otherPurposeKeyRows(ctx context.Context, t *testing.T, domain *pgxpool.Pool, org string) int {
	t.Helper()
	var n int
	if err := domain.QueryRow(ctx, `SELECT count(*) FROM public.categorize_byo_settings($1) WHERE key = ANY($2)`,
		org, llmorgsettings.CategorizeBYOBaseURLCheckKeys()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

type ledgerRow struct {
	status        string
	actual        *int64
	reconciledSet bool
}

func ledgerRows(ctx context.Context, t *testing.T, admin *pgxpool.Pool, orgID string) []ledgerRow {
	t.Helper()
	rows, err := admin.Query(ctx, `SELECT status, actual_micro_usd, reconciled_at IS NOT NULL FROM byo_llm_budget_reservations WHERE org_id = $1::uuid ORDER BY created_at, id`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []ledgerRow
	for rows.Next() {
		var row ledgerRow
		if err := rows.Scan(&row.status, &row.actual, &row.reconciledSet); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func seedOrgSettings(ctx context.Context, t *testing.T, admin *pgxpool.Pool, cipher providerfoundation.FernetDecryptor, orgID string, plain map[string]string, encrypted map[string]string) {
	t.Helper()
	for key, value := range plain {
		category := "llm"
		if key == llmbudget.LimitKey {
			category = llmbudget.Category
		}
		pgseed.Setting(ctx, t, admin, orgID, category, key, value, false)
	}
	for key, value := range encrypted {
		sealed, err := cipher.Encrypt([]byte(value))
		if err != nil {
			t.Fatal(err)
		}
		pgseed.Setting(ctx, t, admin, orgID, "llm", key, sealed.Reveal(), true)
	}
}

// The heavy worker runs categorization on an opted-in org's own model as the
// domain role (CHAOS-11335 Section 2). Under the grants go-river-migrate really
// applies, the production wiring reads the org's config and budget limit through
// the definer function, builds the backend, reserves a budget row and settles it,
// with no statement refused. The role still cannot read settings, delete a
// reservation, or write a reservation column Settle does not write.
func TestTheDomainRoleBuildsAnOptedInOrgsBackendAndKeepsItsBudgetLedger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "byobuild")
	venue.RequireReady(t, ctx)
	// acr_service_model is an explicit-purchase feature: a clef-only org saves
	// no provider the saved-config read counts, so it needs the org grant
	// production orgs hold.
	serviceModelFlag := uuid.NewString()
	pgseed.FeatureFlag(ctx, t, venue.Admin, serviceModelFlag, llmorgsettings.ACRServiceModelFeatureKey, "community", true)
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("default provider", func(t *testing.T) {
		org := uuid.NewString()
		pgseed.Org(ctx, t, venue.Admin, org, "enterprise")
		seedOrgSettings(ctx, t, venue.Admin, cipher, org, map[string]string{
			llmorgsettings.KeyCategorizeBYOEnabled: "true", "provider": "openai", "model": "gpt-5-mini", llmbudget.LimitKey: "100000000",
		}, map[string]string{"api_key": "sk-org-own-key"})

		stub := &ledgerStub{model: "gpt-5-mini"}
		var builtWith string
		byo := stubbedBYO(venue.Domain, cipher, newClefBYO(clefAnswers("clef")), func(kind categorize.ProviderKind, apiKey, baseURL, model string) (categorize.Provider, error) {
			builtWith = string(kind) + "|" + apiKey + "|" + baseURL + "|" + model
			return stub, nil
		})
		optedIn, err := byo.OptedIn(ctx, org)
		if err != nil || !optedIn {
			t.Fatalf("OptedIn = %v, %v", optedIn, err)
		}
		served, err := byo.Build(ctx, org)
		if err != nil || served == nil {
			t.Fatalf("Build = %v, %v", served, err)
		}
		defer served.Close()
		if builtWith != "openai|sk-org-own-key||gpt-5-mini" {
			t.Fatalf("the provider was built from %q", builtWith)
		}

		guarded := byo.guard(stub, org, categorize.ProviderKind("openai"), "")
		if _, err := guarded.Complete(ctx, categorize.CompletionRequest{Prompt: "p", MaxOutputTokens: 16}); err != nil {
			t.Fatalf("a budgeted call: %v", err)
		}
		stub.refuse = true
		if _, err := guarded.Complete(ctx, categorize.CompletionRequest{Prompt: "p", MaxOutputTokens: 16}); err == nil {
			t.Fatal("a call the stub refused before dispatch reported no error")
		}
		rows := ledgerRows(ctx, t, venue.Admin, org)
		if len(rows) != 2 || rows[0].status != "succeeded" || rows[0].actual == nil || !rows[0].reconciledSet ||
			rows[1].status != "voided" || rows[1].actual == nil || *rows[1].actual != 0 || !rows[1].reconciledSet {
			t.Fatalf("ledger rows = %+v, want one succeeded and one voided, both settled", rows)
		}
	})

	t.Run("clef override", func(t *testing.T) {
		org := uuid.NewString()
		pgseed.Org(ctx, t, venue.Admin, org, "enterprise")
		pgseed.OrgOverride(ctx, t, venue.Admin, org, serviceModelFlag, true)
		const baseURL = "https://api.cloudflare.com/client/v4/accounts/0123456789abcdef0123456789abcdef/ai"
		seedOrgSettings(ctx, t, venue.Admin, cipher, org, map[string]string{
			llmorgsettings.KeyCategorizeBYOEnabled: "true", "purpose:categorize:provider": "clef", "purpose:categorize:model": "clef",
			"purpose:categorize:base_url": baseURL, llmbudget.LimitKey: "100000000",
		}, map[string]string{"purpose:categorize:api_key": "cf-org-token-0123456789abcdef0123456789"})

		network := newClefBYO(clefAnswers("clef"))
		byo := stubbedBYO(venue.Domain, cipher, network, func(categorize.ProviderKind, string, string, string) (categorize.Provider, error) {
			return nil, errors.New("the clef path builds no text provider")
		})
		served, err := byo.Build(ctx, org)
		if err != nil || served == nil {
			t.Fatalf("clef Build = %v, %v", served, err)
		}
		defer served.Close()
		postURL, ok := llmorgsettings.ClefPostURL(baseURL, "clef")
		if !ok {
			t.Fatal("the clef base url does not parse")
		}
		guard := byo.clefBudget(org, "clef", postURL)
		if err := guard.Run(ctx, llmbudget.Call{IdempotencyKey: "clef:" + uuid.NewString(), MaxInputTokens: 10, MaxOutputTokens: 16},
			func(context.Context) (llmbudget.Usage, error) {
				in, out, cached := 10, 5, 0
				return llmbudget.UsageFrom(&in, &out, &cached, nil), nil
			}); err != nil {
			t.Fatalf("a budgeted clef call: %v", err)
		}
		if rows := ledgerRows(ctx, t, venue.Admin, org); len(rows) != 1 || rows[0].status != "succeeded" {
			t.Fatalf("clef ledger rows = %+v, want one succeeded", rows)
		}
		if sent := network.network.count(); sent != 0 {
			t.Fatalf("the clef path sent %d request(s)", sent)
		}
	})

	// The other purposes' keys reach the worker only to refuse a base_url that
	// carries one: no provider builder, log line or error ever holds them.
	const investigateKey, explainKey = "sk-investigate-only-0123456789", "sk-explain-only-9876543210"
	t.Run("other purposes' keys reach only the base_url check", func(t *testing.T) {
		org := uuid.NewString()
		pgseed.Org(ctx, t, venue.Admin, org, "enterprise")
		const baseURL = "https://api.cloudflare.com/client/v4/accounts/0123456789abcdef0123456789abcdef/ai"
		const token = "cf-org-token-0123456789abcdef0123456789"
		seedOrgSettings(ctx, t, venue.Admin, cipher, org, map[string]string{
			llmorgsettings.KeyCategorizeBYOEnabled: "true", "purpose:categorize:provider": "clef", "purpose:categorize:model": "clef",
			"purpose:categorize:base_url": baseURL, "purpose:investigate:provider": "openai", "purpose:explain:provider": "openai",
		}, map[string]string{"purpose:categorize:api_key": token, "purpose:investigate:api_key": investigateKey, "purpose:explain:api_key": explainKey})
		if n := otherPurposeKeyRows(ctx, t, venue.Domain, org); n != 2 {
			t.Fatalf("the function returned %d of the other purposes' key rows for an org with a base_url, want 2", n)
		}
		logs := &bytes.Buffer{}
		logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		network := newClefBYO(clefAnswers("clef"))
		var received []string
		recordClef := network.build().newClef
		byo := newOrgCategorizeBYO(venue.Domain, cipher, logger,
			func(kind categorize.ProviderKind, apiKey, baseURL, model string) (categorize.Provider, error) {
				received = append(received, apiKey, baseURL, model)
				return nil, errors.New("the clef path builds no text provider")
			},
			func(cfg categorize.ClefClientConfig) (*categorize.TypeSafeClient, error) {
				received = append(received, cfg.Token.Reveal(), cfg.BaseURL, cfg.Model, fmt.Sprintf("%+v", cfg))
				return recordClef(cfg)
			})
		served, err := byo.Build(ctx, org)
		if err != nil || served == nil {
			t.Fatalf("clef Build = %v, %v", served, err)
		}
		defer served.Close()
		if len(received) == 0 || received[0] != token {
			t.Fatalf("the clef client was built from %q, want the categorize token", received)
		}
		for _, value := range append(received, logs.String()) {
			for _, secret := range []string{investigateKey, explainKey} {
				if strings.Contains(value, secret) {
					t.Fatalf("another purpose's key reached the provider builder or a log line: %q", value)
				}
			}
		}
	})

	t.Run("a base_url holding another purpose's key is refused on the worker path", func(t *testing.T) {
		org := uuid.NewString()
		pgseed.Org(ctx, t, venue.Admin, org, "enterprise")
		seedOrgSettings(ctx, t, venue.Admin, cipher, org, map[string]string{
			llmorgsettings.KeyCategorizeBYOEnabled: "true", "provider": "openai", "model": "gpt-5-mini",
			"base_url": "https://llm.example.invalid/v1?k=" + investigateKey, "purpose:investigate:provider": "openai",
		}, map[string]string{"api_key": "sk-org-own-key", "purpose:investigate:api_key": investigateKey})
		logs := &bytes.Buffer{}
		logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		built := 0
		byo := newOrgCategorizeBYO(venue.Domain, cipher, logger,
			func(categorize.ProviderKind, string, string, string) (categorize.Provider, error) {
				built++
				return &ledgerStub{model: "gpt-5-mini"}, nil
			}, newClefBYO(clefAnswers("clef")).build().newClef)
		served, err := byo.Build(ctx, org)
		var refusal *BYORefusal
		if served != nil || !errors.As(err, &refusal) || built != 0 {
			t.Fatalf("Build = %v, %v (built %d); want a refusal before any provider is built", served, err, built)
		}
		for _, text := range []string{err.Error(), logs.String()} {
			if strings.Contains(text, investigateKey) || strings.Contains(text, "sk-org-own-key") {
				t.Fatalf("a key reached the refusal or a log line: %q", text)
			}
		}
	})

	if denied := venue.Denials.All(); len(denied) != 0 {
		t.Fatalf("the domain role was refused %d statement(s) on the Build and ledger path: %q", len(denied), denied)
	}

	for _, statement := range []string{
		`SELECT count(*) FROM settings`,
		`DELETE FROM byo_llm_budget_reservations`,
		`UPDATE byo_llm_budget_reservations SET reserved_micro_usd = 0`,
		`UPDATE byo_llm_budget_reservations SET org_id = org_id`,
	} {
		_, err := venue.Domain.Exec(ctx, statement)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s as the domain role = %v, want SQLSTATE 42501", statement, err)
		}
	}
}
