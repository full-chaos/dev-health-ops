package investment

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CategorizeBYO is categorization on an org's own model (D6279, CHAOS-11008).
// It is off for every org until the org's admin opts in. jev stays the default
// and the only backend of every org that did not opt in.
type CategorizeBYO interface {
	// OptedIn reads the org's stored opt-in. An error is an unknown fact: the
	// run is refused, never served by jev in silence.
	OptedIn(ctx context.Context, orgID string) (bool, error)
	// Build admits the org through the model gate and builds its SystemOne
	// backend. Every refusal is a *BYORefusal with a code of the closed set.
	Build(ctx context.Context, orgID string) (*ServedDecision, error)
}

// BYORefusal is a refusal to categorize on the org's own model. Code is always
// a member of the closed set (llmorgsettings.SafeCategorizeRefusalCode); no
// base URL, key or provider text reaches it.
type BYORefusal struct{ Code string }

func (refusal *BYORefusal) Error() string {
	return "categorization on the organization's own model is refused: " + llmorgsettings.SafeCategorizeRefusalCode(refusal.Code)
}

// optInUnreadableCounter counts runs refused because the org's opt-in could not
// be read, by reason: permission_denied (SQLSTATE 42501: the worker role lacks
// the grant), function_missing (42883: the worker runs before pgmigrate 0156)
// or unavailable (any other fault).
var optInUnreadableCounter = func() metric.Int64Counter {
	const name = "dev_health_categorize_byo_optin_unreadable_total"
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/jobs/investment").
		Int64Counter(name, metric.WithDescription("investment.materialize runs refused because the org's categorization opt-in could not be read, by reason"))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}()

// recordOptInUnreadable is a variable so a test can observe it.
var recordOptInUnreadable = defaultRecordOptInUnreadable

func defaultRecordOptInUnreadable(ctx context.Context, reason string) {
	optInUnreadableCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func optInUnreadableReason(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501":
			return "permission_denied"
		case "42883":
			return "function_missing"
		}
	}
	return "unavailable"
}

// orgCategorizeBYO is the production CategorizeBYO. Its collaborators are fields
// so a test can swap each for a stub; the production wiring is
// NewOrgCategorizeBYO.
type orgCategorizeBYO struct {
	logger      *slog.Logger
	optedIn     func(ctx context.Context, orgID string) (bool, error)
	gate        func(ctx context.Context, orgID string) llmorgsettings.Decision
	config      func(ctx context.Context, orgID string) (llmorgsettings.OrgConfig, bool, error)
	newProvider func(kind categorize.ProviderKind, apiKey, baseURL, model string) (categorize.Provider, error)
	guard       func(provider categorize.Provider, orgID string, kind categorize.ProviderKind, baseURL string) categorize.Provider
	// newClef builds the client of a clef account; clefBudget the budget guard
	// of one clef request URL and model (nil = unguarded, tests only).
	newClef    func(cfg categorize.ClefClientConfig) (*categorize.TypeSafeClient, error)
	clefBudget func(orgID, model, postURL string) *llmbudget.Guard
}

// NewOrgCategorizeBYO wires the opt-in, the model gate, the org's categorize
// configuration and its budget guard over the registry pool. A zero decryptor
// makes every encrypted row unreadable, which the gate refuses as unreadable_row.
func NewOrgCategorizeBYO(pool *pgxpool.Pool, decryptor providerfoundation.FernetDecryptor, logger *slog.Logger) CategorizeBYO {
	store := llmorgsettings.Store{Pool: pool, Decryptor: decryptor}.ForPurpose(llmorgsettings.PurposeCategorize)
	gate := llmorgsettings.Gate{
		Source: llmorgsettings.PgSource{Licensing: licensing.PostgresStore{Pool: pool}, Queryer: pool, Config: store},
		Logger: logger,
	}
	return &orgCategorizeBYO{
		logger: logger,
		optedIn: func(ctx context.Context, orgID string) (bool, error) {
			return llmorgsettings.CategorizeBYOEnabled(ctx, pool, orgID)
		},
		gate:   gate.Decide,
		config: store.OrgConfig,
		newProvider: func(kind categorize.ProviderKind, apiKey, baseURL, model string) (categorize.Provider, error) {
			return categorize.NewProviderFromCredentials(kind, apiKey, baseURL, model)
		},
		guard: func(provider categorize.Provider, orgID string, kind categorize.ProviderKind, baseURL string) categorize.Provider {
			return categorize.WithBudget(provider, llmbudget.Guard{
				Store: llmbudget.PgStore{Pool: pool}, OrgID: orgID, Provider: string(kind), Model: provider.Model(),
				BaseURL: baseURL, Lookup: secrets.ProcessLookup,
			})
		},
		newClef: categorize.NewClefClient,
		clefBudget: func(orgID, model, postURL string) *llmbudget.Guard {
			return newClefGuard(llmbudget.PgStore{Pool: pool}, orgID, model, postURL)
		},
	}
}

// newClefGuard is the budget guard of one clef request URL: it prices the call
// by the URL it is really sent to, so an unpriced path is refused under a cap.
func newClefGuard(store llmbudget.Store, orgID, model, postURL string) *llmbudget.Guard {
	return &llmbudget.Guard{
		Store: store, OrgID: orgID, Provider: string(categorize.ClefProviderName), Model: model,
		BaseURL: postURL, Lookup: secrets.ProcessLookup,
	}
}

func (byo *orgCategorizeBYO) OptedIn(ctx context.Context, orgID string) (bool, error) {
	return byo.optedIn(ctx, orgID)
}

func refuseBYO(code string) error {
	return &BYORefusal{Code: llmorgsettings.SafeCategorizeRefusalCode(code)}
}

// Build never draws the platform model: the gate must answer Own, and any other
// verdict, or any unusable configuration, is a refusal.
func (byo *orgCategorizeBYO) Build(ctx context.Context, orgID string) (*ServedDecision, error) {
	decision := byo.gate(ctx, orgID)
	switch decision.Kind {
	case llmorgsettings.Own:
	case llmorgsettings.Platform:
		return nil, refuseBYO(llmorgsettings.CodeCategorizeBYONotConfigured)
	default:
		return nil, refuseBYO(decision.Code)
	}
	cfg, found, err := byo.config(ctx, orgID)
	if err != nil {
		byo.logger.ErrorContext(ctx, "categorize byo config unreadable", slog.String("org_id", orgID), slog.String("error", err.Error()))
		return nil, refuseBYO(string(llmorgsettings.ConfigUnreadableRow))
	}
	if !found {
		return nil, refuseBYO(llmorgsettings.CodeCategorizeBYONotConfigured)
	}
	if cfg.Model == "" {
		return nil, refuseBYO("model_required")
	}
	if llmorgsettings.NormalizeProvider(cfg.Provider) == llmorgsettings.ProviderClef {
		return byo.buildClef(ctx, orgID, cfg)
	}
	kind := categorize.ProviderKind(llmorgsettings.NormalizeProvider(cfg.Provider))
	switch kind {
	case categorize.ProviderKindMock, categorize.ProviderKindNone, categorize.ProviderKindTypeSafe:
		return nil, refuseBYO(string(llmorgsettings.ConfigUnknownProvider))
	}
	if !categorize.IsProviderKindImplemented(kind) {
		return nil, refuseBYO(string(llmorgsettings.ConfigUnknownProvider))
	}
	provider, err := byo.newProvider(kind, cfg.APIKey.Reveal(), cfg.BaseURL, cfg.Model)
	if err != nil || provider == nil {
		return nil, refuseBYO("")
	}
	if byo.guard != nil {
		provider = byo.guard(provider, orgID, kind, cfg.BaseURL)
	}
	served, err := newBYOSystemOne(provider, string(kind), byo.logger)
	if err != nil {
		_ = provider.Close()
		return nil, refuseBYO("")
	}
	return served, nil
}

// buildClef builds the backend of an org whose categorize config names clef: the
// SystemOne request goes as it is to the org's Cloudflare account, with the
// org's token. Nothing about the account or the token comes from anywhere but
// the saved config.
func (byo *orgCategorizeBYO) buildClef(ctx context.Context, orgID string, cfg llmorgsettings.OrgConfig) (*ServedDecision, error) {
	if _, ok := llmorgsettings.ClefModelSegment(cfg.Model); !ok {
		return nil, refuseBYO(llmorgsettings.CodeCategorizeBYOModelUnsupported)
	}
	postURL, ok := llmorgsettings.ClefPostURL(cfg.BaseURL, cfg.Model)
	if !ok {
		return nil, refuseBYO(string(llmorgsettings.ConfigInvalidBaseURL))
	}
	client, err := byo.newClef(categorize.ClefClientConfig{Token: cfg.APIKey, BaseURL: cfg.BaseURL, Model: cfg.Model, Logger: byo.logger})
	if err != nil {
		return nil, refuseBYO("")
	}
	var guard *llmbudget.Guard
	if byo.clefBudget != nil {
		guard = byo.clefBudget(orgID, cfg.Model, postURL)
	}
	served, err := newServedDecisionFor(clefSender{client: client, guard: guard}, byo.logger, byoProviderPrefix+llmorgsettings.ProviderClef)
	if err != nil {
		_ = client.Close()
		return nil, refuseBYO("")
	}
	served.byo = true
	return served, nil
}
