package investment

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

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
