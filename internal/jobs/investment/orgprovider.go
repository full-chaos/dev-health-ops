package investment

// orgprovider.go makes the categorize provider org-aware (CHAOS-10041): an org
// with a categorize override, else a default BYO row, categorizes with its own
// provider, model and key. The heavy worker does not hold the settings
// encryption key, so the resolved config comes from the api
// (internal/orgllmconfig); the key never reaches this process in stored form.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/orgllmconfig"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// orgConfigFetcher is what the executor needs of orgllmconfig.Client.
type orgConfigFetcher interface {
	Fetch(ctx context.Context, orgID string) (orgllmconfig.Config, error)
}

// SetOrgLLMConfig turns on org-aware categorization. A nil client keeps the
// platform environment as the only source. budget, when set, caps an org's BYO
// spend exactly as the explain routes do.
func (executor *NativeExecutor) SetOrgLLMConfig(client orgConfigFetcher, budget llmbudget.Store) {
	executor.orgConfig = client
	executor.orgBudget = budget
}

// providerForRun builds the provider of one run and the model to stamp. byo is
// true when the org's own config supplied it. When it is false the stamp
// model is the request's model_ref, as before.
//
// An org whose config is found but cannot be used, or cannot be read, FAILS the
// run: a BYO org is never silently moved to the platform provider, which would
// spend the platform's money on an org that chose its own key.
func (executor *NativeExecutor) providerForRun(
	ctx context.Context, orgID, requested, model string,
) (provider categorize.Provider, kind categorize.ProviderKind, stampModel string, err error) {
	provider, kind, stampModel, _, err = executor.providerForRunSource(ctx, orgID, requested, model)
	return provider, kind, stampModel, err
}

// providerForRunSource is providerForRun that also reports whether the org's
// own config supplied the provider.
func (executor *NativeExecutor) providerForRunSource(
	ctx context.Context, orgID, requested, model string,
) (provider categorize.Provider, kind categorize.ProviderKind, stampModel string, byo bool, err error) {
	byo, kind, stampModel, provider, err = executor.orgProvider(ctx, orgID, requested, model)
	if err != nil {
		return nil, "", "", false, err
	}
	if byo {
		return provider, kind, stampModel, true, nil
	}
	provider, kind, err = executor.newProvider(requested, model)
	return provider, kind, model, false, err
}

func (executor *NativeExecutor) orgProvider(
	ctx context.Context, orgID, requested, model string,
) (byo bool, kind categorize.ProviderKind, stampModel string, provider categorize.Provider, err error) {
	if executor.orgConfig == nil || orgID == "" {
		return false, "", "", nil, nil
	}
	cfg, fetchErr := executor.orgConfig.Fetch(ctx, orgID)
	switch {
	case errors.Is(fetchErr, orgllmconfig.ErrNotConfigured):
		return false, "", "", nil, nil
	case errors.Is(fetchErr, orgllmconfig.ErrRefused):
		executor.logger.Error("investment materialize: the api refused the org llm config; the run fails closed",
			slog.String("org_id", orgID), slog.String("error", fetchErr.Error()))
		return false, "", "", nil, workgraph.Deterministic(workgraph.ClassLLMProviderInvalid,
			fmt.Errorf("org llm config: %w", fetchErr))
	case fetchErr != nil:
		executor.logger.Error("investment materialize: the org llm config could not be read; the run fails closed and is retried",
			slog.String("org_id", orgID), slog.String("error", fetchErr.Error()))
		return false, "", "", nil, fmt.Errorf("org llm config: %w", fetchErr)
	}
	// An explicit provider request for another provider than the org's is a
	// platform request: source-bound, never mixing the org's key with it.
	if asked := strings.ToLower(strings.TrimSpace(requested)); asked != "" && asked != "auto" && asked != cfg.Provider {
		return false, "", "", nil, nil
	}
	refuse := func(reason string) error {
		executor.logger.Error("investment materialize: the org llm config cannot be used; the run fails closed",
			slog.String("org_id", orgID), slog.String("provider", cfg.Provider), slog.String("reason", reason))
		return workgraph.Deterministic(workgraph.ClassLLMProviderInvalid,
			fmt.Errorf("org llm config for provider %q cannot be used: %s", cfg.Provider, reason))
	}
	if !llmorgsettings.IsKnownProvider(cfg.Provider) {
		return false, "", "", nil, refuse("unknown provider")
	}
	// The provider name is already normalized and known; it names its kind
	// directly, never through the platform selection (LLM_PROVIDER).
	kind = categorize.ProviderKind(cfg.Provider)
	if !categorize.IsProviderKindImplemented(kind) || categorize.IsDecisionProviderKind(kind) {
		return false, "", "", nil, refuse("provider has no native client")
	}
	if !llmorgsettings.CredentialsComplete(cfg.Provider, cfg.APIKey.Reveal()) {
		return false, "", "", nil, refuse("api key missing")
	}
	if ok, reason, urlErr := llmorgsettings.ValidateBaseURLChecked(ctx, cfg.BaseURL); urlErr != nil || !ok {
		if reason == "" {
			reason = "base url rejected"
		}
		return false, "", "", nil, refuse(reason)
	}
	// model_ref on the request is an explicit model and wins; else the org's.
	useModel := model
	if useModel == "" {
		useModel = cfg.Model
	}
	built, buildErr := categorize.NewProviderFromCredentials(kind, cfg.APIKey.Reveal(), cfg.BaseURL, useModel)
	if buildErr != nil {
		return false, "", "", nil, refuse(secrets.RedactRegistered(buildErr.Error()))
	}
	if executor.orgBudget != nil {
		built = categorize.WithBudget(built, llmbudget.Guard{
			Store: executor.orgBudget, OrgID: orgID, Provider: string(kind), Model: built.Model(),
			BaseURL: cfg.BaseURL, Lookup: secrets.ProcessLookup,
		})
	}
	executor.logger.Info("investment materialize: categorizing with the org's own llm config",
		slog.String("org_id", orgID), slog.String("provider", string(kind)),
		slog.String("model", built.Model()), slog.String("config_version", cfg.Version))
	return true, kind, built.Model(), built, nil
}
