package authruntime

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// exchangeRoutes is the workload-token exchange route, mounted only with
// AUTH_WORKLOAD_EXCHANGE on. A CA bundle that cannot be read refuses startup.
func exchangeRoutes(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, signer *token.Signer,
) ([]httpapi.Route, error) {
	if !cfg.WorkloadExchange.Enabled {
		return nil, nil
	}
	store, err := workload.New(workload.Config{Pool: pool, Schema: schema, Logger: logger})
	if err != nil {
		return nil, err
	}
	validator, err := workloadexchange.NewReviewValidator(workloadexchange.ReviewConfig{
		APIServerURL:      cfg.WorkloadExchange.KubeAPIURL,
		CAFile:            cfg.WorkloadExchange.KubeCAFile,
		ReviewerTokenFile: cfg.WorkloadExchange.ReviewerTokenFile,
		TrustDomain:       cfg.WorkloadExchange.TrustDomain,
	})
	if err != nil {
		return nil, err
	}
	return workloadexchange.Routes(workloadexchange.Deps{
		Validator:     validator,
		Registry:      workloadRegistry{store: store},
		RefusalReason: workload.RefusalReason,
		Signer:        signer,
		Audit: func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, pool, schema, event)
		},
		IssuerURL:     cfg.TokenIssuerURL,
		ClusterIssuer: cfg.WorkloadExchange.ClusterIssuer,
		Logger:        logger,
	})
}

// workloadRegistry adapts the workload store to the exchange: the exchange
// gets the store's own Narrow, never a copy of its rule.
type workloadRegistry struct{ store *workload.Store }

func (r workloadRegistry) Resolve(ctx context.Context, id workloadexchange.Identity) (workloadexchange.Registration, error) {
	reg, err := r.store.Resolve(ctx, workload.ResolveInput{
		TrustDomain: id.TrustDomain, Issuer: id.Issuer, Namespace: id.Namespace,
		ServiceAccountName: id.ServiceAccountName, ServiceAccountUID: id.ServiceAccountUID,
	})
	if err != nil {
		return workloadexchange.Registration{}, err
	}
	return workloadexchange.Registration{
		PrincipalID: reg.PrincipalID, BindingID: reg.BindingID, Revision: reg.Revision,
		Audiences: append([]string(nil), reg.Audiences...), Narrow: reg.Narrow,
	}, nil
}
