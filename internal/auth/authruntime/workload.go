package authruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// exchangeRoutes is the workload-token exchange route, mounted only with
// AUTH_WORKLOAD_EXCHANGE (the Kubernetes subject), AUTH_WORKLOAD_BOOTSTRAP
// (the Compose bootstrap subject) or AUTH_SERVICE_ACCOUNT_EXCHANGE (the service
// account key subject) on. A CA bundle that cannot be read refuses
// startup.
func exchangeRoutes(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, signer *token.Signer,
) ([]httpapi.Route, error) {
	if !cfg.WorkloadExchange.Mounted() {
		return nil, nil
	}
	store, err := workload.New(workload.Config{Pool: pool, Schema: schema, Logger: logger})
	if err != nil {
		return nil, err
	}
	deps := workloadexchange.Deps{
		RefusalReason: workload.RefusalReason,
		Signer:        signer,
		Audit: func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, pool, schema, event)
		},
		IssuerURL: cfg.TokenIssuerURL,
		Logger:    logger,
	}
	if cfg.WorkloadExchange.Enabled {
		validator, err := workloadexchange.NewReviewValidator(workloadexchange.ReviewConfig{
			APIServerURL:      cfg.WorkloadExchange.KubeAPIURL,
			CAFile:            cfg.WorkloadExchange.KubeCAFile,
			ReviewerTokenFile: cfg.WorkloadExchange.ReviewerTokenFile,
			TrustDomain:       cfg.WorkloadExchange.TrustDomain,
		})
		if err != nil {
			return nil, err
		}
		deps.Validator, deps.Registry, deps.ClusterIssuer = validator, workloadRegistry{store: store}, cfg.WorkloadExchange.ClusterIssuer
	}
	if cfg.WorkloadExchange.Bootstrap {
		deps.Bootstrap = bootstrapRegistry{store: store}
	}
	if cfg.WorkloadExchange.ServiceAccountKey {
		resolver, err := serviceaccount.NewKeyResolver(serviceaccount.KeyResolverConfig{Pool: pool, Schema: schema, Logger: logger})
		if err != nil {
			return nil, err
		}
		deps.ServiceAccountKeys = serviceAccountKeyRegistry{resolver: resolver}
	}
	return workloadexchange.Routes(deps)
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

// bootstrapRegistry adapts the workload store's bootstrap read to the
// exchange: the exchange gets the store's own Narrow over the narrowed
// audiences.
type bootstrapRegistry struct{ store *workload.Store }

func (r bootstrapRegistry) ResolveBootstrap(ctx context.Context, digest [sha256.Size]byte, now time.Time) (workloadexchange.BootstrapRegistration, error) {
	reg, err := r.store.ResolveBootstrap(ctx, digest, now)
	if err != nil {
		return workloadexchange.BootstrapRegistration{}, err
	}
	return workloadexchange.BootstrapRegistration{
		Registration: workloadexchange.Registration{
			PrincipalID: reg.PrincipalID, Revision: reg.Revision,
			Audiences: append([]string(nil), reg.Audiences...), Narrow: reg.Narrow,
		},
		CredentialID: reg.CredentialID, ExpiresAt: reg.ExpiresAt,
	}, nil
}

// keyResolver is the read the key adapter needs (*serviceaccount.KeyResolver).
type keyResolver interface {
	ResolveKey(ctx context.Context, digest [sha256.Size]byte, now time.Time) (serviceaccount.ResolvedKey, error)
}

// serviceAccountKeyRegistry adapts ResolveKey to the exchange. The exchange
// gets the key row, the account, its organization, the expiry and the revision;
// the key's audiences and scopes are not passed on, so no rule can read them.
type serviceAccountKeyRegistry struct{ resolver keyResolver }

func (r serviceAccountKeyRegistry) ResolveServiceAccountKey(ctx context.Context, digest [sha256.Size]byte, now time.Time) (workloadexchange.ServiceAccountKeyRegistration, error) {
	key, err := r.resolver.ResolveKey(ctx, digest, now)
	if err != nil {
		var refusal *serviceaccount.KeyRefusal
		if errors.As(err, &refusal) {
			return workloadexchange.ServiceAccountKeyRegistration{}, &workloadexchange.KeyRefusal{Reason: refusal.Reason}
		}
		return workloadexchange.ServiceAccountKeyRegistration{}, err
	}
	return workloadexchange.ServiceAccountKeyRegistration{
		CredentialID: key.CredentialID, PrincipalID: key.Principal, OrgID: key.OrgID, ExpiresAt: key.ExpiresAt, Revision: key.Revision,
	}, nil
}
