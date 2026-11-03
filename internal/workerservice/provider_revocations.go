// provider_revocations.go registers the sync.provider_oauth_revocations worker
// with the sync coordinator family: the always-on family that already holds the
// settings encryption key and the PagerDuty OAuth client identity.

package workerservice

import (
	"context"
	"log/slog"

	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/providerrevocation"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// registerProviderRevocationsWorker adds the revocation worker and returns its
// spec. It returns (nil, nil) only when the registry does not route the kind to
// River. An unset PagerDuty client id still registers it: each run then touches
// no row and says so.
func registerProviderRevocationsWorker(
	cfg config.Config,
	registry *jobruntime.Registry,
	database *postgresWorkerDatabase,
	cipher providerfoundation.FernetDecryptor,
	observer jobruntime.Observer,
	logger *slog.Logger,
	workers *river.Workers,
) (*jobruntime.HandlerSpec, error) {
	spec, ok := registry.Descriptor(jobcontract.KindProviderOAuthRevocations)
	if !ok {
		return nil, errWorkerDependencyUnavailable
	}
	if !spec.Executable() {
		return nil, nil
	}
	revokeConfig := providerfoundation.PagerDutyRevokeConfig{
		ClientID:     cfg.PagerDutyOAuthClientID.Reveal(),
		ClientSecret: cfg.PagerDutyOAuthSecret.Reveal(),
	}
	revoke := func(ctx context.Context, token string) error {
		return providerfoundation.RevokePagerDutyOAuthToken(ctx, nil, revokeConfig, token)
	}
	handler, err := providerrevocation.NewHandler(
		providerrevocation.PgStore{DB: database.pools.Domain}, cipher, revoke, revokeConfig.ClientID != "", logger,
	)
	if err != nil {
		return nil, err
	}
	idempotency, err := newOperationalIdempotency(database.pools.Domain, observer)
	if err != nil {
		return nil, err
	}
	adapter, err := jobruntime.NewAdapter[jobruntime.ProviderOAuthRevocationsArgs](
		registry, spec, handler, jobruntime.Dependencies{
			Logger: logger, Observer: observer,
			TenantScope: operationalTenantScope{},
			Budget:      newOperationalBudget(database.pools.Domain, observer),
			Idempotency: idempotency,
		},
	)
	if err != nil {
		return nil, err
	}
	if err := river.AddWorkerSafely(workers, adapter); err != nil {
		return nil, err
	}
	registered := adapter.Spec()
	return &registered, nil
}
