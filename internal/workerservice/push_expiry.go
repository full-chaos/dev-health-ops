// push_expiry.go registers the sync.external_push_expiry_sweep worker with the
// sync coordinator family, beside the provider revocation worker.

package workerservice

import (
	"log/slog"
	"strings"

	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/pushexpiry"
	envsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// envAuthSchema names the auth schema (default "auth"), as go-api reads it.
const envAuthSchema = "AUTH_DATABASE_SCHEMA"

// registerPushExpiryWorker adds the expiry sweep worker and returns its spec.
// It returns (nil, nil) only when the registry does not route the kind to
// River. AUTH_PUSH_LIFECYCLE_EVENTS off still registers it: each run then
// touches nothing and says so.
func registerPushExpiryWorker(
	registry *jobruntime.Registry,
	database *postgresWorkerDatabase,
	observer jobruntime.Observer,
	logger *slog.Logger,
	workers *river.Workers,
) (*jobruntime.HandlerSpec, error) {
	spec, ok := registry.Descriptor(jobcontract.KindExternalPushExpirySweep)
	if !ok {
		return nil, errWorkerDependencyUnavailable
	}
	if !spec.Executable() {
		return nil, nil
	}
	on, err := pushevent.ParsePushLifecycleEvents(envsecrets.ProcessLookup)
	if err != nil {
		return nil, err
	}
	schemaName := strings.TrimSpace(envsecrets.ProcessGetenv(envAuthSchema))
	if schemaName == "" {
		schemaName = "auth"
	}
	schema, err := authschema.NewValidatedIdentifier(schemaName)
	if err != nil {
		return nil, err
	}
	handler, err := pushexpiry.NewHandler(pushexpiry.PgStore{DB: database.pools.Domain, Schema: schema}, on, logger)
	if err != nil {
		return nil, err
	}
	idempotency, err := newOperationalIdempotency(database.pools.Domain, observer)
	if err != nil {
		return nil, err
	}
	adapter, err := jobruntime.NewAdapter[jobruntime.ExternalPushExpirySweepArgs](
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
