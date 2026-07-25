package report

import (
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

type RuntimeAdapters struct {
	OnDemand  *jobruntime.Adapter[jobruntime.OnDemandReportExecutionArgs]
	Scheduled *jobruntime.Adapter[jobruntime.ScheduledReportExecutionArgs]
}

func NewProductionDependencies(
	domainPool *pgxpool.Pool,
	clickhouseConnection driver.Conn,
) (Dependencies, error) {
	runs, err := NewPostgresRunStore(domainPool)
	if err != nil {
		return Dependencies{}, err
	}
	loader, err := NewPostgresReportLoader(domainPool)
	if err != nil {
		return Dependencies{}, err
	}
	query, err := NewClickHouseQueryAdapter(loader, clickhouseConnection)
	if err != nil {
		return Dependencies{}, err
	}
	dependencies := Dependencies{
		Runs: runs, Query: query, Renderer: NewDeterministicRenderer(),
		Artifacts: NewSHA256ArtifactAdapter(), Notifications: NewInAppNotificationAdapter(),
	}
	return dependencies, dependencies.validate()
}

func NewProductionRuntimeAdapters(
	registry *jobruntime.Registry,
	domainPool *pgxpool.Pool,
	clickhouseConnection driver.Conn,
	runtimeDependencies jobruntime.Dependencies,
) (*RuntimeAdapters, error) {
	dependencies, err := NewProductionDependencies(domainPool, clickhouseConnection)
	if err != nil {
		return nil, err
	}
	return NewRuntimeAdapters(registry, dependencies, runtimeDependencies)
}

func NewRuntimeAdapters(
	registry *jobruntime.Registry,
	dependencies Dependencies,
	runtimeDependencies jobruntime.Dependencies,
) (*RuntimeAdapters, error) {
	if registry == nil {
		return nil, ErrDependencyUnavailable
	}
	if err := dependencies.validate(); err != nil {
		return nil, err
	}
	onDemandSpec, onDemandOK := registry.Descriptor(jobcontract.KindReportExecuteOnDemand)
	scheduledSpec, scheduledOK := registry.Descriptor(jobcontract.KindReportExecuteScheduled)
	if !onDemandOK || !scheduledOK {
		return nil, ErrContractMismatch
	}
	onDemand, err := jobruntime.NewAdapter(
		registry, onDemandSpec, NewOnDemandHandler(dependencies), runtimeDependencies,
	)
	if err != nil {
		return nil, err
	}
	scheduled, err := jobruntime.NewAdapter(
		registry, scheduledSpec, NewScheduledHandler(dependencies), runtimeDependencies,
	)
	if err != nil {
		return nil, err
	}
	return &RuntimeAdapters{OnDemand: onDemand, Scheduled: scheduled}, nil
}

// Register adds both independently-routed report kinds to a River worker set.
// Production does not call this while their checked-in routes remain Celery.
func (adapters *RuntimeAdapters) Register(workers *river.Workers) error {
	if adapters == nil || adapters.OnDemand == nil || adapters.Scheduled == nil || workers == nil {
		return ErrDependencyUnavailable
	}
	river.AddWorker(workers, adapters.OnDemand)
	river.AddWorker(workers, adapters.Scheduled)
	return nil
}

func (adapters *RuntimeAdapters) Specs() ([]jobruntime.HandlerSpec, error) {
	if adapters == nil || adapters.OnDemand == nil || adapters.Scheduled == nil {
		return nil, ErrDependencyUnavailable
	}
	onDemand := adapters.OnDemand.Spec()
	scheduled := adapters.Scheduled.Spec()
	if onDemand.Kind == scheduled.Kind {
		return nil, errors.New("report route kinds must be independent")
	}
	return []jobruntime.HandlerSpec{onDemand, scheduled}, nil
}

type RouteCapability struct {
	Kind          string
	Compiled      bool
	Route         string
	RollbackRoute string
	Executable    bool
}

func RouteCapabilities(registry *jobruntime.Registry) ([]RouteCapability, error) {
	if registry == nil {
		return nil, ErrDependencyUnavailable
	}
	result := make([]RouteCapability, 0, 2)
	for _, kind := range []string{
		jobcontract.KindReportExecuteOnDemand,
		jobcontract.KindReportExecuteScheduled,
	} {
		descriptor, ok := registry.Descriptor(kind)
		if !ok || descriptor.RollbackRoute != "celery" {
			return nil, ErrContractMismatch
		}
		result = append(result, RouteCapability{
			Kind: kind, Compiled: true, Route: descriptor.Route,
			RollbackRoute: descriptor.RollbackRoute, Executable: descriptor.Executable(),
		})
	}
	return result, nil
}
