package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/operational"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type operationalWorkerComponent struct{ client *river.Client[pgx.Tx] }

func (component operationalWorkerComponent) Name() string { return "river-operational-worker" }
func (component operationalWorkerComponent) Start(ctx context.Context) error {
	return component.client.Start(ctx)
}
func (component operationalWorkerComponent) Shutdown(ctx context.Context) error {
	return component.client.Stop(ctx)
}

func buildOperationalWorker(
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (lifecycle.Component, []jobruntime.HandlerSpec, error) {
	if cfg.Profile != "ops" || registry == nil {
		return nil, nil, nil
	}
	profile := registry.Profile("ops")
	executable := 0
	for _, descriptor := range profile {
		if descriptor.Executable() {
			executable++
		}
	}
	if executable == 0 {
		return nil, nil, nil
	}
	// The process-level readiness contract is all-or-nothing. Never start a
	// partial queue consumer before every checked-in ops handler has concrete
	// coverage; later system-handler work completes the same profile.
	if executable != len(profile) {
		return nil, nil, errWorkerDependencyUnavailable
	}
	specs := make([]jobruntime.HandlerSpec, 0, 2)
	for _, kind := range []string{
		jobcontract.KindBillingNotification,
		jobcontract.KindWebhookDelivery,
	} {
		descriptor, ok := registry.Descriptor(kind)
		if ok && descriptor.Executable() {
			specs = append(specs, descriptor)
		}
	}
	if len(specs) != len(profile) {
		return nil, nil, errWorkerDependencyUnavailable
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || observer == nil || logger == nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	baseURL := strings.TrimRight(cfg.OperationalBridgeURL, "/")
	dispatcher, err := operational.NewHTTPDispatcher(
		&http.Client{Timeout: cfg.OperationalBridgeTimeout},
		operational.HTTPDispatcherConfig{
			WebhookEndpoint: baseURL + "/api/internal/worker-operational/webhook",
			BillingEndpoint: baseURL + "/api/internal/worker-operational/billing",
			BearerToken:     cfg.OperationalBridgeToken.Reveal(),
		},
	)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	store, err := operational.NewPostgresStore(postgresDatabase.pools.Domain)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	idempotency, err := jobruntime.NewPostgresIdempotency(postgresDatabase.pools.Domain)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	dependencies := jobruntime.Dependencies{
		Logger: logger, Observer: observer, TenantScope: operationalTenantScope{},
		Budget: newOperationalBudget(), Idempotency: idempotency,
	}
	workers := river.NewWorkers()
	registered := make([]jobruntime.HandlerSpec, 0, len(specs))
	for _, spec := range specs {
		switch spec.Kind {
		case jobcontract.KindBillingNotification:
			handler, handlerErr := operational.NewBillingHandler(store, dispatcher)
			if handlerErr != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			adapter, adapterErr := jobruntime.NewAdapter[jobruntime.BillingNotificationArgs](
				registry, spec, handler, dependencies,
			)
			if adapterErr != nil || river.AddWorkerSafely(workers, adapter) != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			registered = append(registered, adapter.Spec())
		case jobcontract.KindWebhookDelivery:
			handler, handlerErr := operational.NewWebhookHandler(store, dispatcher)
			if handlerErr != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			adapter, adapterErr := jobruntime.NewAdapter[jobruntime.WebhookDeliveryArgs](
				registry, spec, handler, dependencies,
			)
			if adapterErr != nil || river.AddWorkerSafely(workers, adapter) != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			registered = append(registered, adapter.Spec())
		}
	}
	queues := map[string]river.QueueConfig{"webhooks": {MaxWorkers: 4}}
	client, err := river.NewClient(riverpgxv5.New(postgresDatabase.pools.QueueControl), &river.Config{
		Logger: logger, Queues: queues, Schema: cfg.RiverDatabaseSchema, Workers: workers,
	})
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	return operationalWorkerComponent{client: client}, registered, nil
}

type operationalTenantScope struct{}

func (operationalTenantScope) Supports(scope string) bool {
	return scope == "global" || scope == "tenant"
}

func (operationalTenantScope) Resolve(ctx context.Context, request jobruntime.ScopeRequest) (context.Context, error) {
	if ctx == nil || (request.OrganizationScope != "global" && request.OrganizationScope != "tenant") {
		return nil, errors.New("operational tenant scope is unavailable")
	}
	if request.OrganizationScope == "tenant" {
		if request.OrganizationID == nil {
			return nil, jobruntime.DomainMismatch(errors.New("tenant organization is missing"))
		}
		if _, err := uuid.Parse(*request.OrganizationID); err != nil {
			return nil, jobruntime.DomainMismatch(errors.New("tenant organization is invalid"))
		}
	}
	return ctx, nil
}

type operationalBudget struct {
	mu     sync.Mutex
	limits map[string]chan struct{}
}

func newOperationalBudget() *operationalBudget {
	return &operationalBudget{limits: make(map[string]chan struct{})}
}

func (*operationalBudget) Supports(scope string, limit int) bool {
	return scope == "organization" && limit > 0 && limit <= 32
}

func (budget *operationalBudget) Acquire(ctx context.Context, request jobruntime.BudgetRequest) (jobruntime.BudgetLease, error) {
	if budget == nil || !budget.Supports(request.ConcurrencyScope, request.ConcurrencyLimit) {
		return nil, errors.New("operational budget is unavailable")
	}
	key := "global"
	if request.OrganizationID != nil {
		key = *request.OrganizationID
	}
	budget.mu.Lock()
	semaphore, ok := budget.limits[key]
	if !ok {
		semaphore = make(chan struct{}, request.ConcurrencyLimit)
		budget.limits[key] = semaphore
	}
	budget.mu.Unlock()
	select {
	case semaphore <- struct{}{}:
		return &operationalBudgetLease{release: func() { <-semaphore }}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type operationalBudgetLease struct {
	once    sync.Once
	release func()
}

func (lease *operationalBudgetLease) Release() {
	lease.once.Do(lease.release)
}
