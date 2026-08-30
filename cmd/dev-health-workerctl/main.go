// dev-health-workerctl is the authenticated, payload-redacted River operator
// CLI. It deliberately has no network listener and accepts credentials only
// through WORKER_OPERATOR_TOKEN or WORKER_OPERATOR_TOKEN_FILE.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	serviceName            = "dev-health-workerctl"
	operatorAdvisoryKey    = int64(30330001)
	defaultDomainRole      = "devhealth_domain"
	defaultQueueRole       = "devhealth_queue"
	defaultCoordinatorRole = "devhealth_coordinator"
)

func main() {
	os.Exit(execute(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

type operatorRuntime struct {
	service               *joboperator.Service
	principal             joboperator.Principal
	pools                 *postgresstore.RuntimePools
	lockTx                pgx.Tx
	streamDeploymentState string
	streams               []streamProfileStatus
	queueStatusSource     workerQueueStatusSource
	queueControlMode      platformconfig.QueueControlMode
	// registry is the same job-descriptor registry configureRuntime already
	// loads for jobRouteController -- `metrics daily-redrive` (CHAOS-4358)
	// reuses it to construct a daily.PostgresPublisher without a second
	// contracts/jobs/v1 load.
	registry *jobruntime.Registry
	// lookup is configureRuntime's own env accessor, kept for verbs that need
	// a SECOND secret this binary is not otherwise wired for -- `providersync
	// retire-linear-pseudo-projects` (CHAOS-4530 follow-up) resolves
	// CLICKHOUSE_URI lazily, on dispatch, rather than making every workerctl
	// invocation require a ClickHouse connection it does not otherwise need.
	lookup platformsecrets.LookupEnv
}

type streamProfileStatus struct {
	Profile          string `json:"profile"`
	Owner            string `json:"owner"`
	EnabledByDefault bool   `json:"enabled_by_default"`
	MinReplicas      int    `json:"min_replicas"`
	MaxReplicas      int    `json:"max_replicas"`
}

// workerQueueStatus is the operator view for a River worker group and its
// explicit canonical queue set. Stream runners keep profile-based commands.
type workerQueueStatus struct {
	Group                string   `json:"group"`
	Queues               []string `json:"queues"`
	ConfiguredInManifest bool     `json:"configured_in_manifest"`
	DesiredReplicas      int      `json:"desired_replicas"`
	LiveReplicas         int      `json:"live_replicas"`
	MaxReplicas          int      `json:"max_replicas"`
	QueueBacklog         int64    `json:"queue_backlog"`
	ActiveJobs           int64    `json:"active_jobs"`
	DrainState           string   `json:"drain_state"`
}

type workerQueueStatusSource interface {
	Status(context.Context) (workerQueueStatusResponse, error)
}

type workerQueueStatusSourceFunc func(context.Context) (workerQueueStatusResponse, error)

func (source workerQueueStatusSourceFunc) Status(ctx context.Context) (workerQueueStatusResponse, error) {
	return source(ctx)
}

type streamStatusResponse struct {
	DeploymentState string                `json:"deployment_state"`
	Profiles        []streamProfileStatus `json:"profiles"`
}

type workerQueueStatusResponse struct {
	DeploymentState  string                       `json:"deployment_state"`
	ConnectionBudget workerConnectionBudgetStatus `json:"connection_budget"`
	Groups           []workerQueueStatus          `json:"groups"`
}

type connectionBudgetStatus struct {
	Used     int `json:"used"`
	Limit    int `json:"limit"`
	Headroom int `json:"headroom"`
}

type workerConnectionBudgetStatus struct {
	QueueSession       connectionBudgetStatus `json:"queue_session"`
	CoordinatorSession connectionBudgetStatus `json:"coordinator_session"`
	DomainTransaction  connectionBudgetStatus `json:"domain_transaction"`
	Server             connectionBudgetStatus `json:"server"`
}

type manifestQueueStatusSource struct {
	service   *joboperator.Service
	principal joboperator.Principal
	manifest  deploymentcontract.Manifest
	budget    deploymentcontract.BudgetSummary
	presence  func(context.Context) ([]jobruntime.WorkerPresenceSummary, error)
}

func (source manifestQueueStatusSource) Status(ctx context.Context) (workerQueueStatusResponse, error) {
	if source.service == nil || source.presence == nil {
		return workerQueueStatusResponse{}, errors.New("queue status source is unavailable")
	}
	presenceRows, err := source.presence(ctx)
	if err != nil {
		return workerQueueStatusResponse{}, err
	}
	presence := make(map[string]jobruntime.WorkerPresenceSummary, len(presenceRows))
	for _, summary := range presenceRows {
		if _, duplicate := presence[summary.WorkerGroup]; duplicate {
			return workerQueueStatusResponse{}, errors.New("duplicate worker presence summary")
		}
		presence[summary.WorkerGroup] = summary
	}
	groups := make([]workerQueueStatus, 0, len(source.manifest.Processes)+len(presence))
	appendGroup := func(group string, queues []string, summary *jobruntime.WorkerPresenceSummary, desired, maximum int, configured bool) error {
		queueSummaries, err := source.service.Queues(ctx, source.principal, group, queues)
		if err != nil {
			return err
		}
		status := workerQueueStatus{
			Group: group, Queues: append([]string(nil), queues...),
			ConfiguredInManifest: configured,
			DesiredReplicas:      desired, MaxReplicas: maximum, DrainState: "inactive",
		}
		if summary != nil {
			status.LiveReplicas = summary.Live
			if summary.Draining > 0 {
				status.DrainState = "draining"
			} else {
				status.DrainState = "active"
			}
		}
		for _, queue := range queueSummaries {
			status.QueueBacklog += queue.Available + queue.Retryable + queue.Scheduled
			status.ActiveJobs += queue.Running
			if queue.Paused {
				status.DrainState = "draining"
			}
		}
		groups = append(groups, status)
		return nil
	}
	for _, process := range source.manifest.Processes {
		if process.Runtime != "river" {
			continue
		}
		if len(process.Queues) == 0 {
			return workerQueueStatusResponse{}, errors.New("river process is missing explicit queues")
		}
		queues := append([]string(nil), process.Queues...)
		var live *jobruntime.WorkerPresenceSummary
		if summary, ok := presence[process.Name]; ok {
			if !slices.Equal(summary.Queues, queues) {
				return workerQueueStatusResponse{}, errors.New("worker presence queue set differs from the deployment manifest")
			}
			live = &summary
			delete(presence, process.Name)
		}
		if err := appendGroup(process.Name, queues, live, process.DesiredReplicas, process.MaxReplicas, true); err != nil {
			return workerQueueStatusResponse{}, err
		}
	}
	customGroups := make([]string, 0, len(presence))
	for group := range presence {
		customGroups = append(customGroups, group)
	}
	sort.Strings(customGroups)
	for _, group := range customGroups {
		summary := presence[group]
		if err := appendGroup(group, summary.Queues, &summary, 0, 0, false); err != nil {
			return workerQueueStatusResponse{}, err
		}
	}
	if len(groups) == 0 {
		return workerQueueStatusResponse{}, errors.New("deployment manifest has no River worker groups")
	}
	return workerQueueStatusResponse{
		DeploymentState: source.manifest.DeploymentState,
		ConnectionBudget: workerConnectionBudgetStatus{
			QueueSession: connectionBudgetStatus{
				Used: source.budget.QueueSessionClientConnections,
				Limit: min(source.manifest.PostgresBudget.PgBouncerQueueSessionMaxClientConnections,
					source.manifest.PostgresBudget.PgBouncerQueueSessionPoolSize),
				Headroom: source.budget.QueueSessionHeadroom,
			},
			CoordinatorSession: connectionBudgetStatus{
				Used: source.budget.CoordinatorSessionClientConnections,
				Limit: min(source.manifest.PostgresBudget.PgBouncerCoordinatorSessionMaxClientConnections,
					source.manifest.PostgresBudget.PgBouncerCoordinatorSessionPoolSize),
				Headroom: source.budget.CoordinatorSessionHeadroom,
			},
			DomainTransaction: connectionBudgetStatus{
				Used:     source.budget.DomainTransactionClientConnections,
				Limit:    source.manifest.PostgresBudget.PgBouncerTransactionMaxClientConnections,
				Headroom: source.budget.DomainTransactionHeadroom,
			},
			Server: connectionBudgetStatus{
				Used:     source.budget.ServerConnectionFootprint,
				Limit:    source.manifest.PostgresBudget.ServerMaxConnections,
				Headroom: source.budget.ServerConnectionHeadroom,
			},
		},
		Groups: groups,
	}, nil
}

func (runtime *operatorRuntime) close() {
	if runtime == nil {
		return
	}
	if runtime.lockTx != nil {
		rollbackOperatorLock(runtime.lockTx)
	}
	if runtime.pools != nil {
		runtime.pools.Close()
	}
}

func execute(parent context.Context, args []string, lookup platformsecrets.LookupEnv, stdout, stderr io.Writer) int {
	if len(args) == 1 && args[0] == "--version" {
		if err := version.Current(serviceName).WriteJSON(stdout); err != nil {
			return writeError(stderr, "output_unavailable")
		}
		return 0
	}
	if len(args) > 0 && args[0] == "workers" {
		args = args[1:]
	}
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}

	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	runtime, code := configureRuntime(ctx, lookup, stderr)
	if runtime == nil {
		return code
	}
	defer runtime.close()
	return dispatch(ctx, runtime, args, stdout, stderr)
}

func configureRuntime(ctx context.Context, lookup platformsecrets.LookupEnv, stderr io.Writer) (*operatorRuntime, int) {
	domainURI, ok := resolveRequired("POSTGRES_URI", lookup)
	if !ok {
		return nil, writeError(stderr, "configuration_error")
	}
	queueURI, ok := resolveRequired("WORKER_DATABASE_URI", lookup)
	if !ok {
		return nil, writeError(stderr, "configuration_error")
	}
	// Required, not optional: workerctl is a coordinator binary. Its very first
	// database action (authenticating the operator token against
	// internal_service_credentials) is a coordinator-exclusive read, so without
	// this DSN the whole CLI is non-functional. Failing here with
	// configuration_error is the honest outcome; falling back to the domain pool
	// would reproduce the 42501 this change exists to remove.
	coordinatorURI, ok := resolveRequired("COORDINATOR_DATABASE_URI", lookup)
	if !ok {
		return nil, writeError(stderr, "configuration_error")
	}
	token, ok := resolveRequired("WORKER_OPERATOR_TOKEN", lookup)
	if !ok {
		return nil, writeError(stderr, joboperator.ReasonAuthenticationFailed)
	}
	mode := databaseMode(lookup, "WORKER_DATABASE_MODE")
	if !sessionSafeMode(mode) {
		return nil, writeError(stderr, "queue_control_mode_unsupported")
	}
	coordinatorMode := databaseMode(lookup, "COORDINATOR_DATABASE_MODE")
	if !sessionSafeMode(coordinatorMode) {
		return nil, writeError(stderr, "coordinator_database_mode_unsupported")
	}
	domainRole := resolveName("RIVER_DOMAIN_DATABASE_ROLE", defaultDomainRole, lookup)
	queueRole := resolveName("RIVER_QUEUE_DATABASE_ROLE", defaultQueueRole, lookup)
	coordinatorRole := resolveName("RIVER_COORDINATOR_DATABASE_ROLE", defaultCoordinatorRole, lookup)
	schema := resolveName("RIVER_DATABASE_SCHEMA", "river", lookup)
	domainTransactionPooler := false
	if raw, configured := lookup("PGBOUNCER_TRANSACTION_MODE"); configured && raw != "" {
		var err error
		domainTransactionPooler, err = strconv.ParseBool(raw)
		if err != nil {
			return nil, writeError(stderr, "configuration_error")
		}
	}

	runtimeConfig := postgresstore.DefaultRuntimeConfig(
		domainURI.Reveal(), queueURI.Reveal(), domainRole, queueRole,
	).WithCoordinator()
	runtimeConfig.QueueControlMode = mode
	runtimeConfig.CoordinatorMode = coordinatorMode
	runtimeConfig.RiverSchema = schema
	runtimeConfig.DomainTransactionPooler = domainTransactionPooler
	runtimeConfig.DomainMaxConns = 2
	runtimeConfig.QueueMaxConns = 2
	runtimeConfig.CoordinatorURI = coordinatorURI.Reveal()
	runtimeConfig.CoordinatorRole = coordinatorRole
	// 2 matches operator_cli.coordinator_max_connections in
	// deploy/go-workers/deployment.json, the same way DomainMaxConns and
	// QueueMaxConns above match their fields there.
	runtimeConfig.CoordinatorMaxConns = 2
	pools, err := postgresstore.OpenRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, writeError(stderr, "database_unavailable")
	}
	failed := true
	defer func() {
		if failed {
			pools.Close()
		}
	}()
	// All three postures are checked. Cross-role attribution is a distributed
	// property: each CheckRolePosture call only ever proves its OWN role holds
	// nothing beyond its manifest, so the coordinator check is what would catch
	// a coordinator-exclusive privilege wrongly granted to the coordinator's
	// login, and the domain check is what catches the mirror image.
	coordinatorPool, err := pools.CoordinatorPool()
	if err != nil {
		return nil, writeError(stderr, "database_unavailable")
	}
	if postgresstore.CheckDomainAuthorization(ctx, pools.Domain, domainRole, schema) != nil ||
		postgresstore.CheckQueueAuthorization(ctx, pools.QueueControl, queueRole, schema) != nil ||
		postgresstore.CheckCoordinatorAuthorization(ctx, coordinatorPool, coordinatorRole, schema) != nil {
		return nil, writeError(stderr, "runtime_role_unauthorized")
	}

	// Coordinator pool: reads and updates internal_service_credentials, which
	// is coordinator-exclusive and has no domain grant at all.
	authenticator, err := joboperator.NewAuthenticator(coordinatorPool)
	if err != nil {
		return nil, writeError(stderr, joboperator.ReasonAuthenticationFailed)
	}
	authentication, err := authenticator.Authenticate(ctx, token.Reveal())
	if err != nil {
		// The reason code comes from joboperator's bounded vocabulary rather
		// than from the error text. A 42501 on internal_service_credentials
		// means the connected role lacks its grant -- or that this binary was
		// wired back onto a pool that never had one -- which is a different
		// operator action entirely from a rotated or revoked token. Both codes
		// are compile-time constants, so neither can carry credential or
		// catalog material into the operator's terminal or logs.
		return nil, writeError(stderr, joboperator.AuthenticationReason(err))
	}
	lockTx, err := pools.Domain.Begin(ctx)
	if err != nil {
		return nil, writeError(stderr, "operator_busy")
	}
	lockHeld := true
	defer func() {
		if failed && lockHeld {
			rollbackOperatorLock(lockTx)
		}
	}()
	var lockAcquired bool
	if err := lockTx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", operatorAdvisoryKey).Scan(&lockAcquired); err != nil || !lockAcquired {
		return nil, writeError(stderr, "operator_busy")
	}

	if _, err := riverstore.CheckSchema(ctx, pools.QueueControl, schema, nil); err != nil {
		return nil, writeError(stderr, "river_schema_unavailable")
	}
	registry, err := jobruntime.Load("contracts/jobs/v1")
	if err != nil {
		return nil, writeError(stderr, "contract_registry_invalid")
	}
	contractRegistry, err := jobcontract.LoadRegistry("contracts/jobs/v1")
	if err != nil {
		return nil, writeError(stderr, "contract_registry_invalid")
	}
	manifest, budget, err := deploymentcontract.Load("deploy/go-workers/deployment.json", contractRegistry)
	if err != nil {
		return nil, writeError(stderr, "deployment_contract_invalid")
	}
	routeRegistry, err := syncdispatchcontract.Load("contracts/sync-dispatch/v1")
	if err != nil {
		return nil, writeError(stderr, "contract_registry_invalid")
	}
	routeCapabilities, err := syncroute.NewCapabilities(syncdispatchruntime.RouteCapabilities())
	if err != nil {
		return nil, writeError(stderr, "contract_registry_invalid")
	}
	// Coordinator pool: syncroute.Controller UPDATEs
	// sync_dispatch_transport_routes (control.go:163,249) and takes FOR UPDATE
	// on it (control.go:338-343). That table is dual-granted, but UPDATE exists
	// only on the coordinator side of the split -- domainPosture declares it
	// SELECT-only -- so `workerctl routes pause|drain|resume` is a coordinator
	// path even though `routes status` would work on either. Its LOCK TABLE on
	// sync_dispatch_outbox (control.go:15) needs UPDATE too, which the
	// coordinator posture also carries.
	routeController, err := syncroute.NewController(coordinatorPool, routeRegistry, routeCapabilities)
	if err != nil {
		return nil, writeError(stderr, "operator_backend_unavailable")
	}
	jobRouteController, err := newJobRouteController(
		coordinatorPool, pools.Domain, pools.QueueControl, schema, registry,
	)
	if err != nil {
		return nil, writeError(stderr, "operator_backend_unavailable")
	}
	streams := make([]streamProfileStatus, 0, 2)
	for _, process := range manifest.Processes {
		if process.Runtime != "stream" {
			continue
		}
		streams = append(streams, streamProfileStatus{
			Profile: process.Name, Owner: "celery", EnabledByDefault: process.EnabledByDefault,
			MinReplicas: process.MinReplicas, MaxReplicas: process.MaxReplicas,
		})
	}
	if len(streams) == 0 {
		return nil, writeError(stderr, "deployment_contract_invalid")
	}
	backend, err := joboperator.NewDirectPostgresBackend(pools.QueueControl, schema, registry)
	if err != nil {
		return nil, writeError(stderr, "operator_backend_unavailable")
	}
	// Coordinator pool: INSERTs and UPDATEs worker_operator_audits, which is
	// coordinator-exclusive with no domain grant.
	auditor, err := joboperator.NewPostgresAuditor(coordinatorPool)
	if err != nil {
		return nil, writeError(stderr, "audit_unavailable")
	}
	// Stays on the domain pool: PostgresDomainGuard runs `SELECT true` and
	// touches no relation at all, so it needs nothing the domain role lacks.
	guard, err := joboperator.NewPostgresDomainGuard(pools.Domain)
	if err != nil {
		return nil, writeError(stderr, "domain_precondition_unavailable")
	}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: backend, Authorizer: authentication.Authorizer(),
		DomainGuard: guard, Auditor: auditor,
		RouteController:    routeController,
		JobRouteController: jobRouteController,
	})
	if err != nil {
		return nil, writeError(stderr, "operator_backend_unavailable")
	}
	failed = false
	lockHeld = false
	runtime := &operatorRuntime{
		service: service, principal: authentication.Principal(), pools: pools, lockTx: lockTx,
		streamDeploymentState: manifest.DeploymentState, streams: streams, queueControlMode: mode,
		registry: registry, lookup: lookup,
	}
	runtime.queueStatusSource = manifestQueueStatusSource{
		service: service, principal: runtime.principal, manifest: manifest, budget: budget,
		presence: func(ctx context.Context) ([]jobruntime.WorkerPresenceSummary, error) {
			return jobruntime.ReadWorkerPresence(ctx, pools.Domain)
		},
	}
	return runtime, 0
}

func databaseMode(lookup platformsecrets.LookupEnv, key string) platformconfig.QueueControlMode {
	if raw, configured := lookup(key); configured && raw != "" {
		return platformconfig.QueueControlMode(strings.ToLower(raw))
	}
	return platformconfig.QueueControlDirect
}

func sessionSafeMode(mode platformconfig.QueueControlMode) bool {
	return mode == platformconfig.QueueControlDirect || mode == platformconfig.QueueControlSession
}

// newJobRouteController composes the only currently approved forward cutover:
// sync.provider_unit from its legacy Celery owner to the checked-in River
// canary. The controller remains fail-closed for every other kind because the
// Celery quiescer accepts that exact durable unit ledger only.
// The three pools are not interchangeable here, and the split is per-component
// rather than per-controller:
//
//   - coordinatorPool drives the Controller itself. It SELECTs and UPDATEs
//     worker_job_routes (control.go:159,229,257,270), which is
//     coordinator-exclusive — so even the read path 42501s on the domain role —
//     and Rollback runs LOCK TABLE public.worker_job_outbox IN SHARE ROW
//     EXCLUSIVE MODE (control.go:197), which PostgreSQL treats as requiring
//     UPDATE. The domain role holds only SELECT+INSERT there. That pair is
//     CHAOS-3113.
//   - domainPool stays on the Celery quiescer: it reads sync_run_units
//     (quiescer.go:88), a dual-granted table whose read is genuinely domain
//     work, so there is no reason to widen it to the coordinator.
//   - queuePool stays on the River quiescer, which only touches the River
//     schema.
func newJobRouteController(
	coordinatorPool, domainPool, queuePool *pgxpool.Pool,
	schema string,
	registry *jobruntime.Registry,
) (*jobroute.Controller, error) {
	riverQuiescer, err := jobroute.NewPostgresRiverQuiescer(queuePool, schema)
	if err != nil {
		return nil, err
	}
	celeryQuiescer, err := jobroute.NewPostgresCelerySyncProviderQuiescer(domainPool)
	if err != nil {
		return nil, err
	}
	return jobroute.NewControllerWithCeleryQuiescer(
		coordinatorPool, registry, riverQuiescer, celeryQuiescer,
	)
}

func rollbackOperatorLock(lockTx pgx.Tx) {
	if lockTx == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = lockTx.Rollback(ctx)
}

func dispatch(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	switch args[0] {
	case "status":
		if len(args) != 1 {
			return writeError(stderr, "invalid_request")
		}
		if err := runtime.service.Status(ctx, runtime.principal); err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, map[string]any{
			"queue_control_mode":   runtime.queueControlMode,
			"river_schema_version": riverstore.PinnedSchemaVersion,
			"status":               "ready",
		})
	case "jobs":
		return dispatchJobs(ctx, runtime, args[1:], stdout, stderr)
	case "metrics":
		return dispatchMetrics(ctx, runtime, args[1:], stdout, stderr)
	case "queues":
		return dispatchQueues(ctx, runtime, args[1:], stdout, stderr)
	case "contracts":
		return dispatchContracts(ctx, runtime, args[1:], stdout, stderr)
	case "routes":
		return dispatchRoutes(ctx, runtime, args[1:], stdout, stderr)
	case "job-routes":
		return dispatchJobRoutes(ctx, runtime, args[1:], stdout, stderr)
	case "providersync":
		return dispatchProvidersync(ctx, runtime, args[1:], stdout, stderr)
	case "streams":
		if len(args) != 2 || args[1] != "status" {
			return writeError(stderr, "invalid_request")
		}
		if err := runtime.service.Status(ctx, runtime.principal); err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, streamStatusResponse{
			DeploymentState: runtime.streamDeploymentState,
			Profiles:        runtime.streams,
		})
	default:
		return writeError(stderr, "invalid_request")
	}
}

func dispatchJobRoutes(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 2 && args[0] == "status" {
		state, err := runtime.service.InspectJobRoute(ctx, runtime.principal, args[1])
		if err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, state)
	}
	if len(args) == 0 || (args[0] != "apply" && args[0] != "rollback") {
		return writeError(stderr, "invalid_request")
	}
	flags := quietFlags("job-routes " + args[0])
	reason := flags.String("reason", "", "bounded reason code")
	correlation := flags.String("correlation-id", "", "bounded correlation ID")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 1 || *reason == "" || *correlation == "" {
		return writeError(stderr, "invalid_request")
	}
	var (
		state jobroute.State
		err   error
	)
	if args[0] == "apply" {
		state, err = runtime.service.ApplyCheckedInJobRoute(
			ctx, runtime.principal, flags.Arg(0), *reason, *correlation,
		)
	} else {
		state, err = runtime.service.RollbackJobRoute(
			ctx, runtime.principal, flags.Arg(0), *reason, *correlation,
		)
	}
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, state)
}

func dispatchRoutes(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	if args[0] == "status" {
		if len(args) != 2 {
			return writeError(stderr, "invalid_request")
		}
		state, err := runtime.service.InspectRoute(ctx, runtime.principal, args[1])
		if err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, state)
	}
	flags := quietFlags("routes " + args[0])
	reason := flags.String("reason", "", "bounded reason code")
	correlation := flags.String("correlation-id", "", "bounded correlation ID")
	transport := flags.String("transport", "", "checked-in target transport")
	quiescenceTimeout := flags.Duration("quiescence-timeout", 10*time.Second, "legacy compatibility; no-op for sync-dispatch routes")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 1 || *reason == "" || *correlation == "" {
		return writeError(stderr, "invalid_request")
	}
	kind := flags.Arg(0)
	var (
		state syncroute.RouteState
		err   error
	)
	switch args[0] {
	case "apply":
		if *transport != "" {
			return writeError(stderr, "invalid_request")
		}
		state, err = runtime.service.ApplyCheckedInRoute(
			ctx, runtime.principal, kind, *reason, *correlation,
		)
	case "pause":
		if *transport != "" {
			return writeError(stderr, "invalid_request")
		}
		state, err = runtime.service.PauseRoute(ctx, runtime.principal, kind, *reason, *correlation)
	case "drain":
		if *transport != "" {
			return writeError(stderr, "invalid_request")
		}
		state, err = runtime.service.DrainRoute(ctx, runtime.principal, kind, *reason, *correlation)
	case "resume":
		if *transport == "" {
			return writeError(stderr, "invalid_request")
		}
		state, err = runtime.service.ResumeRoute(
			ctx, runtime.principal, kind, *transport, *reason, *correlation, *quiescenceTimeout,
		)
	default:
		return writeError(stderr, "invalid_request")
	}
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, state)
}

func dispatchJobs(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "list":
		flags := quietFlags("jobs list")
		var states stringList
		flags.Var(&states, "state", "inspectable River state (repeatable)")
		kind := flags.String("kind", "", "exact registered job kind")
		queue := flags.String("queue", "", "exact registered queue")
		limit := flags.Int("limit", 100, "maximum rows (1-500)")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
			return writeError(stderr, "invalid_request")
		}
		if len(states) == 0 {
			states = stringList{"available", "retryable", "running", "scheduled"}
		}
		sort.Strings(states)
		filter := joboperator.ListFilter{Kind: *kind, Queue: *queue, Limit: *limit}
		for _, state := range states {
			filter.States = append(filter.States, joboperator.JobState(state))
		}
		jobs, err := runtime.service.List(ctx, runtime.principal, filter)
		if err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, jobs)
	case "inspect":
		if len(args) != 2 {
			return writeError(stderr, "invalid_request")
		}
		id, ok := positiveID(args[1])
		if !ok {
			return writeError(stderr, "invalid_request")
		}
		job, err := runtime.service.Inspect(ctx, runtime.principal, id)
		if err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, job)
	case "cancel", "retry":
		flags := quietFlags("jobs " + args[0])
		reason := flags.String("reason", "", "bounded reason code")
		correlation := flags.String("correlation-id", "", "bounded correlation ID")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 1 {
			return writeError(stderr, "invalid_request")
		}
		id, ok := positiveID(flags.Arg(0))
		if !ok || *reason == "" || *correlation == "" {
			return writeError(stderr, "invalid_request")
		}
		var (
			job joboperator.JobSummary
			err error
		)
		if args[0] == "cancel" {
			job, err = runtime.service.Cancel(ctx, runtime.principal, id, *reason, *correlation)
		} else {
			job, err = runtime.service.Retry(ctx, runtime.principal, id, *reason, *correlation)
		}
		if err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, job)
	default:
		return writeError(stderr, "invalid_request")
	}
}

// dispatchMetrics handles `metrics daily-redrive` (CHAOS-4358): the operator
// entry point that repairs a daily-metrics run stranded because River
// discarded every daily_partition job it ever dispatched for it, and nothing
// else ever re-enqueues metrics.daily_dispatch for that run on its own.
//
// This deliberately bypasses joboperator.Service's Action/audit pipeline
// (Cancel/Retry's path) -- it is gated only by the same WORKER_OPERATOR_TOKEN
// authentication configureRuntime already requires for every workerctl
// command. See the PR's RISK-NOTES for why that scope limit was accepted
// here rather than adding a new Action end-to-end under time pressure.
func dispatchMetrics(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "remaining":
		return dispatchMetricsRemaining(ctx, runtime, args[1:], stdout, stderr)
	case "daily-redrive":
		flags := quietFlags("metrics daily-redrive")
		org := flags.String("org", "", "organization id (uuid)")
		from := flags.String("from", "", "first target_day, inclusive (YYYY-MM-DD, UTC)")
		to := flags.String("to", "", "last target_day, inclusive (YYYY-MM-DD, UTC)")
		reviewEvidence := flags.String("review-evidence", "", "REQUIRED: what you verified before authorizing retry for ambiguous/stuck-executing ledger rows in this window (e.g. \"confirmed zero output rows for the affected families via ClickHouse readback\") -- see CHAOS-4304 note below")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
			return writeError(stderr, "invalid_request")
		}
		if _, err := uuid.Parse(*org); err != nil {
			return writeError(stderr, "invalid_request")
		}
		// codex review round 3: "ambiguous" means a progress-having failure
		// MAY have partially written real output -- claim expiration alone
		// is explicitly not evidence retry is safe (worker_metrics.py's own
		// _repair_execution requires a human to pick retry_safe vs
		// confirm_succeeded per execution, based on actual review). A bulk
		// path cannot inspect per-row evidence, so it stays restricted to
		// retry_safe only (never confirm_succeeded, which needs per-row
		// output_evidence) and REQUIRES the operator to state in their own
		// words what they verified -- no default, no generic hardcoded
		// string. This is friction by design: an operator who has not
		// actually checked (e.g. the redriven families' zero-row counters,
		// or a fresh ClickHouse readback showing no output yet for these
		// partitions) should not be able to bulk-authorize retries for
		// non-argMax-deduped families (file_hotspots is the known example
		// where a retry-caused duplicate silently inflates scores).
		if strings.TrimSpace(*reviewEvidence) == "" {
			return writeError(stderr, "invalid_request")
		}
		fromDay, err := time.Parse("2006-01-02", *from)
		if err != nil {
			return writeError(stderr, "invalid_request")
		}
		toDay, err := time.Parse("2006-01-02", *to)
		if err != nil {
			return writeError(stderr, "invalid_request")
		}
		if runtime.pools == nil || runtime.registry == nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		store, err := daily.NewPostgresStore(runtime.pools.Domain)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		publisher, err := daily.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		// CHAOS-4304 ordering requirement (codex review, round 1): a
		// partition whose Python compatibility-bridge ledger row is still
		// 'ambiguous'/stuck-'executing' answers ambiguous_refused the
		// instant a redriven job reaches it, which Go classifies Permanent
		// and re-terminalizes failed_permanent -- undoing this same pass's
		// own reset. The ledger repair MUST land before any partition job
		// publishes, not after, so this calls the Python bulk-redrive
		// endpoint first, over every run this org+day window's runs (not
		// just the ones with a currently-dispatchable partition -- a run
		// can carry an ambiguous ledger row on a partition already
		// terminalized failed_permanent, which step 2 below is about to
		// reset back into the redrive set).
		runIDs, err := store.RunningRunIDs(ctx, *org, fromDay, toDay)
		if err != nil {
			return writeServiceError(stderr, err)
		}
		ledgerRepair, err := redriveDailyMetricsLedger(ctx, runIDs, *reviewEvidence)
		if err != nil {
			return writeError(stderr, "ledger_repair_unavailable")
		}
		// codex review round 2: a nonzero skipped_claim_active means at
		// least one ambiguous/stuck-executing ledger row was left
		// unrepaired because its original claim still read as active at
		// that moment. Publishing partition jobs anyway is unsafe -- if
		// that claim is released between this call and the redriven job
		// reaching the bridge (a real, observed race, not hypothetical),
		// the unrepaired row answers ambiguous_refused immediately and the
		// partition is re-terminalized failed_permanent, undoing this same
		// pass. Stop here and report it; the operator re-runs once those
		// claims have settled (their own owning job will finish or expire).
		if ledgerRepairWasIncomplete(ledgerRepair) {
			return writeResult(stdout, stderr, map[string]any{
				"ledger_repair": ledgerRepair,
				"partitions":    nil,
				"status":        "ledger_repair_incomplete_retry_after_claims_settle",
			})
		}
		// codex review round 3 (residual, accepted risk): the ledger repair
		// above takes a SNAPSHOT of run ids and repairs whatever is
		// ambiguous/stuck-executing at that instant; a partition that
		// starts a NEW execution and reaches ambiguous in the window
		// between that call and this one is not covered by it, and this
		// query will still pick it up (still 'failed'/'pending'/expired-
		// lease at the moment it runs). Closing that window fully would
		// need a single fenced transaction spanning both the Python ledger
		// and the Go partition tables across a network call, which does
		// not exist today. This is self-healing, not silent: a fresh
		// ambiguous row here still surfaces as a 409/failed_permanent, and
		// the NEXT invocation of this same command repairs it (the ledger
		// repair step is idempotent by construction -- a row already
		// 'retry_authorized' or 'succeeded' is simply not selected again).
		outcome, err := store.RedriveStrandedPartitions(ctx, publisher, *org, fromDay, toDay, uuid.NewString())
		if err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, map[string]any{
			"ledger_repair": ledgerRepair,
			"partitions":    outcome,
		})
	case "daily-finalize":
		return dispatchMetricsDailyFinalize(ctx, runtime, args[1:], stdout, stderr)
	case "finalize-redrive":
		return dispatchMetricsFinalizeRedrive(ctx, runtime, args[1:], stdout, stderr)
	case "partition-recompute":
		return dispatchMetricsPartitionRecompute(ctx, runtime, args[1:], stdout, stderr)
	default:
		return writeError(stderr, "invalid_request")
	}
}

// dispatchMetricsDailyFinalize handles `metrics daily-finalize` (CHAOS-4389):
// the operator entry point that repairs a daily-metrics run stranded because
// River discarded the ONE metrics.daily_finalize job CompletePartition ever
// enqueues for it (fixed idempotency key, permanently deduped by the
// outbox), leaving the run status='running' forever despite 100% of its
// partitions having succeeded. Exactly one of --run/--all-complete selects
// the scope: --run repairs one named run; --all-complete sweeps every
// organization for runs in this exact stranded shape (bounded by --limit)
// and redrives each one found. Mirrors `metrics daily-redrive`'s shape
// (WORKER_OPERATOR_TOKEN authentication via configureRuntime,
// --review-evidence required, quiet flags, JSON result) and, like it,
// deliberately bypasses joboperator.Service's Action/audit pipeline.
//
// CHAOS-4409: a run's finalize ledger row (metric_compatibility_executions,
// worker_kind='daily', operation='finalize') can be stuck 'ambiguous' or
// stuck-'executing' the exact same way a partition row can -- the api
// process that owned the original finalize claim died before any exception
// handler ran, or a progress-having finalize failure never got a human
// /repair call. Before CHAOS-4409, this command republished a fresh
// metrics.daily_finalize job straight into that wall: ClaimFinalize->
// Finalize reaches the Python bridge fine, but _reserve_execution refuses
// the IDENTICAL execution identity 409 ambiguous_refused forever, because
// nothing had ever repaired the finalize row specifically (the bridge's own
// bulk-redrive endpoint only ever selected operation='partition' rows).
// Prod evidence: 13 daily_metrics_runs stuck in exactly the CHAOS-4389
// stranded shape whose finalize ledger row was the thing actually blocking
// them -- `daily-finalize --run` answered JobCancelError ambiguous_refused
// on every one, forever. This now calls the SAME bulk-redrive endpoint
// `daily-redrive` already calls for partitions (CHAOS-4304's ordering
// requirement applies identically here: the ledger repair MUST land before
// any finalize job publishes, not after) and aborts on the same
// skipped_claim_active>0 signal daily-redrive already treats as unsafe to
// proceed past.
func dispatchMetricsDailyFinalize(
	ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer,
) int {
	flags := quietFlags("metrics daily-finalize")
	run := flags.String("run", "", "single daily_metrics_runs id (uuid) to finalize")
	allComplete := flags.Bool("all-complete", false, "sweep every organization for runs status='running' with 100% partitions succeeded whose finalize was NEVER attempted (finalization_status='pending'), and redrive each one found -- see --run for a run whose finalize already ran at least once")
	limit := flags.Int("limit", 0, "max runs one --all-complete sweep pass redrives (default 500)")
	// codex review round 3 on daily-redrive (CHAOS-4358) established the bar
	// this mirrors: an operator authorizing a repeat execution must state in
	// their own words what they verified, no default, no generic hardcoded
	// string. Finalize is not a mechanical retry either -- CompatibilityExecutor.Finalize
	// writes user_metrics_daily/ic_landscape_rolling_30d directly, so the
	// evidence an operator should state here is that the ORIGINAL finalize
	// never durably wrote (e.g. no completion fence, no rows for the run's
	// target_day yet) rather than the partition-redrive concern about
	// per-row output_evidence.
	//
	// codex review (CHAOS-4389, P1): finalization_status='failed' does NOT
	// mean finalize never ran -- FinalizeHandler.Work sets it both when the
	// compatibility call itself failed AND when it SUCCEEDED (writing real
	// user_metrics_daily/compounding_risk_daily/team_cognitive_load_daily
	// rows) but the bookkeeping CompleteFinalize write failed afterward.
	// --all-complete's bulk sweep only ever redrives the provably-safe
	// 'pending' (never attempted) subset for exactly this reason; a run
	// whose finalize was already claimed at least once needs a human to
	// name it individually with --run, after actually checking whether it
	// already wrote real output -- mirrors `daily-redrive`'s own split
	// between its bulk retry_safe path and confirm_succeeded's
	// single-execution-only endpoint.
	reviewEvidence := flags.String("review-evidence", "", "REQUIRED: what you verified before authorizing a repeat finalize -- for --run, state whether the run's finalize already wrote real output (e.g. \"confirmed all partitions succeeded and no user_metrics_daily/ic_landscape_rolling_30d/compounding_risk_daily rows exist yet for this run's target_day -- the prior metrics.daily_finalize job never reached CompleteFinalize\"); for --all-complete, why this sweep is authorized now (--all-complete never touches a run whose finalize already ran at least once, regardless of this text)")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	if strings.TrimSpace(*reviewEvidence) == "" {
		return writeError(stderr, "invalid_request")
	}
	hasRun := strings.TrimSpace(*run) != ""
	if hasRun == *allComplete {
		// Exactly one of --run/--all-complete must be set: neither named a
		// scope, or both did and this command cannot tell which one governs.
		return writeError(stderr, "invalid_request")
	}
	if hasRun {
		parsedRun, err := uuid.Parse(*run)
		if err != nil {
			return writeError(stderr, "invalid_request")
		}
		// codex review (P2, round 2): uuid.Parse accepts uppercase/mixed-case
		// input, but jobcontract.MarshalCanonical rejects a non-lowercase
		// domain id -- passing the raw (possibly uppercase) *run through
		// would pass this validation and then fail later inside
		// PublishRedriveFinalizeTx, turning a legitimately valid --run into
		// a confusing publish-time error instead of a repair. Canonicalize
		// once, here, so every downstream use (the DB query, the envelope)
		// sees the same lowercase string.
		canonicalRun := parsedRun.String()
		run = &canonicalRun
	}
	if runtime.pools == nil || runtime.registry == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	store, err := daily.NewPostgresStore(runtime.pools.Domain)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	publisher, err := daily.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	var candidates []string
	if hasRun {
		candidates = []string{*run}
	} else {
		// CHAOS-4405 (team-lead escalation, 2026-08-28): close the residual
		// silent-exclusion gap before scanning for stranded runs -- a
		// finalize-redrive event whose published River job was
		// discarded/cancelled (or never reached River at all) before
		// ClaimFinalize ever ran for it stays 'open' forever otherwise,
		// permanently excluding its run from the scan below. Only this
		// branch runs it: --run never consults FindStrandedFinalizeRuns or
		// this table's exclusion at all, so it has nothing to reconcile.
		// riverSchema is resolved directly from the env var here (matching
		// configureRuntime's own default) rather than threading a new field
		// through operatorRuntime, since this is the ONLY call site that
		// needs it.
		riverSchema := os.Getenv("RIVER_DATABASE_SCHEMA")
		if strings.TrimSpace(riverSchema) == "" {
			riverSchema = "river"
		}
		if _, err := store.ReconcileOrphanedFinalizeRedriveRuns(ctx, runtime.pools.QueueControl, riverSchema); err != nil {
			return writeServiceError(stderr, err)
		}
		candidates, err = store.FindStrandedFinalizeRuns(ctx, *limit)
		if err != nil {
			return writeServiceError(stderr, err)
		}
	}
	// CHAOS-4409: repair the named candidate's finalize ledger row BEFORE
	// publishing anything -- see this function's own doc comment for why
	// the ordering matters (a stuck row answers ambiguous_refused the
	// instant the redriven job reaches it, permanently, undoing this same
	// pass). ledgerRepair/abort are reported/checked the identical way
	// daily-redrive already does for its partition-ledger repair.
	//
	// codex review (round 1, P1): this ONLY ever runs for --run, mirroring
	// RedriveStrandedFinalize's own allowPriorAttempt=hasRun boundary
	// exactly. FindStrandedFinalizeRuns' candidates (--all-complete's own
	// input) deliberately include 'failed'/expired-running runs for
	// VISIBILITY, not redrive -- RedriveStrandedFinalize below already
	// refuses to publish for any of them. Repairing their ledger rows
	// anyway (the pre-fix behavior) would authorize retry_authorized for a
	// run --all-complete itself never touches, letting some LATER
	// unrelated call redrive it without an operator ever having reviewed
	// that specific run's output -- exactly the shape --all-complete's own
	// 'pending'-only safety split exists to prevent. A truly
	// never-attempted 'pending' run cannot have a stuck ledger row in the
	// first place (ClaimFinalize never claimed this generation to attempt
	// Finalize() at all), so --all-complete loses no real repair capacity
	// here -- only the unauthorized side effect.
	ledgerRepair := map[string]any{"repaired": 0, "skipped_claim_active": 0}
	if hasRun {
		var abort bool
		ledgerRepair, abort, err = finalizeLedgerRepairGate(ctx, candidates, *reviewEvidence)
		if err != nil {
			return writeError(stderr, "ledger_repair_unavailable")
		}
		if abort {
			return writeResult(stdout, stderr, map[string]any{
				"candidates":    candidates,
				"ledger_repair": ledgerRepair,
				"finalize":      nil,
				"status":        "ledger_repair_incomplete_retry_after_claims_settle",
			})
		}
	}
	// allowPriorAttempt = hasRun: a specific --run is an explicit, reviewed,
	// single-target operator action that MAY authorize redriving a run whose
	// finalize already ran at least once; --all-complete's bulk sweep never
	// does, regardless of --review-evidence's text (see the flag's own help
	// and RedriveStrandedFinalize's doc comment for why).
	outcome, err := store.RedriveStrandedFinalize(ctx, publisher, candidates, uuid.NewString(), hasRun)
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, map[string]any{
		"candidates":    candidates,
		"ledger_repair": ledgerRepair,
		"finalize":      outcome,
	})
}

// finalizeLedgerRepairGate repairs the compatibility-bridge finalize ledger
// for candidates and reports whether the caller must abort rather than
// proceed to republish (CHAOS-4409) -- pulled out of
// dispatchMetricsDailyFinalize as its own function, mirroring
// ledgerRepairWasIncomplete's own reasoning, so the gating decision is unit
// testable against a mock bridge without needing a real Postgres store.
func finalizeLedgerRepairGate(
	ctx context.Context, candidates []string, reviewEvidence string,
) (ledgerRepair map[string]any, abort bool, err error) {
	// "finalize" (codex review, round 1, P1): this call's review_evidence is
	// about finalize output, never partition output -- it must never repair
	// an unrelated partition ledger row under the DailyMetricsRedriveRequest
	// default. See that field's own doc comment.
	ledgerRepair, err = redriveDailyMetricsLedger(ctx, candidates, reviewEvidence, "finalize")
	if err != nil {
		return nil, false, err
	}
	return ledgerRepair, ledgerRepairWasIncomplete(ledgerRepair), nil
}

// dispatchMetricsFinalizeRedrive handles `metrics finalize-redrive`
// (CHAOS-4405): the historical-backfill operator entry point that re-runs
// metrics.daily_finalize for one organization across [--from, --to], one
// calendar day at a time. Distinct from `daily-finalize` (CHAOS-4389, which
// only ever repairs a run still stuck non-terminal): this command's whole
// point is to re-execute a day's finalize AFTER it already completed,
// because run_daily_metrics_finalize now also writes
// compounding_risk_daily(scope='team') and team_cognitive_load_daily
// (CHAOS-4399, #1963) -- a day finalized before that landed has zero rows
// in either table and needs finalize re-run purely to backfill them.
//
// --include-succeeded defaults true (this verb exists specifically to touch
// already-succeeded days); pass --include-succeeded=false to restrict this
// pass to the same safe never-attempted/failed/expired-lease subset
// `daily-finalize --all-complete` uses, scoped to this org+day-range --
// useful as a dry run of the eligibility scan before authorizing the
// state-mutating succeeded case. See RedriveFinalizeForRange's doc comment
// for exactly why an already-'succeeded' row needs a transient state reset
// (not a bare republish) to reach FinalizeHandler.Work again at all.
func dispatchMetricsFinalizeRedrive(
	ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer,
) int {
	flags := quietFlags("metrics finalize-redrive")
	org := flags.String("org", "", "organization id (uuid)")
	from := flags.String("from", "", "first target_day, inclusive (YYYY-MM-DD, UTC)")
	to := flags.String("to", "", "last target_day, inclusive (YYYY-MM-DD, UTC)")
	includeSucceeded := flags.Bool("include-succeeded", true, "also redrive a day whose run already reached status='succeeded' -- the whole point of this verb; set false to restrict to the never-attempted/failed/expired-lease subset")
	// codex review round 3 on daily-redrive (CHAOS-4358) established the bar
	// every operator-authorized repeat execution in this CLI mirrors: state
	// in your own words what you verified, no default, no generic hardcoded
	// string. This verb is the most consequential of the three (it
	// deliberately re-executes a day that already completed, across a
	// potentially wide date range), so the bar applies here too, not just
	// to the narrower single-run daily-finalize --run case. Not required
	// under --dry-run: a preview makes no durable write, so there is
	// nothing yet to justify.
	reviewEvidence := flags.String("review-evidence", "", "REQUIRED unless --dry-run: why this historical re-run is authorized (e.g. \"CHAOS-4405: backfilling compounding_risk_daily(team)/team_cognitive_load_daily for days finalized before #1963 landed the team-aggregation write\")")
	// Team-lead's approval condition (4): list days + current state before
	// any write. Computes and reports the identical eligibility scan a real
	// pass would, under the identical row lock, but every transaction it
	// opens is rolled back, never committed -- see
	// RedriveFinalizeForRange's dryRun doc comment.
	dryRun := flags.Bool("dry-run", false, "report what a real pass would do (which days, which runs, which would need a terminal-state reset) without writing anything -- no reset, no provenance row, no publish")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	if _, err := uuid.Parse(*org); err != nil {
		return writeError(stderr, "invalid_request")
	}
	if !*dryRun && strings.TrimSpace(*reviewEvidence) == "" {
		return writeError(stderr, "invalid_request")
	}
	fromDay, err := time.Parse("2006-01-02", *from)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	toDay, err := time.Parse("2006-01-02", *to)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	if runtime.pools == nil || runtime.registry == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	store, err := daily.NewPostgresStore(runtime.pools.Domain)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	publisher, err := daily.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	nonce := ""
	if !*dryRun {
		nonce = uuid.NewString()
	}
	outcome, err := store.RedriveFinalizeForRange(ctx, publisher, *org, fromDay, toDay, nonce, *includeSucceeded, *reviewEvidence, *dryRun)
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, map[string]any{
		"finalize_redrive": outcome,
		"dry_run":          *dryRun,
	})
}

// dispatchProvidersync handles the `providersync` verb group.
func dispatchProvidersync(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "retire-linear-pseudo-projects":
		return dispatchProvidersyncRetireLinearPseudoProjects(ctx, runtime, args[1:], stdout, stderr)
	case "retire-stale-linear-project-ownership":
		return dispatchProvidersyncRetireStaleLinearProjectOwnership(ctx, runtime, args[1:], stdout, stderr)
	default:
		return writeError(stderr, "invalid_request")
	}
}

// dispatchProvidersyncRetireLinearPseudoProjects handles `providersync
// retire-linear-pseudo-projects` (CHAOS-4530 follow-up, CF/acr finding): a
// ONE-TIME, operator-invoked cleanup that physically deletes every
// {org_id}:linear:{team_key} pseudo-project row still present in `projects`
// -- across every org, or one org with --org -- because CF found neither an
// active row NOR CHAOS-4530's original is_active=0 tombstone shape is a
// signal acr's identity resolution recognizes: the row must be GONE, not
// soft-deleted. providersync.RetireLinearPseudoProjectRows does the actual
// work; this dispatcher only resolves CLICKHOUSE_URI (lazily -- no other
// workerctl verb needs a ClickHouse connection, so this is not required at
// configureRuntime time) and wires --org/--dry-run, mirroring `metrics
// partition-recompute`'s shape (quiet invalid_request on any malformed
// input, --dry-run reports without writing).
func dispatchProvidersyncRetireLinearPseudoProjects(
	ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer,
) int {
	flags := quietFlags("providersync retire-linear-pseudo-projects")
	org := flags.String("org", "", "organization id (uuid) -- omit to run across every org")
	dryRun := flags.Bool("dry-run", false, "report which pseudo-project rows would be deleted, across every org unless --org scopes it, without deleting anything")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	// Canonicalize, never pass the raw flag value onward: uuid.Parse accepts
	// mixed/upper case, but ClickHouse's org_id comparison is case-sensitive
	// string equality (codex review, 2026-08-29) -- an uppercase --org would
	// otherwise silently match zero rows and report a "successful" no-op
	// instead of the actual scoped org.
	scopedOrg := ""
	if *org != "" {
		parsedOrg, err := uuid.Parse(*org)
		if err != nil {
			return writeError(stderr, "invalid_request")
		}
		scopedOrg = parsedOrg.String()
	}
	if runtime.service == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	authorizeResource := scopedOrg
	if authorizeResource == "" {
		authorizeResource = "*"
	}
	// Authorized BEFORE anything ClickHouse-related is even attempted (codex
	// review, 2026-08-29, P1): this is a physically destructive mutation, and
	// authentication alone (a live WORKER_OPERATOR_TOKEN) is not authorization
	// -- a workers:read-only credential must never reach the delete. Every
	// other mutation in this binary goes through runtime.service for exactly
	// this reason; this is the same gate, just for an action with no other
	// natural Service method (see AuthorizeProvidersyncCleanup's doc comment).
	if err := runtime.service.AuthorizeProvidersyncCleanup(ctx, runtime.principal, authorizeResource); err != nil {
		return writeServiceError(stderr, err)
	}
	if runtime.lookup == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	dsn, ok := resolveRequired("CLICKHOUSE_URI", runtime.lookup)
	if !ok {
		return writeError(stderr, "configuration_error")
	}
	conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(dsn.Reveal()))
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	defer func() { _ = conn.Close() }()
	outcome, err := providersync.RetireLinearPseudoProjectRows(ctx, conn, scopedOrg, *dryRun)
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, map[string]any{
		"retire_linear_pseudo_projects": outcome,
	})
}

// dispatchProvidersyncRetireStaleLinearProjectOwnership handles `providersync
// retire-stale-linear-project-ownership` (CHAOS-4548): a ONE-TIME,
// operator-invoked cleanup that physically deletes every
// team_project_ownership row for a REAL Linear project (project_id is the
// provider UUID) that still carries the historical team-key project_key
// stamp (written by every sync cycle before CHAOS-4530's writer fix). Pure
// hygiene, not a correctness fix: acr's projectOwnershipJoinSQL only ever
// matches a `projects` row through project_key, and every real Linear
// project's project_key has been NULL since CHAOS-4530, so these rows were
// never reachable through that join; the project_id-keyed readers
// (loadTeamRepoOwnershipProjectLinks, load_team_attribution_context) never
// select project_key at all. providersync.RetireStaleLinearProjectOwnershipRows
// does the actual work and explicitly excludes the
// {org_id}:linear:{team_key} pseudo-identity row -- CHAOS-4560's still-open
// concern, not this verb's. Mirrors `providersync
// retire-linear-pseudo-projects`'s shape exactly (lazy CLICKHOUSE_URI
// resolution, --org/--dry-run, quiet invalid_request on any malformed
// input, authorize before anything ClickHouse-related is attempted).
func dispatchProvidersyncRetireStaleLinearProjectOwnership(
	ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer,
) int {
	flags := quietFlags("providersync retire-stale-linear-project-ownership")
	org := flags.String("org", "", "organization id (uuid) -- omit to run across every org")
	dryRun := flags.Bool("dry-run", false, "report which stale team_project_ownership rows would be deleted, across every org unless --org scopes it, without deleting anything")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	// An explicitly-provided EMPTY --org (e.g. `--org "$UNSET_VAR"` from a
	// caller's shell scripting mistake) must never be silently treated the
	// same as omitting the flag entirely (codex review, 2026-08-30, P1):
	// this is a physically destructive command, and the difference between
	// "operator omitted --org on purpose" and "operator's script passed an
	// empty value by accident" must not collapse into the same global-scope
	// outcome. flags.Visit only reports flags Parse actually saw on the
	// command line, so this distinguishes the two cases the default value
	// alone cannot.
	orgFlagWasSet := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "org" {
			orgFlagWasSet = true
		}
	})
	if orgFlagWasSet && *org == "" {
		return writeError(stderr, "invalid_request")
	}
	// Canonicalize, never pass the raw flag value onward (same reasoning as
	// retire-linear-pseudo-projects): ClickHouse's org_id comparison is
	// case-sensitive string equality, so an uppercase --org would otherwise
	// silently match zero rows.
	scopedOrg := ""
	if *org != "" {
		parsedOrg, err := uuid.Parse(*org)
		if err != nil {
			return writeError(stderr, "invalid_request")
		}
		scopedOrg = parsedOrg.String()
	}
	if runtime.service == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	authorizeResource := scopedOrg
	if authorizeResource == "" {
		authorizeResource = "*"
	}
	// Authorized BEFORE anything ClickHouse-related is even attempted -- same
	// gate retire-linear-pseudo-projects uses for the same class of action
	// (a physically destructive ClickHouse mutation outside this service's
	// own job/route/queue backends).
	if err := runtime.service.AuthorizeProvidersyncCleanup(ctx, runtime.principal, authorizeResource); err != nil {
		return writeServiceError(stderr, err)
	}
	if runtime.lookup == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	dsn, ok := resolveRequired("CLICKHOUSE_URI", runtime.lookup)
	if !ok {
		return writeError(stderr, "configuration_error")
	}
	conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(dsn.Reveal()))
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	defer func() { _ = conn.Close() }()
	outcome, err := providersync.RetireStaleLinearProjectOwnershipRows(ctx, conn, scopedOrg, *dryRun)
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, map[string]any{
		"retire_stale_linear_project_ownership": outcome,
	})
}

// dispatchMetricsPartitionRecompute handles `metrics partition-recompute`
// (CHAOS-4459): the operator entry point that repairs the class
// `daily-redrive`/`finalize-redrive` cannot touch -- a daily_metrics_run
// whose partitions are ALL status='succeeded' (the ledger reports the day
// complete) but whose family output was computed under a writer that is now
// known to have been wrong (CHAOS-4341: the native repo_user_commit
// executor wrote org_id="" on repo_metrics_daily/user_metrics_daily/
// commit_metrics before PR #1960 -- historical partitions computed under
// the old writer stay wrong forever, since a 'succeeded' partition is never
// dispatchable again through any existing path). Mirrors
// `metrics finalize-redrive`'s command shape exactly (WORKER_OPERATOR_TOKEN
// authentication via configureRuntime, --review-evidence required unless
// --dry-run, quiet invalid_request on any malformed input).
//
// --family is restricted to daily.SupportedPartitionRecomputeFamilies --
// see that var's doc comment for why the reset itself recomputes every
// family in the partition, not just the named one, and why --family is
// audit/intent scoping rather than a real narrowing of the blast radius.
func dispatchMetricsPartitionRecompute(
	ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer,
) int {
	flags := quietFlags("metrics partition-recompute")
	org := flags.String("org", "", "organization id (uuid)")
	from := flags.String("from", "", "first target_day, inclusive (YYYY-MM-DD, UTC)")
	to := flags.String("to", "", "last target_day, inclusive (YYYY-MM-DD, UTC)")
	family := flags.String("family", "", "metrics.daily family this recompute is repairing (supported: repo_user_commit) -- recorded for audit; every family in the partition is recomputed, not just this one (see docs)")
	reviewEvidence := flags.String("review-evidence", "", "REQUIRED unless --dry-run: why this historical re-run is authorized (e.g. \"CHAOS-4459: org-scoped commit_metrics/repo_metrics_daily/user_metrics_daily rows are 0 for this day because the partition succeeded under the pre-#1960 writer that stamped org_id=''\")")
	dryRun := flags.Bool("dry-run", false, "report what a real pass would do (which days, which runs would be reset) without writing anything -- no reset, no provenance row, no publish")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	if _, err := uuid.Parse(*org); err != nil {
		return writeError(stderr, "invalid_request")
	}
	if !slices.Contains(daily.SupportedPartitionRecomputeFamilies, *family) {
		return writeError(stderr, "invalid_request")
	}
	if !*dryRun && strings.TrimSpace(*reviewEvidence) == "" {
		return writeError(stderr, "invalid_request")
	}
	fromDay, err := time.Parse("2006-01-02", *from)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	toDay, err := time.Parse("2006-01-02", *to)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	if runtime.pools == nil || runtime.registry == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	store, err := daily.NewPostgresStore(runtime.pools.Domain)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	publisher, err := daily.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	nonce := ""
	if !*dryRun {
		nonce = uuid.NewString()
	}
	outcome, err := store.RedrivePartitionsForRange(ctx, publisher, *org, fromDay, toDay, nonce, *family, *reviewEvidence, *dryRun)
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, map[string]any{
		"partition_recompute": outcome,
		"dry_run":             *dryRun,
	})
}

// manualBackfillGeneration derives a deterministic generation for one
// `metrics remaining start` request from its own request shape (codex
// review, P1) -- NOT from wall-clock time, so a retried CLI invocation with
// identical flags reuses the SAME generation and lands on
// remaining.PostgresStore's ON CONFLICT DO NOTHING idempotency instead of
// inserting a second run per day while the first is still pending/running
// (StartManualBackfillRun's coverage check only recognizes SUCCEEDED
// partitions). remaining.maxGenerationLength is 128 runes; the widest inputs
// here (a 15-rune family, a 36-rune UUID, two 10-rune dates) fit with room
// to spare.
func manualBackfillGeneration(family, org, day, to string) string {
	return "manual-backfill:" + family + ":" + org + ":" + day + ".." + to
}

// manualBackfillDayResult is one day's outcome from `metrics remaining
// start`, in the shape printed under the response's "days" array.
type manualBackfillDayResult struct {
	Day         string `json:"day"`
	Status      string `json:"status"`
	RunID       string `json:"run_id,omitempty"`
	PartitionID string `json:"partition_id,omitempty"`
	// Generation is the ACTUAL generation this day's run was inserted (or
	// found) under -- codex review round 3, P2: it can differ from the
	// top-level request generation printed once for the whole command, if
	// StartManualBackfillRun had to bump past an exhausted 0-row/failed
	// generation. Use THIS value, not the top-level one, when building a
	// durable lookup query for this specific day's run.
	Generation string `json:"generation,omitempty"`
	Error      string `json:"error,omitempty"`
}

// anyManualBackfillDayErrored reports whether any day's result is an
// unexpected error or a generation-exhaustion failure (codex review, P2:
// both mean no new work was dispatched for that day) -- pulled out of
// dispatchMetricsRemaining as its own function so it can be unit tested
// directly against realistic
// result shapes, the same testability reasoning
// redriveDailyMetricsLedger's ledgerRepairWasIncomplete documents.
func anyManualBackfillDayErrored(results []manualBackfillDayResult) bool {
	for _, result := range results {
		if result.Status == "error" || result.Status == "exhausted" {
			return true
		}
	}
	return false
}

// manualBackfillReadbackTable names the primary ClickHouse table each
// day-scoped remaining-metrics family writes (families.json's "writes"
// list), for the readback hint dispatchMetricsRemaining prints after
// starting a run. All three tables carry an org_id String and a day Date
// column (migrations 023b/024, 007/024, 034), so the same hint shape works
// for all of them.
var manualBackfillReadbackTable = map[string]string{
	"complexity":     "repo_complexity_daily",
	"dora":           "dora_metrics_daily",
	"release_impact": "release_impact_daily",
}

// manualBackfillMaxDays bounds a single `metrics remaining start` command's
// [--day, --to] span. This is a manual, human-invoked recovery tool for a
// handful of never-dispatched historical days (CHAOS-4254's motivating case
// is 3 days), not a bulk backfill mechanism -- an operator who wants more
// than a month of days should be reconsidering whether this is the right
// tool, not stepping through a larger constant.
const manualBackfillMaxDays = 31

// dispatchMetricsRemaining handles `metrics remaining start` (CHAOS-4254):
// the operator entry point that dispatches a NEW remaining-metrics run for a
// historical (organization, family, day) that no automatic trigger
// (post-sync or fixed-schedule) ever dispatched -- the day was never
// computed at all, which is outside what `jobs retry` or `metrics
// daily-redrive` can recover (those both require a run that was dispatched
// and then discarded/stranded). This is also the prod recovery path for
// CHAOS-4384: a day frozen at 0 rows by the pre-fix same-day coverage bug is
// backfillable here even though it already has a "succeeded" partition,
// because that partition wrote nothing.
//
// Mirrors `metrics daily-redrive`'s command shape (WORKER_OPERATOR_TOKEN
// authentication via configureRuntime, --review-evidence required, quiet
// invalid_request on any malformed input) but does NOT wire a
// jobruntime.MetricsCollector to remaining.PostgresStore's
// SetManualBackfillObserver: workerctl is a one-shot CLI with no metrics
// endpoint to scrape, so a counter incremented in this process before exit
// is never observed. dev_health_remaining_metrics_manual_backfill_total
// exists in internal/jobruntime for when a long-running process calls
// StartManualBackfillRun; daily-redrive's own SetRedriveObserver is wired
// the identical way (cmd/dev-health-worker/daily.go only, never here) for
// the same reason -- see this command's PR RISK-NOTES.
func dispatchMetricsRemaining(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "start":
		flags := quietFlags("metrics remaining start")
		family := flags.String("family", "", "day-scoped remaining-metrics family (complexity, dora, release_impact)")
		day := flags.String("day", "", "first target day, inclusive (YYYY-MM-DD, UTC)")
		to := flags.String("to", "", "last target day, inclusive (YYYY-MM-DD, UTC) -- defaults to --day for a single day")
		org := flags.String("org", "", "organization id (uuid)")
		reviewEvidence := flags.String("review-evidence", "", "REQUIRED: why this historical day needs a manual backfill (e.g. \"CHAOS-4384 -- day frozen at 0 rows by the pre-fix same-day coverage bug, source data has since landed\")")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
			return writeError(stderr, "invalid_request")
		}
		if !slices.Contains(remaining.ManualBackfillDayScopedFamilies, *family) {
			return writeError(stderr, "invalid_request")
		}
		if _, err := uuid.Parse(*org); err != nil {
			return writeError(stderr, "invalid_request")
		}
		if strings.TrimSpace(*reviewEvidence) == "" {
			return writeError(stderr, "invalid_request")
		}
		fromDay, err := time.Parse("2006-01-02", *day)
		if err != nil {
			return writeError(stderr, "invalid_request")
		}
		toRaw := *to
		if toRaw == "" {
			toRaw = *day
		}
		toDay, err := time.Parse("2006-01-02", toRaw)
		if err != nil || toDay.Before(fromDay) {
			return writeError(stderr, "invalid_request")
		}
		if int(toDay.Sub(fromDay).Hours()/24)+1 > manualBackfillMaxDays {
			return writeError(stderr, "invalid_request")
		}
		// codex review round 2, P2: this is a HISTORICAL recovery tool -- a
		// future --day/--to (a mistyped year, most likely) would still
		// create a durable run and, for release_impact/dora's
		// backfill_days window, silently compute PAST days as a side
		// effect of an operator error that should have been an
		// invalid_request instead.
		//
		// codex review round 3, P1: today itself is ALSO excluded, not just
		// the future. dora's automatic triggers (post-sync's first-sync-of-
		// day, the fixed-schedule dora_daily_fanout occurrence) both only
		// ever target the current UTC day -- never a closed one. A manual
		// backfill for today could therefore commit its own pending run,
		// release the advisory lock, and then race an automatic trigger
		// that starts a SEPARATE generation for the same day (StartRunTx's
		// dora coverage check only recognizes SUCCEEDED runs, so a manual
		// run still pending is invisible to it): both eventually execute
		// and append duplicate dora_metrics_daily rows. Restricting this
		// command to strictly closed days removes the race entirely --
		// automatic triggers never revisit a day once it has closed -- and
		// matches the command's whole purpose (a day that was never
		// dispatched at all is, by construction, already in the past).
		if !toDay.Before(time.Now().UTC().Truncate(24 * time.Hour)) {
			return writeError(stderr, "invalid_request")
		}
		if runtime.pools == nil || runtime.registry == nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		store, err := remaining.NewPostgresStore(runtime.pools.Domain)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		publisher, err := remaining.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		// codex review, P1: a wall-clock generation made a retried CLI
		// invocation (identical flags, rerun after the first commit but
		// before its result was observed) indistinguishable from a
		// genuinely new request -- coverage only recognizes SUCCEEDED
		// partitions, so the retry could insert and dispatch a second run
		// per day while the first was still pending/running. Deriving
		// generation from the request's own shape makes an identical rerun
		// land on insertRun's ON CONFLICT DO NOTHING path (surfaced as
		// "already_ran") instead of a duplicate.
		generation := manualBackfillGeneration(*family, *org, *day, toRaw)
		var results []manualBackfillDayResult
		for cursor := fromDay; !cursor.After(toDay); cursor = cursor.AddDate(0, 0, 1) {
			dayString := cursor.Format("2006-01-02")
			outcome, startErr := store.StartManualBackfillRun(ctx, *family, *org, dayString, generation, publisher)
			switch {
			case errors.Is(startErr, remaining.ErrDayAlreadyCovered):
				results = append(results, manualBackfillDayResult{
					Day: dayString, Status: "already_covered", RunID: outcome.RunID,
				})
			case errors.Is(startErr, remaining.ErrDayInProgress):
				results = append(results, manualBackfillDayResult{
					Day: dayString, Status: "in_progress", RunID: outcome.RunID,
				})
			case errors.Is(startErr, remaining.ErrManualBackfillGenerationExhausted):
				results = append(results, manualBackfillDayResult{
					Day: dayString, Status: "exhausted", RunID: outcome.RunID, Generation: outcome.Generation,
				})
			case startErr != nil:
				results = append(results, manualBackfillDayResult{Day: dayString, Status: "error", Error: startErr.Error()})
			case outcome.AlreadyRan:
				results = append(results, manualBackfillDayResult{
					Day: dayString, Status: "already_ran", RunID: outcome.RunID, PartitionID: outcome.PartitionID, Generation: outcome.Generation,
				})
			default:
				results = append(results, manualBackfillDayResult{
					Day: dayString, Status: "started", RunID: outcome.RunID, PartitionID: outcome.PartitionID, Generation: outcome.Generation,
				})
			}
		}
		// codex review, P2: an "error" status buried in one day's result
		// object must not read as overall success -- a caller that checks
		// only the process exit code (a shell script, a scheduled job)
		// would otherwise treat a partially or completely undispatched
		// backfill as clean. Print the same full JSON to stdout either way
		// (the per-day detail is the actual diagnostic), but fail the
		// process if anything errored.
		hadDayError := anyManualBackfillDayErrored(results)
		code := writeResult(stdout, stderr, map[string]any{
			"family":          *family,
			"org":             *org,
			"generation":      generation,
			"review_evidence": *reviewEvidence,
			"days":            results,
			"readback_hint": fmt.Sprintf(
				"ClickHouse: SELECT day, count() FROM %s WHERE org_id = '%s' AND day BETWEEN '%s' AND '%s' GROUP BY day ORDER BY day",
				manualBackfillReadbackTable[*family], *org, *day, toRaw,
			),
		})
		if code == 0 && hadDayError {
			return 1
		}
		return code
	default:
		return writeError(stderr, "invalid_request")
	}
}

// redriveDailyMetricsLedger calls the Python compatibility bridge's bulk
// ledger repair (CHAOS-4304, POST /internal/worker/daily-metrics/v1/redrive)
// for the given run ids, BEFORE any Go-side partition job publishes for the
// same redrive -- see the ordering comment at this function's one call site.
// An empty runIDs list is a no-op (nothing to repair): it still returns a
// zero-valued result rather than skipping the call, so a redrive over a
// window with no 'running' runs at all is reported honestly, not silently.
// dailyMetricsRedriveMaxRunIDsPerRequest mirrors
// DailyMetricsRedriveRequest.run_ids's max_length=200 bound in
// worker_metrics.py -- a window bigger than one post_sync fanout's
// generous ceiling (up to 15 daily runs per completed sync) can still
// exceed 200 running runs, so this chunks rather than trusting the caller
// to stay under the bridge's own limit (codex review round 2).
const dailyMetricsRedriveMaxRunIDsPerRequest = 200

// redriveDailyMetricsLedger repairs the compatibility-bridge ledger for
// every run id, chunking into requests no larger than the bridge's own
// max_length bound and summing the aggregate outcome across chunks.
//
// operations (codex review, round 1, P1) scopes which
// metric_compatibility_executions.operation this call is authorized to
// repair -- omit it (the daily-redrive call site does) to keep the
// pre-CHAOS-4409 default `["partition"]` on the bridge side byte-for-byte;
// pass `"finalize"` (the daily-finalize call site does, via
// finalizeLedgerRepairGate) to repair finalize rows instead. A caller's
// review_evidence means something different for each operation, so this is
// never a caller-set default -- see DailyMetricsRedriveRequest.operations'
// own doc comment for why daily-redrive must never repair finalize rows
// under its own partition-scoped review_evidence.
func redriveDailyMetricsLedger(ctx context.Context, runIDs []string, reviewEvidence string, operations ...string) (map[string]any, error) {
	if len(runIDs) == 0 {
		return redriveDailyMetricsLedgerChunk(ctx, nil, reviewEvidence, operations...)
	}
	totalRepaired, totalSkipped := 0, 0
	for start := 0; start < len(runIDs); start += dailyMetricsRedriveMaxRunIDsPerRequest {
		end := min(start+dailyMetricsRedriveMaxRunIDsPerRequest, len(runIDs))
		chunkResult, err := redriveDailyMetricsLedgerChunk(ctx, runIDs[start:end], reviewEvidence, operations...)
		if err != nil {
			return nil, err
		}
		repaired, _ := chunkResult["repaired"].(float64)
		skipped, _ := chunkResult["skipped_claim_active"].(float64)
		totalRepaired += int(repaired)
		totalSkipped += int(skipped)
	}
	return map[string]any{"repaired": totalRepaired, "skipped_claim_active": totalSkipped}, nil
}

// ledgerRepairWasIncomplete reports whether any ambiguous/stuck-executing
// ledger row was left unrepaired (codex review round 2's abort gate),
// pulled out of dispatchMetrics as its own function so it can be unit
// tested directly against redriveDailyMetricsLedger's actual return shape --
// a prior version asserted the wrong dynamic type here (round 3: it reads
// plain Go ints, not the float64 a raw json.Unmarshal produces) and the
// ", _" discard pattern silently swallowed the mismatch, always reading 0
// and defeating the whole safety gate with no test catching it.
func ledgerRepairWasIncomplete(ledgerRepair map[string]any) bool {
	skipped, _ := ledgerRepair["skipped_claim_active"].(int)
	return skipped > 0
}

func redriveDailyMetricsLedgerChunk(ctx context.Context, runIDs []string, reviewEvidence string, operations ...string) (map[string]any, error) {
	if len(runIDs) == 0 {
		return map[string]any{"repaired": 0, "skipped_claim_active": 0}, nil
	}
	baseURL, ok := resolveRequired("WORKER_OPERATIONAL_BRIDGE_URL", os.LookupEnv)
	if !ok {
		return nil, errors.New("WORKER_OPERATIONAL_BRIDGE_URL is not configured")
	}
	token, ok := resolveRequired("WORKER_METRIC_REPAIR_TOKEN", os.LookupEnv)
	if !ok {
		return nil, errors.New("WORKER_METRIC_REPAIR_TOKEN is not configured")
	}
	requestPayload := map[string]any{
		"run_ids":         runIDs,
		"review_evidence": reviewEvidence,
	}
	if len(operations) > 0 {
		requestPayload["operations"] = operations
	}
	body, err := json.Marshal(requestPayload)
	if err != nil {
		return nil, err
	}
	requestURL := strings.TrimRight(baseURL.Reveal(), "/") + "/internal/worker/daily-metrics/v1/redrive"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token.Reveal())
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ledger redrive returned status %d", response.StatusCode)
	}
	var decoded map[string]any
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func dispatchQueues(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "status":
		if len(args) != 1 || runtime.queueStatusSource == nil {
			return writeError(stderr, "invalid_request")
		}
		if err := runtime.service.Status(ctx, runtime.principal); err != nil {
			return writeServiceError(stderr, err)
		}
		status, err := runtime.queueStatusSource.Status(ctx)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		return writeResult(stdout, stderr, status)
	case "pause", "resume":
		action := args[0]
		flags := quietFlags("queues " + action)
		reason := flags.String("reason", "", "bounded reason code")
		correlation := flags.String("correlation-id", "", "bounded correlation ID")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 1 || *reason == "" || *correlation == "" {
			return writeError(stderr, "invalid_request")
		}
		queue := flags.Arg(0)
		var err error
		if action == "pause" {
			err = runtime.service.PauseQueue(ctx, runtime.principal, queue, *reason, *correlation)
		} else {
			err = runtime.service.ResumeQueue(ctx, runtime.principal, queue, *reason, *correlation)
		}
		if err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, map[string]string{"queue": queue, "status": action + "d"})
	case "drain", "undrain":
		action := args[0]
		flags := quietFlags("queues " + action)
		group := flags.String("group", "", "worker group")
		reason := flags.String("reason", "", "bounded reason code")
		correlation := flags.String("correlation-id", "", "bounded correlation ID")
		var queues stringList
		flags.Var(&queues, "queue", "canonical queue (repeatable)")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || *group == "" || *reason == "" || *correlation == "" || len(queues) == 0 {
			return writeError(stderr, "invalid_request")
		}
		var (
			result joboperator.DrainResult
			err    error
		)
		if action == "drain" {
			result, err = runtime.service.Drain(ctx, runtime.principal, *group, queues, *reason, *correlation)
		} else {
			result, err = runtime.service.Undrain(ctx, runtime.principal, *group, queues, *reason, *correlation)
		}
		if err != nil {
			return writeServiceError(stderr, err)
		}
		return writeResult(stdout, stderr, result)
	default:
		return writeError(stderr, "invalid_request")
	}
}

func dispatchContracts(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("contracts")
	var queues stringList
	flags.Var(&queues, "queue", "canonical queue (repeatable)")
	if flags.Parse(args) != nil || flags.NArg() != 0 || len(queues) == 0 {
		return writeError(stderr, "invalid_request")
	}
	contracts, err := runtime.service.Contracts(ctx, runtime.principal, queues)
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, contracts)
}

type stringList []string

func (values *stringList) String() string { return strings.Join(*values, ",") }
func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func quietFlags(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func positiveID(raw string) (int64, bool) {
	id, err := strconv.ParseInt(raw, 10, 64)
	return id, err == nil && id > 0
}

func resolveRequired(key string, lookup platformsecrets.LookupEnv) (platformsecrets.Value, bool) {
	value, configured, err := platformsecrets.Resolve(key, lookup)
	return value, err == nil && configured
}

func resolveName(key, fallback string, lookup platformsecrets.LookupEnv) string {
	if value, configured := lookup(key); configured && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func writeResult(stdout, stderr io.Writer, value any) int {
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(true)
	if err := encoder.Encode(value); err != nil {
		return writeError(stderr, "output_unavailable")
	}
	return 0
}

func writeServiceError(stderr io.Writer, err error) int {
	var serviceError *joboperator.ServiceError
	if errors.As(err, &serviceError) {
		return writeError(stderr, string(serviceError.Code))
	}
	return writeError(stderr, "operator_request_failed")
}

func writeError(stderr io.Writer, code string) int {
	_, _ = fmt.Fprintf(stderr, "{\"error\":{\"code\":%q}}\n", code)
	return 1
}
