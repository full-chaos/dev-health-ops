// dev-health-workerctl is the authenticated, payload-redacted River operator
// CLI. It deliberately has no network listener and accepts credentials only
// through WORKER_OPERATOR_TOKEN or WORKER_OPERATOR_TOKEN_FILE.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
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
	profileStatusSource   workerProfileStatusSource
	queueControlMode      platformconfig.QueueControlMode
}

type streamProfileStatus struct {
	Profile          string `json:"profile"`
	Owner            string `json:"owner"`
	EnabledByDefault bool   `json:"enabled_by_default"`
	MinReplicas      int    `json:"min_replicas"`
	MaxReplicas      int    `json:"max_replicas"`
}

// workerProfileStatus is the operator view for River worker profiles. It is
// separate from streamProfileStatus because stream runners have a different
// ownership contract, while River profiles share queues and scale separately.
type workerProfileStatus struct {
	Profile         string `json:"profile"`
	DesiredReplicas int    `json:"desired_replicas"`
	LiveReplicas    int    `json:"live_replicas"`
	MaxReplicas     int    `json:"max_replicas"`
	QueueBacklog    int64  `json:"queue_backlog"`
	ActiveJobs      int64  `json:"active_jobs"`
	DrainState      string `json:"drain_state"`
}

type workerProfileStatusSource interface {
	Status(context.Context) (workerProfileStatusResponse, error)
}

type workerProfileStatusSourceFunc func(context.Context) (workerProfileStatusResponse, error)

func (source workerProfileStatusSourceFunc) Status(ctx context.Context) (workerProfileStatusResponse, error) {
	return source(ctx)
}

type streamStatusResponse struct {
	DeploymentState string                `json:"deployment_state"`
	Profiles        []streamProfileStatus `json:"profiles"`
}

type workerProfileStatusResponse struct {
	DeploymentState  string                        `json:"deployment_state"`
	ConnectionBudget profileConnectionBudgetStatus `json:"connection_budget"`
	Profiles         []workerProfileStatus         `json:"profiles"`
}

type connectionBudgetStatus struct {
	Used     int `json:"used"`
	Limit    int `json:"limit"`
	Headroom int `json:"headroom"`
}

type profileConnectionBudgetStatus struct {
	QueueSession       connectionBudgetStatus `json:"queue_session"`
	CoordinatorSession connectionBudgetStatus `json:"coordinator_session"`
	DomainTransaction  connectionBudgetStatus `json:"domain_transaction"`
	Server             connectionBudgetStatus `json:"server"`
}

type manifestProfileStatusSource struct {
	service   *joboperator.Service
	principal joboperator.Principal
	manifest  deploymentcontract.Manifest
	budget    deploymentcontract.BudgetSummary
	presence  func(context.Context) ([]jobruntime.ProfilePresenceSummary, error)
}

func (source manifestProfileStatusSource) Status(ctx context.Context) (workerProfileStatusResponse, error) {
	if source.service == nil || source.presence == nil {
		return workerProfileStatusResponse{}, errors.New("profile status source is unavailable")
	}
	presenceRows, err := source.presence(ctx)
	if err != nil {
		return workerProfileStatusResponse{}, err
	}
	presence := make(map[string]jobruntime.ProfilePresenceSummary, len(presenceRows))
	for _, summary := range presenceRows {
		if _, duplicate := presence[summary.Profile]; duplicate {
			return workerProfileStatusResponse{}, errors.New("duplicate profile presence summary")
		}
		presence[summary.Profile] = summary
	}
	profiles := make([]workerProfileStatus, 0, 3)
	for _, process := range source.manifest.Processes {
		if process.Runtime != "river" || process.RegistryProfile == nil {
			continue
		}
		profile := *process.RegistryProfile
		queues, err := source.service.Queues(ctx, source.principal, profile)
		if err != nil {
			return workerProfileStatusResponse{}, err
		}
		status := workerProfileStatus{
			Profile: profile, DesiredReplicas: process.DesiredReplicas,
			MaxReplicas: process.MaxReplicas, DrainState: "inactive",
		}
		if summary, ok := presence[profile]; ok {
			status.LiveReplicas = summary.Live
			if summary.Draining > 0 {
				status.DrainState = "draining"
			} else {
				status.DrainState = "active"
			}
			delete(presence, profile)
		}
		for _, queue := range queues {
			status.QueueBacklog += queue.Available + queue.Retryable + queue.Scheduled
			status.ActiveJobs += queue.Running
			if queue.Paused {
				status.DrainState = "draining"
			}
		}
		profiles = append(profiles, status)
	}
	if len(profiles) == 0 || len(presence) != 0 {
		return workerProfileStatusResponse{}, errors.New("profile presence is outside the deployment contract")
	}
	return workerProfileStatusResponse{
		DeploymentState: source.manifest.DeploymentState,
		ConnectionBudget: profileConnectionBudgetStatus{
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
		Profiles: profiles,
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
		return nil, writeError(stderr, "authentication_failed")
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
	// deploy/go-workers/profiles.json, the same way DomainMaxConns and
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
		return nil, writeError(stderr, "authentication_failed")
	}
	authentication, err := authenticator.Authenticate(ctx, token.Reveal())
	if err != nil {
		return nil, writeError(stderr, "authentication_failed")
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
	manifest, budget, err := deploymentcontract.Load("deploy/go-workers/profiles.json", contractRegistry)
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
	}
	runtime.profileStatusSource = manifestProfileStatusSource{
		service: service, principal: runtime.principal, manifest: manifest, budget: budget,
		presence: func(ctx context.Context) ([]jobruntime.ProfilePresenceSummary, error) {
			return jobruntime.ReadProfilePresence(ctx, pools.Domain)
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
	case "queues":
		return dispatchQueues(ctx, runtime, args[1:], stdout, stderr)
	case "drain":
		return dispatchDrain(ctx, runtime, args[1:], stdout, stderr)
	case "contracts":
		return dispatchContracts(ctx, runtime, args[1:], stdout, stderr)
	case "routes":
		return dispatchRoutes(ctx, runtime, args[1:], stdout, stderr)
	case "job-routes":
		return dispatchJobRoutes(ctx, runtime, args[1:], stdout, stderr)
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
	case "profiles":
		if len(args) != 2 || args[1] != "status" || runtime.profileStatusSource == nil {
			return writeError(stderr, "invalid_request")
		}
		if err := runtime.service.Status(ctx, runtime.principal); err != nil {
			return writeServiceError(stderr, err)
		}
		status, err := runtime.profileStatusSource.Status(ctx)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		return writeResult(stdout, stderr, status)
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

func dispatchQueues(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && (args[0] == "pause" || args[0] == "resume") {
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
	}
	flags := quietFlags("queues")
	profile := flags.String("profile", "ops", "registered worker profile")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	queues, err := runtime.service.Queues(ctx, runtime.principal, *profile)
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, queues)
}

func dispatchDrain(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("drain")
	profile := flags.String("profile", "", "registered worker profile")
	reason := flags.String("reason", "", "bounded reason code")
	correlation := flags.String("correlation-id", "", "bounded correlation ID")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *profile == "" || *reason == "" || *correlation == "" {
		return writeError(stderr, "invalid_request")
	}
	result, err := runtime.service.Drain(ctx, runtime.principal, *profile, *reason, *correlation)
	if err != nil {
		return writeServiceError(stderr, err)
	}
	return writeResult(stdout, stderr, result)
}

func dispatchContracts(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("contracts")
	profile := flags.String("profile", "ops", "registered worker profile")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	contracts, err := runtime.service.Contracts(ctx, runtime.principal, *profile)
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
