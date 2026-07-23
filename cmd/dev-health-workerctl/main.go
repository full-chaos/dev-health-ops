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
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/jackc/pgx/v5"
)

const (
	serviceName         = "dev-health-workerctl"
	operatorAdvisoryKey = int64(30330001)
	defaultDomainRole   = "devhealth_domain"
	defaultQueueRole    = "devhealth_queue"
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
}

type streamProfileStatus struct {
	Profile          string `json:"profile"`
	Owner            string `json:"owner"`
	EnabledByDefault bool   `json:"enabled_by_default"`
	MinReplicas      int    `json:"min_replicas"`
	MaxReplicas      int    `json:"max_replicas"`
}

type streamStatusResponse struct {
	DeploymentState string                `json:"deployment_state"`
	Profiles        []streamProfileStatus `json:"profiles"`
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
	token, ok := resolveRequired("WORKER_OPERATOR_TOKEN", lookup)
	if !ok {
		return nil, writeError(stderr, "authentication_failed")
	}
	mode := platformconfig.QueueControlDirect
	if raw, configured := lookup("WORKER_DATABASE_MODE"); configured && raw != "" {
		mode = platformconfig.QueueControlMode(strings.ToLower(raw))
	}
	if mode != platformconfig.QueueControlDirect {
		return nil, writeError(stderr, "queue_control_mode_unsupported")
	}
	domainRole := resolveName("RIVER_DOMAIN_DATABASE_ROLE", defaultDomainRole, lookup)
	queueRole := resolveName("RIVER_QUEUE_DATABASE_ROLE", defaultQueueRole, lookup)
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
	)
	runtimeConfig.QueueControlMode = mode
	runtimeConfig.RiverSchema = schema
	runtimeConfig.DomainTransactionPooler = domainTransactionPooler
	runtimeConfig.DomainMaxConns = 2
	runtimeConfig.QueueMaxConns = 2
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
	if postgresstore.CheckDomainAuthorization(ctx, pools.Domain, domainRole, schema) != nil ||
		postgresstore.CheckQueueAuthorization(ctx, pools.QueueControl, queueRole, schema) != nil {
		return nil, writeError(stderr, "runtime_role_unauthorized")
	}

	authenticator, err := joboperator.NewAuthenticator(pools.Domain)
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
	manifest, _, err := deploymentcontract.Load("deploy/go-workers/profiles.json", contractRegistry)
	if err != nil {
		return nil, writeError(stderr, "deployment_contract_invalid")
	}
	routeRegistry, err := syncdispatchcontract.Load("contracts/sync-dispatch/v1")
	if err != nil {
		return nil, writeError(stderr, "contract_registry_invalid")
	}
	routeCapabilities, err := syncroute.NewCapabilities(nil)
	if err != nil {
		return nil, writeError(stderr, "contract_registry_invalid")
	}
	routeController, err := syncroute.NewController(pools.Domain, routeRegistry, routeCapabilities)
	if err != nil {
		return nil, writeError(stderr, "operator_backend_unavailable")
	}
	jobQuiescer, err := jobroute.NewPostgresRiverQuiescer(pools.QueueControl, schema)
	if err != nil {
		return nil, writeError(stderr, "operator_backend_unavailable")
	}
	jobRouteController, err := jobroute.NewController(pools.Domain, registry, jobQuiescer)
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
	auditor, err := joboperator.NewPostgresAuditor(pools.Domain)
	if err != nil {
		return nil, writeError(stderr, "audit_unavailable")
	}
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
	return &operatorRuntime{
		service: service, principal: authentication.Principal(), pools: pools, lockTx: lockTx,
		streamDeploymentState: manifest.DeploymentState, streams: streams,
	}, 0
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
			"queue_control_mode":   "direct",
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
	quiescenceTimeout := flags.Duration("quiescence-timeout", 10*time.Second, "bounded external quiescence timeout")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 1 || *reason == "" || *correlation == "" {
		return writeError(stderr, "invalid_request")
	}
	kind := flags.Arg(0)
	var (
		state syncroute.RouteState
		err   error
	)
	switch args[0] {
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
