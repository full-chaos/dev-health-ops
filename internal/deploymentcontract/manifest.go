// Package deploymentcontract validates the checked-in Go worker deployment
// topology against the job registry and PostgreSQL connection budgets.
package deploymentcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

const maxManifestBytes = 512 * 1024

var (
	namePattern  = regexp.MustCompile(`^[a-z][a-z0-9-]+$`)
	queuePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]*$`)
	envPattern   = regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`)
)

// PostgresBudget is the checked-in connection-budget contract for the
// PostgreSQL server the Go deployment's three PgBouncer endpoints share
// (CHAOS-3945). ServerMaxConnections must equal that server's real,
// configured `max_connections` value, not a Go-stack-only figure: the
// transaction pool counted here is the SAME endpoint the Python/Celery
// stack's domain traffic uses (see docs/operate/configure/databases-and-storage.md),
// so this budget already covers the one pooler shared across both runtimes.
// ServerReservedConnections is a deliberate flat buffer for everything this
// model does not enumerate by name — direct/non-pooled sessions (migrations
// are tracked separately via MigrationJob), operator psql sessions, and
// managed-Postgres incidentals (a live host has shown a ~10-connection
// unmodeled role alongside this budget's four). It is NOT a per-consumer
// reconciliation; widening what it needs to cover is a bigger cross-plane
// exercise than this struct, and should be a deliberate size change with its
// own evidence, not a silent one.
type PostgresBudget struct {
	ServerMaxConnections                            int `json:"server_max_connections"`
	ServerReservedConnections                       int `json:"server_reserved_connections"`
	PgBouncerTransactionPoolSize                    int `json:"pgbouncer_transaction_pool_size"`
	PgBouncerTransactionServerPoolCount             int `json:"pgbouncer_transaction_server_pool_count"`
	PgBouncerTransactionMaxClientConnections        int `json:"pgbouncer_transaction_max_client_connections"`
	PgBouncerQueueSessionPoolSize                   int `json:"pgbouncer_queue_session_pool_size"`
	PgBouncerQueueSessionMaxClientConnections       int `json:"pgbouncer_queue_session_max_client_connections"`
	PgBouncerCoordinatorSessionPoolSize             int `json:"pgbouncer_coordinator_session_pool_size"`
	PgBouncerCoordinatorSessionMaxClientConnections int `json:"pgbouncer_coordinator_session_max_client_connections"`
}

type MigrationJob struct {
	Name           string   `json:"name"`
	Binary         string   `json:"binary"`
	MaxConnections int      `json:"max_connections"`
	ConfigEnv      []string `json:"config_env"`
	SecretEnv      []string `json:"secret_env"`
}

type OperatorCLI struct {
	Name                       string   `json:"name"`
	Binary                     string   `json:"binary"`
	MaxConcurrentInvocations   int      `json:"max_concurrent_invocations"`
	QueueControlMaxConnections int      `json:"queue_control_max_connections"`
	DomainMaxConnections       int      `json:"domain_max_connections"`
	CoordinatorMaxConnections  int      `json:"coordinator_max_connections"`
	ConfigEnv                  []string `json:"config_env"`
	SecretEnv                  []string `json:"secret_env"`
}

type QueueWorker struct {
	Queue      string `json:"queue"`
	MaxWorkers int    `json:"max_workers"`
}

type Process struct {
	Name                       string        `json:"name"`
	Binary                     string        `json:"binary"`
	Runtime                    string        `json:"runtime"`
	EnabledByDefault           bool          `json:"enabled_by_default"`
	MinReplicas                int           `json:"min_replicas"`
	DesiredReplicas            int           `json:"desired_replicas"`
	MaxReplicas                int           `json:"max_replicas"`
	ShutdownGraceSeconds       int           `json:"shutdown_grace_seconds"`
	Queues                     []string      `json:"queues"`
	QueueWorkers               []QueueWorker `json:"queue_workers"`
	JobKinds                   []string      `json:"job_kinds"`
	QueueControlMaxConnections int           `json:"queue_control_max_connections"`
	DomainMaxConnections       int           `json:"domain_max_connections"`
	CoordinatorMaxConnections  int           `json:"coordinator_max_connections"`
	RequiresClickHouse         bool          `json:"requires_clickhouse"`
	RequiresValkey             bool          `json:"requires_valkey"`
	SecretEnv                  []string      `json:"secret_env"`
}

type Manifest struct {
	SchemaVersion   int            `json:"schema_version"`
	DeploymentState string         `json:"deployment_state"`
	Registry        string         `json:"registry"`
	RuntimeRoleEnv  []string       `json:"runtime_role_env"`
	PostgresBudget  PostgresBudget `json:"postgres_budget"`
	MigrationJob    MigrationJob   `json:"migration_job"`
	OperatorCLI     OperatorCLI    `json:"operator_cli"`
	Processes       []Process      `json:"processes"`
}

type BudgetSummary struct {
	QueueSessionClientConnections       int
	QueueSessionHeadroom                int
	CoordinatorSessionClientConnections int
	CoordinatorSessionHeadroom          int
	DomainTransactionClientConnections  int
	DomainTransactionHeadroom           int
	ServerConnectionFootprint           int
	ServerConnectionHeadroom            int
}

func Load(path string, registry jobcontract.Registry) (Manifest, BudgetSummary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, BudgetSummary{}, fmt.Errorf("read deployment manifest: %w", err)
	}
	if len(data) == 0 || len(data) > maxManifestBytes || !utf8.Valid(data) {
		return Manifest{}, BudgetSummary{}, errors.New("deployment manifest has invalid encoding or size")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, BudgetSummary{}, fmt.Errorf("decode deployment manifest: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return Manifest{}, BudgetSummary{}, err
	}
	summary, err := manifest.Validate(registry)
	if err != nil {
		return Manifest{}, BudgetSummary{}, err
	}
	return manifest, summary, nil
}

func (manifest Manifest) Validate(registry jobcontract.Registry) (BudgetSummary, error) {
	if manifest.SchemaVersion != 1 || manifest.DeploymentState != "coexistence_disabled" {
		return BudgetSummary{}, errors.New("unsupported deployment manifest identity")
	}
	if manifest.Registry != "contracts/jobs/v1/registry.json" {
		return BudgetSummary{}, errors.New("deployment manifest registry path is not canonical")
	}
	// All three runtime role identities of the Option B split. The coordinator
	// role belongs here even though only the control-runtime processes and
	// workerctl open a coordinator pool: the three names must be pairwise
	// distinct deployment-wide, and platform/config enforces that on every
	// process, including domain-only ones.
	if !equalStrings(manifest.RuntimeRoleEnv, []string{
		"RIVER_COORDINATOR_DATABASE_ROLE",
		"RIVER_DOMAIN_DATABASE_ROLE",
		"RIVER_QUEUE_DATABASE_ROLE",
	}) {
		return BudgetSummary{}, errors.New("runtime role identity configuration is invalid")
	}
	if err := validatePostgresBudget(manifest.PostgresBudget); err != nil {
		return BudgetSummary{}, err
	}
	if err := validateMigrationJob(manifest.MigrationJob, manifest.PostgresBudget); err != nil {
		return BudgetSummary{}, err
	}
	if err := validateOperatorCLI(manifest.OperatorCLI); err != nil {
		return BudgetSummary{}, err
	}
	if len(manifest.Processes) == 0 {
		return BudgetSummary{}, errors.New("deployment manifest has no processes")
	}

	queueCoverage := buildQueueCoverage(registry)
	seenNames := make(map[string]struct{}, len(manifest.Processes))
	previousName := ""
	summary := BudgetSummary{
		QueueSessionClientConnections: manifest.OperatorCLI.MaxConcurrentInvocations *
			manifest.OperatorCLI.QueueControlMaxConnections,
		CoordinatorSessionClientConnections: manifest.OperatorCLI.MaxConcurrentInvocations *
			manifest.OperatorCLI.CoordinatorMaxConnections,
		DomainTransactionClientConnections: manifest.OperatorCLI.MaxConcurrentInvocations *
			manifest.OperatorCLI.DomainMaxConnections,
	}

	for _, process := range manifest.Processes {
		if process.Name <= previousName {
			return BudgetSummary{}, errors.New("deployment processes must be sorted by name")
		}
		previousName = process.Name
		if _, duplicate := seenNames[process.Name]; duplicate {
			return BudgetSummary{}, fmt.Errorf("duplicate deployment process %s", process.Name)
		}
		seenNames[process.Name] = struct{}{}
		if err := validateProcess(process, queueCoverage); err != nil {
			return BudgetSummary{}, fmt.Errorf("deployment process %s: %w", process.Name, err)
		}

		summary.QueueSessionClientConnections += process.MaxReplicas * process.QueueControlMaxConnections
		summary.CoordinatorSessionClientConnections += process.MaxReplicas * process.CoordinatorMaxConnections
		summary.DomainTransactionClientConnections += process.MaxReplicas * process.DomainMaxConnections

		if process.Runtime != "river" {
			continue
		}
		kinds, longestTimeout, err := selectedQueueKinds(queueCoverage, process.Queues)
		if err != nil {
			return BudgetSummary{}, fmt.Errorf("deployment process %s: %w", process.Name, err)
		}
		if !equalStrings(process.JobKinds, kinds) {
			return BudgetSummary{}, fmt.Errorf("deployment process %s queue-kind coverage drift", process.Name)
		}
		if longestTimeout == 0 || process.ShutdownGraceSeconds < longestTimeout+60 {
			return BudgetSummary{}, fmt.Errorf("deployment process %s shutdown grace cannot cover the longest claim and finalization", process.Name)
		}
	}
	summary.ServerConnectionFootprint = manifest.PostgresBudget.ServerReservedConnections +
		manifest.PostgresBudget.PgBouncerTransactionPoolSize*manifest.PostgresBudget.PgBouncerTransactionServerPoolCount +
		manifest.PostgresBudget.PgBouncerQueueSessionPoolSize +
		manifest.PostgresBudget.PgBouncerCoordinatorSessionPoolSize
	if summary.ServerConnectionFootprint > manifest.PostgresBudget.ServerMaxConnections {
		return BudgetSummary{}, fmt.Errorf(
			"PostgreSQL server connection budget exceeded: %d > %d",
			summary.ServerConnectionFootprint,
			manifest.PostgresBudget.ServerMaxConnections,
		)
	}
	if summary.DomainTransactionClientConnections > manifest.PostgresBudget.PgBouncerTransactionMaxClientConnections {
		return BudgetSummary{}, fmt.Errorf(
			"transaction PgBouncer client connection budget exceeded: %d > %d",
			summary.DomainTransactionClientConnections,
			manifest.PostgresBudget.PgBouncerTransactionMaxClientConnections,
		)
	}
	if summary.QueueSessionClientConnections > manifest.PostgresBudget.PgBouncerQueueSessionMaxClientConnections ||
		summary.QueueSessionClientConnections > manifest.PostgresBudget.PgBouncerQueueSessionPoolSize {
		return BudgetSummary{}, errors.New("queue session PgBouncer budget cannot serve every declared River queue connection")
	}
	if summary.CoordinatorSessionClientConnections > manifest.PostgresBudget.PgBouncerCoordinatorSessionMaxClientConnections ||
		summary.CoordinatorSessionClientConnections > manifest.PostgresBudget.PgBouncerCoordinatorSessionPoolSize {
		return BudgetSummary{}, errors.New("coordinator session PgBouncer budget cannot serve every declared coordinator connection")
	}
	queueSessionLimit := minInt(
		manifest.PostgresBudget.PgBouncerQueueSessionMaxClientConnections,
		manifest.PostgresBudget.PgBouncerQueueSessionPoolSize,
	)
	coordinatorSessionLimit := minInt(
		manifest.PostgresBudget.PgBouncerCoordinatorSessionMaxClientConnections,
		manifest.PostgresBudget.PgBouncerCoordinatorSessionPoolSize,
	)
	summary.QueueSessionHeadroom = queueSessionLimit - summary.QueueSessionClientConnections
	summary.CoordinatorSessionHeadroom = coordinatorSessionLimit - summary.CoordinatorSessionClientConnections
	summary.DomainTransactionHeadroom = manifest.PostgresBudget.PgBouncerTransactionMaxClientConnections -
		summary.DomainTransactionClientConnections
	summary.ServerConnectionHeadroom = manifest.PostgresBudget.ServerMaxConnections -
		summary.ServerConnectionFootprint
	if summary.QueueSessionHeadroom <= 0 {
		return BudgetSummary{}, errors.New("queue session PgBouncer headroom must be positive")
	}
	if summary.CoordinatorSessionHeadroom <= 0 {
		return BudgetSummary{}, errors.New("coordinator session PgBouncer headroom must be positive")
	}
	if summary.DomainTransactionHeadroom <= 0 {
		return BudgetSummary{}, errors.New("transaction PgBouncer headroom must be positive")
	}
	if summary.ServerConnectionHeadroom <= 0 {
		return BudgetSummary{}, errors.New("PostgreSQL server connection headroom must be positive")
	}
	return summary, nil
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

type queueCoverage struct {
	kinds          map[string]struct{}
	longestTimeout int
}

func buildQueueCoverage(registry jobcontract.Registry) map[string]queueCoverage {
	coverage := make(map[string]queueCoverage)
	for _, job := range registry.Jobs {
		info := coverage[job.Queue]
		if info.kinds == nil {
			info.kinds = make(map[string]struct{})
		}
		info.kinds[job.Kind] = struct{}{}
		if job.TimeoutSeconds > info.longestTimeout {
			info.longestTimeout = job.TimeoutSeconds
		}
		coverage[job.Queue] = info
	}
	return coverage
}

func selectedQueueKinds(coverage map[string]queueCoverage, queues []string) ([]string, int, error) {
	kinds := make(map[string]struct{})
	longestTimeout := 0
	for _, queue := range queues {
		info, ok := coverage[queue]
		if !ok {
			return nil, 0, fmt.Errorf("queue %s is not registered", queue)
		}
		for kind := range info.kinds {
			kinds[kind] = struct{}{}
		}
		if info.longestTimeout > longestTimeout {
			longestTimeout = info.longestTimeout
		}
	}
	return sortedKeys(kinds), longestTimeout, nil
}

func validatePostgresBudget(budget PostgresBudget) error {
	if budget.ServerMaxConnections < 1 || budget.ServerMaxConnections > 10000 ||
		budget.ServerReservedConnections < 1 ||
		budget.PgBouncerTransactionPoolSize < 1 ||
		budget.PgBouncerTransactionServerPoolCount < 1 || budget.PgBouncerTransactionServerPoolCount > 128 ||
		budget.PgBouncerTransactionMaxClientConnections < 1 ||
		budget.PgBouncerQueueSessionPoolSize < 1 || budget.PgBouncerQueueSessionMaxClientConnections < 1 ||
		budget.PgBouncerCoordinatorSessionPoolSize < 1 || budget.PgBouncerCoordinatorSessionMaxClientConnections < 1 {
		return errors.New("deployment PostgreSQL budget has invalid bounds")
	}
	if budget.ServerReservedConnections+
		budget.PgBouncerTransactionPoolSize*budget.PgBouncerTransactionServerPoolCount+
		budget.PgBouncerQueueSessionPoolSize+
		budget.PgBouncerCoordinatorSessionPoolSize > budget.ServerMaxConnections {
		return errors.New("deployment PostgreSQL budget exceeds the server connection limit")
	}
	return nil
}

func validateMigrationJob(job MigrationJob, budget PostgresBudget) error {
	if !namePattern.MatchString(job.Name) || job.Binary != "dev-hops" {
		return errors.New("migration job identity is invalid")
	}
	if job.MaxConnections < 1 || job.MaxConnections > 4 || job.MaxConnections > budget.ServerReservedConnections {
		return errors.New("migration job connection budget is invalid")
	}
	if !equalStrings(job.ConfigEnv, []string{"RIVER_DATABASE_SCHEMA", "RIVER_DOMAIN_DATABASE_ROLE", "RIVER_QUEUE_DATABASE_ROLE"}) ||
		!sortedUnique(job.SecretEnv) || !validEnvNames(job.SecretEnv) || !contains(job.SecretEnv, "MIGRATION_DATABASE_URI") {
		return errors.New("migration job role and dedicated DSN wiring is invalid")
	}
	if contains(job.SecretEnv, "WORKER_DATABASE_URI") || contains(job.SecretEnv, "POSTGRES_URI") {
		return errors.New("migration job must not receive runtime database DSNs")
	}
	return nil
}

func validateOperatorCLI(operator OperatorCLI) error {
	if operator.Name != "worker-operator" || operator.Binary != "dev-health-workerctl" ||
		operator.MaxConcurrentInvocations != 1 || operator.QueueControlMaxConnections < 1 ||
		operator.QueueControlMaxConnections > 4 || operator.DomainMaxConnections < 1 ||
		operator.DomainMaxConnections > 16 ||
		// workerctl is one of the three coordinator groups (with reconciler
		// and scheduler) under the Option B split. Their dedicated PgBouncer
		// session endpoint is server-counted, while preserving River semantics.
		operator.CoordinatorMaxConnections < 1 || operator.CoordinatorMaxConnections > 4 {
		return errors.New("worker operator deployment identity or connection budget is invalid")
	}
	// COORDINATOR_DATABASE_URI and RIVER_COORDINATOR_DATABASE_ROLE are required
	// here, not optional: workerctl authenticates its operator token against
	// internal_service_credentials, a coordinator-exclusive table, before any
	// command dispatches. Without the coordinator DSN the binary cannot do
	// anything at all, so the deployment contract refuses to describe it as
	// deployable without one.
	if !equalStrings(operator.ConfigEnv, []string{
		"COORDINATOR_DATABASE_MODE",
		"PGBOUNCER_TRANSACTION_MODE",
		"RIVER_COORDINATOR_DATABASE_ROLE",
		"RIVER_DATABASE_SCHEMA",
		"RIVER_DOMAIN_DATABASE_ROLE",
		"RIVER_QUEUE_DATABASE_ROLE",
		"WORKER_DATABASE_MODE",
	}) || !equalStrings(operator.SecretEnv, []string{
		"COORDINATOR_DATABASE_URI",
		"POSTGRES_URI",
		"WORKER_DATABASE_URI",
		"WORKER_OPERATOR_TOKEN",
	}) {
		return errors.New("worker operator deployment configuration is invalid")
	}
	return nil
}

func validateProcess(process Process, coverage map[string]queueCoverage) error {
	if !namePattern.MatchString(process.Name) || process.EnabledByDefault || process.MinReplicas != 0 ||
		process.DesiredReplicas < process.MinReplicas || process.DesiredReplicas > process.MaxReplicas ||
		process.MaxReplicas < 1 || process.MaxReplicas > 8 || process.ShutdownGraceSeconds < 60 {
		return errors.New("identity or coexistence replica policy is invalid")
	}
	if process.DomainMaxConnections < 1 || process.DomainMaxConnections > 16 ||
		process.QueueControlMaxConnections < 0 || process.QueueControlMaxConnections > 4 ||
		process.CoordinatorMaxConnections < 0 || process.CoordinatorMaxConnections > 4 {
		return errors.New("connection limits are invalid")
	}
	if !sortedUnique(process.Queues) || !sortedUnique(process.JobKinds) ||
		!sortedUnique(process.SecretEnv) || !validEnvNames(process.SecretEnv) {
		return errors.New("queues, job kinds, and secret env names must be sorted and unique")
	}
	queueWorkerNames := make([]string, 0, len(process.QueueWorkers))
	for _, queue := range process.QueueWorkers {
		if !queuePattern.MatchString(queue.Queue) || queue.MaxWorkers < 1 || queue.MaxWorkers > 10_000 {
			return errors.New("queue worker limits are invalid")
		}
		queueWorkerNames = append(queueWorkerNames, queue.Queue)
	}
	if !sortedUnique(queueWorkerNames) {
		return errors.New("queue worker limits must be sorted and unique")
	}
	if contains(process.SecretEnv, "MIGRATION_DATABASE_URI") {
		return errors.New("long-running process must not receive the migration DSN")
	}
	if !contains(process.SecretEnv, "POSTGRES_URI") {
		return errors.New("long-running process must receive the domain DSN")
	}
	if process.RequiresClickHouse != contains(process.SecretEnv, "CLICKHOUSE_URI") ||
		process.RequiresValkey != contains(process.SecretEnv, "VALKEY_URI") {
		return errors.New("dependency flags drift from secret env wiring")
	}

	switch process.Runtime {
	case "river":
		if process.Binary != "dev-health-worker" || process.QueueControlMaxConnections < 1 ||
			process.CoordinatorMaxConnections != 0 ||
			!contains(process.SecretEnv, "WORKER_DATABASE_URI") {
			return errors.New("River runtime is missing its binary, queue coverage, or queue-control DSN")
		}
		if !equalStrings(queueWorkerNames, process.Queues) {
			return errors.New("River queue worker limits drift from queue coverage")
		}
		kinds, _, err := selectedQueueKinds(coverage, process.Queues)
		if err != nil {
			return err
		}
		if !equalStrings(process.JobKinds, kinds) {
			return errors.New("River job-kind coverage drifts from queue selection")
		}
	case "control":
		expectedBinary := map[string]string{
			"reconciler": "dev-health-reconciler",
			"scheduler":  "dev-health-scheduler",
		}[process.Name]
		if expectedBinary == "" || process.Binary != expectedBinary ||
			len(process.Queues) != 0 || len(process.JobKinds) != 0 ||
			process.QueueControlMaxConnections < 1 || !contains(process.SecretEnv, "WORKER_DATABASE_URI") ||
			len(process.QueueWorkers) != 0 ||
			// Control-runtime processes are the coordinator role's own worker
			// pool under the Option B split — their control-plane database
			// access uses a dedicated PgBouncer session pool. It is server-counted
			// in the shared budget but preserves control-plane session semantics.
			process.CoordinatorMaxConnections < 1 ||
			// A coordinator budget without the coordinator DSN would describe a
			// process that reserves connections it cannot open: the binary calls
			// RuntimeConfig.WithCoordinator and fails closed at startup without
			// this secret, so the contract must require them together.
			!contains(process.SecretEnv, "COORDINATOR_DATABASE_URI") {
			return errors.New("control runtime wiring is invalid")
		}
	case "stream":
		if process.Binary != "dev-health-stream-runner" ||
			len(process.Queues) != 0 || len(process.JobKinds) != 0 ||
			process.QueueControlMaxConnections != 0 || process.CoordinatorMaxConnections != 0 ||
			!process.RequiresValkey || len(process.QueueWorkers) != 0 {
			return errors.New("stream runtime wiring is invalid")
		}
		// External ingest intentionally has one consumer identity and one
		// processing lane until a separate partition/reclaim design proves
		// horizontal safety. Keep this fail-closed in the checked-in contract;
		// a duplicate deployment must be rejected before readiness can open.
		if process.Name == "stream-external" && process.MaxReplicas != 1 {
			return errors.New("external stream runtime must be a singleton")
		}
	default:
		return errors.New("runtime is invalid")
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("deployment manifest contains multiple JSON values")
	}
	return fmt.Errorf("decode deployment manifest: %w", err)
}

func sortedUnique(values []string) bool {
	for index, value := range values {
		if value == "" {
			return false
		}
		if index > 0 && value <= values[index-1] {
			return false
		}
	}
	return true
}

func validEnvNames(values []string) bool {
	for _, value := range values {
		if !envPattern.MatchString(value) {
			return false
		}
	}
	return true
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
