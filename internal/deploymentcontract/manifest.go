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
	namePattern    = regexp.MustCompile(`^[a-z][a-z0-9-]+$`)
	profilePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	queuePattern   = regexp.MustCompile(`^[a-z][a-z0-9._-]*$`)
	envPattern     = regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`)
)

type PostgresBudget struct {
	ServerMaxConnections          int `json:"server_max_connections"`
	ServerReservedConnections     int `json:"server_reserved_connections"`
	PgBouncerDefaultPoolSize      int `json:"pgbouncer_default_pool_size"`
	PgBouncerServerPoolCount      int `json:"pgbouncer_server_pool_count"`
	PgBouncerMaxClientConnections int `json:"pgbouncer_max_client_connections"`
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
	RegistryProfile            *string       `json:"registry_profile,omitempty"`
	EnabledByDefault           bool          `json:"enabled_by_default"`
	MinReplicas                int           `json:"min_replicas"`
	MaxReplicas                int           `json:"max_replicas"`
	Queues                     []string      `json:"queues"`
	QueueWorkers               []QueueWorker `json:"queue_workers"`
	JobKinds                   []string      `json:"job_kinds"`
	QueueControlMaxConnections int           `json:"queue_control_max_connections"`
	DomainMaxConnections       int           `json:"domain_max_connections"`
	RequiresClickHouse         bool          `json:"requires_clickhouse"`
	RequiresValkey             bool          `json:"requires_valkey"`
	SecretEnv                  []string      `json:"secret_env"`
}

type Manifest struct {
	SchemaVersion   int            `json:"schema_version"`
	DeploymentState string         `json:"deployment_state"`
	Registry        string         `json:"registry"`
	PostgresBudget  PostgresBudget `json:"postgres_budget"`
	MigrationJob    MigrationJob   `json:"migration_job"`
	OperatorCLI     OperatorCLI    `json:"operator_cli"`
	Processes       []Process      `json:"processes"`
}

type BudgetSummary struct {
	DirectQueueControlConnections int
	DomainClientConnections       int
	ServerConnectionFootprint     int
}

func Load(path string, registry jobcontract.Registry) (Manifest, BudgetSummary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, BudgetSummary{}, fmt.Errorf("read deployment profiles: %w", err)
	}
	if len(data) == 0 || len(data) > maxManifestBytes || !utf8.Valid(data) {
		return Manifest{}, BudgetSummary{}, errors.New("deployment profile manifest has invalid encoding or size")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, BudgetSummary{}, fmt.Errorf("decode deployment profiles: %w", err)
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
		return BudgetSummary{}, errors.New("unsupported deployment profile manifest identity")
	}
	if manifest.Registry != "contracts/jobs/v1/registry.json" {
		return BudgetSummary{}, errors.New("deployment profile registry path is not canonical")
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
		return BudgetSummary{}, errors.New("deployment profile manifest has no processes")
	}

	expected := expectedCoverage(registry)
	seenProfiles := make(map[string]struct{})
	seenNames := make(map[string]struct{}, len(manifest.Processes))
	queueOwners := make(map[string]string)
	previousName := ""
	summary := BudgetSummary{
		DirectQueueControlConnections: manifest.OperatorCLI.MaxConcurrentInvocations *
			manifest.OperatorCLI.QueueControlMaxConnections,
		DomainClientConnections: manifest.OperatorCLI.MaxConcurrentInvocations *
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
		if err := validateProcess(process); err != nil {
			return BudgetSummary{}, fmt.Errorf("deployment process %s: %w", process.Name, err)
		}

		summary.DirectQueueControlConnections += process.MaxReplicas * process.QueueControlMaxConnections
		summary.DomainClientConnections += process.MaxReplicas * process.DomainMaxConnections

		if process.Runtime != "river" {
			continue
		}
		profile := *process.RegistryProfile
		if _, duplicate := seenProfiles[profile]; duplicate {
			return BudgetSummary{}, fmt.Errorf("registry profile %s has multiple deployment processes", profile)
		}
		seenProfiles[profile] = struct{}{}
		coverage := expected[profile]
		if !equalStrings(process.Queues, coverage.queues) {
			return BudgetSummary{}, fmt.Errorf("registry profile %s queue coverage drift", profile)
		}
		if !equalStrings(process.JobKinds, coverage.kinds) {
			return BudgetSummary{}, fmt.Errorf("registry profile %s job-kind coverage drift", profile)
		}
		for _, queue := range process.Queues {
			if owner, exists := queueOwners[queue]; exists {
				return BudgetSummary{}, fmt.Errorf("queue %s is assigned to profiles %s and %s", queue, owner, profile)
			}
			queueOwners[queue] = profile
		}
	}

	for profile := range expected {
		if _, ok := seenProfiles[profile]; !ok {
			return BudgetSummary{}, fmt.Errorf("registry profile %s has no deployment process", profile)
		}
	}
	summary.ServerConnectionFootprint = manifest.PostgresBudget.ServerReservedConnections +
		manifest.PostgresBudget.PgBouncerDefaultPoolSize*manifest.PostgresBudget.PgBouncerServerPoolCount +
		summary.DirectQueueControlConnections
	if summary.ServerConnectionFootprint > manifest.PostgresBudget.ServerMaxConnections {
		return BudgetSummary{}, fmt.Errorf(
			"PostgreSQL server connection budget exceeded: %d > %d",
			summary.ServerConnectionFootprint,
			manifest.PostgresBudget.ServerMaxConnections,
		)
	}
	if summary.DomainClientConnections > manifest.PostgresBudget.PgBouncerMaxClientConnections {
		return BudgetSummary{}, fmt.Errorf(
			"PgBouncer client connection budget exceeded: %d > %d",
			summary.DomainClientConnections,
			manifest.PostgresBudget.PgBouncerMaxClientConnections,
		)
	}
	return summary, nil
}

type profileCoverage struct {
	queues []string
	kinds  []string
}

func expectedCoverage(registry jobcontract.Registry) map[string]profileCoverage {
	queueSets := make(map[string]map[string]struct{})
	kindSets := make(map[string]map[string]struct{})
	for _, job := range registry.Jobs {
		if queueSets[job.Profile] == nil {
			queueSets[job.Profile] = make(map[string]struct{})
			kindSets[job.Profile] = make(map[string]struct{})
		}
		queueSets[job.Profile][job.Queue] = struct{}{}
		kindSets[job.Profile][job.Kind] = struct{}{}
	}
	coverage := make(map[string]profileCoverage, len(queueSets))
	for profile, queues := range queueSets {
		coverage[profile] = profileCoverage{
			queues: sortedKeys(queues),
			kinds:  sortedKeys(kindSets[profile]),
		}
	}
	return coverage
}

func validatePostgresBudget(budget PostgresBudget) error {
	if budget.ServerMaxConnections < 1 || budget.ServerMaxConnections > 10000 ||
		budget.ServerReservedConnections < 1 ||
		budget.PgBouncerDefaultPoolSize < 1 ||
		budget.PgBouncerServerPoolCount < 1 || budget.PgBouncerServerPoolCount > 128 ||
		budget.PgBouncerMaxClientConnections < 1 {
		return errors.New("deployment PostgreSQL budget has invalid bounds")
	}
	if budget.ServerReservedConnections+
		budget.PgBouncerDefaultPoolSize*budget.PgBouncerServerPoolCount >= budget.ServerMaxConnections {
		return errors.New("deployment PostgreSQL budget leaves no direct queue-control capacity")
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
		operator.DomainMaxConnections > 16 {
		return errors.New("worker operator deployment identity or connection budget is invalid")
	}
	if !equalStrings(operator.ConfigEnv, []string{
		"PGBOUNCER_TRANSACTION_MODE",
		"RIVER_DATABASE_SCHEMA",
		"WORKER_DATABASE_MODE",
	}) || !equalStrings(operator.SecretEnv, []string{
		"POSTGRES_URI",
		"WORKER_DATABASE_URI",
		"WORKER_OPERATOR_TOKEN",
	}) {
		return errors.New("worker operator deployment configuration is invalid")
	}
	return nil
}

func validateProcess(process Process) error {
	if !namePattern.MatchString(process.Name) || process.EnabledByDefault || process.MinReplicas != 0 ||
		process.MaxReplicas < 1 || process.MaxReplicas > 8 {
		return errors.New("identity or coexistence replica policy is invalid")
	}
	if process.DomainMaxConnections < 1 || process.DomainMaxConnections > 16 ||
		process.QueueControlMaxConnections < 0 || process.QueueControlMaxConnections > 4 {
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
		if process.Binary != "dev-health-worker" || process.RegistryProfile == nil ||
			!profilePattern.MatchString(*process.RegistryProfile) ||
			process.QueueControlMaxConnections < 1 ||
			!contains(process.SecretEnv, "WORKER_DATABASE_URI") {
			return errors.New("River runtime is missing its binary, profile, or queue-control DSN")
		}
		if !equalStrings(queueWorkerNames, process.Queues) {
			return errors.New("River queue worker limits drift from queue coverage")
		}
	case "control":
		expectedBinary := map[string]string{
			"reconciler": "dev-health-reconciler",
			"scheduler":  "dev-health-scheduler",
		}[process.Name]
		if expectedBinary == "" || process.Binary != expectedBinary || process.RegistryProfile != nil ||
			len(process.Queues) != 0 || len(process.JobKinds) != 0 ||
			process.QueueControlMaxConnections < 1 || !contains(process.SecretEnv, "WORKER_DATABASE_URI") ||
			len(process.QueueWorkers) != 0 {
			return errors.New("control runtime wiring is invalid")
		}
	case "stream":
		if process.Binary != "dev-health-stream-runner" || process.RegistryProfile != nil ||
			len(process.Queues) != 0 || len(process.JobKinds) != 0 ||
			process.QueueControlMaxConnections != 0 || !process.RequiresValkey || len(process.QueueWorkers) != 0 {
			return errors.New("stream runtime wiring is invalid")
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
		return errors.New("deployment profile manifest contains multiple JSON values")
	}
	return fmt.Errorf("decode deployment profiles: %w", err)
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
