package deploymentcontract

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func loadFixture(t *testing.T) (Manifest, jobcontract.Registry) {
	t.Helper()
	contractRoot := filepath.Join("..", "..", "contracts", "jobs", "v1")
	registryBytes, err := os.ReadFile(filepath.Join(contractRoot, "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	var registry jobcontract.Registry
	if err := json.Unmarshal(registryBytes, &registry); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join("..", "..", "deploy", "go-workers", "deployment.json")
	manifest, _, err := Load(manifestPath, registry)
	if err != nil {
		t.Fatal(err)
	}
	return manifest, registry
}

func TestCheckedInManifestIsValidAndBounded(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	summary, err := manifest.Validate(registry)
	if err != nil {
		t.Fatal(err)
	}
	if summary.QueueSessionClientConnections != 34 {
		t.Fatalf("queue session clients = %d", summary.QueueSessionClientConnections)
	}
	if summary.QueueSessionHeadroom != 3 {
		t.Fatalf("queue session headroom = %d", summary.QueueSessionHeadroom)
	}
	if summary.CoordinatorSessionClientConnections != 10 {
		t.Fatalf("coordinator session clients = %d", summary.CoordinatorSessionClientConnections)
	}
	if summary.CoordinatorSessionHeadroom != 1 {
		t.Fatalf("coordinator session headroom = %d", summary.CoordinatorSessionHeadroom)
	}
	if summary.DomainTransactionClientConnections != 66 {
		t.Fatalf("domain transaction clients = %d", summary.DomainTransactionClientConnections)
	}
	if summary.DomainTransactionHeadroom != 934 {
		t.Fatalf("domain transaction headroom = %d", summary.DomainTransactionHeadroom)
	}
	if summary.ServerConnectionFootprint != 103 {
		t.Fatalf("server connection footprint = %d", summary.ServerConnectionFootprint)
	}
	if summary.ServerConnectionHeadroom != 97 {
		t.Fatalf("server connection headroom = %d", summary.ServerConnectionHeadroom)
	}
}

func TestManifestKeepsExternalStreamSingleton(t *testing.T) {
	t.Parallel()
	manifest, _ := loadFixture(t)
	for _, process := range manifest.Processes {
		if process.Name == "stream-external" {
			if process.MaxReplicas != 1 {
				t.Fatalf("stream-external max replicas = %d", process.MaxReplicas)
			}
			return
		}
	}
	t.Fatal("stream-external process not found")
}

func TestManifestRejectsReplicaRequestOrShutdownWindowOutsideGroupContract(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*Process)
	}{
		{
			name: "desired replicas exceed reviewed maximum",
			mutate: func(process *Process) {
				process.DesiredReplicas = process.MaxReplicas + 1
			},
		},
		{
			name: "shutdown cannot cover longest claim and finalization",
			mutate: func(process *Process) {
				process.ShutdownGraceSeconds = 7_259
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest, registry := loadFixture(t)
			for index := range manifest.Processes {
				if manifest.Processes[index].Name == "heavy" {
					test.mutate(&manifest.Processes[index])
					break
				}
			}
			if _, err := manifest.Validate(registry); err == nil {
				t.Fatal("invalid river replica contract was accepted")
			}
		})
	}
}

func TestManifestAcceptsReviewedHeavyAndOpsReplicaBudget(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	for index := range manifest.Processes {
		if manifest.Processes[index].Name == "heavy" || manifest.Processes[index].Name == "ops" {
			manifest.Processes[index].MaxReplicas = 2
		}
	}

	summary, err := manifest.Validate(registry)
	if err != nil {
		t.Fatal(err)
	}
	if summary.QueueSessionClientConnections != 34 {
		t.Fatalf("queue session clients = %d", summary.QueueSessionClientConnections)
	}
	if summary.QueueSessionHeadroom != 3 {
		t.Fatalf("queue session headroom = %d", summary.QueueSessionHeadroom)
	}
	if summary.CoordinatorSessionClientConnections != 10 {
		t.Fatalf("coordinator session clients = %d", summary.CoordinatorSessionClientConnections)
	}
	if summary.CoordinatorSessionHeadroom != 1 {
		t.Fatalf("coordinator session headroom = %d", summary.CoordinatorSessionHeadroom)
	}
	if summary.DomainTransactionClientConnections != 66 {
		t.Fatalf("domain transaction clients = %d", summary.DomainTransactionClientConnections)
	}
	if summary.DomainTransactionHeadroom != 934 {
		t.Fatalf("domain transaction headroom = %d", summary.DomainTransactionHeadroom)
	}
	if summary.ServerConnectionFootprint != 103 {
		t.Fatalf("server connection footprint = %d", summary.ServerConnectionFootprint)
	}
	if summary.ServerConnectionHeadroom != 97 {
		t.Fatalf("server connection headroom = %d", summary.ServerConnectionHeadroom)
	}
}

func TestManifestAcceptsReviewedHeavyAndOpsReplicaBudgetAtOneReplica(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	for index := range manifest.Processes {
		if manifest.Processes[index].Name == "heavy" || manifest.Processes[index].Name == "ops" {
			manifest.Processes[index].MaxReplicas = 1
		}
	}

	summary, err := manifest.Validate(registry)
	if err != nil {
		t.Fatal(err)
	}
	if summary.QueueSessionClientConnections != 28 {
		t.Fatalf("queue session clients = %d", summary.QueueSessionClientConnections)
	}
	if summary.QueueSessionHeadroom != 9 {
		t.Fatalf("queue session headroom = %d", summary.QueueSessionHeadroom)
	}
	if summary.CoordinatorSessionClientConnections != 10 {
		t.Fatalf("coordinator session clients = %d", summary.CoordinatorSessionClientConnections)
	}
	if summary.CoordinatorSessionHeadroom != 1 {
		t.Fatalf("coordinator session headroom = %d", summary.CoordinatorSessionHeadroom)
	}
	if summary.DomainTransactionClientConnections != 58 {
		t.Fatalf("domain transaction clients = %d", summary.DomainTransactionClientConnections)
	}
	if summary.DomainTransactionHeadroom != 942 {
		t.Fatalf("domain transaction headroom = %d", summary.DomainTransactionHeadroom)
	}
	if summary.ServerConnectionFootprint != 103 {
		t.Fatalf("server connection footprint = %d", summary.ServerConnectionFootprint)
	}
	if summary.ServerConnectionHeadroom != 97 {
		t.Fatalf("server connection headroom = %d", summary.ServerConnectionHeadroom)
	}
}

func TestManifestRejectsHeavyOrOpsReplicaBudgetXPlusOne(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		changed string
	}{
		{name: "heavy", changed: "heavy"},
		{name: "ops", changed: "ops"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			manifest, registry := loadFixture(t)
			for index := range manifest.Processes {
				if manifest.Processes[index].Name == "heavy" || manifest.Processes[index].Name == "ops" {
					manifest.Processes[index].MaxReplicas = 2
				}
				if manifest.Processes[index].Name == tc.changed {
					manifest.Processes[index].MaxReplicas = 3
				}
			}
			if _, err := manifest.Validate(registry); err == nil {
				t.Fatalf("expected %s at three replicas to fail validation", tc.changed)
			}
		})
	}
}

func TestManifestRejectsRiverQueueCoverageDrift(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	for index := range manifest.Processes {
		if manifest.Processes[index].Name == "ops" {
			manifest.Processes[index].JobKinds = manifest.Processes[index].JobKinds[:len(manifest.Processes[index].JobKinds)-1]
		}
	}
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected missing registry kind to fail validation")
	}
}

func TestManifestRejectsRiverQueueWorkerCoverageDrift(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	for index := range manifest.Processes {
		if manifest.Processes[index].Name == "ops" {
			manifest.Processes[index].QueueWorkers = manifest.Processes[index].QueueWorkers[:1]
		}
	}
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected missing queue worker limit to fail validation")
	}
}

func TestManifestRejectsUnknownRiverQueue(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	for index := range manifest.Processes {
		if manifest.Processes[index].Name == "ops" {
			manifest.Processes[index].Queues = append(manifest.Processes[index].Queues, "zzghost")
			manifest.Processes[index].QueueWorkers = append(manifest.Processes[index].QueueWorkers, QueueWorker{Queue: "zzghost", MaxWorkers: 1})
		}
	}
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected an unregistered queue to fail validation")
	}
}

func TestManifestRejectsQueueHeadroomAtZero(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*Manifest)
	}{
		{
			name: "queue session",
			mutate: func(manifest *Manifest) {
				manifest.PostgresBudget.PgBouncerQueueSessionPoolSize = 22
			},
		},
		{
			name: "coordinator session",
			mutate: func(manifest *Manifest) {
				manifest.PostgresBudget.PgBouncerCoordinatorSessionPoolSize = 10
			},
		},
		{
			name: "transaction pool",
			mutate: func(manifest *Manifest) {
				manifest.PostgresBudget.PgBouncerTransactionMaxClientConnections = 58
			},
		},
		{
			name: "server footprint",
			mutate: func(manifest *Manifest) {
				manifest.PostgresBudget.ServerMaxConnections = 89
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest, registry := loadFixture(t)
			test.mutate(&manifest)
			if _, err := manifest.Validate(registry); err == nil {
				t.Fatalf("expected %s headroom at zero to fail validation", test.name)
			}
		})
	}
}

func TestManifestRejectsConnectionBudgetOverflow(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	manifest.PostgresBudget.ServerMaxConnections = 82
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected server connection budget overflow")
	}
}

func TestManifestRejectsOperatorCredentialOrBudgetDrift(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	manifest.OperatorCLI.SecretEnv = []string{"POSTGRES_URI", "WORKER_DATABASE_URI"}
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected missing operator token to fail validation")
	}

	manifest, registry = loadFixture(t)
	manifest.OperatorCLI.MaxConcurrentInvocations = 2
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected unbudgeted concurrent operator invocation to fail validation")
	}
}

func TestManifestRejectsRuntimeRoleIdentityDrift(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	manifest.RuntimeRoleEnv = []string{"RIVER_DOMAIN_DATABASE_ROLE"}
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected missing queue role identity to fail validation")
	}
}

func TestManifestRejectsMigrationDSNOnRuntimeProcess(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	manifest.Processes[0].SecretEnv = append(manifest.Processes[0].SecretEnv, "MIGRATION_DATABASE_URI")
	sort.Strings(manifest.Processes[0].SecretEnv)
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected migration DSN exposure to fail validation")
	}
}

func TestManifestRejectsDuplicateExternalStreamReplicaConfiguration(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	for index := range manifest.Processes {
		if manifest.Processes[index].Name == "stream-external" {
			manifest.Processes[index].MaxReplicas = 2
		}
	}
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected duplicate external stream replica configuration to fail")
	}
}

// CoordinatorMaxConnections is a direct, server-counted connection budget --
// the same shape QueueControlMaxConnections already models -- reserved for
// the three coordinator roles (reconciler, scheduler, workerctl) under the
// Option B two-role split. Both directions of the constraint must fail
// closed: a control-runtime process silently missing its coordinator pool,
// and a non-coordinator process wrongly granted one.
func TestManifestRejectsCoordinatorConnectionMisconfiguration(t *testing.T) {
	t.Parallel()

	t.Run("control runtime missing its coordinator pool", func(t *testing.T) {
		manifest, registry := loadFixture(t)
		for index := range manifest.Processes {
			if manifest.Processes[index].Name == "reconciler" {
				manifest.Processes[index].CoordinatorMaxConnections = 0
			}
		}
		if _, err := manifest.Validate(registry); err == nil {
			t.Fatal("expected a control-runtime process with no coordinator connections to fail validation")
		}
	})

	t.Run("river runtime wrongly granted a coordinator pool", func(t *testing.T) {
		manifest, registry := loadFixture(t)
		for index := range manifest.Processes {
			if manifest.Processes[index].Name == "sync" {
				manifest.Processes[index].CoordinatorMaxConnections = 2
			}
		}
		if _, err := manifest.Validate(registry); err == nil {
			t.Fatal("expected a River-runtime process granted coordinator connections to fail validation")
		}
	})

	t.Run("stream runtime wrongly granted a coordinator pool", func(t *testing.T) {
		manifest, registry := loadFixture(t)
		for index := range manifest.Processes {
			if manifest.Processes[index].Name == "stream-external" {
				manifest.Processes[index].CoordinatorMaxConnections = 2
			}
		}
		if _, err := manifest.Validate(registry); err == nil {
			t.Fatal("expected a stream-runtime process granted coordinator connections to fail validation")
		}
	})

	t.Run("workerctl missing its coordinator pool", func(t *testing.T) {
		manifest, registry := loadFixture(t)
		manifest.OperatorCLI.CoordinatorMaxConnections = 0
		if _, err := manifest.Validate(registry); err == nil {
			t.Fatal("expected workerctl with no coordinator connections to fail validation")
		}
	})
}

func TestManifestAllowsOverlappingQueueGroups(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	for index := range manifest.Processes {
		switch manifest.Processes[index].Name {
		case "ops":
			manifest.Processes[index].Queues = []string{
				"heartbeat",
				"reports",
				"retention",
				"webhooks",
			}
			manifest.Processes[index].QueueWorkers = []QueueWorker{
				{Queue: "heartbeat", MaxWorkers: 1},
				{Queue: "reports", MaxWorkers: 2},
				{Queue: "retention", MaxWorkers: 1},
				{Queue: "webhooks", MaxWorkers: 4},
			}
			manifest.Processes[index].JobKinds = unionRegistryKindsForQueues(registry,
				manifest.Processes[index].Queues)
		case "heavy":
			manifest.Processes[index].JobKinds = unionRegistryKindsForQueues(registry,
				manifest.Processes[index].Queues)
		}
	}
	if _, err := manifest.Validate(registry); err != nil {
		t.Fatalf("expected overlapping queue groups to validate: %v", err)
	}
}

func TestManifestAllowsIndependentGroupsWithTheSameRiverQueueSet(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	var heavyQueues []string
	for index := range manifest.Processes {
		switch manifest.Processes[index].Name {
		case "heavy":
			heavyQueues = append([]string(nil), manifest.Processes[index].Queues...)
		case "ops":
			manifest.Processes[index].Queues = append([]string(nil), heavyQueues...)
			manifest.Processes[index].ShutdownGraceSeconds = 7_260
			manifest.Processes[index].QueueWorkers = []QueueWorker{
				{Queue: "investment", MaxWorkers: 1},
				{Queue: "metrics", MaxWorkers: 2},
				{Queue: "reports", MaxWorkers: 2},
				{Queue: "workgraph", MaxWorkers: 1},
			}
			manifest.Processes[index].JobKinds = unionRegistryKindsForQueues(registry,
				manifest.Processes[index].Queues)
		}
	}
	if _, err := manifest.Validate(registry); err != nil {
		t.Fatalf("expected independent groups with the same queue set to validate: %v", err)
	}
}

func TestLoadRejectsRegistryProfileFieldInStrictJSON(t *testing.T) {
	t.Parallel()
	manifestBytes, err := os.ReadFile(filepath.Join("..", "..", "deploy", "go-workers", "deployment.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	processes, ok := manifest["processes"].([]any)
	if !ok || len(processes) == 0 {
		t.Fatal("deployment manifest fixture is malformed")
	}
	first, ok := processes[0].(map[string]any)
	if !ok {
		t.Fatal("deployment process fixture is malformed")
	}
	first["registry_profile"] = "ops"
	updated, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var strictManifest strictManifestWithoutRegistryProfile
	decoder := json.NewDecoder(bytes.NewReader(updated))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&strictManifest); err == nil {
		t.Fatal("expected strict JSON loading to reject registry_profile")
	}
}

type strictManifestWithoutRegistryProfile struct {
	SchemaVersion   int                       `json:"schema_version"`
	DeploymentState string                    `json:"deployment_state"`
	Registry        string                    `json:"registry"`
	RuntimeRoleEnv  []string                  `json:"runtime_role_env"`
	PostgresBudget  PostgresBudget            `json:"postgres_budget"`
	MigrationJob    MigrationJob              `json:"migration_job"`
	OperatorCLI     OperatorCLI               `json:"operator_cli"`
	Processes       []strictDeploymentProcess `json:"processes"`
}

type strictDeploymentProcess struct {
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

func unionRegistryKindsForQueues(registry jobcontract.Registry, queues []string) []string {
	allowed := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		allowed[queue] = struct{}{}
	}
	kinds := make(map[string]struct{})
	for _, job := range registry.Jobs {
		if _, ok := allowed[job.Queue]; ok {
			kinds[job.Kind] = struct{}{}
		}
	}
	return sortedKeys(kinds)
}

// CHAOS-3945: the queue-session budget used to count only each replica's
// queue-control pgxpool. A started River client also holds a long-lived
// notifier LISTEN session outside that pool, so a "river" runtime replica
// costs one more connection than it declares. Charging only the pool is what
// let devhealth_queue saturate at exactly DEFAULT_POOL_SIZE while every
// declared maximum still validated.
func TestManifestChargesRiverReplicasForTheirNotifierSession(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)

	withNotifier, err := manifest.Validate(registry)
	if err != nil {
		t.Fatal(err)
	}

	// Removing the notifier term must lower the footprint by exactly one
	// connection per declared replica of every "river" runtime process, and
	// by nothing at all for the control, stream, and operator clients.
	riverReplicas := 0
	for _, process := range manifest.Processes {
		if process.Runtime == "river" && process.QueueControlMaxConnections > 0 {
			riverReplicas += process.MaxReplicas
		}
	}
	if riverReplicas == 0 {
		t.Fatal("fixture declares no river runtime replicas, so this guard proves nothing")
	}

	expected := manifest.OperatorCLI.MaxConcurrentInvocations *
		manifest.OperatorCLI.QueueControlMaxConnections
	for _, process := range manifest.Processes {
		expected += process.MaxReplicas * process.QueueControlMaxConnections
	}
	if withNotifier.QueueSessionClientConnections != expected+riverReplicas {
		t.Fatalf(
			"queue session clients = %d; pool-only total %d plus %d notifier sessions = %d",
			withNotifier.QueueSessionClientConnections,
			expected,
			riverReplicas,
			expected+riverReplicas,
		)
	}
}

// A notifier session is not optional: River always opens one per started
// client. A manifest that declares zero must not validate, or the budget
// silently reverts to the pool-only arithmetic that caused CHAOS-3945.
func TestManifestRejectsAZeroNotifierSessionDeclaration(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	manifest.PostgresBudget.QueueSessionNotifierSessionsPerReplica = 0
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected a manifest declaring no River notifier session to fail validation")
	}
}

// Headroom of one connection is not headroom. A rolling restart runs the
// stopping and starting container together -- the stopping one's session
// lingers until server_idle_timeout -- so the queue pool must be able to
// absorb one whole replica beyond every declared maximum.
func TestManifestRejectsQueuePoolThatCannotAbsorbARollingRestart(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	summary, err := manifest.Validate(registry)
	if err != nil {
		t.Fatal(err)
	}
	manifest.PostgresBudget.PgBouncerQueueSessionPoolSize =
		summary.QueueSessionClientConnections + 1
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected a queue pool with one spare connection to fail validation")
	}
}
