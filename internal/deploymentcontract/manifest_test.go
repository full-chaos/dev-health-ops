package deploymentcontract

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func loadFixture(t *testing.T) (Manifest, jobcontract.Registry) {
	t.Helper()
	contractRoot := filepath.Join("..", "..", "contracts", "jobs", "v1")
	registry, err := jobcontract.LoadRegistry(contractRoot)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join("..", "..", "deploy", "go-workers", "profiles.json")
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
	if summary.QueueSessionClientConnections != 22 {
		t.Fatalf("queue session clients = %d", summary.QueueSessionClientConnections)
	}
	if summary.QueueSessionHeadroom != 0 {
		t.Fatalf("queue session headroom = %d", summary.QueueSessionHeadroom)
	}
	// The three coordinator profiles under the Option B split — reconciler
	// and scheduler at two replicas each (2×2 + 2×2 = 8), plus workerctl's
	// single invocation (1×2) — contribute 10 session-pool client
	// connections alongside (not instead of) their existing domain pools.
	if summary.CoordinatorSessionClientConnections != 10 {
		t.Fatalf("coordinator session clients = %d", summary.CoordinatorSessionClientConnections)
	}
	if summary.CoordinatorSessionHeadroom != 0 {
		t.Fatalf("coordinator session headroom = %d", summary.CoordinatorSessionHeadroom)
	}
	// stream-pagerduty adds two replicas of a four-connection domain pool.
	if summary.DomainTransactionClientConnections != 58 {
		t.Fatalf("domain transaction clients = %d", summary.DomainTransactionClientConnections)
	}
	if summary.DomainTransactionHeadroom != 942 {
		t.Fatalf("domain transaction headroom = %d", summary.DomainTransactionHeadroom)
	}
	if summary.ServerConnectionFootprint != 87 {
		t.Fatalf("server connection footprint = %d", summary.ServerConnectionFootprint)
	}
	if summary.ServerConnectionHeadroom != 13 {
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

func TestManifestRejectsReplicaRequestOrShutdownWindowOutsideProfileContract(t *testing.T) {
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
				t.Fatal("invalid profile replica contract was accepted")
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
	if summary.QueueSessionClientConnections != 22 {
		t.Fatalf("queue session clients = %d", summary.QueueSessionClientConnections)
	}
	if summary.QueueSessionHeadroom != 0 {
		t.Fatalf("queue session headroom = %d", summary.QueueSessionHeadroom)
	}
	if summary.CoordinatorSessionClientConnections != 10 {
		t.Fatalf("coordinator session clients = %d", summary.CoordinatorSessionClientConnections)
	}
	if summary.CoordinatorSessionHeadroom != 0 {
		t.Fatalf("coordinator session headroom = %d", summary.CoordinatorSessionHeadroom)
	}
	if summary.DomainTransactionClientConnections != 58 {
		t.Fatalf("domain transaction clients = %d", summary.DomainTransactionClientConnections)
	}
	if summary.DomainTransactionHeadroom != 942 {
		t.Fatalf("domain transaction headroom = %d", summary.DomainTransactionHeadroom)
	}
	if summary.ServerConnectionFootprint != 87 {
		t.Fatalf("server connection footprint = %d", summary.ServerConnectionFootprint)
	}
	if summary.ServerConnectionHeadroom != 13 {
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
	if summary.QueueSessionClientConnections != 18 {
		t.Fatalf("queue session clients = %d", summary.QueueSessionClientConnections)
	}
	if summary.QueueSessionHeadroom != 4 {
		t.Fatalf("queue session headroom = %d", summary.QueueSessionHeadroom)
	}
	if summary.CoordinatorSessionClientConnections != 10 {
		t.Fatalf("coordinator session clients = %d", summary.CoordinatorSessionClientConnections)
	}
	if summary.CoordinatorSessionHeadroom != 0 {
		t.Fatalf("coordinator session headroom = %d", summary.CoordinatorSessionHeadroom)
	}
	if summary.DomainTransactionClientConnections != 50 {
		t.Fatalf("domain transaction clients = %d", summary.DomainTransactionClientConnections)
	}
	if summary.DomainTransactionHeadroom != 950 {
		t.Fatalf("domain transaction headroom = %d", summary.DomainTransactionHeadroom)
	}
	if summary.ServerConnectionFootprint != 87 {
		t.Fatalf("server connection footprint = %d", summary.ServerConnectionFootprint)
	}
	if summary.ServerConnectionHeadroom != 13 {
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

func TestManifestRejectsRegistryCoverageDrift(t *testing.T) {
	t.Parallel()
	manifest, registry := loadFixture(t)
	for index := range manifest.Processes {
		if manifest.Processes[index].Name == "ops" {
			manifest.Processes[index].JobKinds = manifest.Processes[index].JobKinds[:1]
		}
	}
	if _, err := manifest.Validate(registry); err == nil {
		t.Fatal("expected missing registry kind to fail validation")
	}
}

func TestManifestRejectsQueueWorkerCoverageDrift(t *testing.T) {
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

// CoordinatorMaxConnections is a direct, server-counted connection budget —
// the same shape QueueControlMaxConnections already models — reserved for
// the three coordinator profiles (reconciler, scheduler, workerctl) under
// the Option B two-role split. Both directions of the new constraint must
// fail closed: a coordinator ("control" runtime) profile silently missing
// its coordinator pool, and a non-coordinator profile wrongly granted one.
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
			t.Fatal("expected a control-runtime profile with no coordinator connections to fail validation")
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
			t.Fatal("expected a River-runtime profile granted coordinator connections to fail validation")
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
			t.Fatal("expected a stream-runtime profile granted coordinator connections to fail validation")
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
