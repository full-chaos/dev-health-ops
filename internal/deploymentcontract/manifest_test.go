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
	if summary.DirectQueueControlConnections != 18 {
		t.Fatalf("direct queue-control connections = %d", summary.DirectQueueControlConnections)
	}
	// The three coordinator profiles under the Option B split — reconciler
	// and scheduler at two replicas each (2×2 + 2×2 = 8), plus workerctl's
	// single invocation (1×2) — contribute 10 direct, server-counted
	// connections alongside (not instead of) their existing domain pools.
	if summary.DirectCoordinatorConnections != 10 {
		t.Fatalf("direct coordinator connections = %d", summary.DirectCoordinatorConnections)
	}
	// stream-pagerduty adds two replicas of a four-connection domain pool.
	if summary.DomainClientConnections != 50 {
		t.Fatalf("domain client connections = %d", summary.DomainClientConnections)
	}
	if summary.ServerConnectionFootprint != 93 {
		t.Fatalf("server connection footprint = %d", summary.ServerConnectionFootprint)
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
	manifest.PostgresBudget.ServerMaxConnections = 92
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
