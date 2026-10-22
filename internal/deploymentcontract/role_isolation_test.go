package deploymentcontract

import (
	"slices"
	"strings"
	"testing"
)

// Each row breaks one clause of the database wiring of one worker class and
// names the validator message that clause answers with. The message is part
// of the row: a mutation caught by some other check does not count.

func withoutValue(values []string, drop string) []string {
	var kept []string
	for _, value := range values {
		if value != drop {
			kept = append(kept, value)
		}
	}
	return kept
}

func withValue(values []string, add string) []string {
	merged := append(slices.Clone(values), add)
	slices.Sort(merged)
	return slices.Compact(merged)
}

type classBreak struct {
	name    string
	runtime string
	apply   func(*Process)
	want    string
}

func classBreaks() []classBreak {
	const (
		river   = "River runtime is missing its binary, queue coverage, or queue-control DSN"
		control = "control runtime wiring is invalid"
		stream  = "stream runtime wiring is invalid"
		limits  = "connection limits are invalid"
	)
	return []classBreak{
		{"river without queue-control connections", "river", func(p *Process) { p.QueueControlMaxConnections = 0 }, river},
		{"river with coordinator connections", "river", func(p *Process) { p.CoordinatorMaxConnections = 1 }, river},
		{"river without the queue-control DSN", "river", func(p *Process) { p.SecretEnv = withoutValue(p.SecretEnv, "WORKER_DATABASE_URI") }, river},

		{"control with queues", "control", func(p *Process) { p.Queues = []string{"heartbeat"} }, control},
		{"control with job kinds", "control", func(p *Process) { p.JobKinds = []string{"heartbeat"} }, control},
		{"control with queue workers", "control", func(p *Process) { p.QueueWorkers = []QueueWorker{{Queue: "heartbeat", MaxWorkers: 1}} }, control},
		{"control without queue-control connections", "control", func(p *Process) { p.QueueControlMaxConnections = 0 }, control},
		{"control without the queue-control DSN", "control", func(p *Process) { p.SecretEnv = withoutValue(p.SecretEnv, "WORKER_DATABASE_URI") }, control},
		{"control without coordinator connections", "control", func(p *Process) { p.CoordinatorMaxConnections = 0 }, control},
		{"control without the coordinator DSN", "control", func(p *Process) { p.SecretEnv = withoutValue(p.SecretEnv, "COORDINATOR_DATABASE_URI") }, control},

		{"stream with queue-control connections", "stream", func(p *Process) { p.QueueControlMaxConnections = 1 }, stream},
		{"stream with coordinator connections", "stream", func(p *Process) { p.CoordinatorMaxConnections = 1 }, stream},

		{"any class without the domain DSN", "", func(p *Process) { p.SecretEnv = withoutValue(p.SecretEnv, "POSTGRES_URI") }, "long-running process must receive the domain DSN"},
		{"any class with the migration DSN", "", func(p *Process) { p.SecretEnv = withValue(p.SecretEnv, "MIGRATION_DATABASE_URI") }, "long-running process must not receive the migration DSN"},
		{"any class with no domain connections", "", func(p *Process) { p.DomainMaxConnections = 0 }, limits},
		{"any class over the domain connection limit", "", func(p *Process) { p.DomainMaxConnections = 17 }, limits},
		{"any class with negative queue-control connections", "", func(p *Process) { p.QueueControlMaxConnections = -1 }, limits},
		{"any class over the queue-control connection limit", "", func(p *Process) { p.QueueControlMaxConnections = 5 }, limits},
		{"any class with negative coordinator connections", "", func(p *Process) { p.CoordinatorMaxConnections = -1 }, limits},
		{"any class over the coordinator connection limit", "", func(p *Process) { p.CoordinatorMaxConnections = 5 }, limits},
	}
}

func TestEveryWorkerClassRejectsItsBrokenDatabaseWiring(t *testing.T) {
	covered := map[string]int{}
	for _, row := range classBreaks() {
		t.Run(row.name, func(t *testing.T) {
			_, registry := loadFixture(t)
			mutated := 0
			for index := range len(mustLoad(t).Processes) {
				manifest := mustLoad(t)
				process := &manifest.Processes[index]
				if row.runtime != "" && process.Runtime != row.runtime {
					continue
				}
				row.apply(process)
				_, err := manifest.Validate(registry)
				if err == nil || !strings.Contains(err.Error(), "deployment process "+process.Name+": "+row.want) {
					t.Errorf("process %s: Validate = %v, want %q", process.Name, err, row.want)
				}
				mutated++
			}
			if mutated == 0 {
				t.Fatalf("no process of runtime %q was mutated", row.runtime)
			}
			covered[row.runtime] += mutated
		})
	}
	for _, runtime := range []string{"river", "control", "stream"} {
		if covered[runtime] == 0 {
			t.Errorf("no row broke a %s process", runtime)
		}
	}
}

func TestOperatorAndMigrationJobRejectBrokenDatabaseWiring(t *testing.T) {
	const operator = "worker operator deployment identity or connection budget is invalid"
	for _, test := range []struct {
		name   string
		mutate func(*Manifest)
		want   string
	}{
		{"operator over the queue-control limit", func(m *Manifest) { m.OperatorCLI.QueueControlMaxConnections = 5 }, operator},
		{"operator without domain connections", func(m *Manifest) { m.OperatorCLI.DomainMaxConnections = 0 }, operator},
		{"operator over the domain limit", func(m *Manifest) { m.OperatorCLI.DomainMaxConnections = 17 }, operator},
		{"operator without coordinator connections", func(m *Manifest) { m.OperatorCLI.CoordinatorMaxConnections = 0 }, operator},
		{"operator over the coordinator limit", func(m *Manifest) { m.OperatorCLI.CoordinatorMaxConnections = 5 }, operator},
		{"operator without the coordinator DSN", func(m *Manifest) {
			m.OperatorCLI.SecretEnv = withoutValue(m.OperatorCLI.SecretEnv, "COORDINATOR_DATABASE_URI")
		}, "worker operator deployment configuration is invalid"},
		{"operator without the coordinator role", func(m *Manifest) {
			m.OperatorCLI.ConfigEnv = withoutValue(m.OperatorCLI.ConfigEnv, "RIVER_COORDINATOR_DATABASE_ROLE")
		}, "worker operator deployment configuration is invalid"},
		{"migration job with the queue-control DSN", func(m *Manifest) {
			m.MigrationJob.SecretEnv = withValue(m.MigrationJob.SecretEnv, "WORKER_DATABASE_URI")
		}, "migration job must not receive runtime database DSNs"},
		{"migration job with the domain DSN", func(m *Manifest) {
			m.MigrationJob.SecretEnv = withValue(m.MigrationJob.SecretEnv, "POSTGRES_URI")
		}, "migration job must not receive runtime database DSNs"},
		{"migration job without its own DSN", func(m *Manifest) {
			m.MigrationJob.SecretEnv = withoutValue(m.MigrationJob.SecretEnv, "MIGRATION_DATABASE_URI")
		}, "migration job role and dedicated DSN wiring is invalid"},
		{"migration job without the queue role", func(m *Manifest) {
			m.MigrationJob.ConfigEnv = withoutValue(m.MigrationJob.ConfigEnv, "RIVER_QUEUE_DATABASE_ROLE")
		}, "migration job role and dedicated DSN wiring is invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest, registry := loadFixture(t)
			test.mutate(&manifest)
			if _, err := manifest.Validate(registry); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate = %v, want %q", err, test.want)
			}
		})
	}
}

func mustLoad(t *testing.T) Manifest {
	t.Helper()
	manifest, _ := loadFixture(t)
	return manifest
}
