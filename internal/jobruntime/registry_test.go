package jobruntime

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

// riverRoutedRegistry promotes every checked-in kind to a River route so
// startup validation can be exercised against the real contract. Every kind is
// already checked in at go_default/river, so this fixture is a no-op over the
// real registry today, but it still guards profile-coverage assertions against
// a future rollback that moves some kinds back to Celery, which would make the
// executable set partial and some of these cases pass vacuously.
func riverRoutedRegistry(t *testing.T) *Registry {
	t.Helper()
	contracts, err := jobcontract.LoadRegistry("../../contracts/jobs/v1")
	if err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}
	migration, err := jobcontract.LoadMigrationState("../../contracts/jobs/v1", contracts)
	if err != nil {
		t.Fatalf("LoadMigrationState: %v", err)
	}
	for index := range migration.Jobs {
		migration.Jobs[index].State = "go_default"
		migration.Jobs[index].Route = "river"
		migration.Jobs[index].RollbackRoute = "celery"
	}
	registry, err := newRegistry(contracts, migration)
	if err != nil {
		t.Fatalf("newRegistry: %v", err)
	}
	return registry
}

func opsStartup(registry *Registry) StartupSpec {
	heartbeat, _ := registry.Descriptor(jobcontract.KindHeartbeat)
	retention, _ := registry.Descriptor(jobcontract.KindRetentionCleanup)
	billing, _ := registry.Descriptor(jobcontract.KindBillingNotification)
	webhook, _ := registry.Descriptor(jobcontract.KindWebhookDelivery)
	queues := func() []QueueBudget {
		return []QueueBudget{
			{Queue: "heartbeat", MaxWorkers: 1},
			{Queue: "retention", MaxWorkers: 1},
			{Queue: "webhooks", MaxWorkers: 4},
		}
	}
	return StartupSpec{
		Profile:             "ops",
		Queues:              queues(),
		ManifestQueues:      queues(),
		Handlers:            []HandlerSpec{billing, webhook, heartbeat, retention},
		Connections:         ConnectionBudget{QueueControl: 2, Domain: 4},
		ManifestConnections: ConnectionBudget{QueueControl: 2, Domain: 4},
	}
}

func TestRegistryValidateStartupCoversAllRuntimePolicy(t *testing.T) {
	t.Parallel()
	registry := riverRoutedRegistry(t)
	retention, _ := registry.Descriptor(jobcontract.KindRetentionCleanup)
	if err := registry.ValidateStartup(opsStartup(registry)); err != nil {
		t.Fatalf("ValidateStartup: %v", err)
	}

	tests := []struct {
		field  string
		mutate func(*HandlerSpec)
	}{
		{"current_version", func(spec *HandlerSpec) { spec.CurrentVersion++ }},
		// Derived from the descriptor rather than a literal: pinning {1, 2} here
		// silently stopped drifting once retention_cleanup really did support
		// both versions, which turned this case into a no-op assertion.
		{"supported_versions", func(spec *HandlerSpec) {
			spec.SupportedVersions = append(append([]int(nil), spec.SupportedVersions...), 99)
		}},
		{"profile", func(spec *HandlerSpec) { spec.Profile = "heavy" }},
		{"queue", func(spec *HandlerSpec) { spec.Queue = "other" }},
		{"execution_mode", func(spec *HandlerSpec) { spec.ExecutionMode = "coordinator" }},
		{"priority", func(spec *HandlerSpec) { spec.Priority++ }},
		{"timeout", func(spec *HandlerSpec) { spec.Timeout += time.Second }},
		{"max_attempts", func(spec *HandlerSpec) { spec.MaxAttempts++ }},
		{"retry_policy", func(spec *HandlerSpec) { spec.RetryPolicy = "none" }},
		{"cancellation", func(spec *HandlerSpec) { spec.Cancellation = "other" }},
		{"delivery", func(spec *HandlerSpec) { spec.Delivery = "at_most_once" }},
		{"idempotency", func(spec *HandlerSpec) { spec.Idempotency = "other" }},
		{"concurrency_scope", func(spec *HandlerSpec) { spec.ConcurrencyScope = "process" }},
		{"concurrency_limit", func(spec *HandlerSpec) { spec.ConcurrencyLimit++ }},
		{"sensitive_fields", func(spec *HandlerSpec) { spec.SensitiveFields = []string{"token"} }},
		{"domain_link", func(spec *HandlerSpec) { spec.DomainLink = "other" }},
		{"organization_scope", func(spec *HandlerSpec) { spec.OrganizationScope = "tenant" }},
		{"migration_state", func(spec *HandlerSpec) { spec.MigrationState = "canary" }},
		{"route", func(spec *HandlerSpec) { spec.Route = "river_canary" }},
		{"rollback_route", func(spec *HandlerSpec) { spec.RollbackRoute = "none" }},
	}
	for _, test := range tests {
		t.Run(test.field, func(t *testing.T) {
			drifted := retention
			drifted.SupportedVersions = append([]int(nil), retention.SupportedVersions...)
			drifted.SensitiveFields = append([]string(nil), retention.SensitiveFields...)
			test.mutate(&drifted)
			err := registry.ValidateHandler(drifted)
			if err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("expected %s drift, got %v", test.field, err)
			}
		})
	}
}

func TestRegistryInvestmentDispatchStartupBudgetMatchesMaterialization(t *testing.T) {
	t.Parallel()
	registry, err := Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	promoted := riverRoutedRegistry(t)
	handlers := promoted.Profile("heavy")
	queueSet := make(map[string]struct{})
	for _, handler := range handlers {
		queueSet[handler.Queue] = struct{}{}
	}
	queues := make([]QueueBudget, 0, len(queueSet))
	for queue := range queueSet {
		queues = append(queues, QueueBudget{Queue: queue, MaxWorkers: 2})
	}
	if err := promoted.ValidateStartup(StartupSpec{
		Profile:             "heavy",
		Queues:              queues,
		ManifestQueues:      queues,
		Handlers:            handlers,
		Connections:         ConnectionBudget{QueueControl: 2, Domain: 4},
		ManifestConnections: ConnectionBudget{QueueControl: 2, Domain: 4},
	}); err != nil {
		t.Fatalf("ValidateStartup: %v", err)
	}
	var dispatch Descriptor
	for _, handler := range registry.Profile("heavy") {
		if handler.Kind == jobcontract.KindInvestmentDispatch {
			dispatch = handler
		}
	}
	if dispatch.Kind == "" {
		t.Fatal("heavy startup is missing investment.dispatch")
	}
	if dispatch.Timeout != 2*time.Hour {
		t.Fatalf("investment.dispatch timeout = %s, want 2h", dispatch.Timeout)
	}
	if dispatch.CurrentVersion != 1 || dispatch.Route != "river" {
		t.Fatalf(
			"investment.dispatch rollout contract drifted: version=%d route=%q",
			dispatch.CurrentVersion,
			dispatch.Route,
		)
	}
}

func TestRegistryValidateStartupRejectsCoverageDrift(t *testing.T) {
	t.Parallel()
	registry := riverRoutedRegistry(t)
	heartbeat, _ := registry.Descriptor(jobcontract.KindHeartbeat)

	tests := []struct {
		name   string
		mutate func(*StartupSpec)
	}{
		{"missing queue", func(spec *StartupSpec) { spec.Queues = spec.Queues[:2] }},
		{"extra queue", func(spec *StartupSpec) {
			spec.Queues = append(spec.Queues, QueueBudget{Queue: "reports", MaxWorkers: 1})
		}},
		{"missing handler", func(spec *StartupSpec) { spec.Handlers = spec.Handlers[:3] }},
		{"duplicate handler", func(spec *StartupSpec) {
			spec.Handlers = []HandlerSpec{heartbeat, heartbeat, heartbeat, heartbeat}
		}},
		{"unknown profile", func(spec *StartupSpec) { spec.Profile = "unknown" }},
		{"zero queue budget", func(spec *StartupSpec) { spec.Queues[0].MaxWorkers = 0 }},
		{"queue budget drift", func(spec *StartupSpec) { spec.Queues[0].MaxWorkers = 3 }},
		{"unbudgeted queue", func(spec *StartupSpec) { spec.ManifestQueues = spec.ManifestQueues[1:] }},
		{"missing connection budget", func(spec *StartupSpec) { spec.Connections = ConnectionBudget{} }},
		{"connection budget drift", func(spec *StartupSpec) { spec.Connections.Domain = 8 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			startup := opsStartup(registry)
			test.mutate(&startup)
			if err := registry.ValidateStartup(startup); err == nil {
				t.Fatal("startup drift unexpectedly passed")
			}
		})
	}
}

// TestRegistryValidateStartupRejectsUnexecutableProfile pins the CUT-02 rule
// that a profile with nothing routed to River cannot start. It is the check
// that made the empty latency profile unrepresentable.
func TestRegistryValidateStartupRejectsUnexecutableProfile(t *testing.T) {
	t.Parallel()
	registry, err := Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(registry.ExecutableProfile("ops")) != 0 {
		t.Skip("ops profile has been promoted; rule is covered elsewhere")
	}
	if err := registry.ValidateStartup(opsStartup(registry)); err == nil {
		t.Fatal("celery-routed profile unexpectedly passed startup validation")
	}
	if err := registry.ValidateStartup(StartupSpec{Profile: "latency"}); err == nil {
		t.Fatal("empty profile unexpectedly passed startup validation")
	}
}

func TestRegistryMigrationPairsFailClosed(t *testing.T) {
	t.Parallel()
	contracts := testContractRegistry()
	migration := testMigrationState()
	migration.Jobs[0].Route = "river"
	if _, err := newRegistry(contracts, migration); err == nil || !strings.Contains(err.Error(), "requires route") {
		t.Fatalf("expected migration pair rejection, got %v", err)
	}
}

func TestRegistryDescriptorsAreCompleteSortedDefensiveCopies(t *testing.T) {
	t.Parallel()
	registry, err := Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	descriptors := registry.Descriptors()
	if len(descriptors) != 24 || descriptors[0].Kind != jobcontract.KindInvestmentChunk ||
		descriptors[1].Kind != jobcontract.KindInvestmentDispatch ||
		descriptors[2].Kind != jobcontract.KindInvestmentFinalize ||
		descriptors[3].Kind != jobcontract.KindInvestmentMaterialize ||
		descriptors[4].Kind != jobcontract.KindDailyMetricsDispatch ||
		descriptors[5].Kind != jobcontract.KindDailyMetricsFinalize ||
		descriptors[6].Kind != jobcontract.KindDailyMetricsPartition ||
		descriptors[7].Kind != jobcontract.KindRemainingCapacity ||
		descriptors[8].Kind != jobcontract.KindRemainingComplexity ||
		descriptors[9].Kind != jobcontract.KindRemainingDORA ||
		descriptors[10].Kind != jobcontract.KindRemainingExtraMetrics ||
		descriptors[11].Kind != jobcontract.KindRemainingMembership ||
		descriptors[12].Kind != jobcontract.KindRemainingRecommendations ||
		descriptors[13].Kind != jobcontract.KindRemainingReleaseImpact ||
		descriptors[14].Kind != jobcontract.KindRemainingTeamMetrics ||
		descriptors[15].Kind != jobcontract.KindBillingNotification ||
		descriptors[16].Kind != jobcontract.KindWebhookDelivery ||
		descriptors[17].Kind != jobcontract.KindReportExecuteOnDemand ||
		descriptors[18].Kind != jobcontract.KindReportExecuteScheduled ||
		descriptors[19].Kind != jobcontract.KindSyncProviderUnit ||
		descriptors[20].Kind != jobcontract.KindTeamAutoimport ||
		descriptors[21].Kind != jobcontract.KindHeartbeat ||
		descriptors[22].Kind != jobcontract.KindRetentionCleanup ||
		descriptors[23].Kind != jobcontract.KindWorkGraphBuild {
		t.Fatalf("Descriptors() = %#v", descriptors)
	}
	// Every checked-in kind is executable, and no kind is Celery-routed any
	// more. sync.provider_unit is the single deliberate exception to
	// go_default/river: Go's provider route surface covers one of the 59
	// provider/dataset pairs in contracts/provider-matrix/v1/matrix.json, so it
	// stays on river_canary where ProviderUnitRouteSwitches can confine River to
	// that one ready pair. Asserting its route by name rather than skipping it
	// keeps an accidental promotion visible here.
	for _, descriptor := range descriptors {
		wantRoute := "river"
		if descriptor.Kind == jobcontract.KindSyncProviderUnit {
			wantRoute = "river_canary"
		}
		if descriptor.Route != wantRoute || !descriptor.Executable() {
			t.Fatalf(
				"checked-in policy drifted: kind=%s route=%q want=%q executable=%v",
				descriptor.Kind, descriptor.Route, wantRoute, descriptor.Executable(),
			)
		}
	}

	descriptors[0].SupportedVersions[0] = 99
	descriptors[0].SensitiveFields = append(descriptors[0].SensitiveFields, "secret")
	again := registry.Descriptors()
	if again[0].SupportedVersions[0] != 1 || len(again[0].SensitiveFields) != 0 {
		t.Fatalf("Descriptors() exposed mutable registry state: %#v", again[0])
	}
}

func testContractRegistry() jobcontract.Registry {
	return jobcontract.Registry{
		SchemaVersion: 1, ContractFamily: "dev-health.jobs", EnvelopeSchema: "envelope.schema.json",
		VersionPolicy: jobcontract.VersionPolicy{
			Compatibility: "additive_optional_only", MinimumConsumerWindow: 2,
			SameVersionRollout: "schema_digest_all_live_profiles",
		},
		Jobs: []jobcontract.JobDefinition{{
			Kind: jobcontract.KindRetentionCleanup, CurrentVersion: 1, SupportedVersions: []int{1},
			Profile: "ops", Queue: "retention", ExecutionMode: "command", Priority: 3,
			TimeoutSeconds: 300, MaxAttempts: 3, RetryPolicy: "bounded_exponential_jitter",
			Cancellation: "cooperative_checkpoint", Delivery: "guarded_at_least_once",
			Idempotency: "maintenance_run_checkpoint",
			Concurrency: jobcontract.ConcurrencyPolicy{Scope: "fleet", Limit: 1},
			DomainLink:  "maintenance_run", OrganizationScope: "global",
		}},
	}
}

func testMigrationState() jobcontract.MigrationState {
	return jobcontract.MigrationState{
		SchemaVersion: 1,
		Jobs: []jobcontract.MigrationJob{{
			Kind: jobcontract.KindRetentionCleanup, State: "canary", ProducerVersion: 1,
			ConsumerVersions: []int{1}, RequiredProfiles: []string{"ops"},
			Route: "river_canary", RollbackRoute: "celery", Evidence: []string{"contract_schema"},
		}},
	}
}
