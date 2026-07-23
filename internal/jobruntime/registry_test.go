package jobruntime

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func TestRegistryValidateStartupCoversAllRuntimePolicy(t *testing.T) {
	t.Parallel()
	registry, err := Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	heartbeat, _ := registry.Descriptor(jobcontract.KindHeartbeat)
	retention, _ := registry.Descriptor(jobcontract.KindRetentionCleanup)
	billing, _ := registry.Descriptor(jobcontract.KindBillingNotification)
	webhook, _ := registry.Descriptor(jobcontract.KindWebhookDelivery)
	startup := StartupSpec{
		Profile:  "ops",
		Queues:   []string{"heartbeat", "retention", "webhooks"},
		Handlers: []HandlerSpec{billing, webhook, heartbeat, retention},
	}
	if err := registry.ValidateStartup(startup); err != nil {
		t.Fatalf("ValidateStartup: %v", err)
	}

	tests := []struct {
		field  string
		mutate func(*HandlerSpec)
	}{
		{"current_version", func(spec *HandlerSpec) { spec.CurrentVersion++ }},
		{"supported_versions", func(spec *HandlerSpec) { spec.SupportedVersions = []int{1, 2} }},
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

func TestRegistryValidateStartupRejectsCoverageDrift(t *testing.T) {
	t.Parallel()
	registry, err := Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	heartbeat, _ := registry.Descriptor(jobcontract.KindHeartbeat)
	retention, _ := registry.Descriptor(jobcontract.KindRetentionCleanup)

	tests := []StartupSpec{
		{Profile: "ops", Queues: []string{"heartbeat"}, Handlers: []HandlerSpec{heartbeat, retention}},
		{Profile: "ops", Queues: []string{"heartbeat", "retention"}, Handlers: []HandlerSpec{heartbeat}},
		{Profile: "ops", Queues: []string{"heartbeat", "retention"}, Handlers: []HandlerSpec{heartbeat, heartbeat}},
		{Profile: "unknown", Queues: []string{"heartbeat"}, Handlers: []HandlerSpec{heartbeat}},
	}
	for index, startup := range tests {
		if err := registry.ValidateStartup(startup); err == nil {
			t.Fatalf("case %d unexpectedly passed", index)
		}
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
	if len(descriptors) != 22 || descriptors[0].Kind != jobcontract.KindInvestmentChunk ||
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
		descriptors[19].Kind != jobcontract.KindHeartbeat ||
		descriptors[20].Kind != jobcontract.KindRetentionCleanup ||
		descriptors[21].Kind != jobcontract.KindWorkGraphBuild {
		t.Fatalf("Descriptors() = %#v", descriptors)
	}
	for _, descriptor := range descriptors {
		if descriptor.Route != "celery" || descriptor.Executable() {
			t.Fatalf("checked-in production policy became executable: %#v", descriptor)
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
