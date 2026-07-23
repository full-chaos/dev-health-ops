package jobruntime

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/riverqueue/river"
)

var (
	_ ContractArgs                = HeartbeatArgs{}
	_ ContractArgs                = BillingNotificationArgs{}
	_ ContractArgs                = RetentionCleanupArgs{}
	_ ContractArgs                = WebhookDeliveryArgs{}
	_ ContractArgs                = RemainingCapacityArgs{}
	_ ContractArgs                = RemainingComplexityArgs{}
	_ ContractArgs                = RemainingDORAArgs{}
	_ ContractArgs                = RemainingExtraMetricsArgs{}
	_ ContractArgs                = RemainingMembershipArgs{}
	_ ContractArgs                = RemainingRecommendationsArgs{}
	_ ContractArgs                = RemainingReleaseImpactArgs{}
	_ ContractArgs                = RemainingTeamMetricsArgs{}
	_ river.Worker[HeartbeatArgs] = (*Adapter[HeartbeatArgs])(nil)
)

func TestTypedArgsPreserveVersionedContractEnvelope(t *testing.T) {
	t.Parallel()
	tests := []struct {
		kind string
		args ContractArgs
	}{
		{
			kind: jobcontract.KindHeartbeat,
			args: HeartbeatArgs{EnvelopeArgs: EnvelopeArgs[jobcontract.HeartbeatPayload]{
				ContractVersion: 1,
				CorrelationID:   "corr-heartbeat",
				IdempotencyKey:  "heartbeat:2026-07-21T12:00:00Z",
				Domain: jobcontract.DomainLink{
					Type: "schedule_occurrence",
					ID:   "11111111-1111-4111-8111-111111111111",
				},
				Payload: jobcontract.HeartbeatPayload{ScheduledFor: "2026-07-21T12:00:00Z"},
			}},
		},
		{
			kind: jobcontract.KindRetentionCleanup,
			args: RetentionCleanupArgs{EnvelopeArgs: EnvelopeArgs[jobcontract.RetentionCleanupPayload]{
				ContractVersion: 1,
				CorrelationID:   "corr-retention",
				IdempotencyKey:  "retention:2026-07-21",
				Domain: jobcontract.DomainLink{
					Type: "maintenance_run",
					ID:   "22222222-2222-4222-8222-222222222222",
				},
				Payload: jobcontract.RetentionCleanupPayload{
					BatchSize:       100,
					DeleteBefore:    "2026-07-01T00:00:00Z",
					RetentionPolicy: jobcontract.RetentionWorkerTerminal,
				},
			}},
		},
		{
			kind: jobcontract.KindRemainingCapacity,
			args: RemainingCapacityArgs{EnvelopeArgs: EnvelopeArgs[jobcontract.RemainingMetricsPartitionPayload]{
				ContractVersion: 1,
				OrganizationID:  stringPointer("33333333-3333-4333-8333-333333333333"),
				CorrelationID:   "corr-remaining",
				IdempotencyKey:  "remaining:partition:44444444-4444-4444-8444-444444444444",
				Domain: jobcontract.DomainLink{
					Type: "remaining_metric_partition",
					ID:   "44444444-4444-4444-8444-444444444444",
				},
				Payload: jobcontract.RemainingMetricsPartitionPayload{
					PartitionID: "44444444-4444-4444-8444-444444444444",
				},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			raw, err := json.Marshal(test.args)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			decoded, err := jobcontract.Decode(test.kind, raw)
			if err != nil {
				t.Fatalf("Decode: %v\n%s", err, raw)
			}
			if !reflect.DeepEqual(decoded, test.args.ContractEnvelope()) {
				t.Fatalf("typed/wire drift:\nwire=%+v\ntyped=%+v", decoded, test.args.ContractEnvelope())
			}
		})
	}
}

func stringPointer(value string) *string { return &value }
