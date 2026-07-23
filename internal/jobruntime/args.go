package jobruntime

import (
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/riverqueue/river"
)

// ContractArgs is the typed argument boundary expected by Adapter. The raw
// River encoded_args value is still validated by jobcontract.Decode before
// these already-unmarshaled values may reach a handler.
type ContractArgs interface {
	river.JobArgs
	ContractEnvelope() jobcontract.Envelope
	SupportedContractVersions() []int
}

// EnvelopeArgs preserves the exact versioned JSON envelope while keeping the
// payload statically typed for handlers.
type EnvelopeArgs[T any] struct {
	ContractVersion int                    `json:"contract_version"`
	OrganizationID  *string                `json:"organization_id,omitempty"`
	CorrelationID   string                 `json:"correlation_id"`
	IdempotencyKey  string                 `json:"idempotency_key"`
	Domain          jobcontract.DomainLink `json:"domain"`
	Payload         T                      `json:"payload"`
}

func (args EnvelopeArgs[T]) envelope() jobcontract.Envelope {
	return jobcontract.Envelope{
		ContractVersion: args.ContractVersion,
		OrganizationID:  args.OrganizationID,
		CorrelationID:   args.CorrelationID,
		IdempotencyKey:  args.IdempotencyKey,
		Domain:          args.Domain,
		Payload:         args.Payload,
	}
}

// HeartbeatArgs is the River-facing typed form of system.heartbeat.v1.
type HeartbeatArgs struct {
	EnvelopeArgs[jobcontract.HeartbeatPayload]
}

func (HeartbeatArgs) Kind() string { return jobcontract.KindHeartbeat }

func (HeartbeatArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}

func (args HeartbeatArgs) ContractEnvelope() jobcontract.Envelope {
	return args.envelope()
}

// RetentionCleanupArgs is the River-facing typed form of
// system.retention_cleanup.v1.
type RetentionCleanupArgs struct {
	EnvelopeArgs[jobcontract.RetentionCleanupPayload]
}

type BillingNotificationArgs struct {
	EnvelopeArgs[jobcontract.BillingNotificationPayload]
}

func (BillingNotificationArgs) Kind() string { return jobcontract.KindBillingNotification }

func (BillingNotificationArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}

func (args BillingNotificationArgs) ContractEnvelope() jobcontract.Envelope {
	return args.envelope()
}

type WebhookDeliveryArgs struct {
	EnvelopeArgs[jobcontract.WebhookDeliveryPayload]
}

func (WebhookDeliveryArgs) Kind() string { return jobcontract.KindWebhookDelivery }

func (WebhookDeliveryArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}

func (args WebhookDeliveryArgs) ContractEnvelope() jobcontract.Envelope {
	return args.envelope()
}

// OnDemandReportExecutionArgs is the River-facing form of
// report.execute_on_demand.v1.
type OnDemandReportExecutionArgs struct {
	EnvelopeArgs[jobcontract.OnDemandReportExecutionPayload]
}

func (OnDemandReportExecutionArgs) Kind() string { return jobcontract.KindReportExecuteOnDemand }

func (OnDemandReportExecutionArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}

func (args OnDemandReportExecutionArgs) ContractEnvelope() jobcontract.Envelope {
	return args.envelope()
}

// ScheduledReportExecutionArgs is the River-facing form of
// report.execute_scheduled.v1.
type ScheduledReportExecutionArgs struct {
	EnvelopeArgs[jobcontract.ScheduledReportExecutionPayload]
}

type DailyMetricsDispatchArgs struct {
	EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]
}

func (DailyMetricsDispatchArgs) Kind() string { return jobcontract.KindDailyMetricsDispatch }
func (DailyMetricsDispatchArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}
func (args DailyMetricsDispatchArgs) ContractEnvelope() jobcontract.Envelope { return args.envelope() }

type DailyMetricsPartitionArgs struct {
	EnvelopeArgs[jobcontract.DailyMetricsPartitionPayload]
}

func (DailyMetricsPartitionArgs) Kind() string { return jobcontract.KindDailyMetricsPartition }
func (DailyMetricsPartitionArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}
func (args DailyMetricsPartitionArgs) ContractEnvelope() jobcontract.Envelope { return args.envelope() }

type DailyMetricsFinalizeArgs struct {
	EnvelopeArgs[jobcontract.DailyMetricsFinalizePayload]
}

func (DailyMetricsFinalizeArgs) Kind() string { return jobcontract.KindDailyMetricsFinalize }
func (DailyMetricsFinalizeArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}
func (args DailyMetricsFinalizeArgs) ContractEnvelope() jobcontract.Envelope { return args.envelope() }

func (ScheduledReportExecutionArgs) Kind() string { return jobcontract.KindReportExecuteScheduled }

func (ScheduledReportExecutionArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}

func (args ScheduledReportExecutionArgs) ContractEnvelope() jobcontract.Envelope {
	return args.envelope()
}

func (RetentionCleanupArgs) Kind() string { return jobcontract.KindRetentionCleanup }

func (RetentionCleanupArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}

func (args RetentionCleanupArgs) ContractEnvelope() jobcontract.Envelope {
	return args.envelope()
}
