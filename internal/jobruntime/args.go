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

func (RetentionCleanupArgs) Kind() string { return jobcontract.KindRetentionCleanup }

func (RetentionCleanupArgs) SupportedContractVersions() []int {
	return []int{jobcontract.ContractVersionV1}
}

func (args RetentionCleanupArgs) ContractEnvelope() jobcontract.Envelope {
	return args.envelope()
}
