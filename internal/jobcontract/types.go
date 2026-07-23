package jobcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	// MaxEnvelopeBytes bounds River encoded_args before any job-specific decode.
	MaxEnvelopeBytes = 16 * 1024

	ContractVersionV1            = 1
	KindBillingNotification      = "operational.billing_notification"
	KindWebhookDelivery          = "operational.webhook_delivery"
	KindHeartbeat                = "system.heartbeat"
	KindRetentionCleanup         = "system.retention_cleanup"
	KindReportExecuteOnDemand    = "report.execute_on_demand"
	KindReportExecuteScheduled   = "report.execute_scheduled"
	KindDailyMetricsDispatch     = "metrics.daily_dispatch"
	KindDailyMetricsPartition    = "metrics.daily_partition"
	KindDailyMetricsFinalize     = "metrics.daily_finalize"
	KindRemainingCapacity        = "metrics.remaining.capacity"
	KindRemainingComplexity      = "metrics.remaining.complexity"
	KindRemainingDORA            = "metrics.remaining.dora"
	KindRemainingExtraMetrics    = "metrics.remaining.extra_metrics"
	KindRemainingMembership      = "metrics.remaining.membership_backfill"
	KindRemainingRecommendations = "metrics.remaining.recommendations"
	KindRemainingReleaseImpact   = "metrics.remaining.release_impact"
	KindRemainingTeamMetrics     = "metrics.remaining.team_metrics"
	KindWorkGraphBuild           = "workgraph.build"
	KindInvestmentMaterialize    = "investment.materialize"
	KindInvestmentDispatch       = "investment.dispatch"
	KindInvestmentChunk          = "investment.chunk"
	KindInvestmentFinalize       = "investment.finalize"
	KindSyncProviderUnit         = "sync.provider_unit"
	RetentionWorkerTerminal      = "worker_job_terminal"
)

var (
	kindPattern       = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	safeIDPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`)
	domainTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	uuidPattern       = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

// DomainLink points to authoritative product or schedule state. Queue state is
// never the product state of record.
type DomainLink struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// Envelope is the common, bounded portion of all Dev Health job arguments.
// Payload is a concrete type after Decode succeeds.
type Envelope struct {
	ContractVersion int        `json:"contract_version"`
	OrganizationID  *string    `json:"organization_id,omitempty"`
	CorrelationID   string     `json:"correlation_id"`
	IdempotencyKey  string     `json:"idempotency_key"`
	Domain          DomainLink `json:"domain"`
	Payload         any        `json:"payload"`
}

// HeartbeatPayload is the v1 payload for the unique periodic heartbeat pilot.
type HeartbeatPayload struct {
	ScheduledFor string `json:"scheduled_for"`
}

// RetentionCleanupPayload is the v1 bounded-delete request. It carries policy
// and a cutoff, never rows or rendered data.
type RetentionCleanupPayload struct {
	BatchSize       int    `json:"batch_size"`
	DeleteBefore    string `json:"delete_before"`
	RetentionPolicy string `json:"retention_policy"`
}

// BillingNotificationPayload carries only the durable PostgreSQL reference.
// Rendering inputs and recipient addresses remain authoritative domain state.
type BillingNotificationPayload struct {
	NotificationID string `json:"notification_id"`
}

// WebhookDeliveryPayload carries only the durable PostgreSQL reference. Raw
// provider bodies and credentials are never serialized into River.
type WebhookDeliveryPayload struct {
	DeliveryID string `json:"delivery_id"`
}

// OnDemandReportExecutionPayload identifies the authoritative SavedReport for
// an already-created manual ReportRun. The domain link supplies the run ID.
type OnDemandReportExecutionPayload struct {
	ReportID string `json:"report_id"`
}

// ScheduledReportExecutionPayload identifies the authoritative SavedReport
// for an already-created scheduled ReportRun. It is deliberately a distinct
// kind so the two production routes retain independent rollback controls.
type ScheduledReportExecutionPayload struct {
	ReportID string `json:"report_id"`
}

// DailyMetricsDispatchPayload contains only the authoritative durable run
// identity. The dispatcher reloads its partition plan from PostgreSQL.
type DailyMetricsDispatchPayload struct {
	RunID string `json:"run_id"`
}

// DailyMetricsPartitionPayload contains only the authoritative durable
// partition identity. Repository scopes and compute options never travel in
// River arguments.
type DailyMetricsPartitionPayload struct {
	PartitionID string `json:"partition_id"`
}

// DailyMetricsFinalizePayload contains only the authoritative durable run
// identity. The finalizer reloads completion counts and generation state.
type DailyMetricsFinalizePayload struct {
	RunID string `json:"run_id"`
}

// The P5 contracts deliberately carry only durable identities. Prompts,
// model configuration, credentials, source evidence, and database URLs are
// resolved server-side by the reviewed compatibility boundary.
type WorkGraphBuildPayload struct {
	RequestID string `json:"request_id"`
}
type InvestmentMaterializePayload struct {
	RequestID string `json:"request_id"`
}
type InvestmentDispatchPayload struct {
	RequestID string `json:"request_id"`
}
type InvestmentChunkPayload struct {
	ChunkID string `json:"chunk_id"`
}
type InvestmentFinalizePayload struct {
	RunID string `json:"run_id"`
}

// RemainingMetricsPartitionPayload carries only the authoritative partition
// identity. JobKind is injected by the fixed per-family type and never crosses
// the wire.
type RemainingMetricsPartitionPayload struct {
	PartitionID string `json:"partition_id"`
	JobKind     string `json:"-"`
}

func NewRemainingMetricsPartitionPayload(kind, partitionID string) RemainingMetricsPartitionPayload {
	return RemainingMetricsPartitionPayload{PartitionID: partitionID, JobKind: kind}
}

// ProviderUnitPayload carries only the authoritative SyncRunUnit identifier.
// Provider settings, credentials, routes, and callable names are reloaded in
// the worker and are never serialized through River.
type ProviderUnitPayload struct {
	UnitID string `json:"unit_id"`
}

type wireEnvelope struct {
	ContractVersion int             `json:"contract_version"`
	OrganizationID  *string         `json:"organization_id,omitempty"`
	CorrelationID   string          `json:"correlation_id"`
	IdempotencyKey  string          `json:"idempotency_key"`
	Domain          DomainLink      `json:"domain"`
	Payload         json.RawMessage `json:"payload"`
}

type contractDefinition struct {
	Kind              string
	CurrentVersion    int
	SupportedVersions []int
	DomainLink        string
	OrganizationScope string
}

var definitions = map[string]contractDefinition{
	KindBillingNotification: {
		Kind:              KindBillingNotification,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "billing_notification",
		OrganizationScope: "tenant",
	},
	KindWebhookDelivery: {
		Kind:              KindWebhookDelivery,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "webhook_delivery",
		OrganizationScope: "global",
	},
	KindHeartbeat: {
		Kind:              KindHeartbeat,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "schedule_occurrence",
		OrganizationScope: "global",
	},
	KindRetentionCleanup: {
		Kind:              KindRetentionCleanup,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "maintenance_run",
		OrganizationScope: "global",
	},
	KindReportExecuteOnDemand: {
		Kind:              KindReportExecuteOnDemand,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "report_run",
		OrganizationScope: "global",
	},
	KindReportExecuteScheduled: {
		Kind:              KindReportExecuteScheduled,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "report_run",
		OrganizationScope: "global",
	},
	KindDailyMetricsDispatch: {
		Kind:              KindDailyMetricsDispatch,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "daily_metrics_run",
		OrganizationScope: "tenant",
	},
	KindDailyMetricsPartition: {
		Kind:              KindDailyMetricsPartition,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "daily_metrics_partition",
		OrganizationScope: "tenant",
	},
	KindDailyMetricsFinalize: {
		Kind:              KindDailyMetricsFinalize,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "daily_metrics_run",
		OrganizationScope: "tenant",
	},
	KindWorkGraphBuild:           {Kind: KindWorkGraphBuild, CurrentVersion: ContractVersionV1, SupportedVersions: []int{ContractVersionV1}, DomainLink: "work_graph_request", OrganizationScope: "tenant"},
	KindInvestmentMaterialize:    {Kind: KindInvestmentMaterialize, CurrentVersion: ContractVersionV1, SupportedVersions: []int{ContractVersionV1}, DomainLink: "investment_request", OrganizationScope: "tenant"},
	KindInvestmentDispatch:       {Kind: KindInvestmentDispatch, CurrentVersion: ContractVersionV1, SupportedVersions: []int{ContractVersionV1}, DomainLink: "investment_request", OrganizationScope: "tenant"},
	KindInvestmentChunk:          {Kind: KindInvestmentChunk, CurrentVersion: ContractVersionV1, SupportedVersions: []int{ContractVersionV1}, DomainLink: "investment_chunk", OrganizationScope: "tenant"},
	KindInvestmentFinalize:       {Kind: KindInvestmentFinalize, CurrentVersion: ContractVersionV1, SupportedVersions: []int{ContractVersionV1}, DomainLink: "investment_run", OrganizationScope: "tenant"},
	KindRemainingCapacity:        remainingDefinition(KindRemainingCapacity),
	KindRemainingComplexity:      remainingDefinition(KindRemainingComplexity),
	KindRemainingDORA:            remainingDefinition(KindRemainingDORA),
	KindRemainingExtraMetrics:    remainingDefinition(KindRemainingExtraMetrics),
	KindRemainingMembership:      remainingDefinition(KindRemainingMembership),
	KindRemainingRecommendations: remainingDefinition(KindRemainingRecommendations),
	KindRemainingReleaseImpact:   remainingDefinition(KindRemainingReleaseImpact),
	KindRemainingTeamMetrics:     remainingDefinition(KindRemainingTeamMetrics),
	KindSyncProviderUnit: {
		Kind:              KindSyncProviderUnit,
		CurrentVersion:    ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "sync_run_unit",
		OrganizationScope: "tenant",
	},
}

func remainingDefinition(kind string) contractDefinition {
	return contractDefinition{
		Kind: kind, CurrentVersion: ContractVersionV1,
		SupportedVersions: []int{ContractVersionV1},
		DomainLink:        "remaining_metric_partition", OrganizationScope: "tenant",
	}
}

// Decode strictly decodes a registered kind. The kind is supplied by River's
// job column and deliberately is not duplicated inside encoded_args.
func Decode(kind string, data []byte) (Envelope, error) {
	definition, ok := definitions[kind]
	if !ok {
		return Envelope{}, errors.New("unknown job kind")
	}

	var wire wireEnvelope
	if err := decodeStrict(data, MaxEnvelopeBytes, &wire); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	if !containsVersion(definition.SupportedVersions, wire.ContractVersion) {
		return Envelope{}, fmt.Errorf("unsupported %s contract version %d", kind, wire.ContractVersion)
	}
	if err := validateCommon(definition, wire); err != nil {
		return Envelope{}, err
	}

	var payload any
	switch kind {
	case KindBillingNotification:
		var value BillingNotificationPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindWebhookDelivery:
		var value WebhookDeliveryPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindHeartbeat:
		var value HeartbeatPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindRetentionCleanup:
		var value RetentionCleanupPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindReportExecuteOnDemand:
		var value OnDemandReportExecutionPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindReportExecuteScheduled:
		var value ScheduledReportExecutionPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindDailyMetricsDispatch:
		var value DailyMetricsDispatchPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindDailyMetricsPartition:
		var value DailyMetricsPartitionPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindDailyMetricsFinalize:
		var value DailyMetricsFinalizePayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindWorkGraphBuild:
		var value WorkGraphBuildPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindInvestmentMaterialize:
		var value InvestmentMaterializePayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindInvestmentDispatch:
		var value InvestmentDispatchPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindInvestmentChunk:
		var value InvestmentChunkPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindInvestmentFinalize:
		var value InvestmentFinalizePayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	case KindRemainingCapacity, KindRemainingComplexity, KindRemainingDORA,
		KindRemainingExtraMetrics, KindRemainingMembership,
		KindRemainingRecommendations, KindRemainingReleaseImpact,
		KindRemainingTeamMetrics:
		var value RemainingMetricsPartitionPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode remaining metrics payload: %w", err)
		}
		value.JobKind = kind
		if err := value.validate(); err != nil {
			return Envelope{}, err
		}
		payload = value
	case KindSyncProviderUnit:
		var value ProviderUnitPayload
		if err := decodeStrict(wire.Payload, MaxEnvelopeBytes, &value); err != nil {
			return Envelope{}, fmt.Errorf("decode %s payload: %w", kind, err)
		}
		if err := value.validate(); err != nil {
			return Envelope{}, fmt.Errorf("validate %s payload: %w", kind, err)
		}
		payload = value
	default:
		return Envelope{}, fmt.Errorf("job kind %q has no decoder", kind)
	}

	return Envelope{
		ContractVersion: wire.ContractVersion,
		OrganizationID:  wire.OrganizationID,
		CorrelationID:   wire.CorrelationID,
		IdempotencyKey:  wire.IdempotencyKey,
		Domain:          wire.Domain,
		Payload:         payload,
	}, nil
}

// MarshalCanonical emits the stable golden representation shared with Python.
func MarshalCanonical(envelope Envelope) ([]byte, error) {
	kind := ""
	switch envelope.Payload.(type) {
	case BillingNotificationPayload:
		kind = KindBillingNotification
	case WebhookDeliveryPayload:
		kind = KindWebhookDelivery
	case HeartbeatPayload:
		kind = KindHeartbeat
	case RetentionCleanupPayload:
		kind = KindRetentionCleanup
	case OnDemandReportExecutionPayload:
		kind = KindReportExecuteOnDemand
	case ScheduledReportExecutionPayload:
		kind = KindReportExecuteScheduled
	case DailyMetricsDispatchPayload:
		kind = KindDailyMetricsDispatch
	case DailyMetricsPartitionPayload:
		kind = KindDailyMetricsPartition
	case DailyMetricsFinalizePayload:
		kind = KindDailyMetricsFinalize
	case WorkGraphBuildPayload:
		kind = KindWorkGraphBuild
	case InvestmentMaterializePayload:
		kind = KindInvestmentMaterialize
	case InvestmentDispatchPayload:
		kind = KindInvestmentDispatch
	case InvestmentChunkPayload:
		kind = KindInvestmentChunk
	case InvestmentFinalizePayload:
		kind = KindInvestmentFinalize
	case RemainingMetricsPartitionPayload:
		kind = envelope.Payload.(RemainingMetricsPartitionPayload).JobKind
	case ProviderUnitPayload:
		kind = KindSyncProviderUnit
	default:
		return nil, errors.New("unsupported payload type")
	}
	payload, err := json.Marshal(envelope.Payload)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	wire := wireEnvelope{
		ContractVersion: envelope.ContractVersion,
		OrganizationID:  envelope.OrganizationID,
		CorrelationID:   envelope.CorrelationID,
		IdempotencyKey:  envelope.IdempotencyKey,
		Domain:          envelope.Domain,
		Payload:         payload,
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(wire); err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}
	data := buffer.Bytes()
	if len(data) > MaxEnvelopeBytes {
		return nil, fmt.Errorf("encoded envelope exceeds %d bytes", MaxEnvelopeBytes)
	}
	if _, err := Decode(kind, data); err != nil {
		return nil, fmt.Errorf("validate encoded envelope: %w", err)
	}
	return data, nil
}

func validateCommon(definition contractDefinition, wire wireEnvelope) error {
	if !kindPattern.MatchString(definition.Kind) {
		return errors.New("registered job kind is invalid")
	}
	if wire.OrganizationID != nil {
		if !uuidPattern.MatchString(*wire.OrganizationID) {
			return errors.New("organization_id must be a lowercase UUID")
		}
		if definition.OrganizationScope == "global" {
			return errors.New("organization_id is forbidden for a global job")
		}
	} else if definition.OrganizationScope == "tenant" {
		return errors.New("organization_id is required for a tenant job")
	}
	if err := validateSafeID("correlation_id", wire.CorrelationID, 128); err != nil {
		return err
	}
	if err := validateSafeID("idempotency_key", wire.IdempotencyKey, 256); err != nil {
		return err
	}
	if wire.Domain.Type != definition.DomainLink {
		return fmt.Errorf("domain.type must be %q", definition.DomainLink)
	}
	if !domainTypePattern.MatchString(wire.Domain.Type) || len(wire.Domain.Type) > 64 {
		return errors.New("domain.type is invalid")
	}
	if !uuidPattern.MatchString(wire.Domain.ID) {
		return errors.New("domain.id must be a lowercase UUID")
	}
	return nil
}

func validateSafeID(name, value string, maxLength int) error {
	if len(value) == 0 || len(value) > maxLength || !safeIDPattern.MatchString(value) {
		return fmt.Errorf("%s must be a bounded safe identifier", name)
	}
	return nil
}

func (payload HeartbeatPayload) validate() error {
	return validateUTCTimestamp("scheduled_for", payload.ScheduledFor)
}

func (payload RetentionCleanupPayload) validate() error {
	if payload.BatchSize < 1 || payload.BatchSize > 1000 {
		return errors.New("batch_size must be between 1 and 1000")
	}
	if err := validateUTCTimestamp("delete_before", payload.DeleteBefore); err != nil {
		return err
	}
	if payload.RetentionPolicy != RetentionWorkerTerminal {
		return errors.New("unsupported retention_policy")
	}
	return nil
}

func (payload BillingNotificationPayload) validate() error {
	if !uuidPattern.MatchString(payload.NotificationID) {
		return errors.New("notification_id must be a lowercase UUID")
	}
	return nil
}

func (payload WebhookDeliveryPayload) validate() error {
	if !uuidPattern.MatchString(payload.DeliveryID) {
		return errors.New("delivery_id must be a lowercase UUID")
	}
	return nil
}

func (payload OnDemandReportExecutionPayload) validate() error {
	if !uuidPattern.MatchString(payload.ReportID) {
		return errors.New("report_id must be a lowercase UUID")
	}
	return nil
}

func (payload ScheduledReportExecutionPayload) validate() error {
	if !uuidPattern.MatchString(payload.ReportID) {
		return errors.New("report_id must be a lowercase UUID")
	}
	return nil
}

func (payload DailyMetricsDispatchPayload) validate() error {
	if !uuidPattern.MatchString(payload.RunID) {
		return errors.New("run_id must be a lowercase UUID")
	}
	return nil
}

func (payload DailyMetricsPartitionPayload) validate() error {
	if !uuidPattern.MatchString(payload.PartitionID) {
		return errors.New("partition_id must be a lowercase UUID")
	}
	return nil
}

func (payload DailyMetricsFinalizePayload) validate() error {
	if !uuidPattern.MatchString(payload.RunID) {
		return errors.New("run_id must be a lowercase UUID")
	}
	return nil
}

func (payload WorkGraphBuildPayload) validate() error {
	return validateUUID("request_id", payload.RequestID)
}
func (payload InvestmentMaterializePayload) validate() error {
	return validateUUID("request_id", payload.RequestID)
}
func (payload InvestmentDispatchPayload) validate() error {
	return validateUUID("request_id", payload.RequestID)
}
func (payload InvestmentChunkPayload) validate() error {
	return validateUUID("chunk_id", payload.ChunkID)
}
func (payload InvestmentFinalizePayload) validate() error {
	return validateUUID("run_id", payload.RunID)
}

func validateUUID(name, value string) error {
	if !uuidPattern.MatchString(value) {
		return fmt.Errorf("%s must be a lowercase UUID", name)
	}
	return nil
}

func (payload RemainingMetricsPartitionPayload) validate() error {
	if !remainingKind(payload.JobKind) {
		return errors.New("remaining metrics job kind is invalid")
	}
	if !uuidPattern.MatchString(payload.PartitionID) {
		return errors.New("partition_id must be a lowercase UUID")
	}
	return nil
}

func remainingKind(kind string) bool {
	switch kind {
	case KindRemainingCapacity, KindRemainingComplexity, KindRemainingDORA,
		KindRemainingExtraMetrics, KindRemainingMembership,
		KindRemainingRecommendations, KindRemainingReleaseImpact,
		KindRemainingTeamMetrics:
		return true
	default:
		return false
	}
}

func (payload ProviderUnitPayload) validate() error {
	if !uuidPattern.MatchString(payload.UnitID) {
		return errors.New("unit_id must be a lowercase UUID")
	}
	return nil
}

func validateUTCTimestamp(name, value string) error {
	if !strings.HasSuffix(value, "Z") {
		return fmt.Errorf("%s must use UTC Z notation", name)
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Location() != time.UTC {
		return fmt.Errorf("%s must be an RFC3339 UTC timestamp", name)
	}
	return nil
}

func containsVersion(versions []int, version int) bool {
	for _, candidate := range versions {
		if candidate == version {
			return true
		}
	}
	return false
}
