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

	ContractVersionV1       = 1
	KindHeartbeat           = "system.heartbeat"
	KindRetentionCleanup    = "system.retention_cleanup"
	RetentionWorkerTerminal = "worker_job_terminal"
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
	case HeartbeatPayload:
		kind = KindHeartbeat
	case RetentionCleanupPayload:
		kind = KindRetentionCleanup
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
