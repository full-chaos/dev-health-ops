package externalingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// BatchEnvelope mirrors schemas.py's BatchEnvelope: the wire shape every
// POST /batches and POST /validate body must match.
type BatchEnvelope struct {
	SchemaVersion  string           `json:"schemaVersion"`
	IdempotencyKey string           `json:"idempotencyKey"`
	Source         SourceDescriptor `json:"source"`
	Window         *IngestWindow    `json:"window,omitempty"`
	Records        []Record         `json:"records"`
}

// SourceDescriptor mirrors schemas.py's SourceDescriptor.
type SourceDescriptor struct {
	Type            string  `json:"type,omitempty"`
	System          string  `json:"system"`
	Instance        string  `json:"instance"`
	EntityFamily    string  `json:"entityFamily,omitempty"`
	Producer        *string `json:"producer,omitempty"`
	ProducerVersion *string `json:"producerVersion,omitempty"`
}

// IngestWindow mirrors schemas.py's IngestWindow.
type IngestWindow struct {
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt"`
}

// Record mirrors schemas.py's RecordEnvelope. Payload is validated per kind
// (validate.go), never here -- the same deliberate deferral router.py's
// docstring documents (one bad record must not abort parsing the batch).
type Record struct {
	Kind       string         `json:"kind"`
	ExternalID string         `json:"externalId"`
	Payload    map[string]any `json:"payload"`
}

var sourceSystems = map[string]bool{
	"github": true, "gitlab": true, "jira": true, "linear": true,
	"pagerduty": true, "atlassian": true, "custom": true,
}

var entityFamilies = map[string]bool{legacyEntityFamily: true, operationalEntityFamily: true}

const (
	legacyEntityFamily      = "legacy"
	operationalEntityFamily = "operational"
)

// parseEnvelope decodes and shape-validates raw exactly as
// router.py's _parse_envelope_or_400 (BatchEnvelope.model_validate_json)
// does: unknown top-level keys, missing required fields, wrong JSON types,
// an empty records array, and window.endedAt < window.startedAt are all
// rejected here, before any business rule runs. err is nil only when
// envelope is fully well-formed.
func parseEnvelope(raw []byte) (*BatchEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope BatchEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("malformed batch envelope: %w", err)
	}
	// Decode stops after the first complete JSON value and, on its own,
	// silently accepts trailing garbage after it -- unlike Python's
	// BatchEnvelope.model_validate_json, which parses the whole string
	// strictly and rejects anything left over. A batch this repo's own
	// worker then finds malformed (internal/streamhandlers/
	// external_ingest.go checks for EOF, external_ingest.go:390) must be
	// rejected HERE, at accept time, not acknowledged and permanently
	// stuck failing downstream.
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return nil, fmt.Errorf("malformed batch envelope: trailing data after the JSON value")
	}
	if envelope.SchemaVersion == "" {
		return nil, fmt.Errorf("schemaVersion is required")
	}
	if len(envelope.IdempotencyKey) < 1 || len(envelope.IdempotencyKey) > 255 {
		return nil, fmt.Errorf("idempotencyKey must be 1-255 characters")
	}
	if envelope.Source.Type == "" {
		envelope.Source.Type = "customer_push"
	} else if envelope.Source.Type != "customer_push" {
		return nil, fmt.Errorf("source.type must be %q", "customer_push")
	}
	if !sourceSystems[envelope.Source.System] {
		return nil, fmt.Errorf("source.system is invalid")
	}
	if len(envelope.Source.Instance) < 1 || len(envelope.Source.Instance) > 255 {
		return nil, fmt.Errorf("source.instance must be 1-255 characters")
	}
	if envelope.Source.EntityFamily == "" {
		envelope.Source.EntityFamily = legacyEntityFamily
	} else if !entityFamilies[envelope.Source.EntityFamily] {
		return nil, fmt.Errorf("source.entityFamily is invalid")
	}
	if envelope.Window != nil && envelope.Window.EndedAt.Before(envelope.Window.StartedAt) {
		return nil, fmt.Errorf("window.endedAt must be >= window.startedAt")
	}
	if len(envelope.Records) < 1 {
		return nil, fmt.Errorf("records must be non-empty")
	}
	for index, record := range envelope.Records {
		if record.Kind == "" {
			return nil, fmt.Errorf("records[%d].kind is required", index)
		}
		if len(record.ExternalID) < 1 || len(record.ExternalID) > 512 {
			return nil, fmt.Errorf("records[%d].externalId must be 1-512 characters", index)
		}
	}
	return &envelope, nil
}

func isRFC3339(value string) bool {
	_, err := time.Parse(time.RFC3339, value)
	return err == nil
}
