package externalingest

import (
	"github.com/full-chaos/dev-health-ops/internal/api/recordvalidation"
	"math/big"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
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
	// startedRaw/endedRaw are StartedAt/EndedAt exactly as
	// ValidateEnvelopeJSON parsed them: pytime.DateTime keeps the aware
	// offset and microsecond precision the input JSON string carried,
	// which StartedAt/EndedAt above (UTC-normalized .Time) discard.
	// computePayloadHash needs these -- Python's compute_payload_hash
	// hashes model_dump(mode="json")'s datetime form, which preserves the
	// original offset, not UTC.
	startedRaw, endedRaw pytime.DateTime
}

// Record mirrors schemas.py's RecordEnvelope. Payload is validated per kind
// (validate.go), never here -- the same deliberate deferral router.py's
// docstring documents (one bad record must not abort parsing the batch).
type Record struct {
	Kind       string         `json:"kind"`
	ExternalID string         `json:"externalId"`
	Payload    map[string]any `json:"payload"`
	// ordered is Payload with its keys in input order and exact numbers,
	// which the record validator needs (pydantic reports errors in field
	// and key order). parseEnvelope fills it.
	ordered *pyjson.Object
}

const (
	legacyEntityFamily      = recordvalidation.LegacyEntityFamily
	operationalEntityFamily = recordvalidation.OperationalEntityFamily
)

// parseEnvelope is router.py's _parse_envelope_or_400: the envelope
// validated as BatchEnvelope.model_validate_json does
// (ValidateEnvelopeJSON). A failure is the 400 invalid_envelope "Malformed
// batch envelope" with errors=[dict(e) ...]; where json.dumps cannot write
// those errors (a JSON syntax error's bytes input, a validator's exception
// in ctx, a NaN input) or the window validator raises TypeError, the Python
// api answers its unhandled 500, and so does this. err is nil only for a
// valid envelope.
func parseEnvelope(raw []byte) (*BatchEnvelope, error) {
	valid, errs, typeErr := recordvalidation.ValidateEnvelopeJSON(raw)
	if typeErr != nil {
		return nil, unhandledError()
	}
	if len(errs) > 0 {
		details := make([]pyjson.Value, len(errs))
		for index, item := range errs {
			if item.Unrenderable {
				if refusal := loneSurrogateRefusal(raw, item); refusal != nil {
					return nil, refusal
				}
				return nil, unhandledError()
			}
			details[index] = item.PublicDict()
		}
		if _, err := pyjson.Marshal(details); err != nil {
			return nil, unhandledError()
		}
		failure := newIngestError(http.StatusBadRequest, "invalid_envelope", "Malformed batch envelope")
		failure.Details = details
		return nil, failure
	}
	envelope := &BatchEnvelope{
		SchemaVersion:  valid.SchemaVersion,
		IdempotencyKey: valid.IdempotencyKey,
		Source: SourceDescriptor{
			Type: valid.Source.Type, System: valid.Source.System, Instance: valid.Source.Instance,
			EntityFamily: valid.Source.EntityFamily, Producer: valid.Source.Producer, ProducerVersion: valid.Source.ProducerVersion,
		},
	}
	if valid.Window != nil {
		envelope.Window = &IngestWindow{
			StartedAt: valid.Window.StartedAt.Time, EndedAt: valid.Window.EndedAt.Time,
			startedRaw: valid.Window.StartedAt, endedRaw: valid.Window.EndedAt,
		}
	}
	for _, record := range valid.Records {
		payload, _ := toAny(record.Payload).(map[string]any)
		envelope.Records = append(envelope.Records, Record{
			Kind: record.Kind, ExternalID: record.ExternalID, Payload: payload, ordered: record.Payload,
		})
	}
	return envelope, nil
}

// loneSurrogateMessages are the jiter messages for a "\\uD800"-style escape
// that is not half of a pair.
var loneSurrogateMessages = []string{"lone leading surrogate in hex escape", "unexpected end of hex escape"}

// loneSurrogateRefusal is the 422 string_unicode answer (the answer the rest
// of go-api gives for a lone surrogate) for a json_invalid error that jiter
// raised on a surrogate escape, when the body decodes otherwise. It is nil
// for every other unrenderable error, which stays the unhandled 500.
func loneSurrogateRefusal(raw []byte, item recordvalidation.PydanticError) *ingestError {
	if item.Type != "json_invalid" || !slices.ContainsFunc(loneSurrogateMessages, func(m string) bool {
		return strings.Contains(item.Msg, m)
	}) {
		return nil
	}
	value, err := pyjson.Decode(raw)
	if err != nil {
		return nil
	}
	refused := pybody.RefuseLoneSurrogates(value)
	if len(refused) == 0 {
		return nil
	}
	list, _ := pybody.Detail(refused[:1]).Get("detail")
	failure := newIngestError(http.StatusUnprocessableEntity, "string_unicode", "Request body contains an invalid Unicode string")
	failure.Details = list.([]pyjson.Value)
	return failure
}

// unhandledError is the Python api's unhandled-exception answer on this
// prefix (api/_errors.py): 500 internal_error "Internal Server Error".
func unhandledError() *ingestError {
	failure := newIngestError(http.StatusInternalServerError, "internal_error", "Internal Server Error")
	failure.Unhandled = true
	return failure
}

// toAny converts a pyjson value to the encoding/json shapes the accept path
// stores and hashes (float64 numbers, []any, map[string]any).
func toAny(value pyjson.Value) any {
	switch typed := value.(type) {
	case *pyjson.Object:
		out := make(map[string]any, typed.Len())
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			out[key] = toAny(item)
		}
		return out
	case []pyjson.Value:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = toAny(item)
		}
		return out
	case pyjson.Int:
		f, _ := new(big.Float).SetInt(typed.Int).Float64()
		return f
	case pyjson.Float:
		return float64(typed)
	}
	return value
}

// objectFromMap is an ordered copy of a decoded map, keys sorted.
func objectFromMap(values map[string]any) *pyjson.Object {
	out := pyjson.NewObject()
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out.Set(key, fromAny(values[key]))
	}
	return out
}

func fromAny(value any) pyjson.Value {
	switch typed := value.(type) {
	case map[string]any:
		return objectFromMap(typed)
	case []any:
		out := make([]pyjson.Value, len(typed))
		for index, item := range typed {
			out[index] = fromAny(item)
		}
		return out
	case float64:
		return pyjson.Float(typed)
	}
	return value
}

// validateRecords is recordvalidation.ValidateRecords over parsed envelope
// records.
func validateRecords(records []Record) []recordvalidation.ValidationErrorItem {
	inputs := make([]recordvalidation.RecordInput, len(records))
	for index, record := range records {
		payload := record.ordered
		if payload == nil {
			payload = objectFromMap(record.Payload)
		}
		inputs[index] = recordvalidation.RecordInput{Kind: record.Kind, Payload: payload}
	}
	return recordvalidation.ValidateRecords(inputs)
}
