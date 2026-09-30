package streamhandlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// decodeProductEvents reads a stream entry's `events` field the way the Python consumer's
// json.loads does, which is the way the Python producer wrote it. The producer's
// json.dumps writes a non-finite payload number as the bare words NaN, Infinity and
// -Infinity, and json.loads accepts them; encoding/json refuses them, so a single such
// value made the whole entry permanent and quarantined up to 500 otherwise valid events
// (CHAOS-6299).
//
// The text is decoded with pyjson.Decode (Python's scanner). A non-finite number that is a
// direct value of an event's `payload` object is replaced by null; a non-finite number
// ANYWHERE ELSE (another event field, a nested payload value, the document root) is still
// refused, as before: writing null there would turn, say, a NaN orgIdHash into an accepted
// empty string. The document is then written as strict JSON and read by encoding/json into
// the same typed events as before, so every other decision (field types, unknown fields, key
// case, duplicate keys, the payload value kinds validateProductEvent allows) stays the one
// it always was.
//
// nonFinite is how many NaN or infinite payload numbers were read as null, so the caller can
// make each such value observable (North Star check 12: a value that was not finite is
// stored as missing, and the operator can still see that it happened).
func decodeProductEvents(raw string) (events []productEvent, nonFinite int, err error) {
	value, err := pyjson.DecodeString(raw)
	if err != nil {
		return nil, 0, err
	}
	nullPayloadNonFinite(value, &nonFinite)
	var strict bytes.Buffer
	if err := writeStrictJSON(&strict, value); err != nil {
		return nil, 0, err
	}
	if err := json.Unmarshal(strict.Bytes(), &events); err != nil {
		return nil, 0, err
	}
	return events, nonFinite, nil
}

// nullPayloadNonFinite replaces, in place, each non-finite number that is a direct value of
// an event's payload object with null, counting them. The document is a list of events.
func nullPayloadNonFinite(value pyjson.Value, count *int) {
	list, ok := value.([]pyjson.Value)
	if !ok {
		return
	}
	for _, item := range list {
		event, ok := item.(*pyjson.Object)
		if !ok {
			continue
		}
		payloadValue, present := event.Get("payload")
		payload, ok := payloadValue.(*pyjson.Object)
		if !present || !ok {
			continue
		}
		for _, key := range payload.Keys() {
			entry, _ := payload.Get(key)
			if number, ok := entry.(pyjson.Float); ok && isNonFinite(float64(number)) {
				payload.Set(key, nil)
				*count++
			}
		}
	}
}

func isNonFinite(number float64) bool { return math.IsNaN(number) || math.IsInf(number, 0) }

// errNonFinite refuses a non-finite number outside an event's payload.
var errNonFinite = errors.New("product telemetry events: non-finite number outside an event payload")

func writeStrictJSON(out *bytes.Buffer, value pyjson.Value) error {
	switch typed := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if typed {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return err
		}
		out.Write(encoded)
	case pyjson.Int:
		out.WriteString(typed.String())
	case pyjson.Float:
		number := float64(typed)
		if isNonFinite(number) {
			return errNonFinite
		}
		encoded, err := json.Marshal(number)
		if err != nil {
			return err
		}
		out.Write(encoded)
	case []pyjson.Value:
		out.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				out.WriteByte(',')
			}
			if err := writeStrictJSON(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case *pyjson.Object:
		out.WriteByte('{')
		for index, key := range typed.Keys() {
			if index > 0 {
				out.WriteByte(',')
			}
			encodedKey, err := json.Marshal(key)
			if err != nil {
				return err
			}
			out.Write(encodedKey)
			out.WriteByte(':')
			item, _ := typed.Get(key)
			if err := writeStrictJSON(out, item); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("product telemetry events: unsupported decoded value %T", value)
	}
	return nil
}
