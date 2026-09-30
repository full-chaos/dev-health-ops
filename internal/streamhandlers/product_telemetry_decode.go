package streamhandlers

import (
	"bytes"
	"encoding/json"
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
// The text is decoded with pyjson.Decode (Python's scanner), re-written as strict JSON with
// each non-finite number replaced by nonFiniteJSON, and read by encoding/json into the
// same typed events as before, so every other decision (field types, unknown fields, key
// case, duplicate keys, the payload value kinds validateProductEvent allows) stays the one
// it always was.
//
// nonFinite is how many NaN or infinite numbers were read as null, so the caller can make
// each such value observable (North Star check 12: a value that was not finite is stored as
// missing, and the operator can still see that it happened).
func decodeProductEvents(raw string) (events []productEvent, nonFinite int, err error) {
	value, err := pyjson.DecodeString(raw)
	if err != nil {
		return nil, 0, err
	}
	var strict bytes.Buffer
	if err := writeStrictJSON(&strict, value, &nonFinite); err != nil {
		return nil, 0, err
	}
	if err := json.Unmarshal(strict.Bytes(), &events); err != nil {
		return nil, 0, err
	}
	return events, nonFinite, nil
}

// nonFiniteJSON is the strict-JSON spelling of a NaN or infinity read from the stream: null
// (a missing value, which the readers' JSONExtract* and argMax paths treat as absent, never
// as 0).
func nonFiniteJSON(float64) string { return "null" }

func writeStrictJSON(out *bytes.Buffer, value pyjson.Value, nonFinite *int) error {
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
		if math.IsNaN(number) || math.IsInf(number, 0) {
			*nonFinite++
			out.WriteString(nonFiniteJSON(number))
			return nil
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
			if err := writeStrictJSON(out, item, nonFinite); err != nil {
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
			if err := writeStrictJSON(out, item, nonFinite); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("product telemetry events: unsupported decoded value %T", value)
	}
	return nil
}
