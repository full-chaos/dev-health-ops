package webhookintake

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// canonicalJSON renders value the way router.py's own hashing calls do:
// json.dumps(value, sort_keys=True, separators=(",", ":")) -- plain
// json.dumps, so its DEFAULT ensure_ascii=True applies (opposite of
// pyjson.Marshal, which targets Starlette's JSONResponse and its
// ensure_ascii=False). Used only for the delivery-key/payload-hash
// computation (_delivery_key, _persist_webhook_delivery), never for a wire
// response -- those go through internal/api/policy.WriteJSON.
//
// This is the second package to need a sorted, ensure_ascii canonicalizer
// over pyjson.Value (internal/api/externalingest/bundle.go's canonicalMarshal
// was the first, over a different flag combination for a different purpose:
// the schema-bundle ETag, ensure_ascii=True with insertion order preserved,
// not sorted). A third caller needing this exact combination should pull
// both into one shared package instead of a third copy.
func canonicalJSON(value pyjson.Value) ([]byte, error) {
	var buffer bytes.Buffer
	if err := writeCanonical(&buffer, value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func writeCanonical(buffer *bytes.Buffer, value pyjson.Value) error {
	switch v := value.(type) {
	case nil:
		buffer.WriteString("null")
	case bool:
		if v {
			buffer.WriteString("true")
		} else {
			buffer.WriteString("false")
		}
	case string:
		writeCanonicalString(buffer, v)
	case pyjson.Int:
		buffer.WriteString(v.String())
	case pyjson.Float:
		encoded, err := pyjson.Marshal(v)
		if err != nil {
			return err
		}
		buffer.Write(encoded)
	case []pyjson.Value:
		buffer.WriteByte('[')
		for index, element := range v {
			if index > 0 {
				buffer.WriteByte(',')
			}
			if err := writeCanonical(buffer, element); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
	case *pyjson.Object:
		keys := append([]string(nil), v.Keys()...)
		sort.Strings(keys)
		buffer.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				buffer.WriteByte(',')
			}
			writeCanonicalString(buffer, key)
			buffer.WriteByte(':')
			element, _ := v.Get(key)
			if err := writeCanonical(buffer, element); err != nil {
				return err
			}
		}
		buffer.WriteByte('}')
	default:
		return fmt.Errorf("canonicalJSON: unsupported value %T", value)
	}
	return nil
}

// writeCanonicalString escapes exactly the way Python's json.dumps does
// with ensure_ascii=True: every non-ASCII rune becomes a \uXXXX escape
// (surrogate pairs for astral runes), matching escapeNonASCII's reasoning
// in the sibling externalingest package.
func writeCanonicalString(buffer *bytes.Buffer, s string) {
	buffer.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			buffer.WriteString(`\"`)
		case '\\':
			buffer.WriteString(`\\`)
		case '\n':
			buffer.WriteString(`\n`)
		case '\r':
			buffer.WriteString(`\r`)
		case '\t':
			buffer.WriteString(`\t`)
		default:
			switch {
			case r < 0x20:
				fmt.Fprintf(buffer, `\u%04x`, r)
			case r < 0x7f:
				buffer.WriteRune(r)
			case r <= 0xffff:
				fmt.Fprintf(buffer, `\u%04x`, r)
			default:
				r -= 0x10000
				high := 0xd800 + (r >> 10)
				low := 0xdc00 + (r & 0x3ff)
				fmt.Fprintf(buffer, `\u%04x\u%04x`, high, low)
			}
		}
	}
	buffer.WriteByte('"')
}
