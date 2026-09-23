package pyjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
)

// MarshalCanonical writes value as json.dumps(value, sort_keys=True,
// separators=(",", ":")) does -- plain json.dumps, so its DEFAULT
// ensure_ascii=True applies (every non-ASCII rune becomes \uXXXX, a
// surrogate pair above the BMP), the opposite of Marshal's ensure_ascii=False
// (Starlette's JSONResponse). Used for content-addressed hashing (ETags,
// idempotency keys, payload digests), never for a wire response.
//
// It accepts both this package's own decoded shape (Value/*Object/Int/
// Float, from DecodeString) and the shape encoding/json's stdlib decoder
// produces with UseNumber() (map[string]any, []any, json.Number, float64,
// string, bool, nil, int) -- a caller holding either representation calls
// this directly, with no conversion step. internal/api/externalingest's
// schema-bundle ETag (decoded via stdlib json.Decoder.UseNumber()) and
// internal/api/webhookintake's delivery-key/payload-hash computation
// (decoded via this package's own DecodeString) are its two callers; a
// third caller needing this exact combination should have no reason to
// write a third copy.
func MarshalCanonical(value any) ([]byte, error) {
	var buffer bytes.Buffer
	if err := writeCanonical(&buffer, value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// canonicalWriter reuses writer.writeFloat's float.__repr__ rendering with
// ensure_ascii=True -- MarshalCanonical's own doc comment on why -- via the
// shared writer type instead of a second copy of float formatting.
var canonicalWriter = writer{ascii: true}

func writeCanonical(buffer *bytes.Buffer, value any) error {
	switch typed := value.(type) {
	case nil:
		buffer.WriteString("null")
	case bool:
		if typed {
			buffer.WriteString("true")
		} else {
			buffer.WriteString("false")
		}
	case string:
		writeCanonicalString(buffer, typed)
	case Int:
		if typed.Int == nil {
			buffer.WriteString("0")
		} else {
			buffer.WriteString(typed.String())
		}
	case int:
		buffer.WriteString(strconv.Itoa(typed))
	case int64:
		buffer.WriteString(strconv.FormatInt(typed, 10))
	case json.Number:
		// The exact digit text encoding/json.Decoder.UseNumber() preserved
		// from the source document -- never reparsed and re-rendered,
		// which would risk losing precision on a large integer.
		//
		// NAMED LIMIT: this means a NONCANONICAL numeric spelling is not
		// semantically normalized the way Python's json.loads -> int/float
		// -> json.dumps round-trip would (`1e0` stays `1e0` here, Python
		// renders `1.0`; `1E+2` stays `1E+2`, Python renders `100.0`; `-0`
		// stays `-0`, Python renders `0`). Confirmed via an adversarial
		// review round: a hand-built UseNumber() input containing these
		// spellings mismatches Python byte for byte. Not fixed here because
		// a semantic reparse can't tell "safe to reparse" (an integer,
		// where digit-text preservation is the actual precision guard
		// above) from "already a float" (already precision-lossy at
		// float64) without re-deriving exactly the ambiguity this case
		// exists to avoid. Not currently reachable through either live
		// caller: the externalingest schema-bundle fixture (224 integers,
		// no floats) and every webhookintake payload observed so far use
		// canonical spellings only.
		buffer.WriteString(string(typed))
	case Float:
		return canonicalWriter.writeFloat(buffer, float64(typed))
	case float64:
		return canonicalWriter.writeFloat(buffer, typed)
	case *Object:
		keys := append([]string(nil), typed.Keys()...)
		sort.Strings(keys)
		buffer.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				buffer.WriteByte(',')
			}
			writeCanonicalString(buffer, key)
			buffer.WriteByte(':')
			element, _ := typed.Get(key)
			if err := writeCanonical(buffer, element); err != nil {
				return err
			}
		}
		buffer.WriteByte('}')
	default:
		// A generic slice or map[string]T -- covers every concrete slice
		// type a caller's own decoded shape happens to use ([]Value,
		// []any, []string, []map[string]any, a named type, ...) without
		// this switch needing a case per type. Reflection, not a growing
		// list of concrete cases, is what makes this genuinely reusable
		// across callers with different (but JSON-shaped) Go types.
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			buffer.WriteByte('[')
			for index := 0; index < rv.Len(); index++ {
				if index > 0 {
					buffer.WriteByte(',')
				}
				if err := writeCanonical(buffer, rv.Index(index).Interface()); err != nil {
					return err
				}
			}
			buffer.WriteByte(']')
		case reflect.Map:
			if rv.Type().Key().Kind() != reflect.String {
				return fmt.Errorf("pyjson: MarshalCanonical cannot write %T (non-string map key)", value)
			}
			mapKeys := rv.MapKeys()
			keys := make([]string, len(mapKeys))
			for i, k := range mapKeys {
				keys[i] = k.String()
			}
			sort.Strings(keys)
			buffer.WriteByte('{')
			for index, key := range keys {
				if index > 0 {
					buffer.WriteByte(',')
				}
				writeCanonicalString(buffer, key)
				buffer.WriteByte(':')
				element := rv.MapIndex(reflect.ValueOf(key).Convert(rv.Type().Key()))
				if err := writeCanonical(buffer, element.Interface()); err != nil {
					return err
				}
			}
			buffer.WriteByte('}')
		default:
			return fmt.Errorf("pyjson: MarshalCanonical cannot write %T", value)
		}
	}
	return nil
}

// writeCanonicalString escapes exactly the way Python's json.dumps does
// with its default ensure_ascii=True: every non-ASCII rune becomes a
// \uXXXX escape (a UTF-16 surrogate pair for a rune above the BMP).
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
		case '\b':
			buffer.WriteString(`\b`)
		case '\f':
			buffer.WriteString(`\f`)
		default:
			switch {
			case r < 0x20:
				fmt.Fprintf(buffer, `\u%04x`, r)
			case r < 0x80:
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
