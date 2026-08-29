package providersync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// team_drift_json.go ports the exact byte-for-byte JSON canonicalization
// clickhouse_team_drift_projector.py's _canonical_json and
// clickhouse_identity_drift.py's _canonical_json use everywhere a
// team_drift_changes row's old_value_json/new_value_json/change_id is
// computed:
//
//	json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
//
// Three properties of Python's json.dumps are NOT Go's encoding/json
// defaults and must be replicated by hand, or a value written by Go would
// both hash differently (change_id divergence: the same logical diff would
// stage a new pending row on every run instead of converging on one) and
// read back differently in an admin review UI that has seen Python's output
// before:
//
//  1. ensure_ascii=True (the default) -- every non-ASCII codepoint is
//     \uXXXX-escaped (surrogate pairs above the BMP). Go's encoding/json
//     writes raw UTF-8.
//  2. No HTML-escaping of <, >, & -- Go's encoding/json HTML-escapes these
//     by default.
//  3. default=str -- a value with no native JSON representation (only
//     time.Time/datetime here) is rendered via Python's str(datetime),
//     which has its own quirks (microseconds omitted when zero).
//
// This file's encoder only needs to handle the value shapes the two source
// modules actually pass to _canonical_json: nil, string, []string
// (already sorted+deduped by the caller for the JSON_FIELDS case), and
// (for identity-drift conflict payloads) a sorted-key map[string]any whose
// leaf values are string/*string/int/int32/uint8/uint16/time.Time/*time.Time/nil.
// It is deliberately not a general-purpose JSON encoder.

// pyCanonicalJSON renders value the way Python's
// json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
// would. Panics on an unsupported value shape -- every call site in this
// package passes one of the shapes documented above, and a silent wrong
// encoding of an unhandled shape would be a worse failure than a loud one
// caught in CI.
func pyCanonicalJSON(value any) string {
	var b strings.Builder
	writePyJSON(&b, value)
	return b.String()
}

func writePyJSON(b *strings.Builder, value any) {
	switch v := value.(type) {
	case nil:
		b.WriteString("null")
	case string:
		writePyJSONString(b, v)
	case *string:
		if v == nil {
			b.WriteString("null")
		} else {
			writePyJSONString(b, *v)
		}
	case []string:
		writePyStringArray(b, v)
	case bool:
		if v {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case int:
		fmt.Fprintf(b, "%d", v)
	case int32:
		fmt.Fprintf(b, "%d", v)
	case int64:
		fmt.Fprintf(b, "%d", v)
	case uint8:
		fmt.Fprintf(b, "%d", v)
	case uint16:
		fmt.Fprintf(b, "%d", v)
	case time.Time:
		writePyJSONString(b, pyStrDatetime(v))
	case *time.Time:
		if v == nil {
			b.WriteString("null")
		} else {
			writePyJSONString(b, pyStrDatetime(*v))
		}
	case map[string]any:
		writePyObject(b, v)
	default:
		panic(fmt.Sprintf("providersync: pyCanonicalJSON: unsupported value type %T", value))
	}
}

// writePyObject writes a JSON object with keys in sorted (codepoint) order,
// matching json.dumps(..., sort_keys=True). Go's map iteration order is
// randomized, so keys are collected and sorted explicitly rather than
// relying on encoding/json's own (also-sorting, but HTML-escaping) map
// marshaling.
func writePyObject(b *strings.Builder, obj map[string]any) {
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	b.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		writePyJSONString(b, key)
		b.WriteByte(':')
		writePyJSON(b, obj[key])
	}
	b.WriteByte('}')
}

// writePyStringArray mirrors _canonical_json applied to a plain []string
// (JSON_FIELDS' already-sorted-and-deduped comparison value, and identity
// rows' identity_facets when present).
func writePyStringArray(b *strings.Builder, values []string) {
	b.WriteByte('[')
	for i, v := range values {
		if i > 0 {
			b.WriteByte(',')
		}
		writePyJSONString(b, v)
	}
	b.WriteByte(']')
}

// writePyJSONString ports CPython json.encoder.py_encode_basestring_ascii
// (the ensure_ascii=True path) exactly: '\\' and '"' get their two-char
// escapes; the C0 controls get the named escapes CPython's ESCAPE_DCT
// defines (\b \f \n \r \t) or a bare \u00XX for the rest; anything outside
// printable ASCII (0x20-0x7E) -- both C0 controls and everything above
// 0x7E, including all of Latin-1/BMP/astral -- is \u-escaped, astral
// codepoints as a UTF-16 surrogate pair. This is what ensure_ascii=True
// buys over encoding/json's default (raw UTF-8, HTML-escaped <>&): every
// byte of the output is itself printable ASCII.
func writePyJSONString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			switch {
			case r >= 0x20 && r <= 0x7E:
				b.WriteRune(r)
			case r < 0x20:
				fmt.Fprintf(b, `\u%04x`, r)
			case r <= 0xFFFF:
				fmt.Fprintf(b, `\u%04x`, r)
			default:
				r2 := r - 0x10000
				hi := 0xD800 + (r2 >> 10)
				lo := 0xDC00 + (r2 & 0x3FF)
				fmt.Fprintf(b, `\u%04x\u%04x`, hi, lo)
			}
		}
	}
	b.WriteByte('"')
}

// pyStrDatetime ports Python's str(datetime) for an aware, UTC datetime
// (every datetime this package's Python counterpart passes to
// _canonical_json's default=str fallback is tz-aware UTC, produced by
// datetime.now(timezone.utc) or a ClickHouse DateTime64 read back as
// tz-aware). Format: "YYYY-MM-DD HH:MM:SS[.ffffff]+00:00" -- the
// microseconds component is present ONLY when non-zero (CPython's
// datetime.isoformat, which str() calls, drops it entirely otherwise), and
// the UTC offset always renders as "+00:00", never "Z".
func pyStrDatetime(t time.Time) string {
	t = t.UTC()
	micros := t.Nanosecond() / 1000
	if micros == 0 {
		return t.Format("2006-01-02 15:04:05") + "+00:00"
	}
	return fmt.Sprintf("%s.%06d+00:00", t.Format("2006-01-02 15:04:05"), micros)
}

// sha256Hex is the shared change_id primitive: change_id_for_team_field and
// change_id_for_identity_membership are both
// hashlib.sha256(encoded.encode("utf-8")).hexdigest() over a
// pyCanonicalJSON-encoded payload.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// pyComparisonListField mirrors _comparison_list_field: sorted(set(...)) --
// dedup then sort by Unicode codepoint (Go's sort.Strings on UTF-8 encoded
// strings gives the same ordering as Python's default string comparison for
// every codepoint that fits in a single UTF-8 continuation-free comparison,
// which covers every value these tables carry: provider identifiers, team
// keys, emails). Deliberately does NOT drop empty strings -- _list_field
// only filters None, not "", so an empty-string member/key/pattern is a
// real (if unusual) set member Python would keep.
func pyComparisonListField(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
