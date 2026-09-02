package pythonparity

import (
	"fmt"
	"sort"
	"strconv"
	"unicode/utf8"
)

// MarshalPythonJSON reproduces, byte for byte, what CPython's
//
//	json.dumps(value, sort_keys=True)
//
// writes for the value shapes this repository actually hashes. It is NOT a
// general JSON encoder and deliberately refuses anything outside that set.
//
// # WHY THIS EXISTS AT ALL, GIVEN encoding/json IS RIGHT THERE
//
// Go's encoder and Python's disagree on FIVE points. Measured code point by
// code point across both planes, not recalled:
//
//	                  Python json.dumps        Go json, SetEscapeHTML(false)
//	separators        ", " and ": "            "," and ":"
//	U+007F DEL        escaped,           literal DEL byte
//	U+0080 and up     escaped, é etc      literal UTF-8
//	astral U+10000+   surrogate pair           literal UTF-8
//	invalid UTF-8     not representable        silently replaced, U+FFFD
//
// The last row is real but does NOT arise through the ClickHouse path: the
// Python driver substitutes hex for undecodable values before they reach the
// encoder, so the reader handles it (DecodeClickHouseString) and by this point
// every such value is already valid ASCII.
//
// Three of those fire on any payload containing an accented name, an emoji, or
// a CJK title -- and the separator difference fires on every payload without
// exception. Only the escape table for C0 controls, the short escapes
// (backslash b/t/n/f/r, quote, backslash) and the decision to leave "/"
// unescaped are common to both.
//
// U+2028 and U+2029 agree, but BY COINCIDENCE, via unrelated mechanisms:
// Python escapes them because they are >= 0x7f, Go because of a hard-coded
// JavaScript-safety carve-out that survives SetEscapeHTML(false). The
// agreement is not structural, which is why it is pinned by a test rather than
// relied on.
//
// # WHAT DEPENDS ON THIS BEING EXACT
//
// evidence.build_text_bundle hashes this output:
//
//	serialized = json.dumps(input_payload, sort_keys=True, default=str)
//	input_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
//
// and that hash is the LLM skip-existing key -- materialize.py's
// `WHERE categorization_input_hash IN %(input_hashes)s`. A divergent hash
// matches no stored row, so every work unit re-categorizes on every run: a
// full LLM re-bill, repeated, with no error, no zero-row alarm and no
// telemetry to notice it by. The run "succeeds" and simply costs money while
// churning categorization_run_id on rows whose content never changed.
//
// That is why this function is exact rather than approximate, and why its
// corpus is generated from the Python reference rather than hand-written.
func MarshalPythonJSON(value any) ([]byte, error) {
	return appendValue(make([]byte, 0, 256), value)
}

// appendValue encodes one node. The accepted set is exactly what
// build_text_bundle's payload can contain: strings, and string-keyed maps of
// further accepted values.
//
// Anything else is a hard error, NOT a best-effort encoding. Python's
// `default=str` would stringify an unexpected type silently and still produce
// a hash; guessing which repr Python would have chosen is how a port ends up
// agreeing on the common case and diverging on the one that matters. An error
// is recoverable and visible. A wrong hash is neither.
func appendValue(dst []byte, value any) ([]byte, error) {
	switch typed := value.(type) {
	case string:
		return AppendPythonJSONString(dst, typed), nil

	case map[string]string:
		return appendObject(dst, mapKeys(typed), func(dst []byte, key string) ([]byte, error) {
			return AppendPythonJSONString(dst, typed[key]), nil
		})

	case map[string]map[string]string:
		return appendObject(dst, mapKeys(typed), func(dst []byte, key string) ([]byte, error) {
			return appendValue(dst, typed[key])
		})

	case map[string]any:
		return appendObject(dst, mapKeys(typed), func(dst []byte, key string) ([]byte, error) {
			return appendValue(dst, typed[key])
		})

	default:
		return nil, fmt.Errorf(
			"pythonparity: refusing to encode %T -- this encoder covers only the "+
				"string and string-keyed-map shapes build_text_bundle hashes; "+
				"guessing at Python's repr for other types would produce a "+
				"plausible but wrong input_hash",
			value,
		)
	}
}

// mapKeys collects the keys of any string-keyed map. Order is deliberately not
// established here: appendObject sorts, and Go's map iteration is randomized
// precisely so that code cannot come to depend on an incidental order.
func mapKeys[Value any](values map[string]Value) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

// appendObject writes a sorted object with Python's ", " / ": " separators.
//
// sort.Strings compares Go strings BYTE-WISE, and for well-formed UTF-8 the
// byte order and the code-point order are the same -- UTF-8 was designed so.
// Python's sort_keys=True compares str by code point. The two therefore agree,
// and this is the one place where Go's obvious implementation happens to be
// correct rather than merely close.
func appendObject(
	dst []byte,
	keys []string,
	appendMember func([]byte, string) ([]byte, error),
) ([]byte, error) {
	sort.Strings(keys)

	dst = append(dst, '{')
	for index, key := range keys {
		if index > 0 {
			// Python: separators default to (", ", ": ") when indent is None.
			// Go writes "," here. This single space is present in every
			// multi-member payload the pipeline hashes.
			dst = append(dst, ',', ' ')
		}
		dst = AppendPythonJSONString(dst, key)
		dst = append(dst, ':', ' ')

		var err error
		if dst, err = appendMember(dst, key); err != nil {
			return nil, err
		}
	}
	return append(dst, '}'), nil
}

// AppendPythonJSONString writes one string exactly as CPython's
// py_encode_basestring_ascii does.
//
// The governing rule, which is simpler than the escape table suggests: escape
// the seven characters that have short forms, then escape EVERYTHING from
// U+007F upward as \uXXXX with lowercase hex. That single ">= 0x7f" threshold
// is what produces the escape for DEL, the escape for an accented letter, and
// a surrogate PAIR for anything astral -- ensure_ascii guarantees pure-ASCII
// output, and JSON's \u form is 16-bit, so a code point above the BMP has no
// other spelling available.
func AppendPythonJSONString(dst []byte, value string) []byte {
	dst = append(dst, '"')

	for index := 0; index < len(value); {
		char := value[index]

		// Fast path: ASCII. Note "/" is NOT escaped -- Python leaves it alone
		// and so does Go, one of the few free agreements here.
		if char < utf8.RuneSelf {
			switch char {
			case '"':
				dst = append(dst, '\\', '"')
			case '\\':
				dst = append(dst, '\\', '\\')
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\t':
				dst = append(dst, '\\', 't')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\r':
				dst = append(dst, '\\', 'r')
			default:
				if char < 0x20 || char >= 0x7f {
					// Other C0 controls, and DEL. Go's encoder emits DEL
					// literally; Python escapes it.
					dst = appendUnicodeEscape(dst, rune(char))
				} else {
					dst = append(dst, char)
				}
			}
			index++
			continue
		}

		codePoint, size := utf8.DecodeRuneInString(value[index:])
		if codePoint == utf8.RuneError && size == 1 {
			// Invalid UTF-8.
			//
			// THIS PATH IS NOT REACHED THROUGH THE CLICKHOUSE ROUTE, and the
			// reason is worth stating because an earlier revision of this
			// comment got it wrong in both the layer and the policy.
			//
			// It claimed the driver decoded with errors="replace" and that
			// U+FFFD here matched it. Measured against a real ClickHouse
			// container, the policy is none of strict/replace/surrogateescape:
			// clickhouse-connect substitutes the LOWERCASE HEX OF THE WHOLE
			// VALUE (driver/buffer.py:135-138), so `a\xffb` arrives in Python
			// as the ASCII string "61ff62". See DecodeClickHouseString.
			//
			// The correction moved the problem to a different layer entirely:
			// the reader must apply that substitution at scan time, after
			// which every value reaching this encoder is already valid UTF-8.
			// Fixing it here would have been the wrong place even if the
			// policy had been guessed correctly.
			//
			// The branch stays because this function is not ClickHouse-only:
			// a caller passing a hand-built string can still reach it, and
			// silently emitting invalid bytes would be worse than substituting.
			// It is no longer load-bearing for parity.
			dst = appendUnicodeEscape(dst, utf8.RuneError)
			index++
			continue
		}

		if codePoint > 0xffff {
			// ensure_ascii output is 16-bit \u escapes only, so an astral code
			// point becomes its UTF-16 surrogate pair. Go emits the literal
			// character instead. In practice this is the divergence with the
			// widest blast radius: emoji are common in issue and PR titles.
			adjusted := codePoint - 0x10000
			dst = appendUnicodeEscape(dst, 0xd800+(adjusted>>10))
			dst = appendUnicodeEscape(dst, 0xdc00+(adjusted&0x3ff))
		} else {
			dst = appendUnicodeEscape(dst, codePoint)
		}
		index += size
	}

	return append(dst, '"')
}

// appendUnicodeEscape writes a \uXXXX escape with LOWERCASE hex, zero-padded
// to four digits. Python emits lowercase; uppercase would be a silent
// one-byte-per-escape divergence that no round-trip test could catch, because
// both spellings parse back to the identical string.
func appendUnicodeEscape(dst []byte, codePoint rune) []byte {
	const hexDigits = "0123456789abcdef"
	if codePoint > 0xffff {
		// Unreachable: callers split astral code points before calling here.
		// Guarded rather than assumed, because the failure mode would be a
		// silently truncated escape rather than a visible error.
		return append(dst, []byte(strconv.QuoteRune(codePoint))...)
	}
	return append(dst, '\\', 'u',
		hexDigits[(codePoint>>12)&0xf],
		hexDigits[(codePoint>>8)&0xf],
		hexDigits[(codePoint>>4)&0xf],
		hexDigits[codePoint&0xf],
	)
}
