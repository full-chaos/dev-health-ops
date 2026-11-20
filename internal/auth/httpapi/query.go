package httpapi

import (
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"
)

// QueryLast is how FastAPI/Starlette resolve a repeated SCALAR query
// parameter: the LAST occurrence, "" when the name is absent (a present,
// empty value, e.g. "?limit=", is also "" and must still be validated, never
// treated as absent -- check Has/the raw slice separately when that
// distinction matters). Go's own url.Values.Get returns the FIRST
// occurrence; this is the one place that mismatch is closed, so every route
// reading a scalar query param goes through here (or QueryLastPtr below)
// instead of re-deriving the same slice-index logic per package.
func QueryLast(values url.Values, name string) string {
	raw, present := values[name]
	if !present || len(raw) == 0 {
		return ""
	}
	return raw[len(raw)-1]
}

// QueryLastPtr is QueryLast for a caller that must distinguish "absent"
// (nil) from "present" (a valid pointer, possibly to "").
func QueryLastPtr(values url.Values, name string) *string {
	raw, present := values[name]
	if !present || len(raw) == 0 {
		return nil
	}
	last := raw[len(raw)-1]
	return &last
}

// maxQueryFields is net/url's defaultMaxParams, a guard against memory
// exhaustion that Query keeps on purpose.
const maxQueryFields = 10000

// Query is r's query as Starlette's request.query_params reads it (see
// ParseQuery), except that a query of more than maxQueryFields "&"-separated
// fields reads as empty, as url.ParseQuery leaves it (CPython has no cap).
// Every route reads its query through here, never r.URL.Query().
func Query(r *http.Request) url.Values {
	if strings.Count(r.URL.RawQuery, "&")+1 > maxQueryFields {
		return url.Values{}
	}
	return ParseQuery(r.URL.RawQuery)
}

// ParseQuery is CPython's parse_qsl(raw, keep_blank_values=True), the parse
// behind Starlette's QueryParams. Unlike url.ParseQuery it never drops a pair:
// pairs split on "&" only (a ";" is a literal byte), a pair without "=" has an
// empty value, "+" is a space, each well-formed %XX is one byte and any other
// "%" stays literal, and the bytes read as UTF-8 with errors="replace"
// (CPython's rule: one U+FFFD per maximal ill-formed subpart). One declared
// divergence (CHAOS-14073): Starlette decodes the RAW (not percent-encoded)
// query as latin-1 before parse_qsl, so raw UTF-8 "é" reads there as "Ã©" and a
// raw 0xFF as "ÿ". Here a raw byte >= 0x80 reads as UTF-8 like a percent-encoded
// one ("é", U+FFFD), because the latin-1 reading is mojibake. A percent-encoded
// input, the form every conforming client sends, reads the same in both.
// query_raw_bytes_test.go pins both forms with the recorded Starlette values.
func ParseQuery(raw string) url.Values {
	values := url.Values{}
	for _, pair := range strings.Split(raw, "&") {
		if pair == "" {
			continue
		}
		name, value, _ := strings.Cut(pair, "=")
		key := unquotePlus(name)
		values[key] = append(values[key], unquotePlus(value))
	}
	return values
}

func unquotePlus(s string) string {
	if !strings.ContainsAny(s, "%+") && utf8.ValidString(s) {
		return s
	}
	decoded := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '+':
			decoded = append(decoded, ' ')
		case s[i] == '%' && i+2 < len(s) && isHexDigit(s[i+1]) && isHexDigit(s[i+2]):
			decoded = append(decoded, unhexDigit(s[i+1])<<4|unhexDigit(s[i+2]))
			i += 2
		default:
			decoded = append(decoded, s[i])
		}
	}
	return replaceInvalidUTF8(decoded)
}

func replaceInvalidUTF8(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	var out strings.Builder
	out.Grow(len(b) + 8)
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		if r != utf8.RuneError || size > 1 {
			out.Write(b[:size])
			b = b[size:]
			continue
		}
		out.WriteRune(utf8.RuneError)
		b = b[maximalSubpart(b):]
	}
	return out.String()
}

// maximalSubpart is the length of the ill-formed sequence that starts b: the
// lead byte plus the continuation bytes that still fit a well-formed sequence
// (Unicode Table 3-7), at least 1.
func maximalSubpart(b []byte) int {
	lo, hi := byte(0x80), byte(0xBF)
	var need int
	switch lead := b[0]; {
	case lead >= 0xC2 && lead <= 0xDF:
		need = 1
	case lead == 0xE0:
		need, lo = 2, 0xA0
	case lead == 0xED:
		need, hi = 2, 0x9F
	case lead >= 0xE1 && lead <= 0xEF:
		need = 2
	case lead == 0xF0:
		need, lo = 3, 0x90
	case lead == 0xF4:
		need, hi = 3, 0x8F
	case lead >= 0xF1 && lead <= 0xF3:
		need = 3
	default:
		return 1
	}
	n := 1
	for ; n <= need && n < len(b); n++ {
		if b[n] < lo || b[n] > hi {
			break
		}
		lo, hi = 0x80, 0xBF
	}
	return n
}

func isHexDigit(c byte) bool {
	return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')
}

func unhexDigit(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	default:
		return c - 'A' + 10
	}
}
