package llmorgsettings

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
)

// BaseURLKeyRefusal is the one refusal text for a base url that carries an api
// key; it holds no part of the url or the key.
const BaseURLKeyRefusal = "Invalid LLM base_url"

// BaseURLValidator is the resolver-backed base url check CheckBaseURL fronts.
type BaseURLValidator func(ctx context.Context, baseURL string) (bool, string, error)

// BaseURLCarriesKey is whether baseURL holds apiKey in any form a provider
// could echo back.
func BaseURLCarriesKey(baseURL, apiKey string) bool {
	return CarriesKey(KeyLeakForms(apiKey), baseURL)
}

// CheckBaseURL refuses a base url that carries any of the keys BEFORE the
// validator can resolve its host, so a key in a hostname never reaches DNS.
// Every reader of a stored or submitted base_url runs it.
func CheckBaseURL(ctx context.Context, baseURL string, keys []string, validate BaseURLValidator) (valid bool, reason string, keyHit bool, err error) {
	for _, key := range keys {
		if BaseURLCarriesKey(baseURL, key) {
			return false, BaseURLKeyRefusal, true, nil
		}
	}
	valid, reason, err = validate(ctx, baseURL)
	return valid, reason, false, err
}

// ValidateBaseURLForKeys is CheckBaseURL over the SSRF validator.
func ValidateBaseURLForKeys(ctx context.Context, baseURL string, keys ...string) (valid bool, reason string, keyHit bool, err error) {
	return CheckBaseURL(ctx, baseURL, keys, ValidateBaseURLChecked)
}

// KeyLeakForms are the lower-cased forms in which the saved key could be
// echoed back by a provider: raw, percent-escaped, hex, and base64 at each of
// the three byte alignments (std and url alphabets, no padding).
func KeyLeakForms(apiKey string) []string {
	if apiKey == "" {
		return nil
	}
	forms := []string{strings.ToLower(apiKey)}
	derived := []string{url.QueryEscape(apiKey), url.PathEscape(apiKey), hex.EncodeToString([]byte(apiKey))}
	for shift := 0; shift < 3; shift++ {
		raw := append(make([]byte, shift), apiKey...)
		for _, enc := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
			text := enc.EncodeToString(raw)
			start := []int{0, 2, 3}[shift]
			if len(text) > start+2 {
				derived = append(derived, text[start:len(text)-2])
			}
		}
	}
	for _, form := range derived {
		if len(form) >= minDerivedFormLen {
			forms = append(forms, strings.ToLower(form))
		}
	}
	return forms
}

// minDerivedFormLen is the shortest escaped, hex or base64 form checked: a
// shorter form of a very short key is ordinary url text, not a leak. Every form
// of a key of eight bytes or more is at least this long.
const minDerivedFormLen = 8

// CarriesKey is whether text holds any of the lower-cased key forms, as written
// or once its percent-escapes are decoded (a server reads %73k as sk).
func CarriesKey(forms []string, text string) bool {
	for _, candidate := range percentDecodings(text) {
		lowered := strings.ToLower(candidate)
		for _, form := range forms {
			if form != "" && strings.Contains(lowered, form) {
				return true
			}
		}
	}
	return false
}

// maxPercentDecodings bounds how many layers of escaping are peeled off.
const maxPercentDecodings = 4

// percentDecodings is text, then text with every well-formed %XX replaced by its
// byte, repeated until nothing changes: a malformed escape elsewhere in the
// string never stops the decoding of the rest.
func percentDecodings(text string) []string {
	out := []string{text}
	for len(out) <= maxPercentDecodings {
		next := decodePercentPairs(out[len(out)-1])
		if next == out[len(out)-1] {
			break
		}
		out = append(out, next)
	}
	return out
}

func decodePercentPairs(text string) string {
	if !strings.Contains(text, "%") {
		return text
	}
	var out strings.Builder
	for i := 0; i < len(text); i++ {
		if text[i] == '%' && i+2 < len(text) && isHexDigit(text[i+1]) && isHexDigit(text[i+2]) {
			high, _ := strconv.ParseUint(text[i+1:i+3], 16, 8)
			out.WriteByte(byte(high))
			i += 2
			continue
		}
		out.WriteByte(text[i])
	}
	return out.String()
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}
