package llmorgsettings

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"net/url"
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
	if len(apiKey) < 8 {
		return forms
	}
	forms = append(forms, url.QueryEscape(apiKey), url.PathEscape(apiKey), hex.EncodeToString([]byte(apiKey)))
	for shift := 0; shift < 3; shift++ {
		raw := append(make([]byte, shift), apiKey...)
		for _, enc := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
			text := enc.EncodeToString(raw)
			start := []int{0, 2, 3}[shift]
			if len(text) > start+2 {
				forms = append(forms, text[start:len(text)-2])
			}
		}
	}
	for index, form := range forms {
		forms[index] = strings.ToLower(form)
	}
	return forms
}

// CarriesKey is whether text holds any of the lower-cased key forms.
func CarriesKey(forms []string, text string) bool {
	lowered := strings.ToLower(text)
	for _, form := range forms {
		if form != "" && strings.Contains(lowered, form) {
			return true
		}
	}
	return false
}
