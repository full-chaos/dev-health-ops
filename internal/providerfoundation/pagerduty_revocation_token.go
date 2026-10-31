package providerfoundation

import (
	"encoding/json"
	"errors"
	"time"
)

// The key versions of provider_oauth_revocations.token_encrypted: what the sealed value holds.
const (
	// RevocationKeyToken: the token to revoke itself (every route writer).
	RevocationKeyToken = "v1"
	// RevocationKeyOAuth: a stored credential's sealed OAuthTokens JSON, copied as is from
	// provider_oauth_credentials.token_encrypted (the restore fence).
	RevocationKeyOAuth = "oauth-v1"
)

// ErrRevocationKeyVersion: the row's key version is not one this build reads; nothing is sent.
var ErrRevocationKeyVersion = errors.New("pagerduty revocation: unknown token key version")

// PagerDutyRevocationToken is the token a revocation row's opened value revokes, by its key version.
func PagerDutyRevocationToken(keyVersion string, plaintext []byte) (string, error) {
	switch keyVersion {
	case RevocationKeyToken:
		return string(plaintext), nil
	case RevocationKeyOAuth:
		return PagerDutyStoredRevokeToken(plaintext)
	}
	return "", ErrRevocationKeyVersion
}

// PagerDutyStoredRevokeToken is OAuthTokens.model_validate_json(...) followed by
// `tokens.refresh_token or tokens.access_token` -- the token a live
// credential's decrypted JSON payload actually revokes. The model is
// extra="forbid" with a required access_token (str) and expires_at
// (datetime), an optional refresh_token (str | None) and an optional
// granted_scopes (list of str), so a payload that is valid JSON but not that
// object (an empty object, a missing field, a wrong type, an extra key) is an
// error, as Python's ValidationError (a ValueError) is. Named difference:
// expires_at is accepted as an RFC 3339 string, a date-time without an
// offset, a date, or a JSON number; pydantic accepts a few more spellings.
func PagerDutyStoredRevokeToken(plaintext []byte) (string, error) {
	tokens, err := ParsePagerDutyStoredTokens(plaintext)
	if err != nil {
		return "", err
	}
	if tokens.Refresh != "" {
		return tokens.Refresh, nil
	}
	return tokens.Access, nil
}

// PagerDutyStoredTokens is OAuthTokens as validated from a stored payload.
type PagerDutyStoredTokens struct {
	Access, Refresh string
	HasRefresh      bool
	ExpiresAt       time.Time
	// ExpiresNaive is an expires_at without an offset, which Python reads as
	// a naive datetime (and fails to compare with an aware one).
	ExpiresNaive bool
	Scopes       []string
}

// ParsePagerDutyStoredTokens is OAuthTokens.model_validate_json.
func ParsePagerDutyStoredTokens(plaintext []byte) (PagerDutyStoredTokens, error) {
	var out PagerDutyStoredTokens
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(plaintext, &fields); err != nil || fields == nil {
		return out, errors.New("pagerduty oauth tokens: not a JSON object")
	}
	for key := range fields {
		switch key {
		case "access_token", "refresh_token", "expires_at", "granted_scopes":
		default:
			return out, errors.New("pagerduty oauth tokens: extra field")
		}
	}
	if raw, ok := fields["access_token"]; !ok || json.Unmarshal(raw, &out.Access) != nil || string(raw) == "null" {
		return out, errors.New("pagerduty oauth tokens: access_token")
	}
	var refresh *string
	if raw, ok := fields["refresh_token"]; ok && json.Unmarshal(raw, &refresh) != nil {
		return out, errors.New("pagerduty oauth tokens: refresh_token")
	}
	if refresh != nil {
		out.Refresh, out.HasRefresh = *refresh, true
	}
	raw, ok := fields["expires_at"]
	if !ok {
		return out, errors.New("pagerduty oauth tokens: expires_at")
	}
	expires, naive, valid := pagerDutyTokenExpiry(raw)
	if !valid {
		return out, errors.New("pagerduty oauth tokens: expires_at")
	}
	out.ExpiresAt, out.ExpiresNaive = expires, naive
	if raw, ok := fields["granted_scopes"]; ok {
		if json.Unmarshal(raw, &out.Scopes) != nil || string(raw) == "null" {
			return out, errors.New("pagerduty oauth tokens: granted_scopes")
		}
	}
	return out, nil
}

// pagerDutyTokenExpiry reads an expires_at: an RFC 3339 string, a date-time
// without an offset (naive), a date, or a JSON number (epoch seconds).
func pagerDutyTokenExpiry(raw json.RawMessage) (at time.Time, naive, ok bool) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
			return parsed.UTC(), false, true
		}
		for _, layout := range []string{"2006-01-02 15:04:05.999999999Z07:00"} {
			if parsed, err := time.Parse(layout, text); err == nil {
				return parsed.UTC(), false, true
			}
		}
		for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999", "2006-01-02"} {
			if parsed, err := time.Parse(layout, text); err == nil {
				return parsed.UTC(), true, true
			}
		}
		return time.Time{}, false, false
	}
	var number float64
	if json.Unmarshal(raw, &number) == nil && string(raw) != "null" {
		whole := int64(number)
		return time.Unix(whole, int64((number-float64(whole))*1e9)).UTC(), false, true
	}
	return time.Time{}, false, false
}
