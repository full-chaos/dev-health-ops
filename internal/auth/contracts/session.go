package contracts

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// Wire surfaces added by CHAOS-10155 (Wave 2 A1).
const (
	SessionSurface         = "session.v1"
	RefreshRequestSurface  = "refresh-request.v1"
	RefreshResponseSurface = "refresh-response.v1"
	AuthOutcomeSurface     = "auth-outcome.v1"
)

// AuthOutcomeSuccessReasons are the only reason codes a success may carry.
// TestAuthOutcomeVocabularyMatchesTheSchema keeps this equal to the schema.
var AuthOutcomeSuccessReasons = []string{"accepted", "refresh_grace_replay"}

// AuthOutcomeReasonCodes is the closed reason vocabulary. The schema is the
// authority; this copy exists so callers can emit a code without reading it.
var AuthOutcomeReasonCodes = []string{
	"accepted", "refresh_grace_replay",
	"malformed", "bad_signature", "audience", "issuer", "expired", "not_yet_valid",
	"missing_sub", "missing_type", "type_mismatch", "refresh_unknown", "refresh_expired",
	"refresh_reused", "session_revoked", "session_expired", "principal_not_found",
	"principal_deactivated", "revision_stale", "invalid_credentials", "account_locked",
	"email_unverified", "email_collision", "signup_unavailable", "provider_already_linked",
	"identity_conflict", "rate_limited",
	"replayed", "nonce_mismatch", "provisioning_disabled", "domain_not_allowed", "not_entitled",
	"membership_required",
	"store_unavailable", "dependency_unavailable",
}

// Session is one session.v1 document, validated and parsed.
type Session struct {
	SchemaVersion string     `json:"schema_version"`
	SessionID     string     `json:"session_id"`
	PrincipalID   string     `json:"principal_id"`
	Revision      Revision   `json:"revision"`
	AuthTime      Timestamp  `json:"auth_time"`
	AMR           []string   `json:"amr"`
	CreatedAt     Timestamp  `json:"created_at"`
	ExpiresAt     Timestamp  `json:"expires_at"`
	RevokedAt     *Timestamp `json:"revoked_at"`
}

// SecretString holds a credential. It is a struct, not a named string: a
// named string is written verbatim by encoding/json (and slog's JSON handler)
// when it is a map key, and converts silently with string(). Every way a Go
// program prints a value -- fmt verbs, slog text and JSON handlers,
// json.Marshal of a field or a map key -- yields "[redacted]"; only Reveal
// returns the value. UnmarshalJSON reads the real value, so the parsers below
// fill it from the wire. The structs that hold SecretStrings are decode
// targets: a future emitter must write the wire form from Reveal, never by
// marshalling these structs.
//
// The value sits behind a pointer. fmt prints a bad verb (%p on a struct
// value) with no method of any field called, field by field; a pointer field
// prints as an address, never as the string it points to.
type SecretString struct{ value *string }

const redacted = "[redacted]"

// Reveal returns the credential. Call it only to compare or to send.
func (s SecretString) Reveal() string {
	if s.value == nil {
		return ""
	}
	return *s.value
}

func (SecretString) String() string { return redacted }

// Format serves EVERY fmt verb. Without it, a verb String does not serve
// (%d, %b, %x on a struct ...) makes fmt print the struct's field, the value.
func (SecretString) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(redacted)) }

func (SecretString) GoString() string             { return redacted }
func (SecretString) LogValue() slog.Value         { return slog.StringValue(redacted) }
func (SecretString) MarshalText() ([]byte, error) { return []byte(redacted), nil }
func (SecretString) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }

// UnmarshalJSON reads the wire value.
func (s *SecretString) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.value = &raw
	return nil
}

// RefreshRequest is one refresh-request.v1 document.
type RefreshRequest struct {
	SchemaVersion     string       `json:"schema_version"`
	RefreshCredential SecretString `json:"refresh_credential"`
}

// RefreshResponse is one refresh-response.v1 document.
type RefreshResponse struct {
	SchemaVersion     string       `json:"schema_version"`
	AccessToken       SecretString `json:"access_token"`
	TokenType         string       `json:"token_type"`
	ExpiresIn         int          `json:"expires_in"`
	RefreshCredential SecretString `json:"refresh_credential"`
	RefreshExpiresIn  int          `json:"refresh_expires_in"`
	SessionID         string       `json:"session_id"`
}

// AuthOutcome is one auth-outcome.v1 document, validated and parsed.
type AuthOutcome struct {
	SchemaVersion string    `json:"schema_version"`
	Operation     string    `json:"operation"`
	Result        string    `json:"result"`
	ReasonCode    string    `json:"reason_code"`
	RequestID     string    `json:"request_id"`
	OccurredAt    Timestamp `json:"occurred_at"`
	SessionID     string    `json:"session_id,omitempty"`
}

// credentialCarryingSurfaces are the surfaces whose documents carry credentials.
var credentialCarryingSurfaces = map[string]bool{RefreshRequestSurface: true, RefreshResponseSurface: true}

// decodeWire runs the shared order: duplicate members, schema, then decode.
// Validation sees the decoded JSON value, not dest, so a field dest does not
// declare still reaches additionalProperties:false.
func decodeWire(root, surface string, raw []byte, dest any) error {
	if err := refuseDuplicateMembersIn(surface, raw); err != nil {
		return err
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return fmt.Errorf("%s: document is not valid JSON: %w", surface, err)
	}
	if err := Validate(root, surface, document); err != nil {
		if credentialCarryingSurfaces[surface] {
			// The validator's message repeats the rejected value; on these
			// surfaces that value is a credential.
			return fmt.Errorf("%s: document does not satisfy the schema (violation text withheld: this surface carries credentials)", surface)
		}
		return err
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return fmt.Errorf("%s: decoding validated document: %w", surface, err)
	}
	return nil
}

func inRange(surface, field string, t Timestamp) error {
	return checkInstantInRange(surface, field, t.Time)
}

// ParseSession validates and parses a session.v1 document.
//
// Beyond the schema it refuses what a schema cannot say: an instant outside
// 0001..9999 UTC, expires_at not after created_at, auth_time after created_at,
// and revoked_at before created_at.
func ParseSession(root string, raw []byte) (*Session, error) {
	var s Session
	if err := decodeWire(root, SessionSurface, raw, &s); err != nil {
		return nil, err
	}
	for field, t := range map[string]Timestamp{"auth_time": s.AuthTime, "created_at": s.CreatedAt, "expires_at": s.ExpiresAt} {
		if err := inRange(SessionSurface, field, t); err != nil {
			return nil, err
		}
	}
	if s.RevokedAt != nil {
		if err := inRange(SessionSurface, "revoked_at", *s.RevokedAt); err != nil {
			return nil, err
		}
		if s.RevokedAt.Before(s.CreatedAt.Time) {
			return nil, fmt.Errorf("%s: revoked_at precedes created_at", SessionSurface)
		}
	}
	if !s.ExpiresAt.After(s.CreatedAt.Time) {
		return nil, fmt.Errorf("%s: expires_at is not after created_at", SessionSurface)
	}
	if s.AuthTime.After(s.CreatedAt.Time) {
		return nil, fmt.Errorf("%s: auth_time is after created_at", SessionSurface)
	}
	return &s, nil
}

// Live reports whether the session is neither revoked nor expired at now.
func (s *Session) Live(now time.Time) bool {
	return s.RevokedAt == nil && now.Before(s.ExpiresAt.Time)
}

// ParseRefreshRequest validates and parses a refresh-request.v1 document.
func ParseRefreshRequest(root string, raw []byte) (*RefreshRequest, error) {
	var r RefreshRequest
	if err := decodeWire(root, RefreshRequestSurface, raw, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// ParseRefreshResponse validates and parses a refresh-response.v1 document.
// expires_in and refresh_expires_in decode into int, which refuses 3600.0.
func ParseRefreshResponse(root string, raw []byte) (*RefreshResponse, error) {
	var r RefreshResponse
	if err := decodeWire(root, RefreshResponseSurface, raw, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// ParseAuthOutcome validates and parses an auth-outcome.v1 document.
func ParseAuthOutcome(root string, raw []byte) (*AuthOutcome, error) {
	var o AuthOutcome
	if err := decodeWire(root, AuthOutcomeSurface, raw, &o); err != nil {
		return nil, err
	}
	if err := inRange(AuthOutcomeSurface, "occurred_at", o.OccurredAt); err != nil {
		return nil, err
	}
	return &o, nil
}
