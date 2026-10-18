package contracts

import (
	"errors"
	"fmt"
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
	"email_unverified", "email_collision", "rate_limited",
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

// RefreshRequest is one refresh-request.v1 document. RefreshCredential is a
// secret: never log or format this struct.
type RefreshRequest struct {
	SchemaVersion     string `json:"schema_version"`
	RefreshCredential string `json:"refresh_credential"`
}

// String hides the credential from %v and %+v.
func (RefreshRequest) String() string { return "RefreshRequest{refresh_credential:<redacted>}" }

// RefreshResponse is one refresh-response.v1 document. AccessToken and
// RefreshCredential are secrets: never log or format this struct.
type RefreshResponse struct {
	SchemaVersion     string `json:"schema_version"`
	AccessToken       string `json:"access_token"`
	TokenType         string `json:"token_type"`
	ExpiresIn         int    `json:"expires_in"`
	RefreshCredential string `json:"refresh_credential"`
	RefreshExpiresIn  int    `json:"refresh_expires_in"`
	SessionID         string `json:"session_id"`
}

// String hides both credentials from %v and %+v.
func (r RefreshResponse) String() string {
	return fmt.Sprintf("RefreshResponse{session_id:%s, tokens:<redacted>}", r.SessionID)
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

// Parsers are not implemented yet.
func ParseSession(root string, raw []byte) (*Session, error) {
	return nil, errors.New("not implemented")
}

// Live is not implemented yet.
func (s *Session) Live(now time.Time) bool { return false }

func ParseRefreshRequest(root string, raw []byte) (*RefreshRequest, error) {
	return nil, errors.New("not implemented")
}

func ParseRefreshResponse(root string, raw []byte) (*RefreshResponse, error) {
	return nil, errors.New("not implemented")
}

func ParseAuthOutcome(root string, raw []byte) (*AuthOutcome, error) {
	return nil, errors.New("not implemented")
}
