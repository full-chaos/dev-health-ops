package session

// Reason is an auth-outcome.v1 reason code. TestReasonsAreAuthOutcomeCodes
// keeps every constant inside the contract's closed vocabulary, so a reason
// can go to a metric label or an audit row without carrying anything else.
type Reason string

const (
	ReasonAccepted             Reason = "accepted"
	ReasonGraceReplay          Reason = "refresh_grace_replay"
	ReasonMalformed            Reason = "malformed"
	ReasonRefreshUnknown       Reason = "refresh_unknown"
	ReasonRefreshExpired       Reason = "refresh_expired"
	ReasonRefreshReused        Reason = "refresh_reused"
	ReasonSessionRevoked       Reason = "session_revoked"
	ReasonSessionExpired       Reason = "session_expired"
	ReasonPrincipalDeactivated Reason = "principal_deactivated"
	// ReasonRevisionStale refuses a session without a legacy_token_version pin.
	ReasonRevisionStale    Reason = "revision_stale"
	ReasonStoreUnavailable Reason = "store_unavailable"
	ReasonDependency       Reason = "dependency_unavailable"
)

// Success reports whether the reason is one a successful exchange carries.
func (r Reason) Success() bool { return r == ReasonAccepted || r == ReasonGraceReplay }

var allReasons = []Reason{
	ReasonAccepted, ReasonGraceReplay, ReasonMalformed, ReasonRefreshUnknown, ReasonRefreshExpired,
	ReasonRefreshReused, ReasonSessionRevoked, ReasonSessionExpired, ReasonPrincipalDeactivated, ReasonRevisionStale,
	ReasonStoreUnavailable, ReasonDependency,
}

// revokedReason is the closed set this package writes to revoked_reason.
const (
	revokedByReuse  = string(ReasonRefreshReused)
	revokedByCaller = string(ReasonSessionRevoked)
)
