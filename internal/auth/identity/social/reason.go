package social

// Reason is an auth-outcome.v1 reason code (contracts.AuthOutcomeReasonCodes).
// It goes to the outcome counter and to log lines; it carries nothing else.
type Reason string

const (
	ReasonAccepted             Reason = "accepted"
	ReasonMalformed            Reason = "malformed"
	ReasonAudience             Reason = "audience"
	ReasonInvalidCredentials   Reason = "invalid_credentials"
	ReasonExpired              Reason = "expired"
	ReasonPrincipalNotFound    Reason = "principal_not_found"
	ReasonPrincipalDeactivated Reason = "principal_deactivated"
	ReasonEmailUnverified      Reason = "email_unverified"
	ReasonEmailCollision       Reason = "email_collision"
	ReasonSignupUnavailable    Reason = "signup_unavailable"
	ReasonStoreUnavailable     Reason = "store_unavailable"
	ReasonDependency           Reason = "dependency_unavailable"
)

var allReasons = []Reason{
	ReasonAccepted, ReasonMalformed, ReasonAudience, ReasonInvalidCredentials, ReasonExpired,
	ReasonPrincipalNotFound, ReasonPrincipalDeactivated, ReasonEmailUnverified, ReasonEmailCollision,
	ReasonSignupUnavailable, ReasonStoreUnavailable, ReasonDependency,
}

// Operation names the flow on the outcome counter.
type Operation string

const (
	OperationExchange Operation = "social_exchange"
	OperationLink     Operation = "social_link"
	OperationUnlink   Operation = "social_unlink"
)
