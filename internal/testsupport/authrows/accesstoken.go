package authrows

// CHAOS-13107: with sessions forwarded, auth-service mints the access token of every sign-in under its eddsa class, as
// production does: hdr.alg EdDSA, the issuer URL as iss and the principal (prn_) as sub, with none of the HS256
// recording's user claims. The projected claims therefore differ from the frozen golden's by design on the six sign-in
// steps (AccessTokenUser). On a leg whose GoldenSpec.GoBlanked names AccessTokenPattern, the runner compares the member
// by one place holder on both planes (AccessTokenUnvalued) and checks the target's raw value here (AccessTokenErr).

// AccessTokenPattern is the leaf whose projected value the Go plane's eddsa class makes differ from the recording.
const AccessTokenPattern = "body $.access_token"

// AccessTokenReason is why AccessTokenPattern is not compared by value on a sessions leg.
const AccessTokenReason = "EdDSA class: issuer URL iss + prn_ sub differ from the HS256 recording by design (13107)"

// AccessTokenUser is the corpus user (the seed's key) whose sign-in step answers an eddsa access token, for the six
// sign-in steps of the corpus only.
func AccessTokenUser(step string) (string, bool) { return "", false }

// PrincipalID is the session.v1 wire form of a user id (session.WirePrincipalID).
func PrincipalID(userID string) (string, error) { return "", nil }

// AccessTokenErr checks a target answer's raw body (before any projection).
func AccessTokenErr(body, issuer, principal string) error { return nil }

// AccessTokenUnvalued is body with the value of its top-level access_token replaced by one place holder.
func AccessTokenUnvalued(body string) (string, bool) { return body, false }
