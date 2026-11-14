package social

// refusalReasons is the closed set of detail.reason values a federated sign-in, link or unlink refusal carries on
// the wire, for web to show the right text; detail.reason is the key the SSO refusals use too (membership_required).
// A wire reason is the refusal's Reason, except that the reasons whose answers must not tell accounts apart (the one
// generic 409) fold into ReasonIdentityConflict, so the wire reason says no more than the status and the message
// already do. A reason with no entry is not a refusal (accepted, or a 5xx with no detail object) and carries none.
var refusalReasons = map[Reason]Reason{
	ReasonMalformed:            ReasonMalformed,
	ReasonInvalidCredentials:   ReasonInvalidCredentials,
	ReasonExpired:              ReasonExpired,
	ReasonPrincipalNotFound:    ReasonPrincipalNotFound,
	ReasonPrincipalDeactivated: ReasonPrincipalDeactivated,
	ReasonEmailUnverified:      ReasonEmailUnverified,
	ReasonSignupUnavailable:    ReasonSignupUnavailable,
	ReasonDependency:           ReasonDependency,
	ReasonLastSignInMethod:     ReasonLastSignInMethod,
	ReasonEmailCollision:       ReasonIdentityConflict,
	ReasonProviderLinked:       ReasonIdentityConflict,
	ReasonIdentityConflict:     ReasonIdentityConflict,
}

// RefusalReason is the detail.reason of a refusal for reason, or "" when the answer carries none.
func RefusalReason(reason Reason) string {
	return string(refusalReasons[reason])
}

// RefusalReasons is the closed detail.reason set, each value once, in Reason order.
func RefusalReasons() []string {
	seen := map[Reason]bool{}
	var out []string
	for _, reason := range allReasons {
		wire, ok := refusalReasons[reason]
		if !ok || seen[wire] {
			continue
		}
		seen[wire] = true
		out = append(out, string(wire))
	}
	return out
}
