package social

// refusalCodes is the closed set of detail.code values a federated sign-in refusal carries on the wire, for
// web to show the right text. A code is the refusal's Reason, except that the reasons whose answers must not
// tell accounts apart (the one generic 409) fold into ReasonIdentityConflict, so the code says no more than the
// status and the message already do. A reason with no entry is not a refusal (accepted, or a 5xx with no
// detail object) and carries no code.
var refusalCodes = map[Reason]Reason{
	ReasonMalformed:            ReasonMalformed,
	ReasonInvalidCredentials:   ReasonInvalidCredentials,
	ReasonExpired:              ReasonExpired,
	ReasonPrincipalNotFound:    ReasonPrincipalNotFound,
	ReasonPrincipalDeactivated: ReasonPrincipalDeactivated,
	ReasonEmailUnverified:      ReasonEmailUnverified,
	ReasonSignupUnavailable:    ReasonSignupUnavailable,
	ReasonDependency:           ReasonDependency,
	ReasonEmailCollision:       ReasonIdentityConflict,
	ReasonProviderLinked:       ReasonIdentityConflict,
	ReasonIdentityConflict:     ReasonIdentityConflict,
}

// RefusalCode is the detail.code of a refusal for reason, or "" when the answer carries none.
func RefusalCode(reason Reason) string {
	return string(refusalCodes[reason])
}

// RefusalCodes is the closed detail.code set, each code once, in Reason order.
func RefusalCodes() []string {
	seen := map[Reason]bool{}
	var out []string
	for _, reason := range allReasons {
		code, ok := refusalCodes[reason]
		if !ok || seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, string(code))
	}
	return out
}
