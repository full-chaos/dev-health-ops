package session

// socialLinkOutcome is what a social sign-in does once the provider profile is accepted.
type socialLinkOutcome int

const (
	socialSignIn socialLinkOutcome = iota
	socialRefuseUnverified
	socialRefuseEmailMatch
	socialCreate
)

// decideSocialLink is the link guard: a stored identity signs in; otherwise an unverified e-mail is refused
// (403) before any e-mail lookup; a verified e-mail another account holds is refused (409); else a new account.
// emailHolds runs only for a verified e-mail with no stored identity.
func decideSocialLink(storedIdentity, emailVerified bool, emailHolds func() (bool, error)) (socialLinkOutcome, error) {
	if storedIdentity {
		return socialSignIn, nil
	}
	if !emailVerified {
		return socialRefuseUnverified, nil
	}
	holds, err := emailHolds()
	if err != nil {
		return socialCreate, err
	}
	if holds {
		return socialRefuseEmailMatch, nil
	}
	return socialCreate, nil
}
