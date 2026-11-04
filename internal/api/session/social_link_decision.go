package session

type socialLinkOutcome int

const (
	socialSignIn socialLinkOutcome = iota
	socialRefuseUnverified
	socialRefuseEmailMatch
	socialCreate
)

func decideSocialLink(storedIdentity, emailVerified bool, emailHolds func() (bool, error)) (socialLinkOutcome, error) {
	return socialCreate, nil
}
