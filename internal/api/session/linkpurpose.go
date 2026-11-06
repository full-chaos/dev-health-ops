package session

import "github.com/full-chaos/dev-health-ops/internal/api/authmail"

// invitePurpose and linkTokenPurpose name the flow a link token is accepted by,
// for the legacy-key link counter.
const invitePurpose = authmail.PurposeInvite

var linkTokenPurpose = map[string]string{
	verificationTokens: authmail.PurposeEmailVerification,
	resetTokens:        authmail.PurposePasswordReset,
}
