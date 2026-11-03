package audit_test

import "fmt"

// The authentication-attempt drill for passwords (CHAOS-11834). The rule is pure so it is unit-tested without docker.
const (
	drillKindSuccess             = "success"
	drillKindInvalid             = "invalid_credentials"
	drillKindPasswordUnavailable = "password_unavailable"
)

// drillPasswordTryNames is the fixed order of the four tries on one copy: the password a user had before T, then the one
// it set after T, for the auth-service user ("auth") and the go-api-only user ("legacy").
var drillPasswordTryNames = []string{"auth-old", "auth-new", "legacy-old", "legacy-new"}

type drillPasswordOutcome struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// drillGoAPIPasswordPin is the statement internal/api/session/changepassword.go runs to write a password. The drill's
// legacy write (drillLegacyPasswordSQL) copies it; a unit cell fails when either side drifts.
const drillGoAPIPasswordPin = "UPDATE users SET password_hash = $2, token_version = $3, updated_at = $4 WHERE id = $1::uuid"

const drillLegacyPasswordSQL = "UPDATE public.users SET password_hash = $2, token_version = token_version + 1, updated_at = now() WHERE id = $1"

// kindLabel prints a kind only when it is one of the known ones, so a stray string never reaches a message.
func kindLabel(kind string) string {
	switch kind {
	case drillKindSuccess, drillKindInvalid, drillKindPasswordUnavailable, "locked", "disabled", "email_unverified", "store_error":
		return kind
	}
	return "other"
}

// checkPasswordAttempts is the rule of the password drill. tries are in drillPasswordTryNames order.
//
//	fenced (any copy after a fence apply): every try must be refused as password_unavailable. The restored hash is NULL,
//	so the old password and the new password are both refused (reset required). invalid_credentials means the hash is
//	still there; success means the fence did not hold.
//	end-of-WAL copy (expect F, not fenced): the new password is accepted and the old one refused.
//	unfenced PITR copy (expect T): the old password is accepted (the gap) and the new one refused; a drill that does not
//	reproduce the gap fails rather than passing silently.
//
// Messages name the try label and the kind, never a password or a hash.
func checkPasswordAttempts(expect string, fenced bool, tries []drillPasswordOutcome) error {
	if len(tries) != len(drillPasswordTryNames) {
		return fmt.Errorf("the password attempts are not exactly %v: %d recorded", drillPasswordTryNames, len(tries))
	}
	for i, name := range drillPasswordTryNames {
		if tries[i].Name != name {
			return fmt.Errorf("password attempt %d is %q, want %q", i, tries[i].Name, name)
		}
	}
	for i, try := range tries {
		isNew := i%2 == 1
		var want string
		switch {
		case fenced:
			want = drillKindPasswordUnavailable
		case expect == "F" && isNew, expect != "F" && !isNew:
			want = drillKindSuccess
		default:
			want = drillKindInvalid
		}
		if try.Kind == want {
			continue
		}
		if !fenced && expect != "F" && !isNew {
			return fmt.Errorf("the unfenced PITR copy did not reproduce the gap: %s was %q, want %q", try.Name, kindLabel(try.Kind), want)
		}
		return fmt.Errorf("%s was %q, want %q (expect=%s fenced=%v)", try.Name, kindLabel(try.Kind), want, expect, fenced)
	}
	return nil
}
