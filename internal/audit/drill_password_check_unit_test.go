package audit_test

import (
	"os"
	"strings"
	"testing"
)

func passwordTries(kinds ...string) []drillPasswordOutcome {
	out := make([]drillPasswordOutcome, len(drillPasswordTryNames))
	for i, name := range drillPasswordTryNames {
		out[i] = drillPasswordOutcome{Name: name}
		if i < len(kinds) {
			out[i].Kind = kinds[i]
		}
	}
	return out
}

const (
	pwOK  = drillKindSuccess
	pwBad = drillKindInvalid
	pwNA  = drillKindPasswordUnavailable
)

// Fenced: every try (the old and the new password of both users) is refused as password_unavailable.
func TestPasswordAttemptsFencedRequiresEveryTryRefusedAsPasswordUnavailable(t *testing.T) {
	for _, expect := range []string{"T", "F"} {
		if err := checkPasswordAttempts(expect, true, passwordTries(pwNA, pwNA, pwNA, pwNA)); err != nil {
			t.Fatalf("expect %s: %v", expect, err)
		}
	}
}

// invalid_credentials means a hash is still on the row: not a pass.
func TestPasswordAttemptsFencedRefusesInvalidCredentialsAsNotAPass(t *testing.T) {
	for i := range drillPasswordTryNames {
		kinds := []string{pwNA, pwNA, pwNA, pwNA}
		kinds[i] = pwBad
		if err := checkPasswordAttempts("T", true, passwordTries(kinds...)); err == nil || !strings.Contains(err.Error(), drillPasswordTryNames[i]) {
			t.Errorf("try %s: err = %v, want it to name the try", drillPasswordTryNames[i], err)
		}
	}
	if err := checkPasswordAttempts("T", true, passwordTries("store_error", pwNA, pwNA, pwNA)); err == nil {
		t.Error("a store error passed")
	}
}

// The new password is refused too (reset required): an accepted new password on a fenced copy fails.
func TestPasswordAttemptsFencedRefusesTheNewPasswordAcceptedToo(t *testing.T) {
	for _, i := range []int{1, 3} {
		kinds := []string{pwNA, pwNA, pwNA, pwNA}
		kinds[i] = pwOK
		if err := checkPasswordAttempts("T", true, passwordTries(kinds...)); err == nil {
			t.Errorf("try %s accepted on a fenced copy: no error", drillPasswordTryNames[i])
		}
	}
}

// The unfenced PITR copy is the measured gap: both old passwords accepted, both new refused.
func TestPasswordAttemptsUnfencedMustAcceptTheOldPasswordTheGap(t *testing.T) {
	if err := checkPasswordAttempts("T", false, passwordTries(pwOK, pwBad, pwOK, pwBad)); err != nil {
		t.Fatalf("the gap: %v", err)
	}
	for name, kinds := range map[string][]string{
		"auth old refused":   {pwBad, pwBad, pwOK, pwBad},
		"legacy old refused": {pwOK, pwBad, pwBad, pwBad},
		"none accepted":      {pwBad, pwBad, pwBad, pwBad},
		"unavailable":        {pwNA, pwNA, pwNA, pwNA},
	} {
		err := checkPasswordAttempts("T", false, passwordTries(kinds...))
		if err == nil || !strings.Contains(err.Error(), "gap") {
			t.Errorf("%s: err = %v, want it to say the gap was not reproduced", name, err)
		}
	}
}

func TestPasswordAttemptsUnfencedMustRefuseTheNewPassword(t *testing.T) {
	for _, i := range []int{1, 3} {
		kinds := []string{pwOK, pwBad, pwOK, pwBad}
		kinds[i] = pwOK
		if err := checkPasswordAttempts("T", false, passwordTries(kinds...)); err == nil {
			t.Errorf("new password %s accepted on the unfenced PITR copy: no error", drillPasswordTryNames[i])
		}
	}
}

// The end-of-WAL copy holds the new hash: new accepted, old refused.
func TestPasswordAttemptsEndOfWALAcceptsNewRefusesOld(t *testing.T) {
	if err := checkPasswordAttempts("F", false, passwordTries(pwBad, pwOK, pwBad, pwOK)); err != nil {
		t.Fatalf("end of WAL: %v", err)
	}
	for name, kinds := range map[string][]string{
		"auth old accepted":   {pwOK, pwOK, pwBad, pwOK},
		"legacy old accepted": {pwBad, pwOK, pwOK, pwOK},
		"auth new refused":    {pwBad, pwBad, pwBad, pwOK},
		"legacy new refused":  {pwBad, pwOK, pwBad, pwBad},
		"unavailable":         {pwNA, pwNA, pwNA, pwNA},
	} {
		if err := checkPasswordAttempts("F", false, passwordTries(kinds...)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestPasswordAttemptsRejectsAWrongTrySetOrOrder(t *testing.T) {
	good := passwordTries(pwOK, pwBad, pwOK, pwBad)
	swapped := append([]drillPasswordOutcome(nil), good...)
	swapped[0], swapped[1] = swapped[1], swapped[0]
	renamed := append([]drillPasswordOutcome(nil), good...)
	renamed[2].Name = "x"
	for name, bad := range map[string][]drillPasswordOutcome{"none": nil, "three": good[:3], "five": append(append([]drillPasswordOutcome(nil), good...), good[0]), "swapped": swapped, "renamed": renamed} {
		if err := checkPasswordAttempts("T", false, bad); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// Messages carry labels and kinds, never a password or a hash.
func TestPasswordAttemptMessagesNameLabelsNeverValues(t *testing.T) {
	tries := passwordTries(pwOK, pwBad, pwOK, pwBad)
	tries[0].Kind = "$2b$12$secretlookingvalue"
	err := checkPasswordAttempts("T", true, tries)
	if err == nil {
		t.Fatal("no error")
	}
	if !strings.Contains(err.Error(), drillPasswordTryNames[0]) {
		t.Errorf("the message does not name the try: %v", err)
	}
	if strings.Contains(err.Error(), "secretlookingvalue") {
		t.Errorf("the message carries a value: %v", err)
	}
}

// The legacy password write of the drill is the statement go-api runs (changepassword.go): if either side changes, this
// goes red and the drill's SQL is reviewed against the handler again.
func TestTheLegacyPasswordWriteIsPinnedToTheGoAPIStatement(t *testing.T) {
	source, err := os.ReadFile("../api/session/changepassword.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), drillGoAPIPasswordPin) {
		t.Fatalf("internal/api/session/changepassword.go no longer holds the pinned statement %q: re-read the handler and update the drill's SQL and the pin together", drillGoAPIPasswordPin)
	}
	derived := strings.NewReplacer("UPDATE users", "UPDATE public.users", "token_version = $3", "token_version = token_version + 1",
		"updated_at = $4", "updated_at = now()", "$1::uuid", "$1").Replace(drillGoAPIPasswordPin)
	if drillLegacyPasswordSQL != derived {
		t.Fatalf("the drill's legacy password SQL is not the pinned go-api statement with the same columns:\n got %s\nwant %s", drillLegacyPasswordSQL, derived)
	}
}
