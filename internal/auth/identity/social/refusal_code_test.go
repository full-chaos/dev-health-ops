package social

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// The wire code set is closed, each code is an auth-outcome.v1 reason, and the conflicts whose answer must not tell
// accounts apart share one code.
func TestRefusalCodesAreAClosedSubsetOfTheReasonVocabulary(t *testing.T) {
	want := []string{"malformed", "invalid_credentials", "expired", "principal_not_found", "principal_deactivated",
		"email_unverified", "identity_conflict", "signup_unavailable", "dependency_unavailable"}
	if got := RefusalCodes(); !slices.Equal(got, want) {
		t.Fatalf("RefusalCodes() = %v, want %v", got, want)
	}
	for _, code := range RefusalCodes() {
		if !slices.Contains(contracts.AuthOutcomeReasonCodes, code) {
			t.Errorf("code %q is not an auth-outcome.v1 reason", code)
		}
	}
	for _, reason := range []Reason{ReasonEmailCollision, ReasonProviderLinked, ReasonIdentityConflict} {
		if got := RefusalCode(reason); got != string(ReasonIdentityConflict) {
			t.Errorf("RefusalCode(%s) = %q, want the one conflict code %q", reason, got, ReasonIdentityConflict)
		}
	}
	for _, reason := range []Reason{ReasonAccepted, ReasonAudience, ReasonStoreUnavailable} {
		if got := RefusalCode(reason); got != "" {
			t.Errorf("RefusalCode(%s) = %q, want no code", reason, got)
		}
	}
}

// Every answer writeError gives with a detail object carries the code of the reason Classify names for the same
// error, and a 5xx without a detail object carries none.
func TestEveryRefusalAnswersTheCodeOfItsReason(t *testing.T) {
	failures := []error{
		&ConflictError{ExistingMethod: "password"}, ErrProviderUnavailable, ErrProviderNotConfigured, ErrProviderRefused,
		ErrInvalidProviderID, ErrEmailUnverified, ErrUnusableEmail, ErrPrincipalDisabled, identity.ErrSignupNotAvailable,
		ErrReauthenticationRequired, identity.ErrIdentityConflict, ErrProviderLinked, identity.ErrNotFound,
		identity.ErrLastSignInMethod, errors.New("unclassified"), fmt.Errorf("wrapped: %w", ErrProviderRefused),
	}
	for _, failure := range failures {
		rec := httptest.NewRecorder()
		writeError(rec, GitHub, failure)
		status, reason := Classify(failure)
		if rec.Code != status {
			t.Errorf("%v: writeError %d, Classify %d", failure, rec.Code, status)
		}
		var body struct {
			Detail json.RawMessage `json:"detail"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%v: %v", failure, err)
		}
		var detail struct {
			Message string  `json:"message"`
			Code    *string `json:"code"`
		}
		if json.Unmarshal(body.Detail, &detail) != nil {
			if rec.Code != 500 {
				t.Errorf("%v: %d %s has no detail object", failure, rec.Code, rec.Body)
			}
			continue
		}
		if detail.Code == nil || *detail.Code != RefusalCode(reason) || *detail.Code == "" {
			t.Errorf("%v: body %s, want code %q", failure, rec.Body, RefusalCode(reason))
		}
	}
}
