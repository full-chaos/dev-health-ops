package saml_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// sharedFederatedReasons are the auth-outcome.v1 reason codes agreed for the
// federated sign-in lanes beyond the published vocabulary.
var sharedFederatedReasons = []string{"replayed", "signup_unavailable", "provisioning_disabled", "domain_not_allowed", "not_entitled", "nonce_mismatch"}

func TestSAMLOutcomeReasonsUseTheSharedVocabulary(t *testing.T) {
	refusal := func(reason string) error { return &saml.RefusedError{Reason: reason} }
	for err, want := range map[error]string{
		saml.ErrAssertionReplayed:                       "replayed",
		saml.ErrUnsolicited:                             "nonce_mismatch",
		saml.ErrFlowNotFound:                            "nonce_mismatch",
		saml.ErrFlowConsumed:                            "replayed",
		fmt.Errorf("w: %w", saml.ErrSignupNotAvailable): "signup_unavailable",
		saml.ErrAutoProvisionDisabled:                   "provisioning_disabled",
		saml.ErrDomainNotAllowed:                        "domain_not_allowed",
		saml.ErrNotEntitled:                             "not_entitled",
		saml.ErrAccountDisabled:                         "principal_deactivated",
		&saml.EmailConflictError{Method: "local"}:       "email_collision",
		saml.ErrNotOrgMember:                            "membership_required",
		saml.ErrIdentityAmbiguous:                       "identity_conflict",
		saml.ErrEmailMissing:                            "malformed",
		refusal(saml.ReasonSignatureMissing):            "bad_signature",
		refusal(saml.ReasonSignatureAlgorithm):          "bad_signature",
		refusal(saml.ReasonSignatureReference):          "bad_signature",
		refusal(saml.ReasonSignaturePlacement):          "bad_signature",
		refusal(saml.ReasonSignatureInvalid):            "bad_signature",
		refusal(saml.ReasonAudience):                    "audience",
		refusal(saml.ReasonRecipient):                   "audience",
		refusal(saml.ReasonDestination):                 "audience",
		refusal(saml.ReasonIssuer):                      "issuer",
		refusal(saml.ReasonExpired):                     "expired",
		refusal(saml.ReasonIssueInstant):                "expired",
		refusal(saml.ReasonNotYetValid):                 "not_yet_valid",
		refusal(saml.ReasonInResponseTo):                "nonce_mismatch",
		refusal(saml.ReasonXMLDirective):                "malformed",
		refusal(saml.ReasonDuplicateID):                 "malformed",
		refusal(saml.ReasonStatus):                      "invalid_credentials",
		refusal(saml.ReasonXMLInvalid):                  "malformed",
		refusal(saml.ReasonAssertionCount):              "malformed",
		errors.New("database down"):                     "dependency_unavailable",
	} {
		got := saml.OutcomeReason(err)
		if got != want {
			t.Errorf("OutcomeReason(%v) = %q, want %q", err, got, want)
		}
		if !slices.Contains(contracts.AuthOutcomeReasonCodes, got) && !slices.Contains(sharedFederatedReasons, got) {
			t.Errorf("OutcomeReason(%v) = %q is outside the shared vocabulary", err, got)
		}
	}
}
