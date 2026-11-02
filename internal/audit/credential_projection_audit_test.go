package audit

import "testing"

// The credential projection is a deliberate off-stream mutation: it writes
// state and an audit row and republishes nothing (an external_source principal
// holds no session, so no revision consumer needs an event).
func TestCredentialProjectionIsInTheClosedOffStreamSet(t *testing.T) {
	if !offStreamReasons[OffStreamCredentialProjection] {
		t.Fatal("OffStreamCredentialProjection is not a permitted off-stream reason")
	}
	if OffStreamCredentialProjection != "credential_projection" {
		t.Fatalf("OffStreamCredentialProjection = %q", OffStreamCredentialProjection)
	}
	if offStreamReasons["credential_projection_x"] || offStreamReasons[""] {
		t.Fatal("the off-stream set is no longer closed")
	}
}

func TestProjectionReasonCodesAreAllowedAttributeValues(t *testing.T) {
	for _, code := range []string{"domain_revoked", "owner_request", "compromised", "credential_issued", "credential_rotated", "expired", "denied_policy"} {
		if err := ValidateAttributes(map[string]any{"reason_code": code}); err != nil {
			t.Errorf("reason_code %q: %v", code, err)
		}
	}
	for _, key := range []string{"credential_class", "principal_id", "revision", "new_status", "target_kind"} {
		if _, ok := allowedAttributeKeys[key]; !ok {
			t.Errorf("attribute key %q is not allowed", key)
		}
	}
}
