package secrets

import (
	"reflect"
	"testing"
)

// The list that lets a short-value warning name an undeclared credential key is
// closed: widening it is a reviewed edit of this test.
func TestKnownNonSecretCredentialFieldsAreClosed(t *testing.T) {
	if want := []string{"project_key"}; !reflect.DeepEqual(knownNonSecretCredentialFields, want) {
		t.Fatalf("knownNonSecretCredentialFields = %q, want %q", knownNonSecretCredentialFields, want)
	}
	for _, known := range knownNonSecretCredentialFields {
		if !isSecretField(known) {
			t.Errorf("%q is never registered, so the entry is dead", known)
		}
		if declared, ok := declaredCredentialField(known); ok {
			t.Errorf("%q is the declared field %q: one list or the other", known, declared)
		}
	}
}
