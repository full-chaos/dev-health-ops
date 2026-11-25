package synccli

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
)

// The discovery error is redacted with the stored token and the email before it
// leaves resolveJiraStoredSettings. Every value is made up.
func TestRedactCredentialErrorHidesTheValues(t *testing.T) {
	const token, email = "zz9tok-synthetic-aaaaaaaa", "sync@example.test"
	failure := fmt.Errorf("resolving one failed: %w (credential %s, user %s, escaped %s, key %s)",
		atlassianteams.ErrOrganizationPermission, token, email, "sync%40example.test", "zz9-key=ab")
	got := redactCredentialError(failure, token, email, "zz9-key=ab")
	for _, secret := range []string{token, email, "sync%40example.test", "zz9-key=ab"} {
		if strings.Contains(got.Error(), secret) {
			t.Errorf("%q survives in %q", secret, got.Error())
		}
	}
	if !strings.Contains(got.Error(), "resolving one failed") {
		t.Errorf("the error lost its own text: %q", got.Error())
	}
	if !strings.Contains(got.Error(), atlassianteams.ErrOrganizationPermission.Error()) {
		t.Errorf("the permission text is gone: %q", got.Error())
	}
}

func TestRedactCredentialErrorWithNoValuesIsTheSameText(t *testing.T) {
	failure := errors.New("nothing secret here")
	if got := redactCredentialError(failure); got.Error() != failure.Error() {
		t.Errorf("got %q", got)
	}
}
