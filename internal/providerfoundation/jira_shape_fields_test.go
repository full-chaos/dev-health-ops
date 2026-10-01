package providerfoundation

import (
	"errors"
	"strings"
	"testing"
)

// CHAOS-7132: a stored Jira credential the client builder refuses used to read "provider credential
// is invalid" and nothing else, so an operator could not tell which of the three required fields
// (API token, email, base URL) was absent. The refusal still is ErrCredentialInvalid (errors.Is) and
// now names the missing FIELDS (names only, never a value).
func TestNewJiraClientRefusalNamesTheMissingFields(t *testing.T) {
	t.Parallel()
	const tokenValue, emailValue, urlValue = "tok-SECRET-123", "someone-SECRET@example.test", "https://secret-site.example.test"
	full := map[string]string{"api_token": tokenValue, "email": emailValue, "url": urlValue}
	tests := []struct {
		name    string
		drop    []string
		missing []string
	}{
		{"no token", []string{"api_token"}, []string{"api_token"}},
		{"no email", []string{"email"}, []string{"email"}},
		{"no base url", []string{"url"}, []string{"base_url"}},
		{"nothing but an empty payload", []string{"api_token", "email", "url"}, []string{"api_token", "email", "base_url"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			values := map[string]string{}
			for key, value := range full {
				values[key] = value
			}
			for _, key := range test.drop {
				delete(values, key)
			}
			_, err := NewJiraClient(testCredential("jira", values), &headerCaptureDoer{}, jiraTestRetry(), jiraTestLease())
			if !errors.Is(err, ErrCredentialInvalid) {
				t.Fatalf("err = %v, want it to wrap ErrCredentialInvalid", err)
			}
			text := err.Error()
			for _, name := range test.missing {
				if !strings.Contains(text, name) {
					t.Errorf("refusal %q does not name the missing field %q", text, name)
				}
			}
			for _, present := range []string{"api_token", "email", "base_url"} {
				named := strings.Contains(text, present)
				wanted := false
				for _, name := range test.missing {
					wanted = wanted || name == present
				}
				if named && !wanted {
					t.Errorf("refusal %q names %q, which is present", text, present)
				}
			}
			for _, value := range []string{tokenValue, emailValue, urlValue} {
				if strings.Contains(text, value) {
					t.Fatalf("refusal %q leaks a credential value", text)
				}
			}
		})
	}
}
