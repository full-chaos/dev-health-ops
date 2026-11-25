package synccli

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
)

// The discovery error is redacted with the stored token and the email before it
// leaves resolveJiraStoredSettings; the chain stays. Every value is made up.
func TestRedactCredentialErrorHidesTheValuesAndKeepsTheChain(t *testing.T) {
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
	if !errors.Is(got, atlassianteams.ErrOrganizationPermission) {
		t.Errorf("the chain is gone: %v", got)
	}
}

func TestRedactCredentialErrorWithNoValuesIsTheSameText(t *testing.T) {
	failure := errors.New("nothing secret here")
	if got := redactCredentialError(failure); got.Error() != failure.Error() || !errors.Is(got, failure) {
		t.Errorf("got %q", got)
	}
}

func basicPayloads(email, token string) []string {
	raw := []byte(email + ":" + token)
	return []string{
		base64.StdEncoding.EncodeToString(raw), base64.RawStdEncoding.EncodeToString(raw),
		base64.URLEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(raw),
	}
}

// A gateway that echoes the request's Authorization header carries base64(email:token).
func TestRedactCredentialErrorHidesTheBasicPayload(t *testing.T) {
	const token, email = "zz9tok-synthetic-aaaaaaaa", "sync@example.test"
	for _, payload := range basicPayloads(email, token) {
		failure := fmt.Errorf("refused: Authorization: Basic %s", payload)
		got := redactCredentialError(failure, basicCredentialValues(email, token)...).Error()
		if strings.Contains(got, payload) {
			t.Errorf("payload %q survives in %q", payload, got)
		}
	}
}

func TestSettingsBoundaryHidesThePayloadAndTheTokenOverriddenByTheEnvironment(t *testing.T) {
	const storedToken, storedEmail = "zz9tok-synthetic-aaaaaaaa", "stored@example.test"
	const overrideToken = "zz9tok-synthetic-bbbbbbbb"
	s := settings{email: storedEmail, token: storedToken}.withCredentialOverrides(envOverrides{token: overrideToken})
	if s.token != overrideToken || s.email != storedEmail {
		t.Fatalf("override not applied: %+v", s)
	}
	texts := []string{storedToken, overrideToken, storedEmail}
	texts = append(texts, basicPayloads(storedEmail, overrideToken)...)
	texts = append(texts, basicPayloads(storedEmail, storedToken)...)
	for _, text := range texts {
		if got := s.boundary().RedactText("search failed: " + text); strings.Contains(got, text) {
			t.Errorf("%q survives in %q", text, got)
		}
	}
}
