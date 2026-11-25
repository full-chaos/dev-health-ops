package synccli

import (
	"errors"
	"strconv"
	"testing"
)

func TestBlankAppKeyIsNotAPlanBoundaryValue(t *testing.T) {
	const echo = "provider credential is invalid\tfor  acme"
	for _, key := range []string{" ", " \t\n"} {
		plan := Plan{Provider: "github", GitHub: &GitHubCredentials{Mode: CredentialApp, AppID: "1", PrivateKey: key, InstallationID: "2"}}
		if got := planBoundaryOf(plan).RedactText(echo); got != echo {
			t.Errorf("app key %s: got %q, want the text as it is", strconv.Quote(key), got)
		}
	}
	plan := Plan{Provider: "gitlab", GitLabToken: "  "}
	if got := planBoundaryOf(plan).RedactText(echo); got != echo {
		t.Errorf("blank gitlab token: got %q, want the text as it is", got)
	}
}

func TestBlankValueIsNotAJiraBoundaryValue(t *testing.T) {
	const echo = "gateway said  no\nfor acme"
	got := redactCredentialError(errors.New(echo), " ", " \t\n").Error()
	if got != echo {
		t.Errorf("got %q, want the text as it is", got)
	}
	if values := credentialSpellings(" ", " \t\n", "\n"); len(values) != 0 {
		t.Errorf("credentialSpellings of blank values = %q, want none", values)
	}
}

func TestRealKeyAndTokenAreStillPlanBoundaryValues(t *testing.T) {
	const key = "syn-key-body-1\nsyn-key-body-2\n"
	app := Plan{Provider: "github", GitHub: &GitHubCredentials{Mode: CredentialApp, AppID: "1", PrivateKey: key, InstallationID: "2"}}
	b := planBoundaryOf(app)
	if got := b.RedactText("echo " + key); got != "echo [REDACTED]" {
		t.Errorf("app key with its line feed: got %q", got)
	}
	if got := b.RedactText("echo syn-key-body-1\nsyn-key-body-2"); got != "echo [REDACTED]" {
		t.Errorf("app key without its final line feed: got %q", got)
	}
	pat := Plan{Provider: "github", GitHub: &GitHubCredentials{Mode: CredentialPAT, Token: "syn-token-abc"}}
	if got := planBoundaryOf(pat).RedactText("echo syn-token-abc"); got != "echo [REDACTED]" {
		t.Errorf("pat: got %q", got)
	}
	gitlab := Plan{Provider: "gitlab", GitLabToken: "syn-token-abc"}
	if got := planBoundaryOf(gitlab).RedactText("echo syn-token-abc"); got != "echo [REDACTED]" {
		t.Errorf("gitlab token: got %q", got)
	}
	if values := credentialSpellings("syn-token-abc", "syn-key\n"); len(values) != 3 {
		t.Errorf("credentialSpellings of real values = %q, want 3 spellings", values)
	}
}
