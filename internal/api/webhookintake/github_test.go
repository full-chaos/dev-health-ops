package webhookintake

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func sign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyGitHubSignature(t *testing.T) {
	body := []byte(`{"a":1}`)
	valid := sign("secret", string(body))
	cases := []struct {
		name   string
		header string
		secret string
		want   bool
	}{
		{"valid", valid, "secret", true},
		{"wrong secret", valid, "other", false},
		{"missing header", "", "secret", false},
		{"missing sha256= prefix", hex.EncodeToString([]byte("x")), "secret", false},
		{"tampered body signature", sign("secret", `{"a":2}`), "secret", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := verifyGitHubSignature(body, c.header, c.secret); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestMapGithubEvent(t *testing.T) {
	cases := []struct {
		event, action string
		want          eventType
	}{
		{"push", "", eventPush},
		{"issues", "opened", eventIssueCreated},
		{"issues", "closed", eventIssueClosed},
		{"issues", "deleted", eventIssueDeleted},
		{"issues", "edited", eventIssueUpdated},
		{"issue_comment", "", eventIssueUpdated},
		{"deployment_status", "", eventDeployment},
		{"check_suite", "", eventCheckRun},
		{"installation_repositories", "", eventInstallation},
		{"marketplace_purchase", "", eventMarketplacePurchase},
		{"ping", "", eventUnknown},
	}
	for _, c := range cases {
		if got := mapGithubEvent(c.event, c.action); got != c.want {
			t.Errorf("mapGithubEvent(%q,%q) = %q, want %q", c.event, c.action, got, c.want)
		}
	}
}
