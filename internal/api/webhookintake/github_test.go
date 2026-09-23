package webhookintake

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
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

// TestHandleGitHubWebhookRejectsAMissingRequiredHeaderEvenWithAValidSignature
// pins the fix: Python resolves X-GitHub-Event/X-GitHub-Delivery as required
// Header() params ahead of the signature-checking body dependency, so a
// missing header 422s even when the signature is valid -- confirmed live
// against the real Python route.
func TestHandleGitHubWebhookRejectsAMissingRequiredHeaderEvenWithAValidSignature(t *testing.T) {
	body := []byte(`{"repository":{"full_name":"acme/repo"}}`)
	deps := Deps{Secrets: Secrets{GitHub: "s3cr3t"}}
	handler := deps.handleGitHubWebhook()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	req.Header.Set("X-Hub-Signature-256", sign("s3cr3t", string(body)))
	recorder := httptest.NewRecorder()
	handler(recorder, req)

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body = %s", recorder.Code, recorder.Body.String())
	}
	want := `{"detail":[{"type":"missing","loc":["header","x-github-event"],"msg":"Field required","input":null}]}`
	if got := recorder.Body.String(); got != want {
		t.Fatalf("body = %s, want %s", got, want)
	}
}
