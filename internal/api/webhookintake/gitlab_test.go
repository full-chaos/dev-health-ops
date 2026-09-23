package webhookintake

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVerifyGitLabToken(t *testing.T) {
	cases := []struct {
		name, header, secret string
		want                 bool
	}{
		{"matching", "s3cr3t", "s3cr3t", true},
		{"mismatched", "wrong", "s3cr3t", false},
		{"empty header", "", "s3cr3t", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := verifyGitLabToken(c.header, c.secret); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestMapGitlabEvent(t *testing.T) {
	cases := []struct {
		event, action string
		want          eventType
	}{
		{"Push Hook", "", eventPush},
		{"Tag Push Hook", "", eventPush},
		{"Merge Request Hook", "", eventMergeRequest},
		{"Issue Hook", "open", eventIssueCreated},
		{"Issue Hook", "close", eventIssueClosed},
		{"Issue Hook", "update", eventIssueUpdated},
		{"Pipeline Hook", "", eventPipeline},
		{"Deployment Hook", "", eventDeployment},
		{"Job Hook", "", eventUnknown},
	}
	for _, c := range cases {
		if got := mapGitlabEvent(c.event, c.action); got != c.want {
			t.Errorf("mapGitlabEvent(%q,%q) = %q, want %q", c.event, c.action, got, c.want)
		}
	}
}

// TestHandleGitLabWebhookRejectsAMissingRequiredHeaderEvenWithAValidToken
// mirrors the GitHub case: Python resolves X-Gitlab-Event as a required
// Header() param ahead of the token-checking body dependency, so a missing
// header 422s even when the token is valid.
func TestHandleGitLabWebhookRejectsAMissingRequiredHeaderEvenWithAValidToken(t *testing.T) {
	body := []byte(`{"object_kind":"push"}`)
	deps := Deps{Secrets: Secrets{GitLab: "s3cr3t"}}
	handler := deps.handleGitLabWebhook()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/gitlab", bytes.NewReader(body))
	req.Header.Set("X-Gitlab-Token", "s3cr3t")
	recorder := httptest.NewRecorder()
	handler(recorder, req)

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body = %s", recorder.Code, recorder.Body.String())
	}
	want := `{"detail":[{"type":"missing","loc":["header","x-gitlab-event"],"msg":"Field required","input":null}]}`
	if got := recorder.Body.String(); got != want {
		t.Fatalf("body = %s, want %s", got, want)
	}
}
