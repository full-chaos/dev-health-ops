package webhookintake

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestVerifyJiraSignature(t *testing.T) {
	body := []byte(`{"webhookEvent":"jira:issue_created"}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write(body)
	digest := hex.EncodeToString(mac.Sum(nil))

	cases := []struct {
		name, header string
		want         bool
	}{
		{"bare hex digest", digest, true},
		{"sha256= prefixed digest", "sha256=" + digest, true},
		{"wrong digest", "0000", false},
		{"missing header", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := verifyJiraSignature(body, c.header, "secret"); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestMapJiraEvent(t *testing.T) {
	cases := []struct {
		event string
		want  eventType
	}{
		{"jira:issue_created", eventIssueCreated},
		{"jira:issue_updated", eventIssueUpdated},
		{"jira:issue_deleted", eventIssueDeleted},
		{"jira:worklog_updated", eventUnknown},
	}
	for _, c := range cases {
		if got := mapJiraEvent(c.event); got != c.want {
			t.Errorf("mapJiraEvent(%q) = %q, want %q", c.event, got, c.want)
		}
	}
}
