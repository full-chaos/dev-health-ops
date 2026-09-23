package webhookintake

import "testing"

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
