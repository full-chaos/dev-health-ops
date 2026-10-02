package mail

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the Resend API key to another origin (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	t.Setenv("EMAIL_PROVIDER", "resend")
	t.Setenv("EMAIL_API_KEY", "SECRET")
	t.Setenv("RESEND_API_BASE_URL", probe.Base.URL)
	t.Setenv("EMAIL_FROM", "from@example.test")
	sender, err := NewSenderFromEnv(probe.Client())
	if err != nil {
		t.Fatal(err)
	}
	_ = sender.Send(context.Background(), Message{To: "to@example.test", Subject: "s", HTML: "<p>x</p>"})
	probe.Assert(t)
}

// No client supplied (the nil branch of NewSenderFromEnv) follows no redirect either.
func TestTheDefaultClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	t.Setenv("EMAIL_PROVIDER", "resend")
	t.Setenv("EMAIL_API_KEY", "SECRET")
	t.Setenv("RESEND_API_BASE_URL", probe.Base.URL)
	t.Setenv("EMAIL_FROM", "from@example.test")
	sender, err := NewSenderFromEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = sender.Send(context.Background(), Message{To: "to@example.test", Subject: "s", HTML: "<p>x</p>"})
	probe.Assert(t)
}
