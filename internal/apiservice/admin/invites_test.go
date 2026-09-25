package admin

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/smtpcapture"
)

func TestInviteBaseURL(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  InviteConfig
		want string
	}{
		{"unset takes the default", InviteConfig{}, "http://localhost:3000"},
		{"trailing slashes are stripped", InviteConfig{AppBaseURL: "https://app.example.test///", AppBaseURLSet: true}, "https://app.example.test"},
		// os.getenv returns "" for a set-but-empty variable, which is NOT the
		// default: the link then starts at the path.
		{"set but empty stays empty", InviteConfig{AppBaseURL: "", AppBaseURLSet: true}, ""},
		{"a value given without the set flag is ignored", InviteConfig{AppBaseURL: "https://ignored.test"}, "http://localhost:3000"},
	} {
		if got := tc.cfg.BaseURL(); got != tc.want {
			t.Errorf("%s: baseURL = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestInviterNameChain(t *testing.T) {
	s := func(v string) *string { return &v }
	for _, tc := range []struct {
		name            string
		full, email     *string
		claim, expected string
	}{
		{"full name wins", s("Ada L"), s("ada@x.test"), "claim@x.test", "Ada L"},
		{"empty full name falls to email", s(""), s("ada@x.test"), "claim@x.test", "ada@x.test"},
		{"no full name falls to email", nil, s("ada@x.test"), "claim@x.test", "ada@x.test"},
		{"no user row falls to the token's email", nil, nil, "claim@x.test", "claim@x.test"},
		{"empty email falls to the token's email", nil, s(""), "claim@x.test", "claim@x.test"},
	} {
		if got := inviterName(tc.full, tc.email, tc.claim); got != tc.expected {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.expected)
		}
	}
}

func TestSendInviteEmailDeliversTheInviteMessage(t *testing.T) {
	sink := smtpcapture.Start(t)
	host, port := sink.HostPort(t)
	t.Setenv("EMAIL_PROVIDER", "smtp")
	t.Setenv("EMAIL_FROM_ADDRESS", "Dev Health <invites@example.test>")
	t.Setenv("SMTP_HOST", host)
	t.Setenv("SMTP_PORT", strconv.Itoa(port))
	for _, name := range []string{"EMAIL_API_KEY", "RESEND_API_KEY", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_USE_TLS", "SMTP_TLS_CA_FILE", "SMTP_TLS_SERVER_NAME"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	sender, err := mail.NewSenderFromEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	h := &handlers{
		logger:  slog.New(slog.NewTextHandler(&logs, nil)),
		invites: InviteConfig{Mail: sender, AppBaseURL: "https://app.example.test/", AppBaseURLSet: true},
	}
	invite := &orgInvite{ID: uuid.New(), OrgID: uuid.New(), Email: "invitee@example.test"}
	h.sendInviteEmail(context.Background(), "Acme", "Ada", invite, "tok.en")
	if logs.Len() != 0 {
		t.Fatalf("the send logged a failure: %s", logs.String())
	}
	got := sink.Take(t)
	for _, want := range []string{"Subject: You're invited to join Acme", "To: invitee@example.test", "https://app.example.test/accept-invite?token=tok.en", "Ada"} {
		if !strings.Contains(got.Data, want) {
			t.Errorf("message lacks %q:\n%s", want, got.Data)
		}
	}
}
