package mail

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// Sender-level behavior pins that lived in the billing handler's test file
// while the transport was billing-private (CHAOS-5353/5399/5400): provider
// selection from the environment, empty-vs-absent refusal, and the SMTP /
// Resend ambiguity classification. They moved with the code -- the handler
// tests keep only handler-level behavior against a fake Sender.

func TestEmailSenderSelectionFollowsTheExistingEnvironmentNames(t *testing.T) {
	for _, test := range []struct {
		name     string
		env      map[string]string
		wantName string
		wantErr  bool
	}{
		{"defaults to console", map[string]string{}, "console", false},
		{"console is explicit", map[string]string{"EMAIL_PROVIDER": "console"}, "console", false},
		{"case and spacing are tolerated",
			map[string]string{"EMAIL_PROVIDER": "  SMTP  "}, "smtp", false},
		{"smtp", map[string]string{"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mailpit"}, "smtp", false},
		{"resend via EMAIL_API_KEY",
			map[string]string{"EMAIL_PROVIDER": "resend", "EMAIL_API_KEY": "k"}, "resend", false},
		{"resend via RESEND_API_KEY",
			map[string]string{"EMAIL_PROVIDER": "resend", "RESEND_API_KEY": "k"}, "resend", false},
		{"resend without a key is refused",
			map[string]string{"EMAIL_PROVIDER": "resend"}, "", true},
		{"an unknown provider is refused, never silently defaulted",
			map[string]string{"EMAIL_PROVIDER": "sendgrid"}, "", true},
		{"a bad SMTP_PORT is refused",
			map[string]string{"EMAIL_PROVIDER": "smtp", "SMTP_PORT": "not-a-port"}, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// UNSET every variable first so a value leaking in from the
			// developer's own shell cannot decide the outcome. It must be
			// unset, not blanked: since the CHAOS-5353 r1 fix, blank means
			// "configured to nothing" and is a refusal, which is exactly what
			// TestEmptyEnvironmentValuesAreRefusedNotDefaulted pins. t.Setenv
			// first so the test framework restores the original on cleanup,
			// then Unsetenv to reach the genuinely-absent state.
			for _, name := range []string{
				"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "EMAIL_API_KEY", "RESEND_API_KEY",
				"SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_USE_TLS",
			} {
				t.Setenv(name, "")
				if err := os.Unsetenv(name); err != nil {
					t.Fatal(err)
				}
			}
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			sender, err := NewSenderFromEnv(&http.Client{Timeout: time.Second})
			if test.wantErr {
				if err == nil {
					t.Fatalf("want an error, got provider %q", sender.Name())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if sender.Name() != test.wantName {
				t.Fatalf("provider = %q, want %q", sender.Name(), test.wantName)
			}
		})
	}
}

func TestSmtpUseTLSAcceptsThePythonTruthyValues(t *testing.T) {
	for value, want := range map[string]bool{
		"true": true, "TRUE": true, "1": true, "yes": true, " Yes ": true,
		"false": false, "0": false, "no": false, "": false, "on": false,
	} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("EMAIL_PROVIDER", "smtp")
			t.Setenv("SMTP_USE_TLS", value)
			sender, err := NewSenderFromEnv(nil)
			if err != nil {
				t.Fatal(err)
			}
			smtpSender, ok := sender.(*smtpSender)
			if !ok {
				t.Fatalf("provider = %T", sender)
			}
			if smtpSender.useTLS != want {
				t.Fatalf("SMTP_USE_TLS=%q gave useTLS=%v, want %v", value, smtpSender.useTLS, want)
			}
		})
	}
}

// TestEmptyEnvironmentValuesAreRefusedNotDefaulted is the r1 P1 pin for
// empty-vs-absent. At the fix parent every one of these silently took a
// default -- EMAIL_PROVIDER="" became the console sender, which logs instead
// of sending while the handler marks the notification delivered.
func TestEmptyEnvironmentValuesAreRefusedNotDefaulted(t *testing.T) {
	for _, test := range []struct {
		name string
		env  map[string]string
	}{
		{"empty EMAIL_PROVIDER", map[string]string{"EMAIL_PROVIDER": ""}},
		{"empty EMAIL_FROM_ADDRESS", map[string]string{
			"EMAIL_PROVIDER": "console", "EMAIL_FROM_ADDRESS": ""}},
		{"empty SMTP_HOST", map[string]string{
			"EMAIL_PROVIDER": "smtp", "SMTP_HOST": ""}},
		{"empty SMTP_PORT", map[string]string{
			"EMAIL_PROVIDER": "smtp", "SMTP_PORT": ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			sender, err := NewSenderFromEnv(nil)
			if err == nil {
				t.Fatalf("a configured-but-empty value was silently defaulted to %q; "+
					"billing mail would go nowhere while rows are marked delivered",
					sender.Name())
			}
		})
	}
}

// TestAbsentEnvironmentValuesStillTakeTheirDefaults is the counterpart: the
// fix must reject EMPTY without breaking ABSENT, which is the ordinary case.
func TestAbsentEnvironmentValuesStillTakeTheirDefaults(t *testing.T) {
	// t.Setenv FIRST so the framework registers a cleanup that restores the
	// caller's original value, THEN Unsetenv to reach the genuinely-absent
	// state. Unsetenv alone would leak: it discards whatever the caller had
	// and never puts it back, so a later test in the same binary would see an
	// environment this one silently emptied.
	for _, name := range []string{
		"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "SMTP_HOST", "SMTP_PORT",
	} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	sender, err := NewSenderFromEnv(nil)
	if err != nil {
		t.Fatalf("all variables absent must still yield the console default: %v", err)
	}
	if sender.Name() != "console" {
		t.Fatalf("provider = %q, want console", sender.Name())
	}
}

// TestSMTPExplicitRejectionAfterDataIsNotAmbiguous is the r1 P1 pin for
// sender.go: an explicit SMTP rejection reply (e.g. "550 rejected")
// received after DATA's terminator is a DEFINITE, stated non-send -- no
// different from Mail/Rcpt/Data being rejected earlier -- and must release
// the claim for a real retry, not stall it behind FenceOutcomeAmbiguous.
func TestSMTPExplicitRejectionAfterDataIsNotAmbiguous(t *testing.T) {
	server := newFakeSMTPServer(t, "reject")
	defer server.listener.Close()
	host, port := splitHostPort(t, server.addr())

	sender := &smtpSender{from: "billing@example.test", host: host, port: port}
	err := sender.Send(context.Background(), Message{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if err == nil {
		t.Fatal("Send() = nil, want the explicit rejection error")
	}
	if _, ambiguous := isAmbiguous(err); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; an explicit SMTP rejection reply "+
			"IS the server's definite answer, not a lost acknowledgement", err)
	}
}

// TestResendKnownRejectionSurvivesABodyReadFailure is the r1 P1 pin: the
// status code alone is the whole diagnosis for a non-2xx response. A body
// read failure on TOP of an already-known 4xx must not promote it to
// ambiguous -- the body was never needed to classify a definite rejection in
// the first place.
func TestResendKnownRejectionSurvivesABodyReadFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte("{")) // truncated: body read will fail
		}))
	defer server.Close()
	t.Setenv("RESEND_API_BASE_URL", server.URL)

	sender := &resendSender{
		from: "billing@example.test", apiKey: "k",
		client: &http.Client{Timeout: 5 * time.Second},
	}
	err := sender.Send(context.Background(), Message{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if err == nil {
		t.Fatal("Send() = nil, want the 422 rejection")
	}
	if _, ambiguous := isAmbiguous(err); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; a KNOWN 4xx needs nothing from "+
			"the body to classify, so a body read failure on top of it changes nothing", err)
	}
}

// TestResendTLSHandshakeFailureIsNotAmbiguous is the r1 P1 pin: a TLS
// handshake failure happens before the HTTP request is ever written, so no
// bytes reached the server -- a clean non-send, same as a refused dial. The
// old net.OpError/net.DNSError pattern-matching missed this shape entirely
// (a certificate failure surfaces as neither).
func TestResendTLSHandshakeFailureIsNotAmbiguous(t *testing.T) {
	// httptest.NewTLSServer's certificate is not trusted by a default
	// http.Client, so the handshake fails deterministically before any HTTP
	// request can be written.
	server := httptest.NewTLSServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("the handler must never run -- the handshake should fail first")
		}))
	defer server.Close()
	t.Setenv("RESEND_API_BASE_URL", server.URL)

	sender := &resendSender{
		from: "billing@example.test", apiKey: "k",
		client: &http.Client{Timeout: 5 * time.Second},
	}
	err := sender.Send(context.Background(), Message{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if err == nil {
		t.Fatal("Send() = nil, want a TLS handshake failure")
	}
	if _, ambiguous := isAmbiguous(err); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; a TLS handshake failure happens "+
			"before any HTTP request byte is written, so this is a clean non-send", err)
	}
}
