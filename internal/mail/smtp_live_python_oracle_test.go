package mail

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/smtpcapture"
)

// smtpOracleProgram drives the REAL Python mail path
// (src/dev_health_ops/api/services/email.py): EmailService over
// SmtpEmailProvider, the exact classes `send_invite_email` and every other
// Python mail caller use. Inputs arrive on stdin, so nothing reaches argv.
const smtpOracleProgram = `
import asyncio, json, sys
from dev_health_ops.api.services.email import EmailService, SmtpEmailProvider

spec = json.load(sys.stdin)
service = EmailService(SmtpEmailProvider(host=spec["host"], port=spec["port"]), spec["from"])

async def run():
    if spec.get("template"):
        await service.send_template_email(
            to_address=spec["to"], subject=spec["subject"],
            template_name=spec["template"], context=spec["context"])
    else:
        await service.send_email(
            to_address=spec["to"], subject=spec["subject"], html_content=spec["html"])

asyncio.run(run())
`

type smtpOracleCase struct {
	Name     string
	From     string
	To       string
	Subject  string
	HTML     string
	Template string
	Context  map[string]string
	// Refused marks a message BOTH planes must decline to send: Python raises
	// before any SMTP traffic, Go returns an error before dialing. Nothing may
	// reach the server either way.
	Refused bool
}

func runPythonSMTP(t *testing.T, interpreter, root string, host string, port int, c smtpOracleCase) error {
	t.Helper()
	spec := map[string]any{
		"host": host, "port": port, "from": c.From, "to": c.To, "subject": c.Subject,
	}
	if c.Template != "" {
		spec["template"] = c.Template
		spec["context"] = c.Context
	} else {
		spec["html"] = c.HTML
	}
	input, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(interpreter, "-c", smtpOracleProgram)
	command.Dir = root
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("python send: %w\n%s", err, output)
	}
	return nil
}

func runGoSMTP(t *testing.T, host string, port int, c smtpOracleCase, html string) error {
	t.Helper()
	for _, name := range []string{
		"EMAIL_API_KEY", "RESEND_API_KEY", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_USE_TLS",
		"SMTP_TLS_CA_FILE", "SMTP_TLS_SERVER_NAME",
	} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("EMAIL_PROVIDER", "smtp")
	t.Setenv("EMAIL_FROM_ADDRESS", c.From)
	t.Setenv("SMTP_HOST", host)
	t.Setenv("SMTP_PORT", strconv.Itoa(port))
	sender, err := NewSenderFromEnv(nil)
	if err != nil {
		t.Fatalf("NewSenderFromEnv: %v", err)
	}
	return sender.Send(context.Background(), Message{To: c.To, Subject: c.Subject, HTML: html})
}

// TestSMTPSenderMatchesLivePythonSMTPProvider is the cross-runtime proof for
// the wire format: for every case, the live Python SmtpEmailProvider and the
// Go SMTP sender deliver the same message to the same SMTP server, and the
// bytes on the wire (envelope + DATA, MIME boundary normalized) must match.
func TestSMTPSenderMatchesLivePythonSMTPProvider(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve mail package path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	interpreter := pyoracle.Resolve(t, root)

	const sender = "dev-health@example.com"
	const recipient = "owner@example.test"
	long := func(unit string, n int) string { return strings.Repeat(unit, n) }
	cases := []smtpOracleCase{
		// ---- body: encoding choice, wrapping, line endings ----
		{Name: "ascii html", From: sender, To: recipient, Subject: "Hello", HTML: "<p>Hi there</p>"},
		{Name: "empty body", From: sender, To: recipient, Subject: "Hello", HTML: ""},
		{Name: "non-ascii body", From: sender, To: recipient, Subject: "Hello", HTML: "<p>Caf\u00e9 \u2603 \u65e5\u672c\u8a9e</p>"},
		{Name: "non-ascii body, 57 bytes", From: sender, To: recipient, Subject: "Hello", HTML: "\u00e9" + long("x", 55)},
		{Name: "non-ascii body, 58 bytes", From: sender, To: recipient, Subject: "Hello", HTML: "\u00e9" + long("x", 56)},
		{Name: "non-ascii body, many lines", From: sender, To: recipient, Subject: "Hello", HTML: "<p>" + long("caf\u00e9 ", 200) + "</p>"},
		{Name: "non-ascii body with CRLF", From: sender, To: recipient, Subject: "Hello", HTML: "\u00e9\r\nline\rlast\n"},
		{Name: "4-byte emoji body", From: sender, To: recipient, Subject: "Hello", HTML: "<p>\U0001F600</p>"},
		{Name: "long body lines", From: sender, To: recipient, Subject: "Hello", HTML: "<p>" + long("word ", 400) + "</p>\n<p>tail</p>"},
		{Name: "ascii body, CRLF and lone CR", From: sender, To: recipient, Subject: "Hello", HTML: "<p>x</p>\r\nline\rlast\n"},
		{Name: "ascii body, trailing newlines", From: sender, To: recipient, Subject: "Hello", HTML: "<p>x</p>\n\n\n"},
		{Name: "ascii body, leading newline", From: sender, To: recipient, Subject: "Hello", HTML: "\n<p>x</p>"},
		{Name: "leading-dot body lines", From: sender, To: recipient, Subject: "Hello", HTML: ".first\n<p>x</p>\n.dotted line\n..double\n"},
		{Name: "body with NUL and control chars", From: sender, To: recipient, Subject: "Hello", HTML: "<p>a\x00b\x07c\td</p>"},
		{Name: "body containing a MIME-boundary lookalike", From: sender, To: recipient, Subject: "Hello", HTML: "<p>--===============1234567890123456789==</p>"},
		// ---- subject: verbatim vs encoded word, base64 vs quoted-printable ----
		{Name: "empty subject", From: sender, To: recipient, Subject: "", HTML: "<p>x</p>"},
		{Name: "non-ascii subject (base64 shorter)", From: sender, To: recipient, Subject: "Caf\u00e9 invite \u2603", HTML: "<p>x</p>"},
		{Name: "non-ascii subject (quoted-printable shorter)", From: sender, To: recipient, Subject: "Caf\u00e9 au lait invite", HTML: "<p>x</p>"},
		{Name: "non-ascii subject (exact tie)", From: sender, To: recipient, Subject: "abcdef\u00e9", HTML: "<p>x</p>"},
		{Name: "long ascii subject", From: sender, To: recipient, Subject: "You're invited to join " + long("A Very Long Organization Name ", 4), HTML: "<p>x</p>"},
		{Name: "long non-ascii subject", From: sender, To: recipient, Subject: "You're invited to join " + long("\u00c9cole Sup\u00e9rieure ", 12), HTML: "<p>x</p>"},
		{Name: "long mostly-ascii subject with one accent", From: sender, To: recipient, Subject: long("a", 120) + "\u00e9", HTML: "<p>x</p>"},
		{Name: "emoji subject", From: sender, To: recipient, Subject: "\U0001F389 You're invited", HTML: "<p>x</p>"},
		{Name: "ascii subject that looks like an encoded word", From: sender, To: recipient, Subject: "=?utf-8?q?already?=", HTML: "<p>x</p>"},
		{Name: "subject with tab, quotes and control chars", From: sender, To: recipient, Subject: "a\tb \"c\" \x00 d", HTML: "<p>x</p>"},
		{Name: "subject with trailing space", From: sender, To: recipient, Subject: "Hello ", HTML: "<p>x</p>"},
		// ---- addresses ----
		{Name: "display-name from", From: "Dev Health <dev-health@example.com>", To: recipient, Subject: "Hello", HTML: "<p>x</p>"},
		{Name: "quoted display-name from with a comma", From: "\"Health, Dev\" <dev-health@example.com>", To: recipient, Subject: "Hello", HTML: "<p>x</p>"},
		{Name: "non-ascii display-name from", From: "D\u00e9v Health <dev-health@example.com>", To: recipient, Subject: "Hello", HTML: "<p>x</p>"},
		{Name: "angle-bracketed bare from", From: "<dev-health@example.com>", To: recipient, Subject: "Hello", HTML: "<p>x</p>"},
		{Name: "mixed-case plus-address recipient", From: sender, To: "Owner+Tag@Example.TEST", Subject: "Hello", HTML: "<p>x</p>"},
		{Name: "display-name recipient", From: sender, To: "Owner Name <owner@example.test>", Subject: "Hello", HTML: "<p>x</p>"},
		// ---- templates ----
		{Name: "invite template", From: sender, To: "invitee@example.test", Subject: "You're invited to join Acme", Template: "invite",
			Context: map[string]string{"org_name": "Acme", "inviter_name": "Ada Lovelace", "accept_url": "http://localhost:3000/accept-invite?token=abc.def"}},
		{Name: "invite template, non-ascii and markup values", From: sender, To: "invitee@example.test", Subject: "You're invited to join \u00c9cole", Template: "invite",
			Context: map[string]string{"org_name": "\u00c9cole <b>&amp;</b>", "inviter_name": "Ren\u00e9 \"Rene\" O'Brien", "accept_url": "http://localhost:3000/accept-invite?token=a%2Fb.c"}},
		{Name: "invite template, brace-laden values", From: sender, To: "invitee@example.test", Subject: "You're invited to join {x}", Template: "invite",
			Context: map[string]string{"org_name": "{org_name}", "inviter_name": "}{", "accept_url": "{{}}"}},
		// ---- refusals: header injection ----
		{Name: "refused: newline in ascii subject", From: sender, To: recipient, Subject: "A\nBcc: evil@example.test", HTML: "<p>x</p>", Refused: true},
		{Name: "refused: bare newline in ascii subject", From: sender, To: recipient, Subject: "A\nB", HTML: "<p>x</p>", Refused: true},
		{Name: "refused: CRLF in ascii subject", From: sender, To: recipient, Subject: "A\r\nB", HTML: "<p>x</p>", Refused: true},
		{Name: "refused: newline in from", From: "a@example.test\nBcc: evil@example.test", To: recipient, Subject: "Hello", HTML: "<p>x</p>", Refused: true},
		{Name: "refused: non-ascii recipient address", From: sender, To: "j\u00f6rg@example.test", Subject: "Hello", HTML: "<p>x</p>", Refused: true},
		{Name: "refused: non-ascii recipient in a display-name form", From: sender, To: "Jorg <j\u00f6rg@example.test>", Subject: "Hello", HTML: "<p>x</p>", Refused: true},
		{Name: "refused: non-ascii sender address", From: "d\u00e9v@example.test", To: recipient, Subject: "Hello", HTML: "<p>x</p>", Refused: true},
		{Name: "refused: newline in to", From: sender, To: "owner@example.test\nBcc: evil@example.test", Subject: "Hello", HTML: "<p>x</p>", Refused: true},
	}
	names := make([]string, 0, len(cases))
	for _, c := range cases {
		names = append(names, c.Name)
	}
	sort.Strings(names)

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			server := smtpcapture.Start(t)
			host, port := server.HostPort(t)

			pythonErr := runPythonSMTP(t, interpreter, root, host, port, c)
			html := c.HTML
			if c.Template != "" {
				var err error
				html, err = RenderTemplate(c.Template, c.Context)
				if err != nil {
					t.Fatalf("RenderTemplate: %v", err)
				}
			}
			if c.Refused {
				if pythonErr == nil {
					t.Fatal("Python sent a message the case says must be refused")
				}
				server.AssertNone(t)
				if err := runGoSMTP(t, host, port, c, html); err == nil {
					t.Fatal("Go sent a message the case says must be refused")
				}
				server.AssertNone(t)
				return
			}
			if pythonErr != nil {
				t.Fatalf("%v", pythonErr)
			}
			python := smtpcapture.Normalize(server.Take(t))
			if err := runGoSMTP(t, host, port, c, html); err != nil {
				t.Fatalf("Go send: %v", err)
			}
			goMail := smtpcapture.Normalize(server.Take(t))

			if python.MailFrom != goMail.MailFrom {
				t.Errorf("MAIL FROM differs:\n python: %q\n go:     %q", python.MailFrom, goMail.MailFrom)
			}
			if fmt.Sprint(python.RcptTo) != fmt.Sprint(goMail.RcptTo) {
				t.Errorf("RCPT TO differs:\n python: %q\n go:     %q", python.RcptTo, goMail.RcptTo)
			}
			if python.Data != goMail.Data {
				t.Errorf("DATA differs:\n--- python ---\n%s\n--- go ---\n%s\n--- python (escaped) ---\n%q\n--- go (escaped) ---\n%q",
					python.Data, goMail.Data, python.Data, goMail.Data)
			}
		})
	}

	if err := os.WriteFile(filepath.Join(proofDir, "mail-smtp-oracle"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
