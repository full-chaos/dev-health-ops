package operational

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/smtp"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"time"
)

// CHAOS-5353: the email transport `dev_health_ops.api.services.email` provided
// to the Python billing path, ported to Go under the SAME environment variable
// names -- EMAIL_PROVIDER, EMAIL_FROM_ADDRESS, EMAIL_API_KEY/RESEND_API_KEY,
// SMTP_HOST/PORT/USERNAME/PASSWORD/USE_TLS. This is deliberate parity, not a
// new abstraction.
//
// Parity of NAMES, not of every accepted VALUE. Two deliberate tightenings,
// both fail-closed, both called out in the PR body rather than left implied:
// a variable that is SET BUT EMPTY is refused instead of silently taking its
// default (see configuredValue), and SMTP_PORT outside 1-65535 is refused at
// startup where Python's int() accepted 0, -1 and 65536 and stored them
// unvalidated. Any configuration that was VALID under Python keeps working
// unchanged; configurations that were silently broken now fail loudly.
//
// The Python service itself SURVIVES --
// verification, invite, welcome and password-reset mail still go through it --
// so this file is an additional consumer of the same configuration, not a
// replacement for it.

// ErrEmailProviderUnsupported is a configuration fault, not a per-message one:
// the process is set up to send mail it cannot send.
var ErrEmailProviderUnsupported = errors.New("operational email provider is unsupported")

// EmailMessage is one outbound message.
type EmailMessage struct {
	To      string
	Subject string
	HTML    string
}

// EmailSender delivers one rendered message. Implementations are chosen by
// EMAIL_PROVIDER at construction, never per message.
type EmailSender interface {
	// Name is the provider label used in logs.
	Name() string
	Send(ctx context.Context, message EmailMessage) error
}

// NewEmailSenderFromEnv mirrors Python's `get_email_service()` selection:
// EMAIL_PROVIDER in {console, resend, smtp}, default console; an unknown value
// is an error rather than a silent fallback.
// configuredValue distinguishes an ABSENT variable from an explicitly EMPTY
// one. That distinction is the whole point: `os.Getenv` collapses them, so a
// blank Helm/compose expansion silently took the default. For EMAIL_PROVIDER
// Python did NOT collapse them -- `os.getenv("EMAIL_PROVIDER", "console")`
// preserves "" and falls through to its unsupported-provider raise -- so
// collapsing here turned a loud misconfiguration into billing mail delivered
// to a log line while the durable row was marked completed (CHAOS-5353 r1).
//
// Returns the trimmed value and whether the variable was set at all.
func configuredValue(name string) (string, bool) {
	raw, present := os.LookupEnv(name)
	return strings.TrimSpace(raw), present
}

func NewEmailSenderFromEnv(client *http.Client) (EmailSender, error) {
	provider, providerSet := configuredValue("EMAIL_PROVIDER")
	if !providerSet {
		provider = "console"
	}
	provider = strings.ToLower(provider)
	if provider == "" {
		// Set-but-empty. Python raised here; so do we, rather than sending
		// every billing email to a logger.
		slog.Error("billing notification email provider is configured but empty",
			"variable", "EMAIL_PROVIDER")
		return nil, fmt.Errorf("%w: EMAIL_PROVIDER is set but empty", ErrEmailProviderUnsupported)
	}
	from, fromSet := configuredValue("EMAIL_FROM_ADDRESS")
	if !fromSet {
		from = "dev-health@example.com"
	}
	if from == "" {
		// Python left this empty and sent with a blank From. Refusing is
		// deliberately STRICTER: a blank envelope sender is rejected or
		// silently dropped by most relays, which is the same invisible
		// mail loss in a different place.
		slog.Error("billing notification from-address is configured but empty",
			"variable", "EMAIL_FROM_ADDRESS")
		return nil, errors.New("EMAIL_FROM_ADDRESS is set but empty")
	}
	switch provider {
	case "console":
		return &consoleEmailSender{from: from}, nil
	case "resend":
		// Python accepted either name, preferring EMAIL_API_KEY.
		key := strings.TrimSpace(os.Getenv("EMAIL_API_KEY"))
		if key == "" {
			key = strings.TrimSpace(os.Getenv("RESEND_API_KEY"))
		}
		if key == "" {
			slog.Error("billing notification resend API key is missing or empty",
				"variables", "EMAIL_API_KEY,RESEND_API_KEY")
			return nil, errors.New(
				"EMAIL_API_KEY (or RESEND_API_KEY) is required when EMAIL_PROVIDER=resend")
		}
		if client == nil {
			client = &http.Client{Timeout: 30 * time.Second}
		}
		return &resendEmailSender{from: from, apiKey: key, client: client}, nil
	case "smtp":
		port := 1025
		if raw, set := configuredValue("SMTP_PORT"); set {
			// Python's int(os.getenv("SMTP_PORT", "1025")) raised ValueError
			// on a set-but-empty value; so does this.
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed <= 0 || parsed > 65535 {
				slog.Error("billing notification SMTP port is not a valid port",
					"variable", "SMTP_PORT")
				return nil, fmt.Errorf("SMTP_PORT is not a valid port: %q", raw)
			}
			port = parsed
		}
		host, hostSet := configuredValue("SMTP_HOST")
		if !hostSet {
			host = "localhost"
		}
		if host == "" {
			// Python kept "" and failed later, at connect time, once per
			// notification. Refusing at startup is stricter and fails closed.
			slog.Error("billing notification SMTP host is configured but empty",
				"variable", "SMTP_HOST")
			return nil, errors.New("SMTP_HOST is set but empty")
		}
		useTLS := false
		switch strings.ToLower(strings.TrimSpace(os.Getenv("SMTP_USE_TLS"))) {
		case "true", "1", "yes":
			useTLS = true
		}
		return &smtpEmailSender{
			from:     from,
			host:     host,
			port:     port,
			username: strings.TrimSpace(os.Getenv("SMTP_USERNAME")),
			password: strings.TrimSpace(os.Getenv("SMTP_PASSWORD")),
			useTLS:   useTLS,
		}, nil
	default:
		slog.Error("billing notification email provider is unsupported",
			"variable", "EMAIL_PROVIDER", "value", provider)
		return nil, fmt.Errorf("%w: %q", ErrEmailProviderUnsupported, provider)
	}
}

type consoleEmailSender struct{ from string }

func (sender *consoleEmailSender) Name() string { return "console" }

func (sender *consoleEmailSender) Send(ctx context.Context, message EmailMessage) error {
	slog.InfoContext(ctx, "billing notification: console email provider",
		"from", sender.from,
		"to", message.To,
		"subject", message.Subject,
		"html_bytes", len(message.HTML),
	)
	return nil
}

type resendEmailSender struct {
	from   string
	apiKey string
	client *http.Client
}

func (sender *resendEmailSender) Name() string { return "resend" }

func (sender *resendEmailSender) Send(ctx context.Context, message EmailMessage) error {
	body, err := json.Marshal(map[string]any{
		"from":    sender.from,
		"to":      []string{message.To},
		"subject": message.Subject,
		"html":    message.HTML,
	})
	if err != nil {
		return fmt.Errorf("resend request encoding failed: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, sender.endpoint(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("resend request construction failed: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+sender.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := sender.client.Do(request)
	if err != nil {
		return fmt.Errorf("resend API unreachable: %w", err)
	}
	defer response.Body.Close()
	// Bounded read: an unbounded error body from a third party must not be
	// able to grow this worker's memory.
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if readErr != nil {
		return fmt.Errorf("resend API response unreadable: %w", readErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// The status code is the whole diagnosis and carries no recipient or
		// key material; the body may echo either, so it is not logged.
		return fmt.Errorf("resend API rejected the message: status %d", response.StatusCode)
	}
	// Python checked for an error object even on a 2xx, because some SDK
	// versions returned one instead of raising. Same check here.
	var decoded struct {
		ID    string          `json:"id"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return fmt.Errorf("resend API response invalid: %w", err)
	}
	if len(decoded.Error) > 0 && string(decoded.Error) != "null" {
		return errors.New("resend API returned an error object")
	}
	slog.InfoContext(ctx, "billing notification: resend accepted the message",
		"message_id", decoded.ID, "subject", message.Subject)
	return nil
}

func (sender *resendEmailSender) endpoint() string {
	if override := strings.TrimSpace(os.Getenv("RESEND_API_BASE_URL")); override != "" {
		return strings.TrimRight(override, "/") + "/emails"
	}
	return "https://api.resend.com/emails"
}

type smtpEmailSender struct {
	from     string
	host     string
	port     int
	username string
	password string
	useTLS   bool
}

func (sender *smtpEmailSender) Name() string { return "smtp" }

func (sender *smtpEmailSender) Send(ctx context.Context, message EmailMessage) error {
	payload, err := sender.compose(message)
	if err != nil {
		return err
	}
	address := net.JoinHostPort(sender.host, strconv.Itoa(sender.port))
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("smtp server unreachable: %w", err)
	}
	client, err := smtp.NewClient(conn, sender.host)
	if err != nil {
		// The dialled connection is not owned by a client yet, so it is this
		// function's to close -- otherwise every failure here leaks a socket.
		if closeErr := conn.Close(); closeErr != nil {
			slog.WarnContext(ctx, "billing notification: smtp connection close failed",
				"error", closeErr)
		}
		return fmt.Errorf("smtp handshake failed: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			slog.WarnContext(ctx, "billing notification: smtp client close failed",
				"error", closeErr)
		}
	}()
	if sender.useTLS {
		// Default verification (matching Python's smtplib.starttls() with no
		// custom context): the server certificate must chain and match.
		if err := client.StartTLS(&tls.Config{ServerName: sender.host, MinVersion: tls.VersionTLS12}); err != nil {
			return fmt.Errorf("smtp STARTTLS failed: %w", err)
		}
	}
	if sender.username != "" && sender.password != "" {
		auth := smtp.PlainAuth("", sender.username, sender.password, sender.host)
		if err := client.Auth(auth); err != nil {
			// Never include the credential values, only that auth failed.
			return fmt.Errorf("smtp authentication failed: %w", err)
		}
	}
	if err := client.Mail(sender.from); err != nil {
		return fmt.Errorf("smtp MAIL FROM rejected: %w", err)
	}
	if err := client.Rcpt(message.To); err != nil {
		return fmt.Errorf("smtp RCPT TO rejected: %w", err)
	}
	writer, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp DATA rejected: %w", err)
	}
	if _, err := writer.Write(payload); err != nil {
		if closeErr := writer.Close(); closeErr != nil {
			slog.WarnContext(ctx, "billing notification: smtp data close failed after a write error",
				"error", closeErr)
		}
		return fmt.Errorf("smtp message write failed: %w", err)
	}
	if err := writer.Close(); err != nil {
		// Closing the DATA writer is what commits the message; a failure here
		// means it was NOT accepted and must not be reported as sent.
		return fmt.Errorf("smtp message was not accepted: %w", err)
	}
	if err := client.Quit(); err != nil {
		// The message is already committed by the successful DATA close
		// above, so a QUIT failure is a teardown nuisance, not a send
		// failure -- logging it keeps it visible without duplicating mail.
		slog.WarnContext(ctx, "billing notification: smtp QUIT failed after the message was accepted",
			"error", err)
	}
	return nil
}

// compose builds the same multipart/alternative message Python's
// MIMEMultipart("alternative") produced: a single HTML part, since the billing
// path never supplied text_content.
func (sender *smtpEmailSender) compose(message EmailMessage) ([]byte, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	headers := []string{
		"From: " + sender.from,
		"To: " + message.To,
		// RFC 2047 encoding keeps non-ASCII subjects (they exist: org and
		// tier names reach the subject line) legible instead of mangled.
		"Subject: " + mime.QEncoding.Encode("utf-8", message.Subject),
		"MIME-Version: 1.0",
		`Content-Type: multipart/alternative; boundary="` + writer.Boundary() + `"`,
	}
	var out bytes.Buffer
	out.WriteString(strings.Join(headers, "\r\n"))
	out.WriteString("\r\n\r\n")

	partHeaders := textproto.MIMEHeader{}
	partHeaders.Set("Content-Type", `text/html; charset="utf-8"`)
	partHeaders.Set("Content-Transfer-Encoding", "8bit")
	part, err := writer.CreatePart(partHeaders)
	if err != nil {
		return nil, fmt.Errorf("smtp message construction failed: %w", err)
	}
	if _, err := part.Write([]byte(message.HTML)); err != nil {
		return nil, fmt.Errorf("smtp message construction failed: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("smtp message construction failed: %w", err)
	}
	out.Write(buffer.Bytes())
	return out.Bytes(), nil
}
