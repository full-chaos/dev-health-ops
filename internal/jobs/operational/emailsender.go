package operational

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptrace"
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

// AmbiguousSendError marks a Send failure where the message may already have
// reached the recipient's mail infrastructure despite the caller never
// learning that -- Resend accepting the HTTP request and then the response
// timing out, a Resend 5xx (which happens AFTER the request was received),
// or an SMTP connection lost after DATA's terminator was written but before
// the final reply came back. CHAOS-5399: the billing handler used to treat
// every Send error identically as "nothing was sent" and release its claim
// for a retry, which can duplicate an email that already went out. An
// AmbiguousSendError must never be handled that way -- see
// BillingHandler.deliver.
//
// ProviderMessageID carries the provider's own id when the sender obtained
// one before the ambiguity arose. It is usually empty: today neither sender
// gets an id on an inconclusive path (Resend only returns one in a fully
// decoded 2xx body, and SMTP has no concept of one at all), but the field
// exists so a provider that CAN report one on an ambiguous outcome has
// somewhere to put it, and so the failure-site log line's shape does not
// need to change if one later does.
type AmbiguousSendError struct {
	Err               error
	ProviderMessageID string
}

func (e *AmbiguousSendError) Error() string { return e.Err.Error() }
func (e *AmbiguousSendError) Unwrap() error { return e.Err }

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
		// CHAOS-5400: Go's STARTTLS verifies the server certificate by
		// default (Python's smtplib.starttls() with no context did not --
		// verify_mode=CERT_NONE). That is a deliberate, disclosed
		// tightening, and it stays ON unconditionally: this constructor
		// adds no insecure-skip-verify escape hatch. A relay on a
		// private/self-signed CA is instead trusted EXPLICITLY via
		// SMTP_TLS_CA_FILE (a PEM file of one or more CA certificates),
		// and SMTP_TLS_SERVER_NAME overrides the hostname used for
		// certificate verification when it differs from SMTP_HOST (e.g. a
		// container/service name vs. the cert's real CN/SAN). A bad
		// SMTP_TLS_CA_FILE is refused at startup, fail-closed, same
		// discipline as SMTP_PORT/SMTP_HOST above.
		// CHAOS-5400 r1 (codex P1): a whitespace-only value trims to "" via
		// configuredValue and was being treated as ABSENT (silent fallback
		// to the default, no error) -- inconsistent with this file's own
		// "set but empty is refused" discipline applied to
		// EMAIL_PROVIDER/EMAIL_FROM_ADDRESS/SMTP_HOST above. A misconfigured
		// (whitespace-typo'd) operator value must refuse loudly, not fall
		// back unconfigured.
		tlsServerName := host
		if override, set := configuredValue("SMTP_TLS_SERVER_NAME"); set {
			if override == "" {
				slog.Error("billing notification SMTP TLS server name is configured but empty",
					"variable", "SMTP_TLS_SERVER_NAME")
				return nil, errors.New("SMTP_TLS_SERVER_NAME is set but empty")
			}
			tlsServerName = override
		}
		var tlsConfig *tls.Config
		if useTLS {
			tlsConfig = &tls.Config{ServerName: tlsServerName, MinVersion: tls.VersionTLS12}
			if caFile, set := configuredValue("SMTP_TLS_CA_FILE"); set {
				if caFile == "" {
					slog.Error("billing notification SMTP TLS CA file is configured but empty",
						"variable", "SMTP_TLS_CA_FILE")
					return nil, errors.New("SMTP_TLS_CA_FILE is set but empty")
				}
				pool, err := loadSMTPTLSCAPool(caFile)
				if err != nil {
					slog.Error("billing notification SMTP TLS CA file is invalid",
						"variable", "SMTP_TLS_CA_FILE", "value", caFile, "error", err)
					return nil, fmt.Errorf("SMTP_TLS_CA_FILE is invalid: %w", err)
				}
				tlsConfig.RootCAs = pool
			}
		}
		return &smtpEmailSender{
			from:      from,
			host:      host,
			port:      port,
			username:  strings.TrimSpace(os.Getenv("SMTP_USERNAME")),
			password:  strings.TrimSpace(os.Getenv("SMTP_PASSWORD")),
			useTLS:    useTLS,
			tlsConfig: tlsConfig,
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

	// CHAOS-5399 r1 (codex P1): pattern-matching specific transport error
	// SHAPES (a *net.OpError with Op=="dial", a *net.DNSError) to decide
	// "never reached the network" missed TLS handshake failures entirely --
	// a peer closing mid-handshake surfaces as a bare io.EOF, which matches
	// neither pattern and was wrongly classified ambiguous even though no
	// HTTP request byte had been written yet. httptrace.ClientTrace's
	// WroteRequest hook is the actual, documented signal for "did the
	// request reach the point of being fully written to the connection" --
	// it fires synchronously as part of RoundTrip, so by the time Do()
	// returns, wroteRequest/wroteRequestErr reflect the FINAL attempt
	// (net/http retries once internally on a dead reused connection, which
	// would re-invoke this same hook before Do() returns).
	var wroteRequest bool
	var wroteRequestErr error
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			wroteRequest = true
			wroteRequestErr = info.Err
		},
	}
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))

	response, err := sender.client.Do(request)
	if err != nil {
		if !wroteRequest {
			// The request was never fully written to the wire at all -- a
			// DNS failure, a refused or timed-out dial, or (the case the
			// old net.OpError-based check missed) a TLS handshake that
			// never completed. No bytes reached Resend.
			return fmt.Errorf("resend API unreachable: %w", err)
		}
		if wroteRequestErr != nil {
			// The write itself failed partway through. Some bytes reached
			// the wire, but not necessarily a complete, parseable request --
			// unlike the "never written" case above, this leaves genuine
			// doubt rather than ruling the send out.
			return &AmbiguousSendError{
				Err: fmt.Errorf("resend API request write incomplete: %w", err),
			}
		}
		// The full request WAS written -- Resend had everything it needed to
		// act on it -- and only the response never came back (a timeout, a
		// reset while waiting, etc). CHAOS-5399: this must not be treated as
		// "nothing was sent".
		return &AmbiguousSendError{
			Err: fmt.Errorf("resend API response uncertain: %w", err),
		}
	}
	defer response.Body.Close()
	// Bounded read: an unbounded error body from a third party must not be
	// able to grow this worker's memory.
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	// Best-effort only: never let a read/parse failure on a body we don't
	// even need decide the outcome for a status code that already does.
	messageID := bestEffortResendMessageID(raw)

	// CHAOS-5399 r1 (codex P1): status code is checked BEFORE trusting a body
	// read failure. The previous ordering treated ANY readErr as ambiguous
	// regardless of status -- but a already-known rejection (4xx) needs
	// nothing from the body to classify; only the 2xx path below actually
	// depends on it (to rule out an embedded error object).
	if response.StatusCode >= 500 {
		// A 5xx is Resend's OWN infrastructure failing AFTER it accepted the
		// HTTP request; it does not rule out the message having been queued
		// or sent before that failure. Ambiguous, same reasoning as a
		// pre-response timeout, regardless of whether the body was readable.
		return &AmbiguousSendError{
			Err:               fmt.Errorf("resend API returned a server error: status %d", response.StatusCode),
			ProviderMessageID: messageID,
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// A 4xx (or any other non-5xx, non-2xx) is Resend explicitly
		// refusing the request -- bad auth, validation, rate limiting. This
		// is a clean, definite non-send: the status code alone is the whole
		// diagnosis, and a body-read failure on top of it changes nothing.
		// The body may echo the recipient or key material, so it is not
		// logged.
		return fmt.Errorf("resend API rejected the message: status %d", response.StatusCode)
	}
	// From here the status is 2xx, where the body is load-bearing: it is
	// what would reveal an embedded error object (Python checked for one
	// even on a 2xx, because some SDK versions returned one instead of
	// raising -- same check here).
	if readErr != nil {
		// The status line and headers arrived (2xx), so the request DID
		// reach Resend and it reported acceptance; only the body -- which
		// is what would confirm or rule out an embedded error object --
		// was lost. Ambiguous, not a rejection.
		return &AmbiguousSendError{
			Err:               fmt.Errorf("resend API response unreadable: %w", readErr),
			ProviderMessageID: messageID,
		}
	}
	var decoded struct {
		ID    string          `json:"id"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		// A 2xx status with an unparseable body: the transport-level send
		// succeeded, but the body cannot be trusted. Ambiguous, not a clean
		// success.
		return &AmbiguousSendError{Err: fmt.Errorf("resend API response invalid: %w", err)}
	}
	if len(decoded.Error) > 0 && string(decoded.Error) != "null" {
		// A 2xx wrapping an explicit error object IS a definite rejection --
		// Resend told us, unambiguously, that it did not send.
		return errors.New("resend API returned an error object")
	}
	slog.InfoContext(ctx, "billing notification: resend accepted the message",
		"message_id", decoded.ID, "subject", message.Subject)
	return nil
}

// bestEffortResendMessageID extracts an "id" field from a raw Resend
// response body, ignoring any decode failure. It exists only to enrich an
// ambiguous-outcome log line with a provider id when one happens to be
// present -- it is never used to decide whether a send succeeded.
func bestEffortResendMessageID(raw []byte) string {
	var partial struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &partial); err != nil {
		return ""
	}
	return partial.ID
}

func (sender *resendEmailSender) endpoint() string {
	if override := strings.TrimSpace(os.Getenv("RESEND_API_BASE_URL")); override != "" {
		return strings.TrimRight(override, "/") + "/emails"
	}
	return "https://api.resend.com/emails"
}

// loadSMTPTLSCAPool reads a PEM file at path and returns a cert pool seeded
// with the host's system roots PLUS that file's certificates, so
// SMTP_TLS_CA_FILE ADDS trust for a private/self-signed relay CA rather than
// replacing the system trust store wholesale. A missing file, an unreadable
// file, or a file with no parseable PEM certificate is an error -- never a
// silent empty pool, which would make verification vacuous.
func loadSMTPTLSCAPool(path string) (*x509.CertPool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(raw) {
		return nil, errors.New("no PEM certificates found")
	}
	return pool, nil
}

type smtpEmailSender struct {
	from      string
	host      string
	port      int
	username  string
	password  string
	useTLS    bool
	tlsConfig *tls.Config // built at construction; nil is fine when useTLS is false
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
		// CHAOS-5400: verification stays ON (matching Python's
		// smtplib.starttls() being upgraded, not weakened) -- sender.tlsConfig
		// is built once at construction (NewEmailSenderFromEnv), honoring
		// SMTP_TLS_CA_FILE/SMTP_TLS_SERVER_NAME when set. A direct struct
		// literal with useTLS set but no tlsConfig still verifies against
		// the system roots and sender.host, the same default this file
		// always had -- never an unauthenticated fallback.
		tlsConfig := sender.tlsConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{ServerName: sender.host, MinVersion: tls.VersionTLS12}
		}
		if err := client.StartTLS(tlsConfig); err != nil {
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
		// Unlike a failed Close below, a failed Write means the terminating
		// "." was never sent at all -- an SMTP server does not commit a DATA
		// transaction it never saw the end of, so a connection lost mid-body
		// leaves nothing for the server to have accepted. Definite non-send.
		return fmt.Errorf("smtp message write failed: %w", err)
	}
	if err := writer.Close(); err != nil {
		// Closing the DATA writer sends the terminating "." and then reads
		// the server's final reply via textproto.Reader.ReadResponse.
		//
		// CHAOS-5399 r1 (codex P1): a *textproto.Error here means the reply
		// WAS received and it was an explicit rejection (e.g. "550
		// rejected") -- a definite, stated non-send, no different from
		// Mail/Rcpt/Data being rejected earlier. It is every OTHER error
		// shape (io.EOF, io.ErrUnexpectedEOF, a network read failure or
		// timeout) that means the reply was never seen at all: the
		// terminator may already be on the wire, and the server may commit
		// a message the instant it reads it, before a dropped connection
		// or a client-side read timeout ever lets the "250 OK" back
		// (correcting this comment's previous claim that ANY Close failure
		// meant the message was "NOT accepted" -- that conflated the two
		// cases).
		var rejection *textproto.Error
		if errors.As(err, &rejection) {
			return fmt.Errorf("smtp message was rejected: %w", err)
		}
		return &AmbiguousSendError{
			Err: fmt.Errorf("smtp response to the message was not received: %w", err),
		}
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

	// CHAOS-5401: Python's MIMEText(html, "html") picks its body encoding
	// from an auto-detected charset -- us-ascii (7bit-safe, sent as-is) when
	// every character fits, utf-8 (BASE64 body encoding, always, for that
	// charset) the moment it doesn't. A body that is not 7-bit-safe must be
	// base64-encoded here too, not sent raw under Content-Transfer-Encoding:
	// 8bit -- a relay that never advertised 8BITMIME could reject it.
	partHeaders := textproto.MIMEHeader{}
	partHeaders.Set("Content-Type", `text/html; charset="utf-8"`)
	htmlBytes := []byte(message.HTML)
	if is7BitSafe(message.HTML) {
		partHeaders.Set("Content-Transfer-Encoding", "8bit")
	} else {
		partHeaders.Set("Content-Transfer-Encoding", "base64")
		htmlBytes = base64MIMEBody(htmlBytes)
	}
	part, err := writer.CreatePart(partHeaders)
	if err != nil {
		return nil, fmt.Errorf("smtp message construction failed: %w", err)
	}
	if _, err := part.Write(htmlBytes); err != nil {
		return nil, fmt.Errorf("smtp message construction failed: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("smtp message construction failed: %w", err)
	}
	out.Write(buffer.Bytes())
	return out.Bytes(), nil
}

// is7BitSafe reports whether every byte of s is a 7-bit US-ASCII octet
// (< 0x80), matching the check Python's email package effectively applies
// when it picks between an ASCII-safe charset and utf-8 for MIMEText's body
// encoding (CHAOS-5401).
func is7BitSafe(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7F {
			return false
		}
	}
	return true
}

// base64MIMEBody base64-encodes data and hard-wraps it at 76 characters per
// line, CRLF-terminated -- RFC 2045 §6.8's line-length limit for the base64
// Content-Transfer-Encoding, matching Python email.base64mime's output
// shape (a single unwrapped line is technically non-conformant and some
// relays reject or mangle it).
func base64MIMEBody(data []byte) []byte {
	const lineLength = 76
	encoded := base64.StdEncoding.EncodeToString(data)
	var out bytes.Buffer
	for i := 0; i < len(encoded); i += lineLength {
		end := i + lineLength
		if end > len(encoded) {
			end = len(encoded)
		}
		out.WriteString(encoded[i:end])
		out.WriteString("\r\n")
	}
	return out.Bytes()
}
