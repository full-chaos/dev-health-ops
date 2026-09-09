package operational

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// isAmbiguous reports whether err is (or wraps) an *AmbiguousSendError.
func isAmbiguous(err error) (*AmbiguousSendError, bool) {
	var ambiguous *AmbiguousSendError
	return ambiguous, errors.As(err, &ambiguous)
}

// ---------------------------------------------------------------------------
// CHAOS-5399: resend outcome classification.
// ---------------------------------------------------------------------------

func TestResendSenderClassifiesHTTPStatusOutcomes(t *testing.T) {
	for _, test := range []struct {
		name          string
		status        int
		body          string
		wantErr       bool
		wantAmbiguous bool
	}{
		{"2xx with no error object is a clean send", http.StatusOK, `{"id":"em_1"}`, false, false},
		{"2xx with an explicit error object is a clean rejection",
			http.StatusOK, `{"error":{"message":"invalid recipient"}}`, true, false},
		{"2xx with an unparseable body is ambiguous",
			http.StatusOK, `not json`, true, true},
		{"4xx is a clean rejection", http.StatusUnprocessableEntity, `{"message":"bad request"}`, true, false},
		{"429 is a clean rejection, not ambiguous", http.StatusTooManyRequests, `{}`, true, false},
		{"500 is ambiguous", http.StatusInternalServerError, `{}`, true, true},
		{"503 is ambiguous", http.StatusServiceUnavailable, `{}`, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(test.status)
					_, _ = w.Write([]byte(test.body))
				}))
			defer server.Close()
			t.Setenv("RESEND_API_BASE_URL", server.URL)

			sender := &resendEmailSender{
				from: "billing@example.test", apiKey: "k",
				client: &http.Client{Timeout: 5 * time.Second},
			}
			err := sender.Send(context.Background(), EmailMessage{
				To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
			})
			if !test.wantErr {
				if err != nil {
					t.Fatalf("Send() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Send() = nil, want an error")
			}
			_, ambiguous := isAmbiguous(err)
			if ambiguous != test.wantAmbiguous {
				t.Fatalf("Send() ambiguous = %v, want %v (err: %v)", ambiguous, test.wantAmbiguous, err)
			}
		})
	}
}

// TestResendSenderTreatsAResponseTimeoutAsAmbiguous is the concrete CHAOS-5399
// repro: the connection is established and the request is fully written (the
// server reads it), but the response never comes back before the caller's
// context deadline. This must NOT be classified as "nothing was sent".
func TestResendSenderTreatsAResponseTimeoutAsAmbiguous(t *testing.T) {
	received := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			close(received)
			// Outlast the client's timeout below (so Send() genuinely times
			// out waiting for a response) but still return, so the test
			// server's Close() does not block forever on a hung handler.
			time.Sleep(300 * time.Millisecond)
		}))
	defer server.Close()
	t.Setenv("RESEND_API_BASE_URL", server.URL)

	sender := &resendEmailSender{
		from: "billing@example.test", apiKey: "k",
		client: &http.Client{Timeout: 50 * time.Millisecond},
	}
	err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("the server never saw the request; this test proves nothing about a post-send timeout")
	}
	if err == nil {
		t.Fatal("Send() = nil, want a timeout error")
	}
	if _, ambiguous := isAmbiguous(err); !ambiguous {
		t.Fatalf("Send() = %v, want an *AmbiguousSendError -- the request reached the "+
			"server before the timeout, so this must not be treated as a clean non-send", err)
	}
}

// TestResendSenderTreatsAConnectionRefusalAsDefinitelyNotSent is the negative
// control: a dial failure means NO bytes reached the network, which is the
// one class of transport error that stays a clean non-send.
func TestResendSenderTreatsAConnectionRefusalAsDefinitelyNotSent(t *testing.T) {
	// Bind to get a genuinely free port, then close it immediately so the
	// connection is refused deterministically -- no reliance on a
	// well-known unused port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RESEND_API_BASE_URL", "http://"+addr)

	sender := &resendEmailSender{
		from: "billing@example.test", apiKey: "k",
		client: &http.Client{Timeout: 2 * time.Second},
	}
	sendErr := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if sendErr == nil {
		t.Fatal("Send() = nil, want a connection-refused error")
	}
	if _, ambiguous := isAmbiguous(sendErr); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; a refused connection never "+
			"reached the network and must be a clean non-send", sendErr)
	}
}

// ---------------------------------------------------------------------------
// CHAOS-5399: SMTP outcome classification, via a minimal hand-rolled SMTP
// server. net/smtp has no test double in this codebase; the protocol is
// simple enough that driving it directly over a raw net.Listener is more
// reliable than reaching for a third-party fake.
// ---------------------------------------------------------------------------

// fakeSMTPServer is a bare-bones, single-connection SMTP server. behavior
// controls what happens once the client finishes writing the DATA content
// (the terminating "." line has been read): "ack" replies 250 as usual,
// "hangup" closes the connection with NO reply at all -- exactly "accepted
// DATA, then the connection was lost before the acknowledgement" -- and
// "reject" replies with an explicit SMTP error code, the server's definite,
// stated answer rather than a lost one.
type fakeSMTPServer struct {
	listener net.Listener
	behavior string           // "ack", "hangup", or "reject"
	tlsCert  *tls.Certificate // non-nil enables STARTTLS (CHAOS-5400)
}

func newFakeSMTPServer(t *testing.T, behavior string) *fakeSMTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeSMTPServer{listener: listener, behavior: behavior}
	go server.serveOne(t)
	return server
}

// newFakeSTARTTLSSMTPServer is newFakeSMTPServer plus a STARTTLS extension
// backed by cert -- CHAOS-5400: proves smtpEmailSender's STARTTLS handshake
// against a real certificate chain, not a stubbed-out tls.Config.
func newFakeSTARTTLSSMTPServer(t *testing.T, behavior string, cert tls.Certificate) *fakeSMTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeSMTPServer{listener: listener, behavior: behavior, tlsCert: &cert}
	go server.serveOne(t)
	return server
}

func (server *fakeSMTPServer) addr() string { return server.listener.Addr().String() }

func (server *fakeSMTPServer) serveOne(t *testing.T) {
	conn, err := server.listener.Accept()
	if err != nil {
		return // listener closed by test cleanup
	}
	defer conn.Close()
	server.serveConn(t, conn, true)
}

// serveConn drives the SMTP dialogue over conn. greet controls whether the
// "220" service-ready banner is sent first -- true for the initial plaintext
// connection, false for the recursive call serving the SAME dialogue over
// the upgraded TLS conn after STARTTLS, where the client goes straight to a
// fresh EHLO without expecting a second banner.
func (server *fakeSMTPServer) serveConn(t *testing.T, conn net.Conn, greet bool) {
	reader := bufio.NewReader(conn)
	reply := func(line string) {
		if _, err := conn.Write([]byte(line + "\r\n")); err != nil {
			t.Logf("fake smtp server write failed: %v", err)
		}
	}
	if greet {
		reply("220 fake.smtp.test ESMTP")
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		verb := strings.ToUpper(fields[0])
		switch verb {
		case "EHLO", "HELO":
			if server.tlsCert != nil && greet {
				reply("250-fake.smtp.test")
				reply("250 STARTTLS")
			} else {
				reply("250 fake.smtp.test")
			}
		case "STARTTLS":
			if server.tlsCert == nil {
				reply("500 STARTTLS not supported")
				continue
			}
			reply("220 Ready to start TLS")
			tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{*server.tlsCert}})
			if err := tlsConn.Handshake(); err != nil {
				t.Logf("fake smtp server TLS handshake failed: %v", err)
				return
			}
			// Continue the SAME dialogue over the encrypted conn -- the
			// client re-issues EHLO next, with no second "220" banner.
			server.serveConn(t, tlsConn, false)
			return
		case "MAIL":
			reply("250 OK")
		case "RCPT":
			reply("250 OK")
		case "DATA":
			reply("354 Start mail input; end with <CRLF>.<CRLF>")
			for {
				dataLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if dataLine == ".\r\n" {
					break
				}
			}
			switch server.behavior {
			case "hangup":
				// The message content (including its terminator) was fully
				// received -- then the connection is dropped with no reply
				// at all, simulating a lost connection right after the
				// server committed the message.
				return
			case "reject":
				// The server DID reply -- its definite, stated answer is a
				// refusal, not a lost acknowledgement.
				reply("550 rejected after DATA")
			default:
				reply("250 OK: queued")
			}
		case "QUIT":
			reply("221 Bye")
			return
		default:
			reply("500 unrecognized command")
		}
	}
}

func TestSMTPSenderTreatsALostConnectionAfterDataAsAmbiguous(t *testing.T) {
	server := newFakeSMTPServer(t, "hangup")
	defer server.listener.Close()
	host, port := splitHostPort(t, server.addr())

	sender := &smtpEmailSender{from: "billing@example.test", host: host, port: port}
	err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if err == nil {
		t.Fatal("Send() = nil, want an error -- the final SMTP reply was never received")
	}
	if _, ambiguous := isAmbiguous(err); !ambiguous {
		t.Fatalf("Send() = %v, want an *AmbiguousSendError -- the server read the full "+
			"message before the connection was lost, so this is not a clean non-send", err)
	}
}

func TestSMTPSenderTreatsAnOrdinaryAcknowledgementAsSent(t *testing.T) {
	server := newFakeSMTPServer(t, "ack")
	defer server.listener.Close()
	host, port := splitHostPort(t, server.addr())

	sender := &smtpEmailSender{from: "billing@example.test", host: host, port: port}
	if err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	}); err != nil {
		t.Fatalf("Send() = %v, want nil", err)
	}
}

func TestSMTPSenderTreatsAConnectionRefusalAsDefinitelyNotSent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host, port := splitHostPort(t, listener.Addr().String())
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	sender := &smtpEmailSender{from: "billing@example.test", host: host, port: port}
	sendErr := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if sendErr == nil {
		t.Fatal("Send() = nil, want a connection-refused error")
	}
	if _, ambiguous := isAmbiguous(sendErr); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; a refused connection never "+
			"reached the server and must be a clean non-send", sendErr)
	}
}

func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatal(err)
	}
	return host, port
}

// ---------------------------------------------------------------------------
// CHAOS-5400: STARTTLS certificate verification stays ON; an operator gets
// an explicit lever (SMTP_TLS_CA_FILE) to trust a private/self-signed relay
// CA instead of an insecure-skip-verify escape hatch.
// ---------------------------------------------------------------------------

// generateSelfSignedSMTPCert builds a self-signed leaf certificate for host,
// valid now, and returns both the tls.Certificate (for the fake server to
// present) and its PEM encoding (for a test to hand to a caller as the CA to
// trust -- self-signed means the leaf IS its own issuer).
func generateSelfSignedSMTPCert(t *testing.T, host string) (tls.Certificate, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	// ServerName verification checks IP SANs for an IP literal host and DNS
	// SANs otherwise -- the fake server binds 127.0.0.1, so the cert needs an
	// IP SAN, not (only) a DNSNames entry, or verification fails on a SAN
	// mismatch regardless of trust.
	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("build tls.Certificate: %v", err)
	}
	return cert, certPEM
}

// writeCAFile writes pemBytes to a scratch file under t.TempDir() and
// returns its path, for SMTP_TLS_CA_FILE.
func writeCAFile(t *testing.T, pemBytes []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return path
}

// smtpEnv sets exactly the env vars NewEmailSenderFromEnv reads for
// EMAIL_PROVIDER=smtp, isolating each test from whatever the ambient
// process environment happens to hold.
func smtpEnv(t *testing.T, host string, port int, extra map[string]string) {
	t.Helper()
	t.Setenv("EMAIL_PROVIDER", "smtp")
	t.Setenv("SMTP_HOST", host)
	t.Setenv("SMTP_PORT", fmt.Sprintf("%d", port))
	// Explicitly unset every other SMTP_* var NewEmailSenderFromEnv reads --
	// isolates each test from whatever the ambient process environment
	// happens to hold, not just from earlier t.Setenv calls in this same
	// test binary (those already self-revert via t.Cleanup).
	for _, name := range []string{
		"SMTP_USE_TLS", "SMTP_TLS_CA_FILE", "SMTP_TLS_SERVER_NAME", "SMTP_USERNAME", "SMTP_PASSWORD",
	} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}
	for k, v := range extra {
		t.Setenv(k, v)
	}
}

// TestSMTPSenderRejectsSelfSignedCertificateWithoutConfiguredCA is the
// negative control: with SMTP_USE_TLS on and NO SMTP_TLS_CA_FILE, a relay
// presenting a self-signed certificate must still be refused. Proves the
// CHAOS-5400 fix is an explicit trust lever, never a blanket
// insecure-skip-verify fallback.
func TestSMTPSenderRejectsSelfSignedCertificateWithoutConfiguredCA(t *testing.T) {
	cert, _ := generateSelfSignedSMTPCert(t, "127.0.0.1")
	server := newFakeSTARTTLSSMTPServer(t, "ack", cert)
	defer server.listener.Close()
	host, port := splitHostPort(t, server.addr())

	smtpEnv(t, host, port, map[string]string{"SMTP_USE_TLS": "true"})
	sender, err := NewEmailSenderFromEnv(nil)
	if err != nil {
		t.Fatalf("NewEmailSenderFromEnv: %v", err)
	}
	sendErr := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if sendErr == nil {
		t.Fatal("Send() = nil, want a certificate-verification error -- an untrusted self-signed cert must never be accepted")
	}
}

// TestSMTPSenderTrustsExplicitlyConfiguredCA is CHAOS-5400's proof: before
// this fix, an operator running against a private/self-signed relay had NO
// way to make STARTTLS succeed short of an insecure-skip-verify escape hatch
// this fix deliberately never adds. SMTP_TLS_CA_FILE lets the relay's own CA
// be trusted explicitly, end to end through NewEmailSenderFromEnv -- not by
// injecting a struct field directly.
func TestSMTPSenderTrustsExplicitlyConfiguredCA(t *testing.T) {
	cert, certPEM := generateSelfSignedSMTPCert(t, "127.0.0.1")
	server := newFakeSTARTTLSSMTPServer(t, "ack", cert)
	defer server.listener.Close()
	host, port := splitHostPort(t, server.addr())
	caFile := writeCAFile(t, certPEM)

	smtpEnv(t, host, port, map[string]string{
		"SMTP_USE_TLS":     "true",
		"SMTP_TLS_CA_FILE": caFile,
	})
	sender, err := NewEmailSenderFromEnv(nil)
	if err != nil {
		t.Fatalf("NewEmailSenderFromEnv: %v", err)
	}
	if err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	}); err != nil {
		t.Fatalf("Send() = %v, want nil -- SMTP_TLS_CA_FILE explicitly trusts this relay's CA", err)
	}
}

// TestSMTPSenderRefusesAnUnreadableCAFile: a startup-time configuration
// fault (a bad SMTP_TLS_CA_FILE) must fail loudly and closed, the same
// "parity of names, not every value" discipline the rest of this
// constructor already applies to SMTP_PORT/SMTP_HOST -- never silently
// fall back to an unauthenticated or system-only trust store.
func TestSMTPSenderRefusesAnUnreadableCAFile(t *testing.T) {
	smtpEnv(t, "127.0.0.1", 1025, map[string]string{
		"SMTP_USE_TLS":     "true",
		"SMTP_TLS_CA_FILE": filepath.Join(t.TempDir(), "does-not-exist.pem"),
	})
	if _, err := NewEmailSenderFromEnv(nil); err == nil {
		t.Fatal("NewEmailSenderFromEnv() = nil error, want a refusal for an unreadable SMTP_TLS_CA_FILE")
	}
}

// TestSMTPSenderRefusesSetButEmptyTLSConfig is CHAOS-5400 r1's P1 fix: a
// SMTP_TLS_CA_FILE/SMTP_TLS_SERVER_NAME value that is SET but
// whitespace-only trims to "" and was being silently treated as ABSENT
// (falling back to the default, no error) -- inconsistent with this file's
// own established "set but empty is refused" discipline for
// EMAIL_PROVIDER/EMAIL_FROM_ADDRESS/SMTP_HOST above. A misconfigured
// operator (a typo'd whitespace value) must see a startup refusal, not
// silent fallback to unconfigured defaults.
func TestSMTPSenderRefusesSetButEmptyTLSConfig(t *testing.T) {
	for _, test := range []struct {
		name    string
		extra   map[string]string
		wantErr string
	}{
		{"empty CA file", map[string]string{"SMTP_USE_TLS": "true", "SMTP_TLS_CA_FILE": "   "}, "SMTP_TLS_CA_FILE"},
		{"empty server name", map[string]string{"SMTP_USE_TLS": "true", "SMTP_TLS_SERVER_NAME": "   "}, "SMTP_TLS_SERVER_NAME"},
	} {
		t.Run(test.name, func(t *testing.T) {
			smtpEnv(t, "127.0.0.1", 1025, test.extra)
			_, err := NewEmailSenderFromEnv(nil)
			if err == nil {
				t.Fatalf("NewEmailSenderFromEnv() = nil error, want a refusal naming %s", test.wantErr)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("NewEmailSenderFromEnv() error = %v, want it to name %s", err, test.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CHAOS-5401: MIME transfer-encoding matches Python's charset-driven choice
// -- base64 for a body that is not 7-bit-safe, unchanged (8bit) for one that
// already is.
// ---------------------------------------------------------------------------

func TestSMTPComposeChoosesTransferEncodingBySevenBitSafety(t *testing.T) {
	for _, test := range []struct {
		name    string
		html    string
		wantCTE string
	}{
		{"pure ASCII body keeps 8bit", "<p>hello world</p>", "8bit"},
		{"non-ASCII body uses base64", "<p>café costs €3 ☃</p>", "base64"},
		{"single non-ASCII byte at the very start", "é" + strings.Repeat("x", 40), "base64"},
		{"single non-ASCII byte at the very end", strings.Repeat("x", 40) + "é", "base64"},
	} {
		t.Run(test.name, func(t *testing.T) {
			sender := &smtpEmailSender{from: "billing@example.test"}
			raw, err := sender.compose(EmailMessage{To: "owner@example.test", Subject: "s", HTML: test.html})
			if err != nil {
				t.Fatalf("compose: %v", err)
			}
			partHeader, body := parseComposedSinglePart(t, raw)
			if got := partHeader.Get("Content-Transfer-Encoding"); got != test.wantCTE {
				t.Fatalf("Content-Transfer-Encoding = %q, want %q", got, test.wantCTE)
			}
			decoded := body
			if test.wantCTE == "base64" {
				clean := strings.ReplaceAll(strings.ReplaceAll(string(body), "\r", ""), "\n", "")
				decoded, err = base64.StdEncoding.DecodeString(clean)
				if err != nil {
					t.Fatalf("decode base64 body: %v", err)
				}
			}
			if string(decoded) != test.html {
				t.Fatalf("decoded body = %q, want %q", decoded, test.html)
			}
		})
	}
}

// TestSMTPComposeBase64WrapsAt76CharsAndDecodesIdentically is CHAOS-5401's
// large-body case: RFC 2045 §6.8 requires base64 lines no longer than 76
// characters -- a body big enough to need multiple lines must wrap at
// exactly 76, every line CRLF-terminated, and decode back byte-identical.
func TestSMTPComposeBase64WrapsAt76CharsAndDecodesIdentically(t *testing.T) {
	html := "<p>" + strings.Repeat("café ", 200) + "</p>" // well over 76 base64 chars per line
	sender := &smtpEmailSender{from: "billing@example.test"}
	raw, err := sender.compose(EmailMessage{To: "owner@example.test", Subject: "s", HTML: html})
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	partHeader, body := parseComposedSinglePart(t, raw)
	if got := partHeader.Get("Content-Transfer-Encoding"); got != "base64" {
		t.Fatalf("Content-Transfer-Encoding = %q, want base64", got)
	}
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "\r\n") {
		t.Fatal("base64 body has no CRLF line breaks at all -- not actually wrapped")
	}
	lines := strings.Split(strings.TrimSuffix(bodyStr, "\r\n"), "\r\n")
	for i, line := range lines {
		if i < len(lines)-1 && len(line) != 76 {
			t.Fatalf("line %d length = %d, want exactly 76 (only the last line may be shorter)", i, len(line))
		}
		if len(line) > 76 {
			t.Fatalf("line %d length = %d, exceeds RFC 2045's 76-char limit", i, len(line))
		}
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(bodyStr, "\r\n", ""))
	if err != nil {
		t.Fatalf("decode base64 body: %v", err)
	}
	if string(decoded) != html {
		t.Fatal("decoded large body does not match the original byte-for-byte")
	}
}

// parseComposedSinglePart parses raw (compose()'s output: hand-built RFC822
// headers followed by a multipart/alternative body with exactly one part)
// and returns that part's own header block plus its raw, undecoded body
// bytes -- multipart.Reader does not auto-decode base64, so a test can
// assert on the wire encoding directly.
func parseComposedSinglePart(t *testing.T, raw []byte) (textproto.MIMEHeader, []byte) {
	t.Helper()
	tp := textproto.NewReader(bufio.NewReader(bytes.NewReader(raw)))
	topHeader, err := tp.ReadMIMEHeader()
	if err != nil {
		t.Fatalf("read top-level headers: %v", err)
	}
	_, params, err := mime.ParseMediaType(topHeader.Get("Content-Type"))
	if err != nil {
		t.Fatalf("parse Content-Type: %v", err)
	}
	mr := multipart.NewReader(tp.R, params["boundary"])
	part, err := mr.NextPart()
	if err != nil {
		t.Fatalf("read part: %v", err)
	}
	body, err := io.ReadAll(part)
	if err != nil {
		t.Fatalf("read part body: %v", err)
	}
	return part.Header, body
}
