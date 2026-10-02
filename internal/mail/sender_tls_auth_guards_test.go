package mail

// CHAOS-7906: the STARTTLS, auth and CA-pool guards of smtpSender.Send / loadSMTPTLSCAPool, each pinned by a test that fails when the guard is
// switched off. The server is a recording fake: it keeps every command verb it received, so a test can say what the relay SAW (no MAIL FROM and no
// AUTH after a refused STARTTLS), not only what Send returned.

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// recordingSMTPServer serves ONE connection. Options choose what the relay advertises and answers; commands() is every verb it received, in order.
type recordingSMTPServer struct {
	listener net.Listener

	advertiseSTARTTLS bool
	startTLSReply     string           // "" = upgrade for real (needs cert); else this single-line reply and no upgrade
	cert              *tls.Certificate // presented on a real upgrade
	advertiseAuth     bool
	authReply         string // reply to AUTH, e.g. "235 ok" or "535 refused"

	maxTLSVersion uint16 // 0 = the library default; else the highest version the relay speaks on STARTTLS

	mu        sync.Mutex
	verbs     []string
	authLines []string
	conn      net.Conn
	done      chan struct{}
	upgraded  bool
}

func newRecordingSMTPServer(t *testing.T, configure func(*recordingSMTPServer)) *recordingSMTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &recordingSMTPServer{listener: listener, authReply: "235 2.7.0 accepted", done: make(chan struct{})}
	if configure != nil {
		configure(server)
	}
	go server.serveOne()
	t.Cleanup(func() {
		_ = server.listener.Close()
		server.mu.Lock()
		if server.conn != nil {
			_ = server.conn.Close()
		}
		server.mu.Unlock()
		<-server.done
	})
	return server
}

func (server *recordingSMTPServer) addr() (string, int) {
	host, port, _ := net.SplitHostPort(server.listener.Addr().String())
	var number int
	for _, digit := range port {
		number = number*10 + int(digit-'0')
	}
	return host, number
}

// commands is the verbs received so far, once the relay's connection has ended (the test calls it after Send returned and closed the connection).
func (server *recordingSMTPServer) commands(t *testing.T) []string {
	t.Helper()
	_ = server.listener.Close()
	server.mu.Lock()
	if server.conn != nil {
		_ = server.conn.Close()
	}
	server.mu.Unlock()
	<-server.done
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]string(nil), server.verbs...)
}

// authPayloads is the decoded credential of every "AUTH PLAIN <base64>" line the relay received.
func (server *recordingSMTPServer) authPayloads(t *testing.T) []string {
	t.Helper()
	server.mu.Lock()
	defer server.mu.Unlock()
	var out []string
	for _, line := range server.authLines {
		fields := strings.Fields(line)
		if len(fields) != 3 || !strings.EqualFold(fields[1], "PLAIN") {
			t.Fatalf("the AUTH line is not \"AUTH PLAIN <base64>\": %q", "<redacted>")
		}
		raw, err := base64.StdEncoding.DecodeString(fields[2])
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(raw))
	}
	return out
}

func (server *recordingSMTPServer) record(verb string) {
	server.mu.Lock()
	server.verbs = append(server.verbs, verb)
	server.mu.Unlock()
}

func (server *recordingSMTPServer) serveOne() {
	defer close(server.done)
	conn, err := server.listener.Accept()
	if err != nil {
		return
	}
	server.mu.Lock()
	server.conn = conn
	server.mu.Unlock()
	defer conn.Close()
	server.serve(conn, true)
}

func (server *recordingSMTPServer) serve(conn net.Conn, greet bool) {
	reader := bufio.NewReader(conn)
	reply := func(line string) { _, _ = conn.Write([]byte(line + "\r\n")) }
	if greet {
		reply("220 recording.smtp.test ESMTP")
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
		server.record(verb)
		if verb == "AUTH" {
			server.mu.Lock()
			server.authLines = append(server.authLines, strings.TrimSpace(line))
			server.mu.Unlock()
		}
		switch verb {
		case "EHLO", "HELO":
			reply("250-recording.smtp.test")
			if server.advertiseSTARTTLS && !server.upgraded {
				reply("250-STARTTLS")
			}
			if server.advertiseAuth {
				reply("250-AUTH PLAIN")
			}
			reply("250 8BITMIME")
		case "STARTTLS":
			if server.startTLSReply != "" {
				reply(server.startTLSReply)
				continue
			}
			reply("220 Ready to start TLS")
			tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{*server.cert}, MaxVersion: server.maxTLSVersion})
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			server.upgraded = true
			server.serve(tlsConn, false)
			return
		case "AUTH":
			reply(server.authReply)
		case "MAIL", "RCPT":
			reply("250 OK")
		case "DATA":
			reply("354 go ahead")
			for {
				dataLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if dataLine == ".\r\n" {
					break
				}
			}
			reply("250 OK: queued")
		case "QUIT":
			reply("221 Bye")
			return
		default:
			reply("500 unrecognized command")
		}
	}
}

func hasVerb(verbs []string, want string) bool {
	for _, verb := range verbs {
		if verb == want {
			return true
		}
	}
	return false
}

// syntheticLogin is a login and password made for the test: no fixed credential-shaped literal in the file.
func syntheticLogin() (login, password string) {
	return "user-" + uuid.NewString()[:8], "pw-" + uuid.NewString()[:12]
}

func guardMessage() Message {
	return Message{To: "owner@example.test", Subject: "s", HTML: "<p>h</p>"}
}

// A relay that answers STARTTLS with an error: Send fails and the relay never sees MAIL FROM, RCPT TO, DATA or AUTH (so no credential and no
// message is sent in the clear after the refusal). Pins sender.go's `if err := client.StartTLS(...); err != nil { return ... }`.
func TestSMTPSendStopsWhenSTARTTLSIsRefusedAndSendsNothingInTheClear(t *testing.T) {
	server := newRecordingSMTPServer(t, func(s *recordingSMTPServer) {
		s.advertiseSTARTTLS = true
		s.startTLSReply = "454 4.7.0 TLS not available"
		s.advertiseAuth = true
	})
	host, port := server.addr()
	login, password := syntheticLogin()
	sender := &smtpSender{from: "noreply@example.test", host: host, port: port, username: login, password: password,
		useTLS: true, tlsConfig: &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}}
	err := sender.Send(context.Background(), guardMessage())
	if err == nil || !strings.Contains(err.Error(), "smtp STARTTLS failed") {
		t.Fatalf("Send() = %v, want an \"smtp STARTTLS failed\" error", err)
	}
	seen := server.commands(t)
	for _, verb := range []string{"AUTH", "MAIL", "RCPT", "DATA"} {
		if hasVerb(seen, verb) {
			t.Fatalf("the relay saw %s after it refused STARTTLS (commands: %v): the sender went on in the clear", verb, seen)
		}
	}
}

// A relay that refuses AUTH: Send fails with the auth error and the relay never sees MAIL FROM. Pins `if err := client.Auth(auth); err != nil`.
func TestSMTPSendStopsWhenAuthIsRefused(t *testing.T) {
	server := newRecordingSMTPServer(t, func(s *recordingSMTPServer) {
		s.advertiseAuth = true
		s.authReply = "535 5.7.8 authentication refused"
	})
	host, port := server.addr()
	login, password := syntheticLogin()
	sender := &smtpSender{from: "noreply@example.test", host: host, port: port, username: login, password: password}
	err := sender.Send(context.Background(), guardMessage())
	if err == nil || !strings.Contains(err.Error(), "smtp authentication failed") {
		t.Fatalf("Send() = %v, want an \"smtp authentication failed\" error", err)
	}
	if strings.Contains(err.Error(), password) || strings.Contains(err.Error(), login) {
		t.Fatalf("the auth error names a credential: %v", err)
	}
	seen := server.commands(t)
	if !hasVerb(seen, "AUTH") {
		t.Fatalf("the relay never saw AUTH (commands: %v): the refusal test measured nothing", seen)
	}
	for _, verb := range []string{"MAIL", "RCPT", "DATA"} {
		if hasVerb(seen, verb) {
			t.Fatalf("the relay saw %s after it refused AUTH (commands: %v)", verb, seen)
		}
	}
}

// The auth condition is `username != "" && password != ""`: both set = AUTH is sent and a 235 lets the message through; only one set = no AUTH at all.
func TestSMTPSendAuthenticatesOnlyWhenBothLoginAndPasswordAreSet(t *testing.T) {
	login, password := syntheticLogin()
	for _, test := range []struct {
		name               string
		username, password string
		wantAuth           bool
	}{
		{"both set", login, password, true},
		{"login only", login, "", false},
		{"password only", "", password, false},
		{"neither", "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newRecordingSMTPServer(t, func(s *recordingSMTPServer) { s.advertiseAuth = true })
			host, port := server.addr()
			sender := &smtpSender{from: "noreply@example.test", host: host, port: port, username: test.username, password: test.password}
			if err := sender.Send(context.Background(), guardMessage()); err != nil {
				t.Fatalf("Send() = %v, want nil (the relay accepts)", err)
			}
			seen := server.commands(t)
			if got := hasVerb(seen, "AUTH"); got != test.wantAuth {
				t.Fatalf("AUTH sent = %t, want %t (commands: %v)", got, test.wantAuth, seen)
			}
			if !hasVerb(seen, "MAIL") || !hasVerb(seen, "DATA") {
				t.Fatalf("the message did not go through (commands: %v)", seen)
			}
			if test.wantAuth {
				// AUTH PLAIN carries \0<login>\0<password> (empty authorization identity): login and password in that order.
				payloads := server.authPayloads(t)
				if len(payloads) != 1 || payloads[0] != "\x00"+test.username+"\x00"+test.password {
					t.Fatalf("the AUTH PLAIN payload is not \\0<login>\\0<password> (%d AUTH lines)", len(payloads))
				}
			}
		})
	}
}

// useTLS with NO tlsConfig (a struct literal) still verifies against the system roots and the sender's host: a relay presenting an untrusted
// self-signed certificate is refused by CERTIFICATE VERIFICATION (not by a missing-ServerName error), and the relay sees no MAIL FROM.
// Pins sender.go's `if tlsConfig == nil { tlsConfig = &tls.Config{ServerName: sender.host, ...} }`.
func TestSMTPSendWithoutATLSConfigStillVerifiesTheCertificate(t *testing.T) {
	cert, _ := generateSelfSignedSMTPCert(t, "127.0.0.1")
	server := newRecordingSMTPServer(t, func(s *recordingSMTPServer) {
		s.advertiseSTARTTLS = true
		s.cert = &cert
	})
	host, port := server.addr()
	sender := &smtpSender{from: "noreply@example.test", host: host, port: port, useTLS: true}
	err := sender.Send(context.Background(), guardMessage())
	if err == nil {
		t.Fatal("Send() = nil: an untrusted certificate was accepted")
	}
	if !strings.Contains(err.Error(), "unknown authority") {
		t.Fatalf("Send() = %v, want a certificate-verification refusal (unknown authority), not another failure", err)
	}
	if seen := server.commands(t); hasVerb(seen, "MAIL") {
		t.Fatalf("the relay saw MAIL FROM after the failed verification (commands: %v)", seen)
	}
}

func TestLoadSMTPTLSCAPoolRefusesAMissingFile(t *testing.T) {
	_, err := loadSMTPTLSCAPool(filepath.Join(t.TempDir(), "absent.pem"))
	if err == nil || !strings.Contains(err.Error(), "read:") {
		t.Fatalf("loadSMTPTLSCAPool(missing) = %v, want a \"read:\" error", err)
	}
}

func TestLoadSMTPTLSCAPoolRefusesAFileWithNoPEMCertificate(t *testing.T) {
	for name, content := range map[string]string{"empty": "", "text": "this is not a certificate\n", "key only": "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n"} {
		path := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		pool, err := loadSMTPTLSCAPool(path)
		if err == nil || !strings.Contains(err.Error(), "no PEM certificates found") || pool != nil {
			t.Fatalf("%s: loadSMTPTLSCAPool = (%v, %v), want a nil pool and a \"no PEM certificates found\" error", name, pool, err)
		}
	}
}

// The pool is the host's system roots PLUS the file's certificates (SMTP_TLS_CA_FILE ADDS trust): it equals a clone of the system pool with the
// file appended, and it is not the system pool itself.
func TestLoadSMTPTLSCAPoolAddsTheFileToTheSystemRoots(t *testing.T) {
	system, err := x509.SystemCertPool()
	if err != nil || system == nil || len(system.Subjects()) == 0 { //nolint:staticcheck // Subjects is only used to see that the host has system roots
		t.Fatalf("this host has no system roots (%v): the test cannot measure that the system roots are kept", err)
	}
	_, certPEM := generateSelfSignedSMTPCert(t, "127.0.0.1")
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	pool, err := loadSMTPTLSCAPool(path)
	if err != nil || pool == nil {
		t.Fatalf("loadSMTPTLSCAPool = (%v, %v), want a pool", pool, err)
	}
	want := system.Clone()
	if !want.AppendCertsFromPEM(certPEM) {
		t.Fatal("test certificate did not parse")
	}
	if !pool.Equal(want) {
		t.Fatal("the pool is not the system roots plus the file's certificates")
	}
	if pool.Equal(system) {
		t.Fatal("the pool equals the system roots: the file's certificate was not added")
	}
}

// Through the constructor: a CA file with no PEM certificate is refused at startup, naming the variable.
func TestNewSenderFromEnvRefusesACAFileWithNoPEMCertificate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	smtpEnv(t, "127.0.0.1", 1025, map[string]string{"SMTP_USE_TLS": "true", "SMTP_TLS_CA_FILE": path})
	_, err := NewSenderFromEnv(nil)
	if err == nil || !strings.Contains(err.Error(), "SMTP_TLS_CA_FILE is invalid") || !strings.Contains(err.Error(), "no PEM certificates found") {
		t.Fatalf("NewSenderFromEnv() = %v, want \"SMTP_TLS_CA_FILE is invalid: ... no PEM certificates found\"", err)
	}
}

// A relay that speaks only TLS 1.1 is refused (the sender never negotiates below TLS 1.2), whether the tls.Config is the constructor's or the
// struct-literal default; the relay sees no MAIL FROM. (On the toolchain of this repository Go's own client default is also TLS 1.2, so removing the
// explicit MinVersion from either config is NOT distinguishable here: stated NOT pinned in the PR body, executed.)
func TestSMTPSendRefusesARelayThatOffersOnlyTLS11(t *testing.T) {
	cert, certPEM := generateSelfSignedSMTPCert(t, "127.0.0.1")
	for _, test := range []struct {
		name string
		make func(t *testing.T, host string, port int) Sender
	}{
		{"struct literal without a tlsConfig", func(t *testing.T, host string, port int) Sender {
			return &smtpSender{from: "noreply@example.test", host: host, port: port, useTLS: true}
		}},
		{"constructor config", func(t *testing.T, host string, port int) Sender {
			smtpEnv(t, host, port, map[string]string{"SMTP_USE_TLS": "true", "SMTP_TLS_CA_FILE": writeCAFile(t, certPEM)})
			sender, err := NewSenderFromEnv(nil)
			if err != nil {
				t.Fatal(err)
			}
			return sender
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newRecordingSMTPServer(t, func(s *recordingSMTPServer) {
				s.advertiseSTARTTLS = true
				s.cert = &cert
				s.maxTLSVersion = tls.VersionTLS11
			})
			host, port := server.addr()
			err := test.make(t, host, port).Send(context.Background(), guardMessage())
			if err == nil {
				t.Fatal("Send() = nil: a TLS 1.1-only relay was accepted")
			}
			if seen := server.commands(t); hasVerb(seen, "MAIL") {
				t.Fatalf("the relay saw MAIL FROM after the failed handshake (commands: %v)", seen)
			}
		})
	}
}
