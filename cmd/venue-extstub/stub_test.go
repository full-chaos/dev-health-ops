package main

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
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var stubT0 = time.Date(2026, 11, 7, 16, 0, 0, 0, time.UTC)

func fixedLog(buf *bytes.Buffer) *hitLog {
	return &hitLog{w: buf, now: func() time.Time { return stubT0 }}
}

func hits(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// Every request is logged once, with exactly t, service, method, host and path: the header, body and query never
// reach the log, whatever they hold.
func TestStubLogsEveryRequestWithoutHeaderBodyOrQuery(t *testing.T) {
	var buf bytes.Buffer
	h := handler(fixedLog(&buf), modeRun)
	for _, host := range []string{"api.stripe.com", "API.PagerDuty.com:443", "identity.pagerduty.com", "github.com", "api.github.com", "evil.example"} {
		req := httptest.NewRequest(http.MethodPost, "https://"+host+"/v1/things/x?token=query-secret", strings.NewReader(`{"api_token":"body-secret"}`))
		req.Host = host
		req.Header.Set("Authorization", "Bearer header-secret")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	raw := buf.String()
	for _, secret := range []string{"query-secret", "body-secret", "header-secret", "token="} {
		if strings.Contains(raw, secret) {
			t.Fatalf("the log holds %q:\n%s", secret, raw)
		}
	}
	got := hits(t, raw)
	want := []string{"stripe api.stripe.com", "pagerduty api.pagerduty.com", "pagerduty identity.pagerduty.com", "github github.com", "github api.github.com", "unknown evil.example"}
	if len(got) != len(want) {
		t.Fatalf("%d log lines, want %d:\n%s", len(got), len(want), raw)
	}
	for i, rec := range got {
		if len(rec) != 5 || rec["t"] != stubT0.Format(time.RFC3339Nano) || rec["method"] != "POST" || rec["path"] != "/v1/things/x" {
			t.Fatalf("line %d: %v", i, rec)
		}
		if rec["service"].(string)+" "+rec["host"].(string) != want[i] {
			t.Fatalf("line %d: %v, want %s", i, rec, want[i])
		}
	}
}

// The frozen answers: Stripe's two plan lists empty, every other Stripe call 400; PagerDuty and GitHub 401; another
// host 421. The same request gets the same answer every time.
func TestStubAnswersFromTheFrozenTable(t *testing.T) {
	var buf bytes.Buffer
	h := handler(fixedLog(&buf), modeRun)
	cases := []struct {
		host, method, path string
		status             int
		body               string
	}{
		{"api.stripe.com", "GET", "/v1/products", 200, `"data":[]`},
		{"api.stripe.com", "GET", "/v1/prices", 200, `"data":[]`},
		{"api.stripe.com", "POST", "/v1/subscriptions/sub_x", 400, "invalid_request_error"},
		{"api.stripe.com", "DELETE", "/v1/subscriptions/sub_x", 400, "invalid_request_error"},
		{"api.pagerduty.com", "GET", "/services", 401, "credential refused"},
		{"identity.pagerduty.com", "POST", "/oauth/token", 401, "credential refused"},
		{"github.com", "POST", "/login/oauth/access_token", 401, "credential refused"},
		{"api.github.com", "GET", "/app/installations/1", 401, "credential refused"},
		{"other.example", "GET", "/", 421, "not served"},
	}
	for _, c := range cases {
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest(c.method, "https://"+c.host+c.path, nil)
			req.Host = c.host
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != c.status || !strings.Contains(rec.Body.String(), c.body) || rec.Header().Get("Content-Type") != "application/json" {
				t.Errorf("%s %s%s: %d %q", c.method, c.host, c.path, rec.Code, rec.Body.String())
			}
		}
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, os.ErrClosed }

// A hit that cannot be logged is answered 500, never as a served call.
func TestStubAnswers500WhenTheHitCannotBeLogged(t *testing.T) {
	h := handler(&hitLog{w: failWriter{}, now: time.Now}, modeRun)
	req := httptest.NewRequest(http.MethodGet, "https://api.stripe.com/v1/products", nil)
	req.Host = "api.stripe.com"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d", rec.Code)
	}
}

// The SMTP sink takes a message the way net/smtp sends one and logs one mail hit with no address or text.
func TestSMTPSinkLogsOneHitPerMessage(t *testing.T) {
	var buf bytes.Buffer
	log := fixedLog(&buf)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _ = serveSMTP(ln, log) }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	expect := func(code string) {
		t.Helper()
		line, err := r.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, code) {
			t.Fatalf("want %s, got %q %v", code, line, err)
		}
	}
	send := func(line, code string) {
		t.Helper()
		if _, err := conn.Write([]byte(line + "\r\n")); err != nil {
			t.Fatal(err)
		}
		expect(code)
	}
	expect("220")
	send("EHLO go-api", "250")
	send("STARTTLS", "502")
	send("MAIL FROM:<noreply@venue-proof.fullchaos.dev>", "250")
	send("RCPT TO:<invitee@venue-proof.fullchaos.dev>", "250")
	send("DATA", "354")
	send("Subject: invite\r\n\r\nsecret invite link\r\n.", "250")
	send("QUIT", "221")
	raw := buf.String()
	if strings.Contains(raw, "venue-proof") || strings.Contains(raw, "invite") {
		t.Fatalf("the log holds an address or the text:\n%s", raw)
	}
	got := hits(t, raw)
	if len(got) != 1 || got[0]["service"] != "mail" || got[0]["method"] != "DATA" || got[0]["host"] != smtpHost || got[0]["path"] != "/" || len(got[0]) != 5 {
		t.Fatalf("hits %v", got)
	}
}

// The stub never dials out: no source file of the command names an outbound client or dialer.
func TestStubSourceHasNoOutboundCall(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	banned := []string{"http.Client", "http.DefaultClient", "http.DefaultTransport", "http.Get(", "http.Post(", "http.Head(", "http.NewRequest",
		"net.Dial", "tls.Dial", "net/smtp", "httputil", "ReverseProxy", "http.Transport", "Dialer"}
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, b := range banned {
			if strings.Contains(string(raw), b) {
				t.Errorf("%s names %s: the stub must never dial out", f, b)
			}
		}
	}
	if checked < 4 {
		t.Fatalf("checked %d source files, want the command's 4", checked)
	}
}

// testCA writes a CA certificate and its PKCS #8 key (mode perm) to dir.
func testCA(t *testing.T, dir string, perm os.FileMode) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "venue test CA"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: stubT0.Add(-time.Hour), NotAfter: stubT0.Add(240 * time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}), perm); err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return certPath, keyPath, pool
}

// The leaf is signed by the venue CA for every stub host and no other name; a CA key readable by group or others is
// refused.
func TestLeafCertCoversTheStubHostsOnly(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, pool := testCA(t, dir, 0o600)
	certPEM, _ := os.ReadFile(certPath)
	leaf, err := leafCert(certPEM, keyPath, stubT0)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(leaf.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	for host := range Hosts {
		if _, err := parsed.Verify(x509.VerifyOptions{DNSName: host, Roots: pool, CurrentTime: stubT0}); err != nil {
			t.Errorf("%s: %v", host, err)
		}
	}
	if _, err := parsed.Verify(x509.VerifyOptions{DNSName: "api.example.com", Roots: pool, CurrentTime: stubT0}); err == nil {
		t.Error("the leaf verifies for a host the stub does not serve")
	}
	_, loose, _ := testCA(t, t.TempDir(), 0o640)
	if _, err := leafCert(certPEM, loose, stubT0); err == nil || !strings.Contains(err.Error(), "owner-only") {
		t.Fatalf("a 0640 CA key: %v", err)
	}
}

// End to end over TLS: a client that trusts only the venue CA reaches the stub as api.stripe.com, the hit is logged,
// and the log file must be new.
func TestRunServesTLSWithTheVenueCA(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, pool := testCA(t, dir, 0o600)
	logPath := filepath.Join(dir, "hits.jsonl")
	statusPath := filepath.Join(dir, "status.txt")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpsAddr := ln.Addr().String()
	ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int)
	var stderr bytes.Buffer
	go func() {
		done <- run(ctx, []string{"-ca-cert", certPath, "-ca-key", keyPath, "-log", logPath, "-status", statusPath, "-https", httpsAddr, "-smtp", "127.0.0.1:0"}, &stderr)
	}()
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "api.stripe.com"}}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	var resp *http.Response
	for i := 0; i < 50; i++ {
		req, _ := http.NewRequest(http.MethodGet, "https://"+httpsAddr+"/v1/products", nil)
		req.Host = "api.stripe.com"
		if resp, err = client.Do(req); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("run exit %d: %s", code, stderr.String())
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(logPath)
	got := hits(t, string(raw))
	if resp.StatusCode != 200 || len(got) != 1 || got[0]["service"] != "stripe" || info.Mode().Perm() != 0o600 {
		t.Fatalf("status %d, hits %v, mode %v", resp.StatusCode, got, info.Mode().Perm())
	}
	again, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if code := run(again, []string{"-ca-cert", certPath, "-ca-key", keyPath, "-log", logPath, "-status", filepath.Join(dir, "status2.txt"), "-https", "127.0.0.1:0", "-smtp", "127.0.0.1:0"}, &stderr); code != exitUsage {
		t.Fatalf("an existing log file: exit %d", code)
	}
}
