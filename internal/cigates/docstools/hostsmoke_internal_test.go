package docstools

import (
	"bytes"
	"context"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Python's timeout=20 is a per-operation socket timeout; the Go port keeps it (hsTimeout, per read, dial and handshake).
// The live case is recorded (testdata/hostsmoke/fail-request-timeout, 20 s of Python) and replayed here with a short
// timeout. Go's error text differs from urllib's "The read operation timed out" (named row).
func TestHostSmokeRequestTimeout(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/use/" {
			time.Sleep(2 * time.Second)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	oldTimeout, oldRoots := hsTimeout, hsRoots
	hsTimeout, hsRoots = 300*time.Millisecond, pool
	defer func() { hsTimeout, hsRoots = oldTimeout, oldRoots }()

	tsv := filepath.Join(t.TempDir(), "r.tsv")
	if err := os.WriteFile(tsv, []byte("source_path\ttarget_path\n/old/\t/new/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := hostSmokeVerb([]string{"--origin", srv.URL, "--redirects", tsv}, &stdout, &stderr)
	if code != 1 || !strings.HasPrefix(stdout.String(), "ERROR: request failed: ") || strings.Count(stdout.String(), "\n") != 1 {
		t.Errorf("exit %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	if time.Since(start) > 1500*time.Millisecond {
		t.Errorf("took %s; the timeout did not fire", time.Since(start))
	}
}

func TestHostSmokeDefaultsMatchPython(t *testing.T) {
	if hsTimeout != 20*time.Second || hsRoots != nil || hsUserAgent != "dev-health-docs-smoke/1" {
		t.Errorf("timeout %s roots %v agent %q", hsTimeout, hsRoots, hsUserAgent)
	}
}

// A header block of 325 KB (6 lines under the 65536-byte line limit) must be parsed whole: a later duplicate bad value
// wins in Python's dict, so the recording may never cut the block short; a recording that ends inside the block is an error.
func TestHostSmokeRawHeadersKeepALargeBlockWholeAndRefuseATruncatedOne(t *testing.T) {
	var b strings.Builder
	b.WriteString("HTTP/1.1 200 OK\r\nX-Content-Type-Options: nosniff\r\n")
	for i := 0; i < 5; i++ {
		b.WriteString("X-Pad-" + strconv.Itoa(i) + ": " + strings.Repeat("a", 65000) + "\r\n")
	}
	b.WriteString("X-Content-Type-Options: bad\r\n\r\nDev Health")
	if b.Len() <= 256<<10 {
		t.Fatalf("block is %d bytes, not over the old 256 KiB cap", b.Len())
	}
	pairs, ok, err := hsRawHeaders([]byte(b.String()))
	if err != nil || !ok {
		t.Fatalf("ok %v err %v", ok, err)
	}
	if got := hsLastWins(pairs)["x-content-type-options"]; got != "bad" {
		t.Errorf("later duplicate lost: %q", got)
	}
	cut := []byte("HTTP/1.1 200 OK\r\nX-A: " + strings.Repeat("b", hsRecCap))
	if _, _, err := hsRawHeaders(cut[:hsRecCap]); err == nil {
		t.Error("a recording that ends inside the header block was accepted")
	}
}

func TestHostSmokeRecorderKeepsAWholeLargeHeaderBlock(t *testing.T) {
	block := "HTTP/1.1 200 OK\r\n" + strings.Repeat("X-Pad: "+strings.Repeat("a", 60000)+"\r\n", 6) + "\r\nDev Health"
	server, client := net.Pipe()
	go func() {
		_, _ = server.Write([]byte(block))
		server.Close()
	}()
	rec := &hsRec{}
	if _, err := io.Copy(io.Discard, &hsRecConn{Conn: client, rec: rec}); err != nil {
		t.Fatal(err)
	}
	if len(rec.buf) != len(block) || len(block) <= 256<<10 {
		t.Errorf("recorded %d of %d bytes (block must exceed 256 KiB)", len(rec.buf), len(block))
	}
}

// The dial configuration, pinned without a listener: SNI/Host and roots, no ALPN, no proxy bypass, per-operation timeouts.
func TestHostSmokeTLSConfigPinsSNIRootsAndNoALPN(t *testing.T) {
	for _, host := range []string{"docs.fullchaos.dev", "127.0.0.1"} {
		cfg := hsTLSConfig(host)
		if cfg.ServerName != host || cfg.RootCAs != nil || len(cfg.NextProtos) != 0 || cfg.InsecureSkipVerify {
			t.Errorf("%s: ServerName %q RootCAs %v NextProtos %v InsecureSkipVerify %v", host, cfg.ServerName, cfg.RootCAs, cfg.NextProtos, cfg.InsecureSkipVerify)
		}
	}
}

func TestHostSmokeTransportHasNoTotalTimeoutAndNoSecondProxyHook(t *testing.T) {
	tr := hsTransport()
	if tr.ResponseHeaderTimeout != 0 || tr.TLSHandshakeTimeout != 0 {
		t.Errorf("ResponseHeaderTimeout %s, TLSHandshakeTimeout %s: total deadlines urllib does not have", tr.ResponseHeaderTimeout, tr.TLSHandshakeTimeout)
	}
	if tr.DialTLSContext == nil || !tr.DisableKeepAlives || !tr.DisableCompression || tr.ForceAttemptHTTP2 || len(tr.TLSNextProto) != 0 {
		t.Errorf("transport is not HTTP/1.1, one request per connection, raw bodies")
	}
	u, _ := url.Parse("https://docs.fullchaos.dev/")
	if proxy, err := tr.Proxy(&http.Request{URL: u}); proxy != nil || err != nil {
		t.Errorf("https goes through hsDialTLS, the transport must not also proxy it: %v %v", proxy, err)
	}
}

// urllib's timeout is per operation: a response that keeps trickling in within the timeout is read to the end; one that
// stalls for the timeout is an error.
func TestHostSmokeIdleDeadlineIsPerOperation(t *testing.T) {
	old := hsTimeout
	hsTimeout = 150 * time.Millisecond
	defer func() { hsTimeout = old }()
	server, client := net.Pipe()
	go func() {
		for i := 0; i < 8; i++ {
			_, _ = server.Write([]byte("x"))
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(600 * time.Millisecond)
		server.Close()
	}()
	conn := hsIdleConn{client}
	buf := make([]byte, 1)
	for i := 0; i < 8; i++ {
		if _, err := conn.Read(buf); err != nil {
			t.Fatalf("read %d of a slow trickle (total over the timeout) failed: %v", i, err)
		}
	}
	if _, err := conn.Read(buf); err == nil {
		t.Error("a stalled read did not time out")
	}
	if _, err := conn.Write([]byte("y")); err == nil {
		t.Error("a stalled write did not time out")
	}
}

// With a proxy in the environment the dial goes to the proxy and nowhere else (no direct connection to the target).
func TestHostSmokeDialWithProxyEnvironmentNeverDialsTheTarget(t *testing.T) {
	for _, name := range []string{"https_proxy", "NO_PROXY", "no_proxy"} {
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	proxy := hsProxyFor("https", "docs.fullchaos.dev:443")
	if proxy == nil || proxy.Host != "127.0.0.1:1" {
		t.Fatalf("proxy %v", proxy)
	}
	_, err := hsDialTLS(context.Background(), "tcp", "docs.fullchaos.dev:443")
	if err == nil || !strings.Contains(err.Error(), "127.0.0.1:1") || strings.Contains(err.Error(), "docs.fullchaos.dev") {
		t.Errorf("dial error %v: want it to name the proxy 127.0.0.1:1 only", err)
	}
	t.Setenv("NO_PROXY", "fullchaos.dev")
	if hsProxyFor("https", "docs.fullchaos.dev:443") != nil {
		t.Error("NO_PROXY suffix did not bypass the proxy")
	}
}

type hsFakeTransport struct {
	requests []string
	reply    func(path string) (int, map[string]string)
}

func (f *hsFakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.requests = append(f.requests, req.URL.String())
	status, header := f.reply(req.URL.Path)
	resp := &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("Dev Health")), Request: req}
	for k, v := range header {
		resp.Header.Set(k, v)
	}
	return resp, nil
}

func TestHostSmokeRedirectCheckNeverFollows(t *testing.T) {
	fake := &hsFakeTransport{reply: func(string) (int, map[string]string) { return 301, map[string]string{"Location": "/new/"} }}
	result, err := hsGet(fake, false, "https://docs.fullchaos.dev/old/")
	if err != nil || result.status != 301 || len(fake.requests) != 1 {
		t.Errorf("status %d err %v requests %v", result.status, err, fake.requests)
	}
}

// urllib follows at most 10 distinct URLs and visits one URL at most 4 times, per request chain; other schemes than
// http and https are not followed; a Location it would re-quote is refused.
func TestHostSmokeFollowBoundsEveryRequestChain(t *testing.T) {
	loop := &hsFakeTransport{reply: func(string) (int, map[string]string) { return 302, map[string]string{"Location": "/loop/"} }}
	result, err := hsGet(loop, true, "https://h/loop/")
	if err != nil || result.status != 302 || len(loop.requests) != 5 {
		t.Errorf("loop: status %d err %v requests %d, want the 302 after 5 requests", result.status, err, len(loop.requests))
	}
	again := &hsFakeTransport{reply: loop.reply}
	if _, err := hsGet(again, true, "https://h/loop/"); err != nil || len(again.requests) != 5 {
		t.Errorf("a second chain shares the first one's visits: %d requests, %v", len(again.requests), err)
	}
	chain := &hsFakeTransport{reply: func(path string) (int, map[string]string) {
		n, _ := strconv.Atoi(strings.Trim(path, "/h"))
		return 301, map[string]string{"Location": "/h" + strconv.Itoa(n+1) + "/"}
	}}
	if result, err := hsGet(chain, true, "https://h/h0/"); err != nil || result.status != 301 || len(chain.requests) != 11 {
		t.Errorf("chain: status %d err %v requests %d, want the 301 after 11 requests (10 distinct redirect targets)", result.status, err, len(chain.requests))
	}
	ftp := &hsFakeTransport{reply: func(string) (int, map[string]string) { return 302, map[string]string{"Location": "ftp://h/x"} }}
	if result, err := hsGet(ftp, true, "https://h/"); err != nil || result.status != 302 || len(ftp.requests) != 1 {
		t.Errorf("ftp: status %d err %v requests %v", result.status, err, ftp.requests)
	}
	requote := &hsFakeTransport{reply: func(string) (int, map[string]string) { return 302, map[string]string{"Location": "/new/?"} }}
	if _, err := hsGet(requote, true, "https://h/"); err == nil || len(requote.requests) != 1 {
		t.Errorf("re-quote: err %v requests %v", err, requote.requests)
	}
}
