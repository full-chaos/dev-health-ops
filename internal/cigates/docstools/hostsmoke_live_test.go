package docstools_test

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Live replay of the host-smoke parity cases (testdata/hostsmoke): each case is a raw-bytes TLS server (routes.json),
// a redirects.tsv and the arguments; the Python run was recorded once from scripts/check_docs_cloudflare_host.py at the
// merge base (testdata/hostsmoke/README.md). The BUILT cigate binary runs against the server; stdout, stderr, exit code
// and the ordered request log must equal the recording, except where a go/ directory names the Go answer: the cases
// where Go is stricter or its error text differs, each listed in testdata/refusals/host-smoke.tsv.

// hostSmokeCaseCount pins the number of recorded cases; adding or dropping one changes it on purpose.
const hostSmokeCaseCount = 145

type hsTLS struct {
	certFile string
	cert     tls.Certificate
}

func newHSTLS(t *testing.T, dir string, names ...string) hsTLS {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "host-smoke test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              append([]string{"localhost", "smoke.test"}, names...),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return hsTLS{certFile: certFile, cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}}
}

// hsRawServer answers each connection with the raw bytes of routes[path] (or routes["*"]), logging "METHOD path UA".
type hsRawServer struct {
	ln     net.Listener
	mu     sync.Mutex
	log    []string
	routes map[string]string
	done   chan struct{}
}

func startHSRawServer(t *testing.T, cert tls.Certificate, routes map[string]string, alpn ...string) *hsRawServer {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: alpn})
	if err != nil {
		t.Fatal(err)
	}
	s := &hsRawServer{ln: ln, routes: routes, done: make(chan struct{})}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handle(conn)
		}
	}()
	t.Cleanup(func() { close(s.done); ln.Close() })
	return s
}

func (s *hsRawServer) hostPort() string { return s.ln.Addr().String() }

func (s *hsRawServer) requests() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.log, "\n")
}

func (s *hsRawServer) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return
	}
	ua := ""
	for {
		header, err := reader.ReadString('\n')
		if err != nil || strings.TrimRight(header, "\r\n") == "" {
			break
		}
		if name, value, ok := strings.Cut(header, ":"); ok && strings.EqualFold(name, "user-agent") {
			ua = strings.TrimSpace(value)
		}
	}
	s.mu.Lock()
	s.log = append(s.log, fmt.Sprintf("%s %s %s", parts[0], parts[1], ua))
	s.mu.Unlock()
	resp, ok := s.routes[parts[1]]
	if !ok {
		resp = s.routes["*"]
	}
	switch resp {
	case "HANG":
		select {
		case <-s.done:
		case <-time.After(30 * time.Second):
		}
		return
	case "RESET":
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nX-A: b"))
		return
	}
	hostPort := s.hostPort()
	resp = strings.ReplaceAll(resp, "{ORIGIN}", "https://"+hostPort)
	resp = strings.ReplaceAll(resp, "{HOSTPORT}", hostPort)
	_, _ = conn.Write([]byte(resp))
}

func hsSplitArgs(line string) []string {
	var out []string
	for _, field := range strings.Fields(line) {
		if field == "''" {
			field = ""
		}
		out = append(out, field)
	}
	return out
}

func hsRead(t *testing.T, path string) (string, bool) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false
		}
		t.Fatal(err)
	}
	return string(raw), true
}

func hsRunBinary(t *testing.T, bin, work string, args []string, env []string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(bin, append([]string{"docs", "host-smoke"}, args...)...)
	cmd.Dir = work
	cmd.Env = env
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	return out.String(), errb.String(), code
}

func TestHostSmokeCasesRunLiveThroughTheCLI(t *testing.T) {
	bin := buildCigate(t)
	certs := newHSTLS(t, t.TempDir())
	cases, err := filepath.Glob(filepath.Join("testdata", "hostsmoke", "*", "routes.json"))
	if err != nil || len(cases) != hostSmokeCaseCount {
		t.Fatalf("host-smoke cases: found %d (err %v), pinned %d", len(cases), err, hostSmokeCaseCount)
	}
	for _, routesFile := range cases {
		dir := filepath.Dir(routesFile)
		name := filepath.Base(dir)
		t.Run(name, func(t *testing.T) {
			if _, hang := hsRead(t, filepath.Join(dir, "hang")); hang {
				t.Skip("replayed in-process by TestHostSmokeRequestTimeout")
			}
			var routes map[string]string
			raw, _ := hsRead(t, routesFile)
			if err := json.Unmarshal([]byte(raw), &routes); err != nil {
				t.Fatal(err)
			}
			var alpn []string
			if _, h2 := hsRead(t, filepath.Join(dir, "alpn-h2")); h2 {
				alpn = []string{"h2"}
			}
			srv := startHSRawServer(t, certs.cert, routes, alpn...)
			hostPort := srv.hostPort()
			sub := func(s string) string {
				return strings.ReplaceAll(strings.ReplaceAll(s, "{ORIGIN}", "https://"+hostPort), "{HOSTPORT}", hostPort)
			}
			work := t.TempDir()
			tsv, _ := hsRead(t, filepath.Join(dir, "redirects.tsv"))
			if err := os.WriteFile(filepath.Join(work, "redirects.tsv"), []byte(tsv), 0o644); err != nil {
				t.Fatal(err)
			}
			argLine := "--origin {ORIGIN} --redirects redirects.tsv"
			if line, ok := hsRead(t, filepath.Join(dir, "args")); ok {
				argLine = strings.TrimSpace(line)
			}
			env := []string{"PATH=" + os.Getenv("PATH")}
			if _, untrusted := hsRead(t, filepath.Join(dir, "untrusted")); !untrusted {
				env = append(env, "SSL_CERT_FILE="+certs.certFile)
			}
			stdout, stderr, code := hsRunBinary(t, bin, work, hsSplitArgs(sub(argLine)), env)

			pick := func(file string) string {
				if override, ok := hsRead(t, filepath.Join(dir, "go", file)); ok {
					return sub(override)
				}
				want, _ := hsRead(t, filepath.Join(dir, file))
				return sub(want)
			}
			wantCode := strings.TrimSpace(pick("rc"))
			if fmt.Sprint(code) != wantCode {
				t.Errorf("exit %d, want %s\nstdout %q\nstderr %q", code, wantCode, stdout, stderr)
			}
			if want := pick("stdout"); stdout != want {
				t.Errorf("stdout %q, want %q", stdout, want)
			}
			wantErr := pick("stderr")
			if wantCode == "2" {
				_, wantAfter, _ := strings.Cut(wantErr, "error: ")
				if _, after, ok := strings.Cut(stderr, "error: "); !ok || after != wantAfter {
					t.Errorf("usage error %q, want message %q", stderr, wantAfter)
				}
			} else if stderr != wantErr {
				t.Errorf("stderr %q, want %q", stderr, wantErr)
			}
			wantReq := pick("requests")
			if got := srv.requests(); got != strings.TrimSuffix(wantReq, "\n") {
				t.Errorf("requests:\n%s\nwant:\n%s", got, wantReq)
			}
		})
	}
}

// hsConnectProxy is a plain HTTP proxy that only tunnels CONNECT to the real TLS server, whatever host the client names.
func startHSConnectProxy(t *testing.T, target string) (addr string, seen func() []string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var mu sync.Mutex
	var connects []string
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer client.Close()
				reader := bufio.NewReader(client)
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				auth := ""
				for {
					header, err := reader.ReadString('\n')
					if err != nil || strings.TrimRight(header, "\r\n") == "" {
						break
					}
					if name, value, ok := strings.Cut(header, ":"); ok && strings.EqualFold(name, "proxy-authorization") {
						auth = strings.TrimSpace(value)
					}
				}
				mu.Lock()
				connects = append(connects, strings.TrimSpace(line)+"|"+auth)
				mu.Unlock()
				upstream, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer upstream.Close()
				_, _ = client.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(upstream, reader); done <- struct{}{} }()
				go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return ln.Addr().String(), func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), connects...)
	}
}

// Python urllib honours HTTPS_PROXY and NO_PROXY; Go uses http.ProxyFromEnvironment (HTTPS_PROXY, https_proxy,
// NO_PROXY, no_proxy). The origin is a name no resolver knows, so a pass proves the CONNECT tunnel carried it.
func TestHostSmokeHonoursHTTPSProxyAndNoProxy(t *testing.T) {
	bin := buildCigate(t)
	certs := newHSTLS(t, t.TempDir())
	routes := map[string]string{
		"/":     "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: strict-origin-when-cross-origin\r\n\r\nDev Health",
		"/use/": "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
		"/reference/metrics/weighting-and-aggregation/": "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
		"/old/": "HTTP/1.1 301 Moved\r\nContent-Length: 0\r\nLocation: /new/\r\n\r\n",
		"*":     "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
	}
	srv := startHSRawServer(t, certs.cert, routes)
	proxyAddr, seen := startHSConnectProxy(t, srv.hostPort())
	_, port, _ := net.SplitHostPort(srv.hostPort())
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "r.tsv"), []byte("source_path\ttarget_path\n/old/\t/new/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"--origin", "https://smoke.test:" + port, "--redirects", "r.tsv"}
	base := []string{"PATH=" + os.Getenv("PATH"), "SSL_CERT_FILE=" + certs.certFile}

	stdout, stderr, code := hsRunBinary(t, bin, work, args, append(append([]string{}, base...), "HTTPS_PROXY=http://"+proxyAddr))
	if code != 0 {
		t.Fatalf("through HTTPS_PROXY: exit %d stdout %q stderr %q", code, stdout, stderr)
	}
	if got := seen(); len(got) != 5 || got[0] != "CONNECT smoke.test:"+port+" HTTP/1.1|" {
		t.Errorf("proxy saw %v, want 5 CONNECT smoke.test:%s", got, port)
	}
	if got := strings.Count(srv.requests(), "\n") + 1; got != 5 {
		t.Errorf("server saw %d requests, want 5", got)
	}

	before := len(seen())
	stdout, _, code = hsRunBinary(t, bin, work, args, append(append([]string{}, base...), "HTTPS_PROXY=http://"+proxyAddr, "NO_PROXY=smoke.test"))
	if code != 1 || !strings.HasPrefix(stdout, "ERROR: request failed: ") || len(seen()) != before {
		t.Errorf("NO_PROXY: exit %d stdout %q, proxy used %d times more", code, stdout, len(seen())-before)
	}
}

// urllib sends Proxy-Authorization: Basic from the userinfo of the proxy URL; the Go dialer does the same.
func TestHostSmokeSendsProxyAuthorizationFromTheProxyURL(t *testing.T) {
	bin := buildCigate(t)
	certs := newHSTLS(t, t.TempDir())
	routes := map[string]string{"*": "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"}
	srv := startHSRawServer(t, certs.cert, routes)
	proxyAddr, seen := startHSConnectProxy(t, srv.hostPort())
	_, port, _ := net.SplitHostPort(srv.hostPort())
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "r.tsv"), []byte("source_path\ttarget_path\n/old/\t/new/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "SSL_CERT_FILE=" + certs.certFile, "HTTPS_PROXY=http://user%40x:p%3Aw@" + proxyAddr}
	hsRunBinary(t, bin, work, []string{"--origin", "https://smoke.test:" + port, "--redirects", "r.tsv"}, env)
	got := seen()
	want := "CONNECT smoke.test:" + port + " HTTP/1.1|Basic dXNlckB4OnA6dw=="
	if len(got) == 0 || got[0] != want {
		t.Errorf("proxy saw %v, want first %q", got, want)
	}
}

// Python reads the proxy variables in two passes, so a lower-case name beats an upper-case one (https_proxy over
// HTTPS_PROXY, no_proxy over NO_PROXY); net/http's ProxyFromEnvironment prefers the upper-case one, so the port has its own.
func TestHostSmokeProxyEnvironmentPrecedenceIsPythons(t *testing.T) {
	bin := buildCigate(t)
	certs := newHSTLS(t, t.TempDir())
	routes := map[string]string{
		"/":     "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: strict-origin-when-cross-origin\r\n\r\nDev Health",
		"/old/": "HTTP/1.1 301 Moved\r\nContent-Length: 0\r\nLocation: /new/\r\n\r\n",
		"*":     "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
	}
	routes["/phase-11-intentional-missing-page/"] = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"
	srv := startHSRawServer(t, certs.cert, routes)
	_, port, _ := net.SplitHostPort(srv.hostPort())
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "r.tsv"), []byte("source_path\ttarget_path\n/old/\t/new/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"--origin", "https://smoke.test:" + port, "--redirects", "r.tsv"}
	base := []string{"PATH=" + os.Getenv("PATH"), "SSL_CERT_FILE=" + certs.certFile}
	dead := "http://127.0.0.1:1"
	cases := []struct {
		name    string
		env     func(good string) []string
		pass    bool
		proxied bool
	}{
		{"lower-case https_proxy beats HTTPS_PROXY (dead lower)", func(good string) []string { return []string{"HTTPS_PROXY=" + good, "https_proxy=" + dead} }, false, false},
		{"lower-case https_proxy beats HTTPS_PROXY (good lower)", func(good string) []string { return []string{"HTTPS_PROXY=" + dead, "https_proxy=" + good} }, true, true},
		{"lower-case no_proxy beats NO_PROXY", func(good string) []string {
			return []string{"HTTPS_PROXY=" + good, "NO_PROXY=smoke.test", "no_proxy=other.test"}
		}, true, true},
		{"empty lower-case https_proxy removes HTTPS_PROXY", func(good string) []string { return []string{"HTTPS_PROXY=" + dead, "https_proxy="} }, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			proxyAddr, seen := startHSConnectProxy(t, srv.hostPort())
			stdout, stderr, code := hsRunBinary(t, bin, work, args, append(append([]string{}, base...), c.env("http://"+proxyAddr)...))
			if c.pass && code != 0 {
				t.Errorf("exit %d stdout %q stderr %q", code, stdout, stderr)
			}
			if !c.pass && code == 0 {
				t.Errorf("passed through %q", c.name)
			}
			if got := len(seen()) > 0; got != c.proxied {
				t.Errorf("proxy used = %v, want %v", got, c.proxied)
			}
		})
	}
}
