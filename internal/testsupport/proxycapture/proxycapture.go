// Package proxycapture proves that an outbound client never dials a proxy taken from the process environment.
//
// net/http reads HTTP_PROXY, HTTPS_PROXY and NO_PROXY once per process (the first time a request asks), so a test cannot
// set them for itself. Check runs the calling test again in a child process of the same test binary, with both
// variables pointing at a capture proxy on 127.0.0.1, and fails the test when any connection reaches that proxy. The
// child sends to a name that is not loopback (net/http never proxies a loopback host), so a client that honours the
// environment proxy hands its request, credential included, to the capture proxy; a client that does not tries the
// name directly and gets a resolver error.
package proxycapture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// Target is the URL a child sends to: plain http, so a proxied request reaches the capture proxy in full (headers
// included), and a reserved name that no resolver answers.
const Target = "http://upstream.invalid/"

// Canary is the credential value a child puts on its request: the capture proxy reports whether it saw it.
const Canary = "proxycapture-canary-credential"

const childEnv = "PROXYCAPTURE_CHILD"

// Capture is what reached the capture proxy while the child ran.
type Capture struct {
	Connections int
	SawCanary   bool
	FirstLines  []string
}

// Check runs send in a child process whose environment points HTTP_PROXY and HTTPS_PROXY at a capture proxy, and
// returns what reached the proxy (in the child it runs send and returns a zero Capture). send gets a context that
// ends after a few seconds; its errors are expected (the target does not resolve) and ignored.
func Check(t *testing.T, send func(ctx context.Context)) Capture {
	t.Helper()
	if os.Getenv(childEnv) == t.Name() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		send(ctx)
		return Capture{}
	}
	proxy := startCapture(t)
	cmd := exec.Command(os.Args[0], "-test.run="+runPattern(t.Name()), "-test.count=1", "-test.timeout=2m")
	address := "http://" + proxy.address
	cmd.Env = append(cleanEnv(os.Environ()),
		childEnv+"="+t.Name(),
		"HTTP_PROXY="+address, "http_proxy="+address,
		"HTTPS_PROXY="+address, "https_proxy="+address,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the child test run failed: %v\n%s", err, output)
	}
	if !bytes.Contains(output, []byte("PASS")) || bytes.Contains(output, []byte("no tests to run")) {
		t.Fatalf("the child test run did not run %s:\n%s", t.Name(), output)
	}
	return proxy.close()
}

// AssertNothingReached fails t when any connection reached the capture proxy.
func (c Capture) AssertNothingReached(t *testing.T) {
	t.Helper()
	if c.Connections != 0 {
		t.Fatalf("%d connection(s) reached the environment proxy (credential seen: %v); first lines: %q", c.Connections, c.SawCanary, c.FirstLines)
	}
}

func runPattern(name string) string {
	parts := strings.Split(name, "/")
	for i, part := range parts {
		parts[i] = "^" + regexp.QuoteMeta(part) + "$"
	}
	return strings.Join(parts, "/")
}

// cleanEnv drops every proxy variable and REQUEST_METHOD (net/http ignores HTTP_PROXY when it is set, as under CGI).
func cleanEnv(env []string) []string {
	drop := map[string]bool{"HTTP_PROXY": true, "HTTPS_PROXY": true, "NO_PROXY": true, "ALL_PROXY": true, "REQUEST_METHOD": true, childEnv: true}
	var out []string
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if drop[strings.ToUpper(name)] {
			continue
		}
		out = append(out, entry)
	}
	return out
}

type captureProxy struct {
	address  string
	listener net.Listener
	mu       sync.Mutex
	capture  Capture
	done     sync.WaitGroup
}

func startCapture(t *testing.T) *captureProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &captureProxy{address: listener.Addr().String(), listener: listener}
	proxy.done.Add(1)
	go proxy.serve()
	t.Cleanup(func() { _ = listener.Close() })
	return proxy
}

func (p *captureProxy) serve() {
	defer p.done.Done()
	var handlers sync.WaitGroup
	defer handlers.Wait()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			p.handle(conn)
		}()
	}
}

func (p *captureProxy) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	buffer := make([]byte, 64<<10)
	n, _ := conn.Read(buffer)
	received := buffer[:n]
	firstLine, _, _ := strings.Cut(string(received), "\r\n")
	p.mu.Lock()
	p.capture.Connections++
	p.capture.FirstLines = append(p.capture.FirstLines, firstLine)
	if bytes.Contains(received, []byte(Canary)) {
		p.capture.SawCanary = true
	}
	p.mu.Unlock()
	_, _ = fmt.Fprint(conn, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
}

func (p *captureProxy) close() Capture {
	_ = p.listener.Close()
	p.done.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.capture
}
