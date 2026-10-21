package egressguard

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

type syncBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

func allowOnly(t *testing.T, cidrs ...string) {
	t.Helper()
	prefixes := make([]netip.Prefix, len(cidrs))
	for i, cidr := range cidrs {
		prefixes[i] = netip.MustParsePrefix(cidr)
	}
	t.Cleanup(SetAllowForTest(prefixes...))
}

func mapNames(t *testing.T, names map[string][]netip.Addr) *atomic.Int32 {
	t.Helper()
	var calls atomic.Int32
	previous := lookup
	lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
		calls.Add(1)
		if addrs, ok := names[host]; ok {
			return addrs, nil
		}
		return previous(ctx, host)
	}
	t.Cleanup(func() { lookup = previous })
	return &calls
}

func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	logs := &syncBuffer{}
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(logger) })
	return logs
}

func countingServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	t.Cleanup(server.Close)
	return server, &hits
}

func get(t *testing.T, client *http.Client, target string) error {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret-token")
	response, err := client.Do(request)
	if err == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	return err
}

// A stored provider URL that is, or resolves to, a loopback address is refused at connect, before any byte (the
// bearer token included) reaches the server, with the class in the error and in one WARN that names no host or address.
func TestWrapRefusesANonPublicStoredURL(t *testing.T) {
	allowOnly(t)
	server, hits := countingServer(t)
	logs := captureLogs(t)
	client := &http.Client{Transport: Wrap(nil), Timeout: 5 * time.Second}
	for _, target := range []string{server.URL, strings.Replace(server.URL, "127.0.0.1", "localhost", 1)} {
		err := get(t, client, target)
		var refused *RefusedError
		if !errors.Is(err, ErrRefused) || !errors.As(err, &refused) || refused.Class != "loopback" {
			t.Fatalf("GET %s error = %v, want the loopback egress refusal", target, err)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("the server received %d requests", hits.Load())
	}
	if !strings.Contains(logs.String(), `"class":"loopback"`) {
		t.Fatalf("no refusal log with the class: %s", logs.String())
	}
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	for _, leak := range []string{"127.0.0.1", "localhost", port, "secret-token"} {
		if strings.Contains(logs.String(), leak) {
			t.Fatalf("the refusal log carries %q: %s", leak, logs.String())
		}
	}
}

// The allow list is the dev and test path: an address inside it is dialled.
func TestWrapDialsAnAllowedAddress(t *testing.T) {
	allowOnly(t, "127.0.0.0/8", "::1/128")
	server, hits := countingServer(t)
	client := &http.Client{Transport: Wrap(nil), Timeout: 5 * time.Second}
	if err := get(t, client, server.URL); err != nil {
		t.Fatalf("GET to an allowed address: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1", hits.Load())
	}
}

func TestCheckNeverAdmitsMetadataLinkLocalMulticastOrUnspecified(t *testing.T) {
	allowOnly(t, "0.0.0.0/0", "::/0")
	_ = captureLogs(t)
	for address, class := range map[string]string{
		"169.254.169.254": "metadata", "169.254.1.1": "link_local", "fe80::1": "link_local",
		"224.0.0.1": "multicast", "ff02::1": "multicast", "0.0.0.0": "unspecified", "::": "unspecified",
	} {
		var refused *RefusedError
		if err := check(netip.MustParseAddr(address)); !errors.As(err, &refused) || refused.Class != class {
			t.Errorf("check(%s) = %v, want class %s even with an allow-all list", address, err, class)
		}
	}
	for _, address := range []string{"10.1.2.3", "127.0.0.1", "fd00::1", "192.168.1.1", "93.184.216.34"} {
		if err := check(netip.MustParseAddr(address)); err != nil {
			t.Errorf("check(%s) = %v, want allowed", address, err)
		}
	}
}

func TestCheckDefaultDeniesEveryNonPublicClass(t *testing.T) {
	allowOnly(t)
	_ = captureLogs(t)
	for _, address := range []string{"10.1.2.3", "172.16.0.1", "192.168.1.1", "127.0.0.1", "::1", "fd00::1", "::ffff:10.0.0.1", "240.0.0.1", "100.64.0.1"} {
		if err := check(netip.MustParseAddr(address)); !errors.Is(err, ErrRefused) {
			t.Errorf("check(%s) = %v, want refused", address, err)
		}
	}
	for _, address := range []string{"93.184.216.34", "8.8.8.8", "2606:2800:220:1::1"} {
		if err := check(netip.MustParseAddr(address)); err != nil {
			t.Errorf("check(%s) = %v, want allowed", address, err)
		}
	}
}

func TestParseAllowFailsClosed(t *testing.T) {
	logs := captureLogs(t)
	if got := parseAllow(""); len(got) != 0 {
		t.Fatalf("empty = %v", got)
	}
	got := parseAllow(" 10.20.0.0/16 , fd00::/8,,192.168.1.7/24")
	want := []netip.Prefix{netip.MustParsePrefix("10.20.0.0/16"), netip.MustParsePrefix("fd00::/8"), netip.MustParsePrefix("192.168.1.0/24")}
	if len(got) != len(want) {
		t.Fatalf("parse = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parse = %v, want %v", got, want)
		}
	}
	for _, bad := range []string{"10.0.0.1", "10.0.0.0/8,gitlab.internal", "10.0.0.0/33"} {
		if got := parseAllow(bad); got != nil {
			t.Errorf("parseAllow(%q) = %v, want nil (fail closed)", bad, got)
		}
	}
	if !strings.Contains(logs.String(), AllowEnv) || strings.Contains(logs.String(), "gitlab.internal") {
		t.Fatalf("the parse error log must name the setting and no entry: %s", logs.String())
	}
}

// A name that resolves to a public AND a private address is refused as a whole, and nothing is dialled.
func TestWrapRefusesWhenAnyAnswerIsNonPublic(t *testing.T) {
	allowOnly(t)
	_ = captureLogs(t)
	mapNames(t, map[string][]netip.Addr{"mixed.example": {netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("10.0.0.1")}})
	var dials atomic.Int32
	source := httpguard.Transport()
	source.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("dialled")
	}
	client := &http.Client{Transport: Wrap(source), Timeout: 5 * time.Second}
	if err := get(t, client, "http://mixed.example/"); !errors.Is(err, ErrRefused) {
		t.Fatalf("error = %v, want the egress refusal", err)
	}
	if dials.Load() != 0 {
		t.Fatalf("%d dials, want none", dials.Load())
	}
}

// DNS rebinding: the name is resolved once and the connection goes to that checked address literal, so no second
// lookup can land somewhere else between check and connect.
func TestWrapDialsTheCheckedAddressAfterOneLookup(t *testing.T) {
	allowOnly(t, "127.0.0.0/8")
	server, hits := countingServer(t)
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	lookups := mapNames(t, map[string][]netip.Addr{"provider.example": {netip.MustParseAddr("127.0.0.1")}})
	var dialled []string
	source := httpguard.Transport()
	source.DisableKeepAlives = true
	source.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialled = append(dialled, address)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	client := &http.Client{Transport: Wrap(source), Timeout: 5 * time.Second}
	if err := get(t, client, "http://provider.example:"+port+"/"); err != nil {
		t.Fatal(err)
	}
	if lookups.Load() != 1 || len(dialled) != 1 || dialled[0] != "127.0.0.1:"+port || hits.Load() != 1 {
		t.Fatalf("lookups=%d dialled=%v hits=%d, want one lookup and one dial to the checked literal", lookups.Load(), dialled, hits.Load())
	}
}

// Dialling the checked address keeps TLS on the request's name: SNI and Host are the stored host, and the certificate
// is verified against it.
func TestWrapKeepsSNIAndHostOnTheStoredName(t *testing.T) {
	allowOnly(t, "127.0.0.0/8")
	var serverName, host atomic.Value
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverName.Store(r.TLS.ServerName)
		host.Store(r.Host)
	}))
	defer server.Close()
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	mapNames(t, map[string][]netip.Addr{"example.com": {netip.MustParseAddr("127.0.0.1")}})
	client := &http.Client{Transport: Wrap(server.Client().Transport), Timeout: 5 * time.Second}
	if err := get(t, client, "https://example.com:"+port+"/"); err != nil {
		t.Fatalf("TLS GET through the guard: %v", err)
	}
	if serverName.Load() != "example.com" || host.Load() != "example.com:"+port {
		t.Fatalf("SNI=%v Host=%v, want example.com and example.com:%s", serverName.Load(), host.Load(), port)
	}
}

// A redirect hop dials through the same guard: a 302 from an allowed host to a blocked one is refused at its connect.
func TestWrapRefusesARedirectHopToABlockedAddress(t *testing.T) {
	allowOnly(t, "127.0.0.1/32")
	_ = captureLogs(t)
	listener, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Skipf("no 127.0.0.2 on this host: %v", err)
	}
	var blockedHits atomic.Int32
	blocked := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { blockedHits.Add(1) }))
	blocked.Listener.Close()
	blocked.Listener = listener
	blocked.Start()
	defer blocked.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, blocked.URL+"/next", http.StatusFound)
	}))
	defer origin.Close()
	client := &http.Client{Transport: Wrap(nil), Timeout: 5 * time.Second}
	if err := get(t, client, origin.URL); !errors.Is(err, ErrRefused) {
		t.Fatalf("error = %v, want the egress refusal on the redirect hop", err)
	}
	if blockedHits.Load() != 0 {
		t.Fatal("the redirect target received a request")
	}
}

// A proxy is never a bypass: the guarded transport has no Proxy, even when the transport it guards had one, so the
// connection goes to the checked target address, never to the proxy.
func TestWrapNeverDialsAProxy(t *testing.T) {
	allowOnly(t, "127.0.0.0/8")
	_ = captureLogs(t)
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var proxyAccepts atomic.Int32
	go func() {
		for {
			conn, err := proxyListener.Accept()
			if err != nil {
				return
			}
			proxyAccepts.Add(1)
			conn.Close()
		}
	}()
	defer proxyListener.Close()
	proxyURL := &url.URL{Scheme: "http", Host: proxyListener.Addr().String()}
	t.Setenv("HTTP_PROXY", proxyURL.String())
	t.Setenv("HTTPS_PROXY", proxyURL.String())
	server, hits := countingServer(t)
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	mapNames(t, map[string][]netip.Addr{
		"provider.example": {netip.MustParseAddr("127.0.0.1")},
		"internal.example": {netip.MustParseAddr("10.0.0.1")},
	})
	source := httpguard.Transport()
	source.Proxy = http.ProxyURL(proxyURL)
	for _, rt := range []http.RoundTripper{Wrap(source), Wrap(nil)} {
		if transport, ok := rt.(*http.Transport); !ok || transport.Proxy != nil {
			t.Fatalf("guarded transport %T has a proxy", rt)
		}
		client := &http.Client{Transport: rt, Timeout: 5 * time.Second}
		if err := get(t, client, "http://provider.example:"+port+"/"); err != nil {
			t.Fatalf("GET to an allowed target: %v", err)
		}
		if err := get(t, client, "http://internal.example:"+port+"/"); !errors.Is(err, ErrRefused) {
			t.Fatalf("GET to a blocked target: %v, want the egress refusal", err)
		}
	}
	if proxyAccepts.Load() != 0 || hits.Load() != 2 {
		t.Fatalf("proxy accepts=%d target hits=%d, want 0 and 2", proxyAccepts.Load(), hits.Load())
	}
}

type wrapperTransport struct{ inner http.RoundTripper }

func (w wrapperTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return w.inner.RoundTrip(r)
}
func (w wrapperTransport) UnwrapTransport() http.RoundTripper { return w.inner }
func (w wrapperTransport) RewrapTransport(inner http.RoundTripper) http.RoundTripper {
	return wrapperTransport{inner: inner}
}

type fakeTransport struct{}

func (fakeTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, errors.New("fake") }

func TestWrapShapes(t *testing.T) {
	shared := Wrap(nil)
	if Wrap(httpguard.SharedTransport()) != shared || Wrap((*http.Transport)(nil)) != shared {
		t.Fatal("nil, a typed nil and httpguard.SharedTransport() must share one guarded transport (one pool)")
	}
	if Wrap(shared) != shared {
		t.Fatal("guarding a guarded transport must return it unchanged")
	}
	source := httpguard.Transport()
	if Wrap(source) != Wrap(source) {
		t.Fatal("one source transport must map to one guarded clone")
	}
	if source.DialContext == nil && Wrap(source).(*http.Transport).DialContext == nil {
		t.Fatal("the guarded clone has no guarded dialer")
	}
	wrapped := Wrap(wrapperTransport{inner: httpguard.SharedTransport()}).(wrapperTransport)
	if wrapped.inner != shared {
		t.Fatal("a TransportWrapper must be rebuilt around the guarded inner transport")
	}
	if _, ok := Wrap(fakeTransport{}).(fakeTransport); !ok {
		t.Fatal("a test double must be returned as it is")
	}
	custom := httpguard.Transport()
	custom.DialTLSContext = func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("custom") }
	_ = captureLogs(t)
	if _, err := Wrap(custom).RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/", nil)); !errors.Is(err, ErrRefused) {
		t.Fatalf("a transport with its own TLS dialer: %v, want the refusal (the guard cannot see that dial)", err)
	}
}

// The allow list is loosened only by its environment setting; SetAllowForTest is for tests.
func TestNoProductionCallOfTheTestSeams(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", root, err)
	}
	var offenders []string
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, _ := filepath.Rel(root, path)
		if relative == filepath.Join("internal", "egressguard", "egressguard.go") || strings.HasPrefix(relative, filepath.Join("internal", "testsupport")+string(filepath.Separator)) {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), "SetAllowForTest(") || strings.Contains(string(content), "SetLookupForTest(") {
			offenders = append(offenders, relative)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("production files call a test seam of egressguard: %v", offenders)
	}
}
