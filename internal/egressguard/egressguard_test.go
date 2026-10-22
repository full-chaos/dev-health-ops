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
	"runtime"
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

// An allow list that covers the range a metadata address sits in (RFC 6598 for Alibaba's 100.100.100.200, NAT64 and
// 6to4 for an embedded 169.254.169.254, the AWS IPv6 ULA) still never admits the metadata address itself.
func TestCheckNeverAdmitsAMetadataAddressInsideAnAllowedRange(t *testing.T) {
	allowOnly(t, "100.64.0.0/10", "64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "fd00::/8")
	_ = captureLogs(t)
	for _, address := range []string{"100.100.100.200", "64:ff9b::a9fe:a9fe", "64:ff9b:1::a9fe:a9fe", "2002:a9fe:a9fe::", "fd00:ec2::254"} {
		var refused *RefusedError
		if err := check(netip.MustParseAddr(address)); !errors.As(err, &refused) || refused.Class != "metadata" {
			t.Errorf("check(%s) = %v, want the metadata refusal", address, err)
		}
	}
	for _, address := range []string{"64:ff9b::", "64:ff9b:1::", "2002::"} {
		var refused *RefusedError
		if err := check(netip.MustParseAddr(address)); !errors.As(err, &refused) || refused.Class != "unspecified" {
			t.Errorf("check(%s) = %v, want the unspecified refusal (it embeds 0.0.0.0)", address, err)
		}
	}
	if err := check(netip.MustParseAddr("100.64.0.1")); err != nil {
		t.Errorf("an allowed shared address was refused: %v", err)
	}
}

// A translation address (NAT64, 6to4, Teredo) is decided on the IPv4 address it embeds: an allow list for the IPv6
// translation range never admits a non-public embedded target, and a public embedded target is public.
func TestCheckDecidesATranslationAddressOnItsEmbeddedIPv4(t *testing.T) {
	allowOnly(t, "64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "2001::/32")
	_ = captureLogs(t)
	for address, class := range map[string]string{
		"64:ff9b::7f00:1": "loopback", "64:ff9b::a00:1": "private", "2002:7f00:1::": "loopback", "2002:a00:1::": "private",
		"2001:0:a00:1::": "private", "64:ff9b::6440:1": "shared_address",
	} {
		var refused *RefusedError
		if err := check(netip.MustParseAddr(address)); !errors.As(err, &refused) || refused.Class != class {
			t.Errorf("check(%s) = %v, want the %s refusal", address, err, class)
		}
	}
	for _, address := range []string{"64:ff9b::808:808", "2002:808:808::"} {
		if err := check(netip.MustParseAddr(address)); err != nil {
			t.Errorf("check(%s) = %v, want allowed (it embeds a public address)", address, err)
		}
	}
	// The embedded IPv4 is what an allow list admits.
	t.Cleanup(SetAllowForTest(netip.MustParsePrefix("10.0.0.0/8")))
	if err := check(netip.MustParseAddr("64:ff9b::a00:1")); err != nil {
		t.Errorf("check(64:ff9b::a00:1) with 10.0.0.0/8 allowed = %v, want allowed", err)
	}
	if err := check(netip.MustParseAddr("64:ff9b::7f00:1")); err == nil {
		t.Error("check(64:ff9b::7f00:1) with 10.0.0.0/8 allowed was admitted")
	}
}

// The IPv6 and the less common metadata endpoints are never admitted, even with an allow list for their range.
// The IPv4-compatible (::/96) and IPv4-translated (::ffff:0:0:0/96) forms of a metadata address are metadata even with
// every IPv6 address allowed.
func TestCheckNeverAdmitsTheIPv4CompatibleAndTranslatedMetadataForms(t *testing.T) {
	allowOnly(t, "::/0")
	_ = captureLogs(t)
	for _, address := range []string{"::a9fe:a9fe", "::ffff:0:a9fe:a9fe"} {
		var refused *RefusedError
		if err := check(netip.MustParseAddr(address)); !errors.As(err, &refused) || refused.Class != "metadata" {
			t.Errorf("check(%s) = %v, want the metadata refusal", address, err)
		}
	}
	for _, address := range []string{"::a00:1", "::ffff:0:7f00:1"} {
		if err := check(netip.MustParseAddr(address)); err == nil {
			t.Errorf("check(%s) was admitted: an allow list for ::/0 must not admit an embedded non-public IPv4", address)
		}
	}
}

func TestCheckNeverAdmitsTheIPv6AndWireServerMetadataEndpoints(t *testing.T) {
	allowOnly(t, "fd00::/8", "fc00::/7", "168.63.0.0/16")
	_ = captureLogs(t)
	for _, address := range []string{"fd20:ce::254", "fd00:c1::a9fe:a9fe", "fd12:3456:789a::a9fe:a9fe", "fc00::a9fe:a9fe", "168.63.129.16"} {
		var refused *RefusedError
		if err := check(netip.MustParseAddr(address)); !errors.As(err, &refused) || refused.Class != "metadata" {
			t.Errorf("check(%s) = %v, want the metadata refusal", address, err)
		}
	}
	if err := check(netip.MustParseAddr("fd00::1")); err != nil {
		t.Errorf("an allowed unique-local address was refused: %v", err)
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

type offlineTransport struct{ fakeTransport }

func (offlineTransport) EgressOffline() {}

// recordingTransport is a RoundTripper the guard cannot reach into: it would send the request somewhere unchecked.
type recordingTransport struct{ calls *atomic.Int32 }

func (r recordingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls.Add(1)
	return nil, errors.New("sent unchecked")
}

// A RoundTripper the guard cannot reach into (not an *http.Transport, a TransportWrapper or Offline) is refused, fail
// closed: the request never reaches it, whatever URL it names. A test binary may let its doubles through explicitly.
func TestWrapRefusesATransportItCannotReachInto(t *testing.T) {
	_ = captureLogs(t)
	var calls atomic.Int32
	client := &http.Client{Transport: Wrap(recordingTransport{calls: &calls}), Timeout: 5 * time.Second}
	err := get(t, client, "http://169.254.169.254/latest/meta-data/")
	var refused *RefusedError
	if !errors.As(err, &refused) || refused.Class != "unguarded_transport" || calls.Load() != 0 {
		t.Fatalf("err=%v calls=%d, want the unguarded_transport refusal and no call", err, calls.Load())
	}
	t.Cleanup(SetUnguardedTransportsForTest(true))
	if _, ok := Wrap(recordingTransport{calls: &calls}).(recordingTransport); !ok {
		t.Fatal("SetUnguardedTransportsForTest(true) must let a test double through")
	}
}

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
	if _, ok := Wrap(offlineTransport{}).(offlineTransport); !ok {
		t.Fatal("an Offline transport must be returned as it is")
	}
	custom := httpguard.Transport()
	custom.DialTLSContext = func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("custom") }
	_ = captureLogs(t)
	if _, err := Wrap(custom).RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.com/", nil)); !errors.Is(err, ErrRefused) {
		t.Fatalf("a transport with its own TLS dialer: %v, want the refusal (the guard cannot see that dial)", err)
	}
}

// seamNames are the identifiers no production file may mention: the test seams and the Offline marker method. Any
// mention counts (a call, a method value, a declaration), so an alias or a stored func value cannot hide one.
var seamNames = []string{"SetAllowForTest", "SetLookupForTest", "SetUnguardedTransportsForTest", "EgressOffline"}

// scanForSeams walks the production .go files under root (not *_test.go, testdata, internal/testsupport, or this
// package's own source) and returns the files that mention a seam name, and how many files it read.
func scanForSeams(root string) (offenders []string, scanned int, err error) {
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
		scanned++
		for _, name := range seamNames {
			if strings.Contains(string(content), name) {
				offenders = append(offenders, relative)
				break
			}
		}
		return nil
	})
	return offenders, scanned, err
}

// The allow list is loosened only by its environment setting; the test seams and the Offline marker are for tests.
func TestNoProductionCallOfTheTestSeams(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", root, err)
	}
	offenders, scanned, err := scanForSeams(root)
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 1000 {
		t.Fatalf("the scan read %d production files: it reads too little", scanned)
	}
	if len(offenders) > 0 {
		t.Fatalf("production files mention a test seam of egressguard: %v", offenders)
	}
	t.Logf("%d production files scanned", scanned)
}

// Positive control: planted offenders (a call, a method value, a marker method) are found; a test file, a testsupport
// file and the guard's own source are not.
func TestTheSeamScanFindsPlantedOffenders(t *testing.T) {
	root := t.TempDir()
	plant := func(relative, content string) {
		path := filepath.Join(root, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	plant("internal/a/a.go", "package a\nfunc f() { egressguard.SetAllowForTest() }\n")
	plant("internal/b/b.go", "package b\nvar seam = eg.SetUnguardedTransportsForTest\n")
	plant("internal/c/c.go", "package c\ntype t struct{}\nfunc (t) EgressOffline() {}\n")
	plant("internal/d/d_test.go", "package d\nfunc f() { egressguard.SetAllowForTest() }\n")
	plant("internal/testsupport/e/e.go", "package e\nfunc f() { egressguard.SetLookupForTest(nil) }\n")
	plant("internal/egressguard/egressguard.go", "package egressguard\nfunc SetAllowForTest() {}\n")
	plant("internal/clean/clean.go", "package clean\n")
	offenders, scanned, err := scanForSeams(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join("internal", "a", "a.go"), filepath.Join("internal", "b", "b.go"), filepath.Join("internal", "c", "c.go")}
	if strings.Join(offenders, ",") != strings.Join(want, ",") || scanned != 4 {
		t.Fatalf("offenders=%v scanned=%d, want %v and 4", offenders, scanned, want)
	}
}

// nilRewrapTransport is a TransportWrapper whose rebuild returns nil (or a typed nil): an http.Client would then fall
// back to http.DefaultTransport, unguarded and with the environment proxy.
type nilRewrapTransport struct{ typed bool }

func (nilRewrapTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("not used")
}
func (nilRewrapTransport) UnwrapTransport() http.RoundTripper { return nil }
func (w nilRewrapTransport) RewrapTransport(http.RoundTripper) http.RoundTripper {
	if w.typed {
		var transport *http.Transport
		return transport
	}
	return nil
}

func TestWrapRefusesAWrapperWhoseRebuildReturnsNil(t *testing.T) {
	allowOnly(t)
	_ = captureLogs(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	for _, typed := range []bool{false, true} {
		rt := Wrap(nilRewrapTransport{typed: typed})
		if rt == nil {
			t.Fatalf("typed=%v: Wrap returned nil (an http.Client would use http.DefaultTransport)", typed)
		}
		client := &http.Client{Transport: rt, Timeout: 5 * time.Second}
		var refused *RefusedError
		if err := get(t, client, server.URL); !errors.As(err, &refused) || refused.Class != "unguarded_transport" {
			t.Fatalf("typed=%v: GET = %v, want the unguarded_transport refusal", typed, err)
		}
	}
}

type trackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func newTrackedConn() *trackedConn {
	client, server := net.Pipe()
	_ = server.Close()
	return &trackedConn{Conn: client}
}

type dialRecord struct {
	mutex sync.Mutex
	calls []string
}

func (r *dialRecord) add(network, address string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.calls = append(r.calls, network+" "+address)
}

func (r *dialRecord) all() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return append([]string(nil), r.calls...)
}

func settledGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if runtime.NumGoroutine() <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want <= %d: a racing dial leaked", runtime.NumGoroutine(), before)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

const (
	publicV6 = "2606:4700::1111"
	publicV4 = "1.1.1.1"
)

// A dual-stack name whose first family never answers falls back to the other family after about 300 ms (RFC 8305, the
// net.Dialer rule), in either order, and the dead attempt is cancelled.
func TestGuardedDialFallsBackToTheOtherFamily(t *testing.T) {
	for _, order := range [][2]string{{publicV6, publicV4}, {publicV4, publicV6}} {
		t.Run(order[0], func(t *testing.T) {
			allowOnly(t)
			mapNames(t, map[string][]netip.Addr{"dual.example": {netip.MustParseAddr(order[0]), netip.MustParseAddr(order[1])}})
			before := runtime.NumGoroutine()
			var record dialRecord
			var cancelled atomic.Bool
			winner := newTrackedConn()
			inner := func(ctx context.Context, network, address string) (net.Conn, error) {
				record.add(network, address)
				host, _, _ := net.SplitHostPort(address)
				if host == order[0] {
					<-ctx.Done()
					cancelled.Store(true)
					return nil, ctx.Err()
				}
				return winner, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started := time.Now()
			conn, err := guardedDial(inner)(ctx, "tcp", "dual.example:443")
			elapsed := time.Since(started)
			if err != nil || conn != net.Conn(winner) {
				t.Fatalf("conn=%v err=%v, want the other family's connection", conn, err)
			}
			if elapsed < 250*time.Millisecond || elapsed > time.Second {
				t.Fatalf("dial took %v, want about 300 ms", elapsed)
			}
			deadline := time.Now().Add(2 * time.Second)
			for !cancelled.Load() && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if !cancelled.Load() {
				t.Fatal("the dead primary attempt was not cancelled")
			}
			settledGoroutines(t, before)
		})
	}
}

// A primary that fails at once starts the fallback at once (no 300 ms wait).
func TestGuardedDialStartsTheFallbackWhenThePrimaryFails(t *testing.T) {
	allowOnly(t)
	mapNames(t, map[string][]netip.Addr{"dual.example": {netip.MustParseAddr(publicV6), netip.MustParseAddr(publicV4)}})
	inner := func(_ context.Context, _, address string) (net.Conn, error) {
		if strings.HasPrefix(address, "[") {
			return nil, errors.New("v6 unreachable")
		}
		return newTrackedConn(), nil
	}
	started := time.Now()
	if _, err := guardedDial(inner)(context.Background(), "tcp", "dual.example:443"); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("dial took %v, want an immediate fallback", elapsed)
	}
}

// The slow primary that connects after the fallback already won is closed, not leaked.
func TestGuardedDialClosesTheLosingConnection(t *testing.T) {
	allowOnly(t)
	mapNames(t, map[string][]netip.Addr{"dual.example": {netip.MustParseAddr(publicV6), netip.MustParseAddr(publicV4)}})
	before := runtime.NumGoroutine()
	slow, fast := newTrackedConn(), newTrackedConn()
	inner := func(_ context.Context, _, address string) (net.Conn, error) {
		if strings.HasPrefix(address, "[") {
			time.Sleep(600 * time.Millisecond)
			return slow, nil
		}
		return fast, nil
	}
	conn, err := guardedDial(inner)(context.Background(), "tcp", "dual.example:443")
	if err != nil || conn != net.Conn(fast) {
		t.Fatalf("conn=%v err=%v, want the fallback connection", conn, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !slow.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !slow.closed.Load() || fast.closed.Load() {
		t.Fatalf("slow closed=%v fast closed=%v, want the loser closed and the winner open", slow.closed.Load(), fast.closed.Load())
	}
	settledGoroutines(t, before)
}

// Both families dead: the primary's error comes back, nothing leaks, and a cancelled context ends the dial.
func TestGuardedDialReturnsThePrimaryErrorWhenEveryFamilyFails(t *testing.T) {
	allowOnly(t)
	mapNames(t, map[string][]netip.Addr{"dual.example": {netip.MustParseAddr(publicV6), netip.MustParseAddr(publicV4)}})
	before := runtime.NumGoroutine()
	primaryErr := errors.New("primary down")
	inner := func(_ context.Context, _, address string) (net.Conn, error) {
		if strings.HasPrefix(address, "[") {
			time.Sleep(400 * time.Millisecond)
			return nil, primaryErr
		}
		return nil, errors.New("fallback down")
	}
	if _, err := guardedDial(inner)(context.Background(), "tcp", "dual.example:443"); !errors.Is(err, primaryErr) {
		t.Fatalf("error = %v, want the primary error", err)
	}
	settledGoroutines(t, before)

	blocked := func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	started := time.Now()
	if _, err := guardedDial(blocked)(ctx, "tcp", "dual.example:443"); err == nil || time.Since(started) > time.Second {
		t.Fatalf("a cancelled context must end the dial promptly (err=%v after %v)", err, time.Since(started))
	}
	settledGoroutines(t, before)
}

// Inside one family the time left is split across the addresses (net/dial.go partialDeadline): a dead first address
// cannot eat the whole budget.
func TestGuardedDialSplitsTheDeadlineAcrossOneFamily(t *testing.T) {
	allowOnly(t)
	mapNames(t, map[string][]netip.Addr{"multi.example": {netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("1.0.0.1")}})
	inner := func(ctx context.Context, _, address string) (net.Conn, error) {
		if strings.HasPrefix(address, "1.1.1.1:") {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return newTrackedConn(), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started := time.Now()
	if _, err := guardedDial(inner)(ctx, "tcp", "multi.example:443"); err != nil {
		t.Fatalf("error = %v, want the second address to connect inside its share", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("dial took %v, want about 2 s (half of the budget)", elapsed)
	}
}

// The security pin: whatever the race does, only checked address literals reach the dialer, and one non-public answer
// (in any position, any family, any network) refuses the whole dial with zero dialler calls.
func TestGuardedDialOnlyDialsCheckedLiterals(t *testing.T) {
	allowOnly(t)
	bad := []string{"127.0.0.1", "::1", "10.0.0.1", "192.168.1.1", "169.254.169.254", "169.254.1.1", "fd00:ec2::254", "fe80::1", "100.64.0.1"}
	for _, network := range []string{"tcp", "tcp4", "tcp6"} {
		for _, badAddr := range bad {
			for _, answers := range [][]string{{badAddr, publicV6, publicV4}, {publicV6, publicV4, badAddr}, {publicV4, badAddr, publicV6}} {
				name := network + "/" + badAddr + "/" + answers[0]
				t.Run(name, func(t *testing.T) {
					parsed := make([]netip.Addr, len(answers))
					for i, answer := range answers {
						parsed[i] = netip.MustParseAddr(answer)
					}
					mapNames(t, map[string][]netip.Addr{"mixed.example": parsed})
					var record dialRecord
					inner := func(_ context.Context, network, address string) (net.Conn, error) {
						record.add(network, address)
						return newTrackedConn(), nil
					}
					_, err := guardedDial(inner)(context.Background(), network, "mixed.example:443")
					if !errors.Is(err, ErrRefused) {
						t.Fatalf("error = %v, want the egress refusal", err)
					}
					if calls := record.all(); len(calls) != 0 {
						t.Fatalf("dialler called %v on a refused answer", calls)
					}
				})
			}
		}
	}

	for _, network := range []string{"tcp", "tcp4", "tcp6"} {
		t.Run("checked/"+network, func(t *testing.T) {
			mapNames(t, map[string][]netip.Addr{"ok.example": {netip.MustParseAddr(publicV6), netip.MustParseAddr(publicV4)}})
			var record dialRecord
			inner := func(_ context.Context, network, address string) (net.Conn, error) {
				record.add(network, address)
				return nil, errors.New("down")
			}
			_, _ = guardedDial(inner)(context.Background(), network, "ok.example:443")
			want := map[string]bool{}
			switch network {
			case "tcp":
				want["tcp ["+publicV6+"]:443"], want["tcp "+publicV4+":443"] = true, true
			case "tcp4":
				want["tcp4 "+publicV4+":443"] = true
			case "tcp6":
				want["tcp6 ["+publicV6+"]:443"] = true
			}
			got := record.all()
			if len(got) != len(want) {
				t.Fatalf("dialled %v, want exactly %v", got, want)
			}
			for _, call := range got {
				if !want[call] {
					t.Fatalf("dialled unchecked or wrong-network address %q (want %v)", call, want)
				}
			}
		})
	}
}

// The whole path through Wrap: an http.Client, a dual-stack name whose IPv6 answer never connects and whose IPv4
// answer is a real loopback listener (allowed), succeeds within about a second.
func TestWrapFallsBackPerFamilyThroughTheTransport(t *testing.T) {
	allowOnly(t, "127.0.0.0/8", "::1/128")
	server, hits := countingServer(t)
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	mapNames(t, map[string][]netip.Addr{"dual.example": {netip.MustParseAddr("::1"), netip.MustParseAddr("127.0.0.1")}})
	var record dialRecord
	source := httpguard.Transport()
	source.DisableKeepAlives = true
	source.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		record.add(network, address)
		if strings.HasPrefix(address, "[::1]") {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	client := &http.Client{Transport: Wrap(source), Timeout: 5 * time.Second}
	started := time.Now()
	if err := get(t, client, "http://dual.example:"+port+"/"); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second || hits.Load() != 1 {
		t.Fatalf("took %v with %d hits, want one hit within about a second (dialled %v)", elapsed, hits.Load(), record.all())
	}
}
