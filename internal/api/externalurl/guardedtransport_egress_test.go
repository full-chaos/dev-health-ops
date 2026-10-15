package externalurl

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDNS answers A queries over UDP on loopback: the first answer is public,
// every later one is 127.0.0.1 -- a name that passes a check-time lookup and
// rebinds to an internal address by dial time. AAAA queries get an empty answer.
type fakeDNS struct {
	conn    net.PacketConn
	aCalls  atomic.Int32
	publicA [4]byte
}

func newFakeDNS(t *testing.T) *fakeDNS {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeDNS{conn: conn, publicA: [4]byte{93, 184, 216, 34}}
	go server.serve()
	t.Cleanup(func() { conn.Close() })
	return server
}

func (d *fakeDNS) serve() {
	buffer := make([]byte, 512)
	for {
		n, from, err := d.conn.ReadFrom(buffer)
		if err != nil {
			return
		}
		query := append([]byte(nil), buffer[:n]...)
		d.conn.WriteTo(d.answer(query), from)
	}
}

func (d *fakeDNS) answer(query []byte) []byte {
	end := 12
	for end < len(query) && query[end] != 0 {
		end += int(query[end]) + 1
	}
	end += 5 // root label + qtype + qclass
	question := query[12:end]
	qtype := binary.BigEndian.Uint16(query[end-4 : end-2])
	reply := append([]byte(nil), query[:2]...)
	flags := []byte{0x81, 0x80}
	answers := uint16(0)
	var record []byte
	if qtype == 1 {
		address := [4]byte{127, 0, 0, 1}
		if d.aCalls.Add(1) == 1 {
			address = d.publicA
		}
		answers = 1
		record = append([]byte{0xC0, 0x0C, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4}, address[:]...)
	}
	reply = append(reply, flags...)
	reply = append(reply, 0, 1, byte(answers>>8), byte(answers), 0, 0, 0, 0)
	reply = append(reply, question...)
	return append(reply, record...)
}

func (d *fakeDNS) resolver() *net.Resolver {
	address := d.conn.LocalAddr().String()
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp", address)
	}}
}

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

// CHAOS-2558: DNS rebinding. The URL's host resolves public when Validate
// checks it and resolves to loopback when the transport dials; the dial is
// refused before any request byte (a bearer key included) is written.
func TestGuardedTransportRefusesAHostThatRebindsAfterTheCheck(t *testing.T) {
	dns := newFakeDNS(t)
	previous := dialResolver
	dialResolver = dns.resolver()
	t.Cleanup(func() { dialResolver = previous })

	reached := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached <- r.Header.Get("Authorization")
	}))
	defer server.Close()
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	target := "http://rebind.example:" + port + "/v1"

	lookup := func(ctx context.Context, host string) ([]netip.Addr, error) {
		return resolveWith(ctx, dns.resolver(), host)
	}
	if ok, detail := Validate(context.Background(), target, lookup); !ok {
		t.Fatalf("the check-time lookup must see the public address, got %q", detail)
	}

	var logs syncBuffer
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(logger) })

	client := &http.Client{Transport: GuardedTransport(), Timeout: 5 * time.Second}
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret-token")
	response, err := client.Do(request)
	if err == nil {
		response.Body.Close()
		t.Fatalf("the rebound host was reached (status %d)", response.StatusCode)
	}
	if !strings.Contains(err.Error(), "Connection to private/internal networks is not allowed") {
		t.Fatalf("error = %v, want the internal-network refusal", err)
	}
	select {
	case token := <-reached:
		t.Fatalf("the server received a request (Authorization %q)", token)
	default:
	}
	if dns.aCalls.Load() < 2 {
		t.Fatalf("the dial never re-resolved the name (%d A queries): the test did not rebind", dns.aCalls.Load())
	}
	if !strings.Contains(logs.String(), `"class":"loopback"`) {
		t.Fatalf("no refusal log with the class: %s", logs.String())
	}
	for _, leak := range []string{"rebind.example", "127.0.0.1", "secret-token"} {
		if strings.Contains(logs.String(), leak) {
			t.Fatalf("the refusal log carries %q: %s", leak, logs.String())
		}
	}
}

func TestRefuseInternalAddressBlocksEveryInternalClass(t *testing.T) {
	for address, class := range map[string]string{
		"10.1.2.3:443":           "private",
		"172.16.0.1:443":         "private",
		"192.168.1.1:443":        "private",
		"127.0.0.1:443":          "loopback",
		"[::1]:443":              "loopback",
		"169.254.169.254:80":     "metadata",
		"169.254.1.1:80":         "link_local",
		"[fe80::1]:443":          "link_local",
		"[fd00::1]:443":          "unique_local",
		"[fc00::1]:443":          "unique_local",
		"224.0.0.1:443":          "multicast",
		"239.255.255.250:443":    "multicast",
		"[ff02::1]:443":          "multicast",
		"0.0.0.0:443":            "unspecified",
		"[::]:443":               "unspecified",
		"[::ffff:10.0.0.1]:443":  "private",
		"[::ffff:127.0.0.1]:443": "loopback",
		"240.0.0.1:443":          "reserved",
		"not-an-address":         "invalid_address",
		"[not-an-ip]:443":        "invalid_address",
	} {
		var logs syncBuffer
		logger := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		err := refuseInternalAddress("tcp", address, nil)
		slog.SetDefault(logger)
		if err == nil {
			t.Errorf("%s was not refused", address)
			continue
		}
		if !strings.Contains(logs.String(), `"class":"`+class+`"`) {
			t.Errorf("%s: log %s lacks class %q", address, logs.String(), class)
		}
	}
}

func TestRefuseInternalAddressAllowsAPublicHost(t *testing.T) {
	for _, address := range []string{"93.184.216.34:443", "[2606:2800:220:1::1]:443", "8.8.8.8:443"} {
		if err := refuseInternalAddress("tcp", address, nil); err != nil {
			t.Errorf("%s was refused: %v", address, err)
		}
	}
}

func resolveWith(ctx context.Context, resolver *net.Resolver, host string) ([]netip.Addr, error) {
	ips, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	out := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		if addr, ok := netip.AddrFromSlice(ip.IP); ok {
			out = append(out, addr)
		}
	}
	return out, nil
}
