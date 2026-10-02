package llmorgsettings

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// The SSRF guard refuses a base_url whose address is not a safe public target. Python's rule
// (llm/credentials.py _ip_is_safe_public_target) is is_global and not loopback, private, link-local, multicast,
// unspecified or reserved, over the interpreter's special-purpose tables. This file holds the Go guard to the recorded
// Python answer for every special-purpose class: for each range of the tables (and each range's global exceptions) the
// first, an inner and the last address, the address just below and just above, in v4 form and in IPv4-mapped IPv6 form.
//
// The Python answers were recorded once on the pinned build (testdata/golden/ssrf_address_classes.json; recipe in the
// golden's spec) and are read from it: a frozen run starts no Python. The probe list is part of the request, so a changed
// list is refused until it is recorded again.

const pythonAddressProgram = `
import json, sys
from dev_health_ops.llm.credentials import _ip_is_safe_public_target
answers = {}
for address in json.loads(sys.stdin.read()):
    answers[address] = bool(_ip_is_safe_public_target(address))
print(json.dumps(answers, sort_keys=True))
`

// referencePrefixes are the special-purpose ranges the probe list is built from. They are the tables of CPython's
// ipaddress module (IPv4Address/IPv6Address _private_networks, _private_networks_exceptions, _reserved_networks, plus the
// 100.64.0.0/10 carve-out of is_global) and the ranges of the netip helpers the guard also uses. They only choose WHICH
// addresses are probed; what each answer is comes from the recording, not from this list.
var referencePrefixes = []string{
	// IPv4
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24",
	"192.0.0.0/29", "192.0.0.170/31", "192.0.0.9/32", "192.0.0.10/32", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16",
	"198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "255.255.255.255/32",
	// IPv6 private networks and their exceptions
	"::1/128", "::/128", "::ffff:0:0/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32",
	"2002::/16", "3fff::/20", "fc00::/7", "fe80::/10", "fec0::/10", "ff00::/8",
	"2001:1::1/128", "2001:1::2/128", "2001:3::/32", "2001:4:112::/48", "2001:20::/28", "2001:30::/28",
	// IPv6 reserved networks
	"::/8", "100::/8", "200::/7", "400::/6", "800::/5", "1000::/4", "4000::/3", "6000::/3", "8000::/3", "a000::/3",
	"c000::/3", "e000::/4", "f000::/5", "f800::/6", "fe00::/9",
	// Global unicast: the space the rule accepts
	"2000::/3", "2400::/12", "2600::/12", "2a00::/12",
}

// publicProbes are addresses of public services that must stay accepted.
var publicProbes = []string{
	"1.1.1.1", "8.8.8.8", "9.9.9.9", "93.184.216.34", "104.18.33.45", "160.79.104.10", "172.15.255.255", "172.32.0.0",
	"2606:4700:4700::1111", "2001:4860:4860::8888", "2a00:1450:4001:80b::200e", "2620:0:ccc::2",
}

// probeAddresses is the sorted, de-duplicated probe list.
func probeAddresses(t testing.TB) []string {
	t.Helper()
	seen := map[string]bool{}
	add := func(addr netip.Addr) {
		seen[addr.String()] = true
		if addr.Is4() {
			seen[netip.AddrFrom16(addr.As16()).String()] = true // ::ffff:a.b.c.d
		}
	}
	for _, text := range referencePrefixes {
		prefix := netip.MustParsePrefix(text)
		first := prefix.Masked().Addr()
		last := lastAddress(prefix)
		inner := first
		for i := 0; i < 5 && inner.Next().IsValid() && prefix.Contains(inner.Next()); i++ {
			inner = inner.Next()
		}
		for _, addr := range []netip.Addr{first, inner, last, first.Prev(), last.Next()} {
			if addr.IsValid() {
				add(addr)
			}
		}
	}
	for _, text := range publicProbes {
		add(netip.MustParseAddr(text))
	}
	out := make([]string, 0, len(seen))
	for text := range seen {
		out = append(out, text)
	}
	sort.Strings(out)
	return out
}

func lastAddress(prefix netip.Prefix) netip.Addr {
	raw := prefix.Masked().Addr().AsSlice()
	for bit := prefix.Bits(); bit < len(raw)*8; bit++ {
		raw[bit/8] |= 0x80 >> (bit % 8)
	}
	addr, _ := netip.AddrFromSlice(raw)
	return addr
}

// recordedAddressAnswers returns, per probe address, whether the recorded Python judged it a safe public target.
func recordedAddressAnswers(t *testing.T) map[string]bool {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(probeAddresses(t))
	if err != nil {
		t.Fatal(err)
	}
	spec := rotguard.Spec("testdata/golden/ssrf_address_classes.json", "cdf1200ab2e5568210187db89342af727a7aa4c7597ae36311c86129937fbeda",
		"./internal/llmorgsettings/", "^TestAddressClassesMatchTheRecordedPython$")
	answers := programoracle.Run(t, spec, root, []programoracle.Program{{Name: "_ip_is_safe_public_target over the probe list", Text: pythonAddressProgram, Stdin: request}})
	if answers[0].ExitCode != 0 {
		t.Fatalf("the recorded Python run exited %d (stdout %q)", answers[0].ExitCode, answers[0].Stdout)
	}
	var recorded map[string]bool
	if err := json.Unmarshal([]byte(strings.TrimSpace(answers[0].Stdout)), &recorded); err != nil {
		t.Fatalf("decode the recorded answers: %v", err)
	}
	return recorded
}

// TestAddressClassesMatchTheRecordedPython is the one test the golden belongs to (the record verb records one test per golden).
// Its subtests: the recording is what the probe list asked for; the guard agrees with it for every class; the pinned stricter
// ranges are still stricter; the whole guard agrees on literal and resolved addresses.
func TestAddressClassesMatchTheRecordedPython(t *testing.T) {
	recorded := recordedAddressAnswers(t)
	t.Run("the recording is what the probe list asked for", func(t *testing.T) { checkRecording(t, recorded) })
	t.Run("the guard agrees with it for every special-purpose class", func(t *testing.T) { checkGuard(t, recorded) })
	t.Run("the pinned stricter ranges are still stricter", func(t *testing.T) { checkKnownStricter(t, recorded) })
	t.Run("the whole guard agrees on literal and resolved addresses", func(t *testing.T) { checkWholeGuard(t, recorded) })
}

func checkRecording(t *testing.T, recorded map[string]bool) {
	t.Helper()
	probes := probeAddresses(t)
	if len(recorded) != len(probes) {
		t.Fatalf("the recording holds %d answers for %d probes", len(recorded), len(probes))
	}
	safe, refused := 0, 0
	for _, address := range probes {
		answer, present := recorded[address]
		if !present {
			t.Fatalf("the recording has no answer for %s", address)
		}
		if answer {
			safe++
		} else {
			refused++
		}
	}
	if safe < 20 || refused < 100 {
		t.Fatalf("the recording is lopsided (%d safe, %d refused): the reference did not run as the guard", safe, refused)
	}
	for _, address := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !recorded[address] {
			t.Fatalf("the recorded Python refuses the public address %s", address)
		}
	}
	for _, address := range []string{"127.0.0.1", "10.0.0.1", "::1", "169.254.169.254", "::ffff:10.0.0.1"} {
		if recorded[address] {
			t.Fatalf("the recorded Python accepts %s", address)
		}
	}
}

// knownStricter are the ranges where the Go guard refuses what the reference accepts, on purpose: fec0::/10 and the whole of
// 192.0.0.0/24 (the reference refuses only 192.0.0.0/29 and 192.0.0.170/31 and accepts the rest, including 192.0.0.9 and
// 192.0.0.10, which RFC 7600 and RFC 8155 name globally reachable) and 192.88.99.0/24 (RFC 7526 6to4 relay anycast,
// deprecated; the reference accepts it). Any other disagreement is a defect.
var knownStricter = []netip.Prefix{
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("fec0::/10"), // deprecated site-local (RFC 3879): internal-use space the reference accepts
}

func inKnownStricter(addr netip.Addr) bool {
	for _, prefix := range knownStricter {
		if prefix.Contains(addr.Unmap()) {
			return true
		}
	}
	return false
}

func checkGuard(t *testing.T, recorded map[string]bool) {
	t.Helper()
	for _, text := range probeAddresses(t) {
		t.Run(text, func(t *testing.T) {
			addr := netip.MustParseAddr(text)
			got := isSafePublicIP(addr)
			want := recorded[text]
			switch {
			case got == want:
			case !got && want && inKnownStricter(addr):
				// Go is stricter here by design (knownStricter).
			case got && !want:
				t.Fatalf("%s: the guard ACCEPTS an address the recorded Python refuses", text)
			default:
				t.Fatalf("%s: the guard refuses an address the recorded Python accepts, outside the pinned stricter ranges", text)
			}
		})
	}
}

// checkKnownStricter pins the Known: every range in knownStricter holds an address the
// recorded Python accepts and the guard refuses. If the reference or the guard moves so that this is no longer true, the
// entry is stale and goes.
func checkKnownStricter(t *testing.T, recorded map[string]bool) {
	t.Helper()
	for _, prefix := range knownStricter {
		t.Run(prefix.String(), func(t *testing.T) {
			found := false
			for _, text := range probeAddresses(t) {
				addr := netip.MustParseAddr(text)
				if addr.Is4In6() || !prefix.Contains(addr) {
					continue
				}
				if recorded[text] && !isSafePublicIP(addr) {
					found = true
				}
			}
			if !found {
				t.Fatalf("no probe in %s is accepted by the recorded Python and refused by the guard: the entry is stale", prefix)
			}
		})
	}
}

// checkWholeGuard sends each probe through the whole guard
// twice: as a literal in the URL (v4 as written, v6 in brackets) and as the answer of a stub resolver for an ordinary
// name. The reference applies the same address test on both paths.
func checkWholeGuard(t *testing.T, recorded map[string]bool) {
	t.Helper()
	for _, text := range probeAddresses(t) {
		addr := netip.MustParseAddr(text)
		// The whole guard accepts what the recorded Python accepts, except inside the pinned stricter ranges.
		want := recorded[text] && !inKnownStricter(addr)
		host := text
		if addr.Is6() {
			host = "[" + text + "]"
		}
		t.Run("literal "+text, func(t *testing.T) {
			ok, reason := validateBaseURLText(context.Background(), "https://"+host+"/v1", func(context.Context, string) ([]net.IP, error) { return nil, fmt.Errorf("no DNS in this test") })
			if ok != want {
				t.Fatalf("literal %s: ok=%v reason=%q, want ok=%v", text, ok, reason, want)
			}
		})
		t.Run("resolved "+text, func(t *testing.T) {
			resolver := func(context.Context, string) ([]net.IP, error) { return []net.IP{net.ParseIP(text)}, nil }
			ok, reason := validateBaseURLText(context.Background(), "https://gateway.example.test/v1", resolver)
			if ok != want {
				t.Fatalf("resolved %s: ok=%v reason=%q, want ok=%v", text, ok, reason, want)
			}
		})
	}
}

// TestIsSafePublicIPRefusesAnInvalidAddress: Python's _ip_is_safe_public_target answers False for text that is not an
// address; the Go guard answers False for the zero netip.Addr.
func TestIsSafePublicIPRefusesAnInvalidAddress(t *testing.T) {
	if isSafePublicIP(netip.Addr{}) {
		t.Fatal("the zero netip.Addr was accepted as a safe public address")
	}
}
