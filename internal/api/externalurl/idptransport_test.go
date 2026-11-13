package externalurl

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

const testIdPAllowEnv = "SSO_EGRESS_ALLOW_CIDRS"

// countingServer is a server on 127.0.0.1 that counts the requests it receives; its body is bounded.
func countingServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

// captureLogs runs fn with the default logger writing JSON lines without the time, so two runs compare byte for byte.
func captureLogs(fn func()) string {
	var logs syncBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}})))
	defer slog.SetDefault(previous)
	fn()
	return logs.String()
}

// controlOutcome is the error text and the log lines of one dial check.
func controlOutcome(control dialControl, address string) string {
	var err error
	logs := captureLogs(func() { err = control("tcp", address, nil) })
	text := "<nil>"
	if err != nil {
		text = err.Error()
	}
	return text + "\n" + logs
}

func logLines(logs string) []map[string]any {
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(logs), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			panic(err)
		}
		lines = append(lines, line)
	}
	return lines
}

// classTable is every class address of TestRefuseInternalAddressBlocksEveryInternalClass
// (guardedtransport_egress_test.go) plus public addresses.
var classTable = []string{
	"10.1.2.3:443", "172.16.0.1:443", "192.168.1.1:443", "127.0.0.1:443", "[::1]:443", "169.254.169.254:80",
	"169.254.1.1:80", "[fe80::1]:443", "[fd00::1]:443", "[fc00::1]:443", "224.0.0.1:443", "239.255.255.250:443",
	"[ff02::1]:443", "0.0.0.0:443", "[::]:443", "[::ffff:10.0.0.1]:443", "[::ffff:127.0.0.1]:443", "240.0.0.1:443",
	"not-an-address", "[not-an-ip]:443",
	"93.184.216.34:443", "[2606:2800:220:1::1]:443", "8.8.8.8:443",
}

// C3: with the list unset or empty, the SSO dial check IS refuseInternalAddress (the same function), and every class
// address and public address gets the same error text and the same log line, byte for byte; parsing writes nothing.
func TestIdPTransportWithTheListUnsetIsGuardedTransport(t *testing.T) {
	for name, value := range map[string]*string{"unset": nil, "empty": new(string), "blank entries": ptr(" , ,")} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(testIdPAllowEnv, "x")
			if value == nil {
				os.Unsetenv(testIdPAllowEnv)
			} else {
				t.Setenv(testIdPAllowEnv, *value)
			}
			var control dialControl
			if logs := captureLogs(func() { control = idpDialControl(os.Getenv(testIdPAllowEnv)); _ = IdPTransport() }); logs != "" {
				t.Fatalf("parsing an unset list wrote %q", logs)
			}
			if reflect.ValueOf(control).Pointer() != reflect.ValueOf(refuseInternalAddress).Pointer() {
				t.Fatal("with the list unset the dial check is not refuseInternalAddress")
			}
			for _, address := range classTable {
				if got, want := controlOutcome(control, address), controlOutcome(refuseInternalAddress, address); got != want {
					t.Errorf("%s:\nIdPTransport  %q\nGuarded       %q", address, got, want)
				}
			}
			for _, address := range []string{"127.0.0.1:1", "[::1]:1", "10.1.2.3:443", "169.254.169.254:80", "192.168.1.1:443"} {
				var idpErr, guardedErr error
				idpLogs := captureLogs(func() { _, idpErr = IdPTransport().DialContext(context.Background(), "tcp", address) })
				guardedLogs := captureLogs(func() { _, guardedErr = GuardedTransport().DialContext(context.Background(), "tcp", address) })
				if idpErr == nil || guardedErr == nil || idpErr.Error() != guardedErr.Error() || idpLogs != guardedLogs {
					t.Errorf("dial %s: IdPTransport %v %q, GuardedTransport %v %q", address, idpErr, idpLogs, guardedErr, guardedLogs)
				}
			}
			server, hits := countingServer(t)
			client := &http.Client{Transport: IdPTransport(), Timeout: 5 * time.Second}
			for _, target := range []string{server.URL, strings.Replace(server.URL, "127.0.0.1", "localhost", 1)} {
				response, err := client.Get(target)
				if err == nil {
					response.Body.Close()
					t.Fatalf("GET %s reached 127.0.0.1 with the list %s", target, name)
				}
				if !strings.Contains(err.Error(), "Connection to private/internal networks is not allowed") {
					t.Fatalf("GET %s error = %v, want the internal-network refusal", target, err)
				}
			}
			if hits.Load() != 0 {
				t.Fatalf("the server saw %d request(s)", hits.Load())
			}
		})
	}
}

func ptr(s string) *string { return &s }

// With the list naming 127.0.0.1, an IdP on 127.0.0.1 is reached by address and by the name localhost (::1 is
// refused, the dial goes on to 127.0.0.1).
func TestIdPTransportReachesAListedIdP(t *testing.T) {
	t.Setenv(testIdPAllowEnv, "127.0.0.1/32")
	server, hits := countingServer(t)
	client := &http.Client{Transport: IdPTransport(), Timeout: 5 * time.Second}
	for _, target := range []string{server.URL, strings.Replace(server.URL, "127.0.0.1", "localhost", 1)} {
		response, err := client.Get(target)
		if err != nil {
			t.Fatalf("GET %s with 127.0.0.1/32 listed: %v", target, err)
		}
		response.Body.Close()
	}
	if hits.Load() != 2 {
		t.Fatalf("the server saw %d request(s), want 2", hits.Load())
	}
}

// C4: no list admits a never-allowed class, even 0.0.0.0/0 and ::/0 injected below the parser.
func TestIdPAllowNeverAdmitsANeverAllowedClass(t *testing.T) {
	control := refuseUnlessIdPAllowed([]netip.Prefix{netip.MustParsePrefix("0.0.0.0/0"), netip.MustParsePrefix("::/0")})
	for address, class := range map[string]string{
		"169.254.169.254:80": "metadata", "169.254.170.2:80": "metadata", "100.100.100.200:80": "metadata",
		"[fd00:ec2::254]:80": "metadata", "[fd00:c1::a9fe:a9fe]:80": "metadata", "168.63.129.16:80": "metadata",
		"169.254.1.1:80": "link_local", "[fe80::1]:443": "link_local", "224.0.0.1:443": "multicast",
		"[ff02::1]:443": "multicast", "0.0.0.0:443": "unspecified", "[::]:443": "unspecified",
		"not-an-address": "invalid_address", "[not-an-ip]:443": "invalid_address",
	} {
		got := controlOutcome(control, address)
		if want := controlOutcome(refuseInternalAddress, address); got != want || !strings.Contains(got, `"class":"`+class+`"`) {
			t.Errorf("%s: %q, want the %s refusal %q", address, got, class, want)
		}
	}
	if err := control("tcp", "10.1.2.3:443", nil); err != nil {
		t.Fatalf("the injected list does not admit a private address (the cell would prove nothing): %v", err)
	}
	t.Setenv(testIdPAllowEnv, "169.254.169.254/32")
	if err := idpDialControl(os.Getenv(testIdPAllowEnv))("tcp", "169.254.169.254:80", nil); err == nil {
		t.Fatal("a metadata entry in the list admitted the metadata address")
	}
}

// C5: a list naming 127.0.0.1 admits nothing else.
func TestIdPAllowAdmitsOnlyItsEntries(t *testing.T) {
	control := idpDialControl("127.0.0.1/32")
	for _, address := range []string{"127.0.0.2:443", "10.0.0.1:443", "[::1]:443", "192.168.1.1:443"} {
		if got, want := controlOutcome(control, address), controlOutcome(refuseInternalAddress, address); got != want {
			t.Errorf("%s: %q, want the refusal of today %q", address, got, want)
		}
	}
}

// C6: a translation address is never admitted by the list, whatever IPv4 address it carries; an IPv4-mapped address
// is unmapped first and is the listed address itself.
func TestIdPAllowNeverAdmitsATranslationAddress(t *testing.T) {
	control := idpDialControl("127.0.0.1/32")
	for _, address := range []string{"[64:ff9b::7f00:1]:443", "[2002:7f00:1::]:443"} {
		if got, want := controlOutcome(control, address), controlOutcome(refuseInternalAddress, address); got != want || !strings.HasPrefix(got, errInternalDial.Error()) {
			t.Errorf("%s: %q, want the refusal of today %q", address, got, want)
		}
	}
	if err := control("tcp", "[::ffff:127.0.0.1]:443", nil); err != nil {
		t.Errorf("::ffff:127.0.0.1 is 127.0.0.1 and is listed: %v", err)
	}
	// Below the parser (which refuses an entry over a translation prefix), a list that covers the IPv6 form still does
	// not admit it.
	injected := refuseUnlessIdPAllowed([]netip.Prefix{netip.MustParsePrefix("::/0"), netip.MustParsePrefix("0.0.0.0/0")})
	for _, address := range []string{"[64:ff9b::7f00:1]:443", "[2002:7f00:1::]:443", "[64:ff9b::a00:1]:443", "[64:ff9b:1:7f00:1::]:443"} {
		if got, want := controlOutcome(injected, address), controlOutcome(refuseInternalAddress, address); got != want || !strings.HasPrefix(got, errInternalDial.Error()) {
			t.Errorf("injected ::/0, %s: %q, want the refusal of today %q", address, got, want)
		}
	}
	var allow []netip.Prefix
	logs := captureLogs(func() { allow = parseIdPAllow("64:ff9b::/96") })
	if allow != nil || len(logLines(logs)) != 1 || logLines(logs)[0]["level"] != "ERROR" {
		t.Fatalf("a translation prefix entry: list %v, logs %q", allow, logs)
	}
}

// C7: one bad entry, alone or after a good one, empties the whole list with one ERROR that holds the entry count and
// not the entry; 127.0.0.1 is then refused. ::1/128 is refused too: it lies inside the IPv4-compatible range ::/96.
func TestIdPAllowOneBadEntryEmptiesTheList(t *testing.T) {
	bad := []string{"not-a-cidr", "127.0.0.1", "0.0.0.0/0", "::/0", "0.0.0.0/1", "128.0.0.0/1", "::ffff:127.0.0.1/128",
		"169.254.0.0/16", "fe80::/10", "224.0.0.0/4", "100.64.0.0/10", "fd00::/8", "::1/128", "64:ff9b::/96"}
	for _, entry := range bad {
		for _, value := range []string{entry, "127.0.0.1/32," + entry} {
			var allow []netip.Prefix
			logs := captureLogs(func() { allow = parseIdPAllow(value) })
			if allow != nil {
				t.Errorf("%q: list %v, want empty", value, allow)
			}
			lines := logLines(logs)
			entries := float64(strings.Count(value, ",") + 1)
			if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["entries"] != entries ||
				lines[0]["msg"] != "externalurl: SSO_EGRESS_ALLOW_CIDRS has an invalid or forbidden entry; no non-public address is allowed" {
				t.Errorf("%q: logs %q, want one ERROR with entries=%v", value, logs, entries)
			}
			if strings.Contains(logs, entry) {
				t.Errorf("%q: the ERROR line carries the entry: %q", value, logs)
			}
			control := idpDialControl(value)
			for _, address := range []string{"127.0.0.1:443", "10.0.0.1:443"} {
				if err := control("tcp", address, nil); err == nil {
					t.Errorf("%q: %s admitted", value, address)
				}
			}
		}
	}
}

// C8: an allowed dial writes one INFO with the class only; a refused dial keeps the WARN of today.
func TestIdPAllowLogsTheClassOnly(t *testing.T) {
	control := idpDialControl("127.0.0.1/32")
	logs := captureLogs(func() {
		if err := control("tcp", "127.0.0.1:9443", nil); err != nil {
			t.Fatalf("the listed address was refused: %v", err)
		}
	})
	lines := logLines(logs)
	if len(lines) != 1 || lines[0]["level"] != "INFO" || lines[0]["msg"] != "externalurl: outbound connection allowed by SSO_EGRESS_ALLOW_CIDRS" ||
		lines[0]["class"] != "loopback" || len(lines[0]) != 3 {
		t.Fatalf("allow log %q, want one INFO with the class only", logs)
	}
	for _, leak := range []string{"127.0.0.1", "/32", "9443"} {
		if strings.Contains(logs, leak) {
			t.Fatalf("the allow log carries %q: %s", leak, logs)
		}
	}
	if got, want := controlOutcome(control, "10.0.0.1:443"), controlOutcome(refuseInternalAddress, "10.0.0.1:443"); got != want {
		t.Fatalf("refused dial %q, want %q", got, want)
	}
}

// C-g (G-1): building the transport with a set, valid list writes one WARN with the entry count and no value; unset
// writes nothing; an invalid list writes no WARN and one ERROR.
func TestIdPTransportWarnsOnceWhenTheListIsInForce(t *testing.T) {
	for _, tc := range []struct {
		value         *string
		level         string
		entries       float64
		msg           string
		forbiddenText []string
	}{
		{ptr("127.0.0.1/32"), "WARN", 1, "externalurl: SSO_EGRESS_ALLOW_CIDRS in force", []string{"127.0.0.1"}},
		{ptr("127.0.0.1/32, 10.20.0.0/16"), "WARN", 2, "externalurl: SSO_EGRESS_ALLOW_CIDRS in force", []string{"127.0.0.1", "10.20"}},
		{nil, "", 0, "", nil},
		{ptr("127.0.0.1/32,0.0.0.0/0"), "ERROR", 2, "externalurl: SSO_EGRESS_ALLOW_CIDRS has an invalid or forbidden entry; no non-public address is allowed", []string{"0.0.0.0"}},
	} {
		t.Setenv(testIdPAllowEnv, "x")
		if tc.value == nil {
			os.Unsetenv(testIdPAllowEnv)
		} else {
			t.Setenv(testIdPAllowEnv, *tc.value)
		}
		logs := captureLogs(func() { _ = IdPTransport() })
		lines := logLines(logs)
		if tc.level == "" {
			if len(lines) != 0 {
				t.Errorf("unset: %q, want no line", logs)
			}
			continue
		}
		if len(lines) != 1 || lines[0]["level"] != tc.level || lines[0]["msg"] != tc.msg || lines[0]["entries"] != tc.entries {
			t.Errorf("%q: %q, want one %s with entries=%v", *tc.value, logs, tc.level, tc.entries)
		}
		for _, leak := range tc.forbiddenText {
			if strings.Contains(logs, leak) {
				t.Errorf("%q: the line carries %q: %s", *tc.value, leak, logs)
			}
		}
	}
}

func TestIdPTransportNeverDialsTheEnvironmentProxy(t *testing.T) {
	if IdPTransport().Proxy != nil {
		t.Fatal("IdPTransport consults a proxy; the checked address would no longer be the dialled one")
	}
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
		if response, err := (&http.Client{Transport: IdPTransport()}).Do(request); err == nil {
			_ = response.Body.Close()
		}
	}).AssertNothingReached(t)
}

// The SSO list never reaches GuardedTransport: credentials, teams identity, the LLM readiness probes and categorize
// share it, and it still refuses 127.0.0.1 with the list naming it.
func TestSSOEgressAllowNeverWidensGuardedTransport(t *testing.T) {
	t.Setenv(testIdPAllowEnv, "127.0.0.1/32")
	server, hits := countingServer(t)
	client := &http.Client{Transport: GuardedTransport(), Timeout: 5 * time.Second}
	response, err := client.Get(server.URL)
	if err == nil {
		response.Body.Close()
		t.Fatal("GuardedTransport reached 127.0.0.1 with the SSO list set")
	}
	if !strings.Contains(err.Error(), "Connection to private/internal networks is not allowed") {
		t.Fatalf("error = %v, want the internal-network refusal", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("the server saw %d request(s)", hits.Load())
	}
}
