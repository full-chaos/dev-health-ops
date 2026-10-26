package devhealth_test

import (
	"fmt"
	"net/netip"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

const trustedEnvName = "AUTH_SERVICE_TRUSTED_PROXIES"

var authTrusted = []string{"--set-json", `authService.trustedProxies=["10.8.0.0/28","10.9.0.5/32"]`}

func envValue(container any, name string) (string, bool) {
	for _, e := range aList(at(container, "env")) {
		if at(e, "name") == name {
			return aStr(at(e, "value")), true
		}
	}
	return "", false
}

// With the go-api auth proxy on, auth-service believes the one forwarded address only from the listed peers, and
// admits no pod but go-api's: trust without that boundary would let any caller name its own address.
func TestAuthServiceTrustedProxiesRenderWithTheProxyOnAndTheBoundary(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth("--set", "authService.phase=2")...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
	if got, ok := envValue(container, trustedEnvName); !ok || got != "10.8.0.0/28,10.9.0.5/32" {
		t.Fatalf("%s = %q (set %v), want the listed networks", trustedEnvName, got, ok)
	}
	from := authServiceAPIFrom(t, docs)
	if len(from) != 1 {
		t.Fatalf("the API port admits %d peers, want exactly the go-api pods: %v", len(from), from)
	}
	selector := aMap(at(from[0], "podSelector", "matchLabels"))
	if selector["app.kubernetes.io/component"] != "go-api" || at(from[0], "namespaceSelector") != nil || at(from[0], "ipBlock") != nil {
		t.Fatalf("the one API-port peer is not the go-api pod selector: %v", from[0])
	}
}

func TestAuthServiceProxyOnWithNoTrustedProxiesFailsTheRender(t *testing.T) {
	for _, phase := range []string{"2", "3"} {
		args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), sessionReaders...)
		args = append(args, "--set", "authService.sessionKeys.successor.secretName=auth-session")
		args = append(args, authValkey...)
		args = append(args, phase3Secrets...)
		args = append(args, "--set", "authService.phase="+phase)
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "authService.trustedProxies is required") {
			t.Errorf("phase %s with no trusted proxies: err=%v\n%.400s", phase, err, out)
		}
	}
}

func TestAuthServiceProxyOffRendersWithoutTrustedProxies(t *testing.T) {
	args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), sessionReaders...)
	docs, out, err := renderAuth(t, append(args, sessionSuccessor...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
	if _, ok := envValue(container, trustedEnvName); ok {
		t.Errorf("%s is set with the proxy off and no list", trustedEnvName)
	}
}

func TestAuthServiceTrustedProxiesRefuseTheWholeInternet(t *testing.T) {
	for _, entry := range []string{"0.0.0.0/0", "::/0", "::ffff:0.0.0.0/96", "::ffff:10.0.0.0/104", "10.0.0.0/8, 0.0.0.0/0", "*", "0:0:0:0:0:ffff:10.0.0.9", "0:0:0:0:0:FFFF:a00:9", "::0:ffff:10.0.0.9", "0::ffff:10.0.0.9", "::0000:ffff:a00:9", "0:0:0:0:0:ffff:a00:9", "::FFFF:a00:9", "0000:0:0:0:0:ffff:a00:9", "::ffff:a00:9", "0:0:0:0:0:ffff:0:0/96", "::0:ffff:0:0/96", "0:0:0:0:0:FFFF::/96"} {
		args := append(goAPIWithAuth("--set", "authService.phase=2"), "--set-json", `authService.trustedProxies=["`+entry+`"]`)
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "authService.trustedProxies") {
			t.Errorf("entry %q rendered or named the wrong key: err=%v\n%.300s", entry, err, out)
		}
	}
}

func TestAuthServiceExtraEnvCannotSetTheTrustedProxies(t *testing.T) {
	args := append(append([]string{}, authBase...), "--set-json", `authService.extraEnv=[{"name":"`+trustedEnvName+`","value":"0.0.0.0/0"}]`)
	if _, out, err := renderAuth(t, args...); err == nil || !strings.Contains(out, trustedEnvName) {
		t.Errorf("extraEnv set the trusted proxies: err=%v\n%.300s", err, out)
	}
}

// Both ways on the switch that turns the session routes on (authService.sessionKeys.successor, which renders
// AUTH_JWT_SECRET_KEY and AUTH_SESSION_SUCCESSOR_KEY together, the two secrets authconfig's Session.Enabled needs): on
// with no store = fail, off = renders.
func TestAuthServiceStoreIsRequiredExactlyWhenTheSessionRoutesAreOn(t *testing.T) {
	_, out, err := renderAuth(t, append(append([]string{}, authBase...), "--set", "authService.sessionKeys.successor.secretName=auth-session")...)
	if err == nil || !strings.Contains(out, "authService.valkey.secretName is required") {
		t.Errorf("session routes on without the store: err=%v\n%.300s", err, out)
	}
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("session routes off: %v\n%.300s", err, out)
	}
	container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
	if envNames(container)["AUTH_SESSION_SUCCESSOR_KEY"] || envNames(container)["AUTH_JWT_SECRET_KEY"] {
		t.Error("the session secrets render although the switch is off")
	}
}

// Parity: every entry goes through BOTH the chart render and clientaddr.ParseTrust, the parser authconfig calls for
// AUTH_SERVICE_TRUSTED_PROXIES (internal/auth/authconfig/config.go:439; entry rules internal/auth/clientaddr/clientaddr.go:53-71).
// A value that renders but the service refuses would stop auth-service at start: that is the failure. The chart may be
// stricter only where it says so on purpose (a /0 network, an IPv4-mapped form, a padded entry).
func TestAuthServiceTrustedProxiesRenderIffTheServiceParses(t *testing.T) {
	render := func(entry string) bool {
		args := append(goAPIWithAuth("--set", "authService.phase=2"), "--set-json", `authService.trustedProxies=["`+entry+`"]`)
		_, _, err := renderAuth(t, args...)
		return err == nil
	}
	stricterOnPurpose := map[string]string{
		"0.0.0.0/0": "/0", "::/0": "/0", "10.0.0.0/0": "/0", "::ffff:10.0.0.9": "IPv4-mapped", " 10.0.0.1": "padded",
		"10.0.0.0/8, 0.0.0.0/0":   "a list in one entry",
		"0:0:0:0:0:ffff:10.0.0.9": "IPv4-mapped", "0:0:0:0:0:FFFF:a00:9": "IPv4-mapped", "::0:ffff:10.0.0.9": "IPv4-mapped", "0::ffff:10.0.0.9": "IPv4-mapped",
		"::0000:ffff:a00:9": "IPv4-mapped", "0:0:0:0:0:ffff:a00:9": "IPv4-mapped", "::FFFF:a00:9": "IPv4-mapped", "0000:0:0:0:0:ffff:a00:9": "IPv4-mapped",
		"::ffff:a00:9": "IPv4-mapped",
	}
	entries := []string{
		"10.8.0.0/16", "10.9.0.5", "10.9.0.5/32", "172.16.0.0/12", "10.0.0.0/8", "2001:db8::/32", "fd00::/8", "2001:db8::1", "fd00::1/128",
		"fd00:0:0:0:0:0:0:1", "2001:db8:1:2::/64", "fe80::/10",
		"10.0.0.0/28", "10.1.2.0/30", "10.0.0.16/28", "2001:db8::/124", "2001:db8::10/124", "fd00::2", "10.42.0.0/24", "10.42.0.0/27",
		"10.0.0.0/33", "::/129", "256.1.1.1", "10.0.0.1/8", "10.0.0.01", "1.2.3", "1.2.3.4.5", "10.0.0.0/08", "10.0.0.0/-1", "10.0.0.0/8/9",
		"1::2::3", ":::1", "12345::1", "1:2:3:4:5:6:7", "1:2:3:4:5:6:7:8:9", "g::1", "fd00::/130", "fd00::1/1x", "10.0.0.0/", "fe80::1%eth0",
		"2001:db8::1/32", "fe80::/7", "192.168.1.0/23", "not-an-ip", "garbage/8", "*", "10.0.0.0/8, 0.0.0.0/0",
		"0.0.0.0/0", "::/0", "10.0.0.0/0", "::ffff:10.0.0.9", " 10.0.0.1",
		"0:0:0:0:0:ffff:10.0.0.9", "0:0:0:0:0:FFFF:a00:9", "::0:ffff:10.0.0.9", "0::ffff:10.0.0.9", "::0000:ffff:a00:9", "0:0:0:0:0:ffff:a00:9", "::FFFF:a00:9", "0000:0:0:0:0:ffff:a00:9", "::ffff:a00:9", "0:0:0:0:0:ffff:0:0/96", "::0:ffff:0:0/96", "0:0:0:0:0:FFFF::/96",
	}
	var parsedAndRendered, refusedByBoth int
	for _, entry := range entries {
		_, parseErr := clientaddr.ParseTrust(entry)
		parses, renders := parseErr == nil, render(entry)
		if parses && !renders && wideTier(entry) != "" {
			stricterOnPurpose[entry] = "wider than the ack band or needs the ack"
		}
		switch {
		case renders && !parses:
			t.Errorf("%q renders but the service refuses it (%v): auth-service would not start", entry, parseErr)
		case parses && !renders:
			if _, ok := stricterOnPurpose[entry]; !ok {
				t.Errorf("%q parses in the service but the chart refuses it, and it is not a listed stricter case", entry)
			}
		case parses && renders:
			if _, ok := stricterOnPurpose[entry]; ok && wideTier(entry) == "" {
				t.Errorf("%q is listed as refused on purpose but rendered", entry)
			}
			parsedAndRendered++
		default:
			refusedByBoth++
		}
	}
	if parsedAndRendered < 10 || refusedByBoth < 20 {
		t.Fatalf("the table does not exercise both outcomes: %d accepted by both, %d refused by both", parsedAndRendered, refusedByBoth)
	}
}

// Trust of the forwarded address is safe only while the API port admits go-api and no other pod, so an operator-set
// allowedFrom beside a proxy flag fails the render; the operator port (metricsFrom) is untouched.
func TestAuthServiceProxyOnRefusesAnExtraAPIPortPeer(t *testing.T) {
	extra := []string{"--set-json", `authService.networkPolicy.allowedFrom=[{"matchLabels":{"app":"other-pod"}}]`}
	_, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=2"}, extra...)...)...)
	if err == nil || !strings.Contains(out, "authService.networkPolicy.allowedFrom") {
		t.Fatalf("proxy on with an extra API-port peer rendered or named the wrong key: err=%v\n%.400s", err, out)
	}
}

func TestAuthServiceProxyOffKeepsAllowedFromAndMetricsFromOnTheirPorts(t *testing.T) {
	args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...),
		"--set-json", `authService.networkPolicy.allowedFrom=[{"matchLabels":{"app":"consumer"}}]`,
		"--set-json", `authService.networkPolicy.metricsFrom=[{"matchLabels":{"app":"scraper"}}]`)
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if from := authServiceAPIFrom(t, docs); len(from) != 1 || at(from[0], "podSelector", "matchLabels", "app") != "consumer" {
		t.Fatalf("API-port peers = %v, want only the listed consumer", from)
	}
	policy := authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	var operator []any
	for _, rule := range aList(at(policy, "spec", "ingress")) {
		for _, port := range aList(at(rule, "ports")) {
			if at(port, "port") == 8096 {
				operator = aList(at(rule, "from"))
			}
		}
	}
	if len(operator) != 1 || at(operator[0], "podSelector", "matchLabels", "app") != "scraper" {
		t.Fatalf("operator-port peers = %v, want only the listed scraper", operator)
	}
}

func TestAuthServiceProxyOnKeepsMetricsFromOnTheOperatorPort(t *testing.T) {
	args := goAPIWithAuth("--set", "authService.phase=2", "--set-json", `authService.networkPolicy.metricsFrom=[{"matchLabels":{"app":"scraper"}}]`)
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	policy := authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	var operator []any
	for _, rule := range aList(at(policy, "spec", "ingress")) {
		for _, port := range aList(at(rule, "ports")) {
			if at(port, "port") == 8096 {
				operator = aList(at(rule, "from"))
			}
		}
	}
	if len(operator) != 1 || at(operator[0], "podSelector", "matchLabels", "app") != "scraper" {
		t.Fatalf("operator-port peers = %v, want only the listed scraper", operator)
	}
}

// The trusted proxies are safe only while the auth-service NetworkPolicy admits go-api alone, so a disabled policy
// beside a proxy flag fails the render; with the proxy off a disabled policy still renders.
func TestAuthServiceProxyOnRefusesADisabledNetworkPolicy(t *testing.T) {
	_, out, err := renderAuth(t, goAPIWithAuth("--set", "authService.phase=2", "--set", "authService.networkPolicy.enabled=false")...)
	if err == nil || !strings.Contains(out, "authService.networkPolicy.enabled") {
		t.Fatalf("proxy on with the policy disabled rendered or named the wrong key: err=%v\n%.400s", err, out)
	}
}

func TestAuthServiceProxyOffRendersWithADisabledNetworkPolicy(t *testing.T) {
	args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), "--set", "authService.networkPolicy.enabled=false")
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("proxy off with the policy disabled: %v\n%.400s", err, out)
	}
	if authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service") != nil {
		t.Error("the auth-service NetworkPolicy rendered although it is disabled")
	}
}

// wideTier names the width band of one entry: "" is a narrow entry (IPv4 /28 or longer, IPv6 /124 or longer, a bare
// address), "ack" is the band that needs authService.trustedProxiesWideAccepted (IPv4 /24../27, IPv6 /64../123), and
// "refused" is wider than the node pod range (IPv4 /0../23, IPv6 /0../63), which no ack allows.
func wideTier(entry string) string {
	prefix, err := netip.ParsePrefix(entry)
	if err != nil {
		return ""
	}
	bits, v4 := prefix.Bits(), prefix.Addr().Is4()
	switch {
	case v4 && bits >= 28, !v4 && bits >= 124:
		return ""
	case v4 && bits >= 24, !v4 && bits >= 64:
		return "ack"
	}
	return "refused"
}

const (
	wideAckKey     = "authService.trustedProxiesWideAccepted"
	wideNeedsAck   = "trustedProxiesWideAccepted"
	wideTooWideMsg = "wider than a /24 (IPv4) or a /64 (IPv6)"
)

func renderWide(t *testing.T, entry string, extra ...string) ([]map[string]any, string, error) {
	t.Helper()
	args := append(goAPIWithAuth("--set", "authService.phase=2"), "--set-json", `authService.trustedProxies=["`+entry+`"]`)
	return renderAuth(t, append(args, extra...)...)
}

// The pod range of the node is the widest list entry: a wide entry needs the explicit ack, a wider one is refused with it.
func TestAuthServiceWideTrustedProxiesRefusedWithoutTheAck(t *testing.T) {
	for _, entry := range []string{"10.42.0.0/24", "10.42.0.0/27", "10.42.0.0/25", "fd00:0:0:1::/64", "fd00::/120"} {
		_, out, err := renderWide(t, entry)
		if err == nil || !strings.Contains(out, wideNeedsAck) {
			t.Errorf("wide entry %q without the ack: err=%v\n%.400s", entry, err, out)
		}
	}
}

func TestAuthServiceWideTrustedProxiesRenderWithTheAck(t *testing.T) {
	for _, entry := range []string{"10.42.0.0/24", "10.42.0.0/27", "fd00:0:0:1::/64", "fd00::/120"} {
		docs, out, err := renderWide(t, entry, "--set", wideAckKey+"=true")
		if err != nil {
			t.Errorf("wide entry %q with the ack: %v\n%.400s", entry, err, out)
			continue
		}
		container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
		if got, _ := envValue(container, trustedEnvName); got != entry {
			t.Errorf("%s = %q, want %q", trustedEnvName, got, entry)
		}
	}
}

func TestAuthServiceTooWideTrustedProxiesRefusedEvenWithTheAck(t *testing.T) {
	for _, entry := range []string{"10.42.0.0/23", "10.42.0.0/16", "10.0.0.0/8", "172.16.0.0/12", "fd00::/32", "fd00::/63", "2001:db8::/48"} {
		_, out, err := renderWide(t, entry, "--set", wideAckKey+"=true")
		if err == nil || !strings.Contains(out, wideTooWideMsg) {
			t.Errorf("too wide entry %q with the ack: err=%v\n%.400s", entry, err, out)
		}
	}
}

func TestAuthServiceNarrowTrustedProxiesNeedNoAck(t *testing.T) {
	for _, entry := range []string{"10.42.0.46", "10.42.0.46/32", "10.42.0.0/28", "10.42.0.16/28", "fd00::1", "fd00::1/128", "fd00::/124"} {
		if _, out, err := renderWide(t, entry); err != nil {
			t.Errorf("narrow entry %q: %v\n%.400s", entry, err, out)
		}
	}
}

func TestAuthServiceWideTrustedProxiesAckIsAPermissionNotARequirement(t *testing.T) {
	if _, out, err := renderWide(t, "10.42.0.0/28", "--set", wideAckKey+"=true"); err != nil {
		t.Errorf("ack with a narrow entry: %v\n%.400s", err, out)
	}
	args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), sessionReaders...)
	docs, out, err := renderAuth(t, append(append(args, sessionSuccessor...), "--set", wideAckKey+"=true")...)
	if err != nil {
		t.Fatalf("ack at phase 1 with no list: %v\n%.400s", err, out)
	}
	container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
	if _, ok := envValue(container, trustedEnvName); ok {
		t.Errorf("%s is set with the proxy off and no list", trustedEnvName)
	}
}

// Every prefix length of both families lands in exactly one band, whole-internet and IPv4-mapped forms stay refused.
func TestAuthServiceTrustedProxiesWidthBands(t *testing.T) {
	for bits := 0; bits <= 32; bits++ {
		entry := fmt.Sprintf("10.0.0.0/%d", bits)
		checkBand(t, entry, wideTier(entry))
	}
	for bits := 0; bits <= 128; bits++ {
		entry := fmt.Sprintf("fd00::/%d", bits)
		checkBand(t, entry, wideTier(entry))
	}
}

func checkBand(t *testing.T, entry, tier string) {
	t.Helper()
	if strings.HasSuffix(entry, "/0") {
		if _, _, err := renderWide(t, entry, "--set", wideAckKey+"=true"); err == nil {
			t.Errorf("%s rendered", entry)
		}
		return
	}
	prefix, err := netip.ParsePrefix(entry)
	if err != nil || prefix.Masked() != prefix {
		return
	}
	_, _, noAck := renderWide(t, entry)
	_, _, withAck := renderWide(t, entry, "--set", wideAckKey+"=true")
	switch tier {
	case "":
		if noAck != nil || withAck != nil {
			t.Errorf("%s (narrow): no ack err=%v, ack err=%v, want both to render", entry, noAck, withAck)
		}
	case "ack":
		if noAck == nil || withAck != nil {
			t.Errorf("%s (ack band): no ack err=%v, ack err=%v, want refused then rendered", entry, noAck, withAck)
		}
	default:
		if noAck == nil || withAck == nil {
			t.Errorf("%s (too wide): no ack err=%v, ack err=%v, want both refused", entry, noAck, withAck)
		}
	}
}

// authServiceAPIRules returns the from-list of EVERY ingress rule of the auth-service NetworkPolicy that holds the API
// port: a second rule on that port adds peers the first-rule helper authServiceAPIFrom never sees.
func authServiceAPIRules(t *testing.T, docs []map[string]any) [][]any {
	t.Helper()
	policy := authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	if policy == nil {
		t.Fatal("no auth-service NetworkPolicy in the render")
	}
	ingress, _ := at(policy, "spec", "ingress").([]any)
	var rules [][]any
	for _, rule := range ingress {
		ports, _ := at(rule, "ports").([]any)
		for _, port := range ports {
			if at(port, "port") == 8095 {
				from, _ := at(rule, "from").([]any)
				rules = append(rules, from)
				break
			}
		}
	}
	return rules
}

// goAPIOnlyProblem is "" when exactly one ingress rule holds the API port and its only peer is the go-api pod selector.
func goAPIOnlyProblem(rules [][]any) string {
	if len(rules) != 1 {
		return fmt.Sprintf("%d ingress rules hold the API port, want exactly one", len(rules))
	}
	from := rules[0]
	if len(from) != 1 {
		return fmt.Sprintf("the API port admits %d peers, want exactly the go-api pods: %v", len(from), from)
	}
	selector := aMap(at(from[0], "podSelector", "matchLabels"))
	if selector["app.kubernetes.io/component"] != "go-api" || at(from[0], "namespaceSelector") != nil || at(from[0], "ipBlock") != nil {
		return fmt.Sprintf("the one API-port peer is not the go-api pod selector: %v", from[0])
	}
	return ""
}

// With the ack and the pod range, the only thing that makes the trust safe is the policy: the one ingress rule on the API
// port admits the go-api pod selector alone (no ipBlock, no namespace, no second peer, no second rule), and an extra peer or
// a policy turned off is refused.
func TestAuthServiceWideTrustedProxiesNeedTheGoAPIOnlyBoundary(t *testing.T) {
	docs, out, err := renderWide(t, "10.42.0.0/24", "--set", wideAckKey+"=true")
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if problem := goAPIOnlyProblem(authServiceAPIRules(t, docs)); problem != "" {
		t.Fatal(problem)
	}
	extra := `authService.networkPolicy.allowedFrom=[{"matchLabels":{"app":"other-pod"}}]`
	if _, out, err := renderWide(t, "10.42.0.0/24", "--set", wideAckKey+"=true", "--set-json", extra); err == nil || !strings.Contains(out, "authService.networkPolicy.allowedFrom") {
		t.Errorf("ack with an extra API-port peer: err=%v\n%.300s", err, out)
	}
	if _, out, err := renderWide(t, "10.42.0.0/24", "--set", wideAckKey+"=true", "--set", "authService.networkPolicy.enabled=false"); err == nil || !strings.Contains(out, "authService.networkPolicy.enabled") {
		t.Errorf("ack with the policy off: err=%v\n%.300s", err, out)
	}
}

// The boundary check sees every rule on the API port: a planted second rule (a fixture, since the chart renders only one)
// is reported, and so is a second peer, an ipBlock and a wrong selector.
func TestAuthServiceAPIPortBoundaryCheckSeesASecondIngressRule(t *testing.T) {
	goAPI := map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/component": "go-api"}}}
	rule := func(peers ...any) map[string]any {
		return map[string]any{"from": peers, "ports": []any{map[string]any{"protocol": "TCP", "port": 8095}}}
	}
	policy := func(rules ...any) []map[string]any {
		return []map[string]any{{"kind": "NetworkPolicy", "metadata": map[string]any{"name": "a-dev-health-auth-service"}, "spec": map[string]any{"ingress": rules}}}
	}
	for name, tc := range map[string]struct {
		docs []map[string]any
		ok   bool
	}{
		"one go-api rule":         {policy(rule(goAPI)), true},
		"a second rule":           {policy(rule(goAPI), rule(map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{"app": "other"}}})), false},
		"a second go-api rule":    {policy(rule(goAPI), rule(goAPI)), false},
		"a second peer":           {policy(rule(goAPI, map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{"app": "other"}}})), false},
		"an ipBlock":              {policy(rule(map[string]any{"ipBlock": map[string]any{"cidr": "10.42.0.0/24"}})), false},
		"another selector":        {policy(rule(map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/component": "web"}}})), false},
		"no rule on the API port": {policy(), false},
	} {
		if got := goAPIOnlyProblem(authServiceAPIRules(t, tc.docs)) == ""; got != tc.ok {
			t.Errorf("%s: boundary check passes = %v, want %v", name, got, tc.ok)
		}
	}
}

// A wide list needs the ack at every phase, because the entry is rendered into the env whatever the phase and the API port
// is open to any listed peer while the proxy is off. A list wider than the pod range is refused at every phase.
func TestAuthServiceWideTrustedProxiesNeedTheAckAtEveryPhase(t *testing.T) {
	phase1 := func(entry string, extra ...string) (string, error) {
		args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), sessionReaders...)
		args = append(append(args, sessionSuccessor...), "--set", "authService.phase=1", "--set-json", `authService.trustedProxies=["`+entry+`"]`)
		_, out, err := renderAuth(t, append(args, extra...)...)
		return out, err
	}
	if out, err := phase1("10.42.0.0/24"); err == nil || !strings.Contains(out, wideNeedsAck) {
		t.Errorf("phase 1, wide entry, no ack: err=%v\n%.300s", err, out)
	}
	if out, err := phase1("10.42.0.0/24", "--set", wideAckKey+"=true"); err != nil {
		t.Errorf("phase 1, wide entry, ack: %v\n%.300s", err, out)
	}
	if out, err := phase1("10.42.0.0/16", "--set", wideAckKey+"=true"); err == nil || !strings.Contains(out, wideTooWideMsg) {
		t.Errorf("phase 1, too wide entry, ack: err=%v\n%.300s", err, out)
	}
	if out, err := phase1("10.42.0.0/28"); err != nil {
		t.Errorf("phase 1, narrow entry, no ack: %v\n%.300s", err, out)
	}
}
