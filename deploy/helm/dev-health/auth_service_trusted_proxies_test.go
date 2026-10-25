package devhealth_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

const trustedEnvName = "AUTH_SERVICE_TRUSTED_PROXIES"

var authTrusted = []string{"--set-json", `authService.trustedProxies=["10.8.0.0/16","10.9.0.5/32"]`}

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
	docs, out, err := renderAuth(t, goAPIWithAuth("--set", "goApi.authProxy.sessions=true")...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
	if got, ok := envValue(container, trustedEnvName); !ok || got != "10.8.0.0/16,10.9.0.5/32" {
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
	for _, flag := range []string{"sessions", "localLogin", "credentials", "social", "oidc", "saml"} {
		args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), sessionReaders...)
		args = append(args, "--set", "authService.sessionKeys.successor.secretName=auth-session")
		args = append(args, authValkey...)
		args = append(args, "--set", "goApi.authProxy.sessions=true", "--set", "goApi.authProxy."+flag+"=true")
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "authService.trustedProxies is required") {
			t.Errorf("proxy flag %s with no trusted proxies: err=%v\n%.400s", flag, err, out)
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
		args := append(goAPIWithAuth("--set", "goApi.authProxy.sessions=true"), "--set-json", `authService.trustedProxies=["`+entry+`"]`)
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
		args := append(goAPIWithAuth("--set", "goApi.authProxy.sessions=true"), "--set-json", `authService.trustedProxies=["`+entry+`"]`)
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
		switch {
		case renders && !parses:
			t.Errorf("%q renders but the service refuses it (%v): auth-service would not start", entry, parseErr)
		case parses && !renders:
			if _, ok := stricterOnPurpose[entry]; !ok {
				t.Errorf("%q parses in the service but the chart refuses it, and it is not a listed stricter case", entry)
			}
		case parses && renders:
			if _, ok := stricterOnPurpose[entry]; ok {
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
	_, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "goApi.authProxy.sessions=true"}, extra...)...)...)
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
	args := goAPIWithAuth("--set", "goApi.authProxy.sessions=true", "--set-json", `authService.networkPolicy.metricsFrom=[{"matchLabels":{"app":"scraper"}}]`)
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
	_, out, err := renderAuth(t, goAPIWithAuth("--set", "goApi.authProxy.sessions=true", "--set", "authService.networkPolicy.enabled=false")...)
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
