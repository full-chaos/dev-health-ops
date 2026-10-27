package devhealth_test

import (
	"encoding/json"
	"strings"
	"testing"
)

var tracingOn = []string{
	"--set", "authService.tracing.enabled=true",
	"--set", "authService.tracing.endpoint=http://10.0.0.151:4317",
	"--set-json", `authService.networkPolicy.egress.otlp.to=[{"ipBlock":{"cidr":"10.0.0.151/32"}}]`,
}

func tracingDocs(t *testing.T, args ...string) (deployment, policy map[string]any) {
	t.Helper()
	docs, out, err := renderAuth(t, authWith(args...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, out)
	}
	deployment = authDoc(docs, "Deployment", "a-dev-health-auth-service")
	policy = authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	if deployment == nil || policy == nil {
		t.Fatal("no auth-service Deployment or NetworkPolicy rendered")
	}
	return deployment, policy
}

// otlpRules are the policy's egress rules that open port 4317.
func otlpRules(policy map[string]any) []any {
	var rules []any
	for _, rule := range aList(at(policy, "spec", "egress")) {
		for _, port := range aList(at(rule, "ports")) {
			if at(port, "port") == 4317 {
				rules = append(rules, rule)
			}
		}
	}
	return rules
}

func TestAuthServiceTracingIsOffByDefault(t *testing.T) {
	deployment, policy := tracingDocs(t)
	if got, set := containerEnvValue(t, deployment, "OTEL_ENABLED"); !set || got != "false" {
		t.Errorf("OTEL_ENABLED = %q (set %v), want \"false\" always rendered", got, set)
	}
	for _, name := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_SAMPLE_RATE", "OTEL_SERVICE_NAME"} {
		if _, set := containerEnvValue(t, deployment, name); set {
			t.Errorf("%s is rendered while tracing is off", name)
		}
	}
	if rules := otlpRules(policy); len(rules) != 0 {
		t.Errorf("the policy opens 4317 while tracing is off: %v", rules)
	}
}

func TestAuthServiceTracingOnRendersTheExportAndItsEgress(t *testing.T) {
	for name, peer := range map[string]string{
		"node ipBlock":        `[{"ipBlock":{"cidr":"10.0.0.151/32"}}]`,
		"v6 host ipBlock":     `[{"ipBlock":{"cidr":"2001:db8::7/128"}}]`,
		"v4 /8 ipBlock":       `[{"ipBlock":{"cidr":"10.0.0.0/8"}}]`,
		"v6 /32 ipBlock":      `[{"ipBlock":{"cidr":"2001:db8::/32"}}]`,
		"v6 next to NAT64":    `[{"ipBlock":{"cidr":"64:ff9a::/32"}}]`,
		"v6 beside NAT64 /48": `[{"ipBlock":{"cidr":"64:ff9b:100::/40"}}]`,
		"collector selector":  `[{"podSelector":{"matchLabels":{"app.kubernetes.io/name":"otel-collector"}}}]`,
		"namespace and pods":  `[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"observability"}},"podSelector":{"matchLabels":{"app":"collector"}}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			deployment, policy := tracingDocs(t, append(append([]string{}, tracingOn...), "--set-json", "authService.networkPolicy.egress.otlp.to="+peer)...)
			for env, want := range map[string]string{
				"OTEL_ENABLED": "true", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://10.0.0.151:4317",
				"OTEL_SAMPLE_RATE": "1", "OTEL_SERVICE_NAME": "auth-service",
			} {
				if got, _ := containerEnvValue(t, deployment, env); got != want {
					t.Errorf("%s = %q, want %q", env, got, want)
				}
			}
			rules := otlpRules(policy)
			if len(rules) != 1 {
				t.Fatalf("the policy has %d rules opening 4317, want 1: %v", len(rules), aList(at(policy, "spec", "egress")))
			}
			ports := aList(at(rules[0], "ports"))
			if len(ports) != 1 || at(ports[0], "protocol") != "TCP" {
				t.Errorf("the 4317 rule ports = %v, want TCP 4317 only", ports)
			}
			got, _ := json.Marshal(at(rules[0], "to"))
			var wantTo any
			_ = json.Unmarshal([]byte(peer), &wantTo)
			want, _ := json.Marshal(wantTo)
			if string(got) != string(want) {
				t.Errorf("the 4317 rule peers = %s, want %s", got, want)
			}
		})
	}
}

func TestAuthServiceTracingSampleRateIsRendered(t *testing.T) {
	deployment, _ := tracingDocs(t, append(append([]string{}, tracingOn...), "--set-string", "authService.tracing.sampleRate=0.25")...)
	if got, _ := containerEnvValue(t, deployment, "OTEL_SAMPLE_RATE"); got != "0.25" {
		t.Errorf("OTEL_SAMPLE_RATE = %q, want 0.25", got)
	}
}

func TestAuthServiceTracingRefusals(t *testing.T) {
	cases := map[string]struct {
		args []string
		want string
	}{
		"on without an endpoint": {[]string{"--set", "authService.tracing.enabled=true",
			"--set-json", `authService.networkPolicy.egress.otlp.to=[{"ipBlock":{"cidr":"10.0.0.151/32"}}]`}, "authService.tracing.endpoint"},
		"on with the policy and no egress peer": {[]string{"--set", "authService.tracing.enabled=true",
			"--set", "authService.tracing.endpoint=http://10.0.0.151:4317"}, "authService.networkPolicy.egress.otlp.to"},
		"a rate above 1":           {append(append([]string{}, tracingOn...), "--set-string", "authService.tracing.sampleRate=2"), "authService.tracing.sampleRate"},
		"a rate not a number":      {append(append([]string{}, tracingOn...), "--set-string", "authService.tracing.sampleRate=all"), "authService.tracing.sampleRate"},
		"a whole-internet peer":    {append(append([]string{}, tracingOn...), "--set-json", `authService.networkPolicy.egress.otlp.to=[{"ipBlock":{"cidr":"0.0.0.0/0"}}]`), "authService.networkPolicy.egress.otlp.to"},
		"a whole-internet v6 peer": {append(append([]string{}, tracingOn...), "--set-json", `authService.networkPolicy.egress.otlp.to=[{"ipBlock":{"cidr":"::/0"}}]`), "authService.networkPolicy.egress.otlp.to"},
	}
	for _, cidr := range []string{
		"0:0:0:0:0:0:0:0/0", "::0/0", " ::/0", "0.0.0.0/1", "128.0.0.0/1", "10.0.0.0/7", "::/1", "2001:db8::/31",
		"::ffff:0:0/96", "0:0:0:0:0:ffff:0:0/96", "::FFFF:0:0/96", "0::ffff:0:0/96", "::ffff:0.0.0.0/96", "::ffff:10.0.0.151/128",
		"10.0.0.151", "10.0.0.151/x", "10.0.0.151/32/1", "10.0.0.151/", "10.0.0.151/33", "2001:db8::7/129",
		"::/32", "0:0::/64",
		"64:ff9b::/96", "64:ff9b::a00:97/128", "0064:FF9B:0000::/96", "64:ff9b::/32", "64:ff9b::/64", "64:ff9b:0:0:0:0:0:0/96",
		"64:ff9b:1::/48", "64:ff9b:1:2::/64", "64:ff9b:1:0:0:0:0:7/128", "64:ff9b:0::/40",
	} {
		cases["peer cidr "+cidr] = struct {
			args []string
			want string
		}{append(append([]string{}, tracingOn...), "--set-json", `authService.networkPolicy.egress.otlp.to=[{"ipBlock":{"cidr":"`+cidr+`"}}]`), "authService.networkPolicy.egress.otlp.to"}
	}
	// An embedded dotted IPv4 part is named as such, not only as an address that does not parse.
	for _, cidr := range []string{"::ffff:0.0.0.0/96", "::ffff:10.0.0.151/128"} {
		c := cases["peer cidr "+cidr]
		c.want = "embedded IPv4 part"
		cases["peer cidr "+cidr] = c
	}
	for _, name := range []string{"OTEL_ENABLED", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_SAMPLE_RATE", "OTEL_SERVICE_NAME"} {
		cases["extraEnv "+name] = struct {
			args []string
			want string
		}{[]string{"--set-json", `authService.extraEnv=[{"name":"` + name + `","value":"x"}]`}, name}
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, authWith(tc.args...)...)
			if err == nil {
				t.Fatalf("render passed; want a refusal naming %s", tc.want)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("refusal does not name %s:\n%.600s", tc.want, out)
			}
		})
	}
}

// With tracing off a listed peer opens nothing.
func TestAuthServiceOTLPPeerWithTracingOffOpensNothing(t *testing.T) {
	_, policy := tracingDocs(t, "--set-json", `authService.networkPolicy.egress.otlp.to=[{"ipBlock":{"cidr":"10.0.0.151/32"}}]`)
	if rules := otlpRules(policy); len(rules) != 0 {
		t.Errorf("the policy opens 4317 while tracing is off: %v", rules)
	}
}
