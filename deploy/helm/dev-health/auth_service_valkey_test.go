package devhealth_test

import (
	"strings"
	"testing"
)

var authValkey = []string{
	"--set", "authService.valkey.secretName=auth-ratelimit",
	"--set-json", `authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"valkey"}}}]`,
}

// The rate-limit store's login reaches the container by secretKeyRef only
// while authService.valkey is set; otherwise the pod has no such variable and
// no egress to the store.
func TestAuthServiceRateLimitStoreLoginIsOptionalAndByReference(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if envNames(at(deployment, "spec", "template", "spec", "containers", 0))["AUTH_VALKEY_URI"] {
		t.Error("AUTH_VALKEY_URI is set with no authService.valkey.secretName")
	}
	docs, out, err = renderAuth(t, append(append([]string{}, authBase...), authValkey...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	deployment = authDoc(docs, "Deployment", "a-dev-health-auth-service")
	container := at(deployment, "spec", "template", "spec", "containers", 0)
	var found bool
	for _, e := range at(container, "env").([]any) {
		if at(e, "name") != "AUTH_VALKEY_URI" {
			continue
		}
		found = true
		if at(e, "value") != nil {
			t.Error("AUTH_VALKEY_URI carries a literal value")
		}
		if at(e, "valueFrom", "secretKeyRef", "name") != "auth-ratelimit" || at(e, "valueFrom", "secretKeyRef", "key") != "AUTH_VALKEY_URI" {
			t.Errorf("AUTH_VALKEY_URI is not the secretKeyRef of the named Secret: %v", e)
		}
	}
	if !found {
		t.Error("AUTH_VALKEY_URI is not set although authService.valkey.secretName is")
	}
	for _, doc := range docs {
		if doc["kind"] == "Secret" && at(doc, "metadata", "name") == "auth-ratelimit" {
			t.Error("the chart creates the rate-limit Secret")
		}
	}
}

func TestAuthNetworkPolicyOpensTheRateLimitStoreOnlyWhenItIsSet(t *testing.T) {
	policy := func(args ...string) map[string]any {
		docs, out, err := renderAuth(t, append(append(append([]string{}, authBase...), authNetworkPolicy...), args...)...)
		if err != nil {
			t.Fatalf("render failed: %v\n%.600s", err, out)
		}
		return authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	}
	without := at(policy(), "spec", "egress").([]any)
	with := at(policy(authValkey...), "spec", "egress").([]any)
	if len(with) != len(without)+1 {
		t.Fatalf("egress rules: %d with the store, %d without; want exactly one more", len(with), len(without))
	}
	var port any
	for _, rule := range with {
		if ports, _ := at(rule, "ports").([]any); len(ports) == 1 && at(ports, 0, "port") == 6379 {
			port = at(ports, 0, "port")
			if to := at(rule, "to").([]any); len(to) != 1 || at(to, 0, "podSelector", "matchLabels", "app") != "valkey" {
				t.Errorf("the store rule reaches %v, want the listed peer only", to)
			}
		}
	}
	if port == nil {
		t.Error("no egress rule on the store port")
	}
}

func TestAuthServiceRateLimitStoreRefusals(t *testing.T) {
	cases := map[string]struct {
		args []string
		want string
	}{
		"secret without a peer": {append(append(append([]string{}, authBase...), authNetworkPolicy...), "--set", "authService.valkey.secretName=auth-ratelimit"), "authService.networkPolicy.egress.valkey.to is required"},
		"direct env":            {append(append([]string{}, authBase...), "--set", "authService.extraEnv[0].name=AUTH_VALKEY_URI", "--set", "authService.extraEnv[0].value=redis://x:y@h:6379/1"), "AUTH_VALKEY_URI"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, tc.args...)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("the refusal does not say %q: %.400s", tc.want, out)
			}
		})
	}
}
