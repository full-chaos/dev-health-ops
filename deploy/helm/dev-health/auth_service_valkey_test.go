package devhealth_test

import (
	"fmt"
	"strings"
	"testing"
)

var authValkey = []string{
	"--set", "authService.valkey.secretName=" + storeSecret,
	"--set-json", `authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"valkey"}}}]`,
}

const storeSecret = "auth-ratelimit"

// secretRefs lists every Secret reference of a container, as "name/key" for a
// secretKeyRef and "name/*" for an envFrom secretRef, with each reference's
// `optional` value ("" when the key is absent).
func secretRefs(container any) (refs []string, optional map[string]string) {
	optional = map[string]string{}
	for _, e := range aList(at(container, "env")) {
		if name, ok := at(e, "valueFrom", "secretKeyRef", "name").(string); ok {
			ref := name + "/" + aStr(at(e, "valueFrom", "secretKeyRef", "key"))
			refs = append(refs, ref)
			if v, present := aMap(at(e, "valueFrom", "secretKeyRef"))["optional"]; present {
				optional[ref] = strings.ToLower(fmt.Sprint(v))
			}
		}
	}
	for _, e := range aList(at(container, "envFrom")) {
		if name, ok := at(e, "secretRef", "name").(string); ok {
			refs = append(refs, name+"/*")
		}
	}
	return refs, optional
}

func authContainer(t *testing.T, args ...string) (container any, out string) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if deployment == nil {
		t.Fatal("no auth-service Deployment rendered")
	}
	return at(deployment, "spec", "template", "spec", "containers", 0), out
}

// The rate-limit store's login reaches the container by ONE secretKeyRef only
// while authService.valkey is set. Roll 18 ships with the Secret absent: with
// the name unset the auth-service container has no reference to the store
// Secret (no secretKeyRef, no envFrom, no AUTH_VALKEY_URI variable), and with it
// set the container gains exactly one reference, to its own key, without an
// `optional` key (a named but missing Secret must fail loudly).
func TestAuthServiceRateLimitStoreLoginIsOptionalAndByReference(t *testing.T) {
	unset, unsetOut := authContainer(t, authBase...)
	if envNames(unset)["AUTH_VALKEY_URI"] {
		t.Error("AUTH_VALKEY_URI is set with no authService.valkey.secretName")
	}
	baseRefs, _ := secretRefs(unset)
	if len(baseRefs) == 0 {
		t.Fatal("the base render has no Secret reference at all: the reference reader is not reading")
	}
	for _, ref := range baseRefs {
		if strings.HasPrefix(ref, storeSecret+"/") {
			t.Errorf("with the name unset the container refers to the store Secret: %s", ref)
		}
		if strings.HasSuffix(ref, "/*") {
			t.Errorf("the container takes a whole Secret through envFrom: %s", ref)
		}
	}
	if strings.Contains(unsetOut, "AUTH_VALKEY_URI") || strings.Contains(unsetOut, storeSecret) {
		t.Error("the render mentions the store Secret or its variable although authService.valkey.secretName is unset")
	}

	set, _ := authContainer(t, append(append([]string{}, authBase...), authValkey...)...)
	setRefs, optional := secretRefs(set)
	var storeRefs int
	for _, ref := range setRefs {
		if strings.HasPrefix(ref, storeSecret+"/") {
			storeRefs++
			if ref != storeSecret+"/AUTH_VALKEY_URI" {
				t.Errorf("the container refers to %s, want only the AUTH_VALKEY_URI key", ref)
			}
			if v := optional[ref]; v != "" && v != "false" {
				t.Errorf("the store reference carries optional=%q: a missing Secret must fail the pod, not start it quietly", v)
			}
		}
		if strings.HasSuffix(ref, "/*") {
			t.Errorf("the container takes a whole Secret through envFrom: %s", ref)
		}
	}
	if storeRefs != 1 {
		t.Errorf("%d references to the store Secret, want exactly one", storeRefs)
	}
	if len(setRefs) != len(baseRefs)+1 {
		t.Errorf("setting the name changed the container's Secret references from %d to %d, want exactly one more", len(baseRefs), len(setRefs))
	}
	var literal bool
	for _, e := range aList(at(set, "env")) {
		if at(e, "name") == "AUTH_VALKEY_URI" && at(e, "value") != nil {
			literal = true
		}
	}
	if literal {
		t.Error("AUTH_VALKEY_URI carries a literal value")
	}
	docs, _, err := renderAuth(t, append(append([]string{}, authBase...), authValkey...)...)
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range docs {
		if doc["kind"] == "Secret" && at(doc, "metadata", "name") == storeSecret {
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

func aList(v any) []any { l, _ := v.([]any); return l }

func aMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

func aStr(v any) string { s, _ := v.(string); return s }

// The session routes count the compatibility refresh in the shared store, so
// the chart refuses to render them without the store's login.
func TestAuthServiceSessionKeysNeedTheRateLimitStore(t *testing.T) {
	sessions := []string{"--set", "authService.sessionKeys.successor.secretName=auth-session"}
	_, out, err := renderAuth(t, append(append([]string{}, authBase...), sessions...)...)
	if err == nil || !strings.Contains(out, "authService.valkey.secretName is required") {
		t.Fatalf("sessions without the store rendered: err %v\n%.400s", err, out)
	}
	withStore := append(append(append([]string{}, authBase...), sessions...), authValkey...)
	if _, out, err := renderAuth(t, withStore...); err != nil {
		t.Fatalf("sessions with the store refused: %v\n%.400s", err, out)
	}
}
