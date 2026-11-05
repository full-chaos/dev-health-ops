package devhealth_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// CHAOS-12928: a web render that turns SSO on needs the literal eddsa token class.

func ssoEnvArgs(t *testing.T, env ...map[string]any) []string {
	t.Helper()
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]string{}, authBase...), "--set-json", "web.extraEnv="+string(raw))
}

func ssoVal(name string, value any) map[string]any {
	return map[string]any{"name": name, "value": value}
}

func ssoRef(name string) map[string]any {
	return map[string]any{"name": name, "valueFrom": map[string]any{"configMapKeyRef": map[string]any{"name": "web-cfg", "key": name}}}
}

func webEnvDump(t *testing.T, args []string) string {
	t.Helper()
	docs, out, err := renderAuthRaw(t, args...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, out)
	}
	var web map[string]any
	for _, d := range docs {
		if d["kind"] == "Deployment" && strings.HasSuffix(fmt.Sprint(at(d, "metadata", "name")), "-web") {
			web = d
		}
	}
	if web == nil {
		t.Fatal("no web Deployment")
	}
	raw, _ := json.Marshal(at(web, "spec", "template", "spec", "containers", 0, "env"))
	return string(raw)
}

func wantSSORefusal(t *testing.T, label string, args []string) {
	t.Helper()
	_, out, err := renderAuthRaw(t, args...)
	if err == nil {
		t.Errorf("%s: rendered; want a refusal", label)
		return
	}
	for _, s := range []string{"AUTH_SSO_ENABLED", "AUTH_ACCESS_TOKEN_CLASS", "eddsa", "CHAOS-12928"} {
		if !strings.Contains(out, s) {
			t.Errorf("%s: message lacks %q: %.400s", label, s, out)
		}
	}
}

func TestWebSSOOnNeedsEddsaClass(t *testing.T) {
	for _, on := range []any{"true", "TRUE", " 1 ", true} {
		for _, class := range []*map[string]any{nil, ptr(ssoVal("AUTH_ACCESS_TOKEN_CLASS", "")), ptr(ssoVal("AUTH_ACCESS_TOKEN_CLASS", "hs256_compat")), ptr(ssoVal("AUTH_ACCESS_TOKEN_CLASS", "rs256")), ptr(ssoRef("AUTH_ACCESS_TOKEN_CLASS"))} { // gitleaks:allow
			env := []map[string]any{ssoVal("AUTH_SSO_ENABLED", on)}
			if class != nil {
				env = append(env, *class)
			}
			wantSSORefusal(t, fmt.Sprintf("sso=%v class=%v", on, class), ssoEnvArgs(t, env...))
		}
	}
}

func TestWebSSOByValueFromFailsClosed(t *testing.T) {
	wantSSORefusal(t, "sso ref, no class", ssoEnvArgs(t, ssoRef("AUTH_SSO_ENABLED")))
	wantSSORefusal(t, "sso ref, hs256", ssoEnvArgs(t, ssoRef("AUTH_SSO_ENABLED"), ssoVal("AUTH_ACCESS_TOKEN_CLASS", "hs256_compat"))) // gitleaks:allow
	wantSSORefusal(t, "sso ref, class ref", ssoEnvArgs(t, ssoRef("AUTH_SSO_ENABLED"), ssoRef("AUTH_ACCESS_TOKEN_CLASS")))
	// literal eddsa passes with SSO by reference
	got := webEnvDump(t, ssoEnvArgs(t, ssoRef("AUTH_SSO_ENABLED"), ssoVal("AUTH_ACCESS_TOKEN_CLASS", "eddsa")))
	if !strings.Contains(got, "AUTH_SSO_ENABLED") {
		t.Errorf("env lacks AUTH_SSO_ENABLED: %s", got)
	}
}

func TestWebSSOOnWithEddsaRenders(t *testing.T) {
	got := webEnvDump(t, ssoEnvArgs(t, ssoVal("AUTH_SSO_ENABLED", "true"), ssoVal("AUTH_ACCESS_TOKEN_CLASS", "eddsa")))
	for _, s := range []string{`"AUTH_SSO_ENABLED"`, `"AUTH_ACCESS_TOKEN_CLASS"`} {
		if !strings.Contains(got, s) {
			t.Errorf("env lacks %s: %s", s, got)
		}
	}
}

// web reads the class with an exact match (resolveTokenClass), so the chart does not trim or fold case.
func TestWebSSOClassIsTheExactLiteral(t *testing.T) {
	for _, class := range []string{"EdDSA", "EDDSA", " eddsa ", " EdDSA ", "eddsa\n"} {
		wantSSORefusal(t, fmt.Sprintf("sso on, class %q", class), ssoEnvArgs(t, ssoVal("AUTH_SSO_ENABLED", "true"), ssoVal("AUTH_ACCESS_TOKEN_CLASS", class)))
		wantSSORefusal(t, fmt.Sprintf("sso ref, class %q", class), ssoEnvArgs(t, ssoRef("AUTH_SSO_ENABLED"), ssoVal("AUTH_ACCESS_TOKEN_CLASS", class)))
	}
}

func TestWebSSOOffRendersAsToday(t *testing.T) {
	base := webEnvDump(t, append([]string{}, authBase...))
	for _, off := range []any{"false", "0", "", "FALSE"} {
		for _, class := range [][]map[string]any{nil, {ssoVal("AUTH_ACCESS_TOKEN_CLASS", "hs256_compat")}, {ssoVal("AUTH_ACCESS_TOKEN_CLASS", "eddsa")}, {ssoRef("AUTH_ACCESS_TOKEN_CLASS")}} { // gitleaks:allow
			env := append([]map[string]any{ssoVal("AUTH_SSO_ENABLED", off)}, class...)
			got := webEnvDump(t, ssoEnvArgs(t, env...))
			if !strings.HasPrefix(got, strings.TrimSuffix(base, "]")) || !strings.Contains(got, `"AUTH_SSO_ENABLED"`) {
				t.Errorf("sso=%q: env is not the base env plus the extra entries:\nbase=%s\ngot=%s", off, base, got)
			}
		}
	}
	for _, class := range []string{"hs256_compat", "rs256", ""} {
		got := webEnvDump(t, ssoEnvArgs(t, ssoVal("AUTH_ACCESS_TOKEN_CLASS", class)))
		if !strings.HasPrefix(got, strings.TrimSuffix(base, "]")) {
			t.Errorf("class-only %q changed the base env: %s", class, got)
		}
	}
}

func ptr(m map[string]any) *map[string]any { return &m }

func ssoEmptyRef(name string) map[string]any {
	m := ssoRef(name)
	m["value"] = ""
	return m
}

func TestWebSSOEmptyValueWithValueFromFailsClosed(t *testing.T) {
	sso := ssoEmptyRef("AUTH_SSO_ENABLED")
	wantSSORefusal(t, "sso empty+ref, no class", ssoEnvArgs(t, sso))
	wantSSORefusal(t, "sso empty+ref, hs256", ssoEnvArgs(t, sso, ssoVal("AUTH_ACCESS_TOKEN_CLASS", "hs256_compat"))) // gitleaks:allow
	wantSSORefusal(t, "sso empty+ref, class ref", ssoEnvArgs(t, sso, ssoRef("AUTH_ACCESS_TOKEN_CLASS")))
	wantSSORefusal(t, "sso on, class eddsa+ref", ssoEnvArgs(t, ssoVal("AUTH_SSO_ENABLED", "true"), func() map[string]any { m := ssoRef("AUTH_ACCESS_TOKEN_CLASS"); m["value"] = "eddsa"; return m }()))
	got := webEnvDump(t, ssoEnvArgs(t, sso, ssoVal("AUTH_ACCESS_TOKEN_CLASS", "eddsa")))
	if !strings.Contains(got, "AUTH_SSO_ENABLED") || !strings.Contains(got, "AUTH_ACCESS_TOKEN_CLASS") {
		t.Errorf("sso empty+ref, literal eddsa: env lacks an entry: %s", got)
	}
}
