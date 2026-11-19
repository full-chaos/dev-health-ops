package devhealth_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Web's session secret (AUTH_SECRET) is its own Secret key. Web never names the shared Secret the worker groups envFrom.

func helmOut(t *testing.T, off bool, args ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cmd := helmCommand(append([]string{"template", "hs256", ".", "--set", "goApi.enabled=true"}, args...)...)
	if off {
		cmd.Env = append(os.Environ(), "HELM_SHIM_OFF=1")
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func docByName(t *testing.T, rendered, kind, name string) (string, bool) {
	t.Helper()
	for _, document := range strings.Split(rendered, "\n---") {
		if strings.Contains(document, "\nkind: "+kind+"\n") && strings.Contains(document, "\n  name: "+name+"\n") {
			return document, true
		}
	}
	return "", false
}

func TestWebAuthSecretIsItsOwnSecretKey(t *testing.T) {
	out, err := helmOut(t, false, "--set", "goWorkers.enabled=false")
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, out)
	}
	web, _ := docByName(t, out, "Deployment", "hs256-dev-health-web")
	if strings.Contains(web, "JWT_SECRET_KEY") {
		t.Fatalf("web mentions JWT_SECRET_KEY:\n%s", web)
	}
	if !strings.Contains(web, "name: AUTH_SECRET\n              valueFrom:\n                secretKeyRef:\n                  name: \"web-auth-placeholder\"\n                  key: \"AUTH_SECRET\"") {
		t.Fatalf("web AUTH_SECRET is not its own Secret key:\n%s", web)
	}
	out, err = helmOut(t, true, "--set", "web.env.BACKEND_URL=http://x")
	if err == nil || !strings.Contains(out, "web.authSecret.secretName (or web.env.AUTH_SECRET) is required whenever web.enabled=true") {
		t.Fatalf("a web render without an auth secret was not refused: err=%v\n%.300s", err, out)
	}
	out, err = helmOut(t, true, "--set", "web.env.BACKEND_URL=http://x", "--set", "web.env.AUTH_SECRET=literal")
	web, _ = docByName(t, out, "Deployment", "hs256-dev-health-web")
	if err != nil || !strings.Contains(web, "name: AUTH_SECRET\n              value: \"literal\"") {
		t.Fatalf("a literal web.env.AUTH_SECRET did not render: %v", err)
	}
}

// web.localDevelopment is not a way to render a production web without a session secret.
func TestWebLocalDevelopmentStillNeedsAnAuthSecretInProduction(t *testing.T) {
	out, err := helmOut(t, true, "--set", "web.env.BACKEND_URL=http://x", "--set", "web.localDevelopment.enabled=true")
	if err == nil || !strings.Contains(out, "web.authSecret.secretName (or web.env.AUTH_SECRET) is required") {
		t.Fatalf("localDevelopment with NODE_ENV=production and no auth secret was not refused: err=%v\n%.300s", err, out)
	}
	out, err = helmOut(t, true, "--set", "web.env.BACKEND_URL=http://x", "--set", "web.localDevelopment.enabled=true", "--set", "web.env.NODE_ENV=development")
	if err != nil {
		t.Fatalf("localDevelopment with NODE_ENV=development must render without an auth secret: %v\n%.300s", err, out)
	}
}

const requiredValuesHeading = "Required values change: a separate Secret for web"

// The fail-closed web render tells the operator what to set and where to read about it; both docs carry the section.
func TestWebAuthSecretRefusalPointsAtTheDocs(t *testing.T) {
	out, err := helmOut(t, true, "--set", "web.env.BACKEND_URL=http://x")
	if err == nil {
		t.Fatalf("a web render without an auth secret succeeded")
	}
	for _, want := range []string{"web.authSecret.secretName", "docs/operate/install/production.md"} {
		if !strings.Contains(out, want) {
			t.Fatalf("the refusal does not say %q:\n%.600s", want, out)
		}
	}
	for _, doc := range []string{"../../../docs/operate/install/production.md", "../../../docs/operate/maintain/upgrade.md"} {
		raw, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, want := range []string{"## " + requiredValuesHeading, "web.authSecret", "required values change"} {
			if !strings.Contains(text, want) {
				t.Fatalf("%s lacks %q", doc, want)
			}
		}
	}
}

// web.authSecret must not name the shared Secret the worker groups envFrom.
func TestWebAuthSecretCannotPointAtTheSharedSecret(t *testing.T) {
	cases := map[string][]string{
		"chart-created shared Secret": {"--set", "web.authSecret.secretName=hs256-dev-health-secrets"},
		"case-folded shared Secret":   {"--set", "web.authSecret.secretName= HS256-Dev-Health-Secrets "},
		"external shared Secret":      {"--set", "secrets.create=false", "--set", "secrets.externalSecretName=shared-ext", "--set", "web.authSecret.secretName=Shared-Ext"},
	}
	for name, args := range cases {
		args = append([]string{"--set", "web.authSecret.secretName=web-auth-ok"}, args...)
		out, err := helmOut(t, true, append([]string{"--set", "web.env.BACKEND_URL=http://x"}, args...)...)
		if err == nil || !strings.Contains(out, "web.authSecret must not") {
			t.Fatalf("%s: was not refused: err=%v\n%.300s", name, err, out)
		}
	}
	out, err := helmOut(t, true, "--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=web-auth-ok")
	if err != nil {
		t.Fatalf("a distinct web.authSecret must render: %v\n%.300s", err, out)
	}
}

// Class rule: nothing that web can mount or read may name the shared Secret: web.authSecret, web and
// web.localDevelopment secret volumes (plain and projected), and a csi publish reference. web.extraEnv secretKeyRef may
// read single keys of it.
func TestWebCannotReachTheSharedSecretThroughAnySelector(t *testing.T) {
	base := []string{"--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=web-auth-ok"}
	ext := []string{"--set", "secrets.create=false", "--set", "secrets.externalSecretName=Shared-EXT"}
	envJSON := func(name, key string) []string {
		return []string{"--set-json", `web.extraEnv=[{"name":"X","valueFrom":{"secretKeyRef":{"name":"` + name + `","key":"` + key + `"}}}]`}
	}
	volJSON := func(path, body string) []string {
		return []string{"--set-json", path + `=[{"name":"v","` + body + `}]`}
	}
	dev := []string{"--set", "web.localDevelopment.enabled=true", "--set", "web.env.NODE_ENV=development"}
	cases := map[string][]string{
		"authSecret external shared, upper in values": append(append([]string{}, ext...), "--set", "web.authSecret.secretName=shared-ext"),
		"extraVolumes secret shared":                  volJSON("web.extraVolumes", `secret":{"secretName":"hs256-dev-health-secrets"}`),
		"extraVolumes projected shared":               volJSON("web.extraVolumes", `projected":{"sources":[{"secret":{"name":"hs256-dev-health-secrets"}}]}`),
		"localDevelopment volumes secret shared":      append(append([]string{}, dev...), volJSON("web.localDevelopment.extraVolumes", `secret":{"secretName":"hs256-dev-health-secrets"}`)...),
		"localDevelopment volumes projected shared":   append(append([]string{}, dev...), volJSON("web.localDevelopment.extraVolumes", `projected":{"sources":[{"secret":{"name":"hs256-dev-health-secrets"}}]}`)...),
		"external projected shared, upper in values":  append(append([]string{}, ext...), volJSON("web.extraVolumes", `projected":{"sources":[{"secret":{"name":"shared-ext"}}]}`)...),
	}
	for name, args := range cases {
		out, err := helmOut(t, true, append(append([]string{}, base...), args...)...)
		if err == nil || !strings.Contains(out, "must not name the shared Secret") {
			t.Fatalf("%s: was not refused: err=%v\n%.300s", name, err, out)
		}
	}
	controls := [][]string{
		envJSON("other-secret", "OTHER"),
		append(append([]string{}, ext...), envJSON("shared-ext", "OTHER")...),
		volJSON("web.extraVolumes", `secret":{"secretName":"other-secret"}`),
		volJSON("web.extraVolumes", `projected":{"sources":[{"secret":{"name":"other-secret"}}]}`),
		append(append([]string{}, dev...), volJSON("web.localDevelopment.extraVolumes", `secret":{"secretName":"other-secret"}`)...),
	}
	for _, args := range controls {
		out, err := helmOut(t, true, append(append([]string{}, base...), args...)...)
		if err != nil {
			t.Fatalf("a web selector naming another Secret must render: %v\n%.300s", err, out)
		}
		web, _ := docByName(t, out, "Deployment", "hs256-dev-health-web")
		if strings.Contains(web, "envFrom") {
			t.Fatalf("the web Deployment carries an envFrom:\n%s", web)
		}
	}
}

// Names web.authSecret owns, and a csi publish reference to the shared Secret, are refused.
func TestWebOwnedNamesAndCSIRefsAreRefused(t *testing.T) {
	base := []string{"--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=web-auth-ok"}
	extra := func(entry string) []string { return []string{"--set-json", "web.extraEnv=[" + entry + "]"} }
	cases := map[string]struct {
		args []string
		want string
	}{
		"extraEnv AUTH_SECRET literal":     {extra(`{"name":"AUTH_SECRET","value":"x"}`), "own the names AUTH_SECRET and NEXTAUTH_SECRET"},
		"extraEnv NEXTAUTH_SECRET literal": {extra(`{"name":"NEXTAUTH_SECRET","value":"x"}`), "own the names AUTH_SECRET and NEXTAUTH_SECRET"},
		"csi publish ref shared, folded":   {[]string{"--set-json", `web.extraVolumes=[{"name":"v","csi":{"driver":"d","nodePublishSecretRef":{"name":" HS256-Dev-Health-Secrets "}}}]`}, "must not name the shared Secret"},
	}
	for name, c := range cases {
		out, err := helmOut(t, true, append(append([]string{}, base...), c.args...)...)
		if err == nil || !strings.Contains(out, c.want) {
			t.Fatalf("%s: was not refused with %q: err=%v\n%.300s", name, c.want, err, out)
		}
	}
	out, err := helmOut(t, true, append(append([]string{}, base...), extra(`{"name":"X","value":"unrelated"}`)...)...)
	if err != nil {
		t.Fatalf("an unrelated literal extraEnv must render: %v\n%.300s", err, out)
	}
}

// Production web reads OAuth client keys from the shared Secret through web.extraEnv secretKeyRef: that must render.
func TestWebExtraEnvMayReadOtherKeysOfTheSharedSecret(t *testing.T) {
	base := []string{"--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=web-auth-ok", "--set", "secrets.create=false", "--set", "secrets.externalSecretName=dev-health-ops"}
	out, err := helmOut(t, true, append(append([]string{}, base...),
		"--set-json", `web.extraEnv=[{"name":"X","valueFrom":{"secretKeyRef":{"name":"dev-health-ops","key":"GITHUB_APP_CLIENT_ID","optional":true}}}]`)...)
	if err != nil {
		t.Fatalf("a prod-shaped web.extraEnv key of the shared Secret must render: %v\n%.400s", err, out)
	}
}

// The four OAuth keys production web reads from the shared Secret render, together, in one extraEnv.
func TestProdShapedWebExtraEnvOAuthKeysRender(t *testing.T) {
	keys := []string{"GITHUB_APP_CLIENT_ID", "GITHUB_APP_CLIENT_SECRET", "GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET"}
	var entries []string
	for _, key := range keys {
		entries = append(entries, `{"name":"`+key+`","valueFrom":{"secretKeyRef":{"name":"dev-health-ops","key":"`+key+`","optional":true}}}`)
	}
	out, err := helmOut(t, true, "--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=dev-health-web-auth",
		"--set", "secrets.create=false", "--set", "secrets.externalSecretName=dev-health-ops",
		"--set-json", "web.extraEnv=["+strings.Join(entries, ",")+"]")
	if err != nil {
		t.Fatalf("the prod OAuth refs must render: %v\n%.400s", err, out)
	}
	web, _ := docByName(t, out, "Deployment", "hs256-dev-health-web")
	for _, key := range keys {
		if !strings.Contains(web, "key: "+key) {
			t.Fatalf("the web Deployment lacks %s:\n%s", key, web)
		}
	}
}

// web.authSecret.key is checked for Secret key form; a numeric key is a valid Secret key once rendered as a string.
func TestWebAuthSecretKeyForm(t *testing.T) {
	base := []string{"--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=web-auth-ok"}
	for name, key := range map[string]string{"space": "bad key", "slash": "a/b", "dot": ".", "dotdot prefix": "..x"} {
		out, err := helmOut(t, true, append(append([]string{}, base...), "--set-string", "web.authSecret.key="+key)...)
		if err == nil || !strings.Contains(out, "web.authSecret.key must be a Secret key") {
			t.Errorf("%s: want a refusal naming web.authSecret.key, got err=%v\n%.300s", name, err, out)
		}
	}
	out, err := helmOut(t, true, append(append([]string{}, base...), "--set-string", "web.authSecret.key=")...)
	if err == nil || !strings.Contains(out, "web.authSecret.key must be a Secret key") {
		t.Errorf("empty key: want a refusal naming web.authSecret.key, got err=%v\n%.300s", err, out)
	}
	for _, key := range []string{"123", "0"} {
		out, err := helmOut(t, true, append(append([]string{}, base...), "--set-json", "web.authSecret.key="+key)...)
		if err != nil {
			t.Fatalf("numeric key %s must render: %v\n%.300s", key, err, out)
		}
		web, _ := docByName(t, out, "Deployment", "hs256-dev-health-web")
		if !strings.Contains(web, "key: \""+key+"\"") {
			t.Fatalf("web AUTH_SECRET key is not %q:\n%s", key, web)
		}
	}
}
