package devhealth_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10446: the HS256 access-token key (JWT_SECRET_KEY) lives in its own Secret, referenced explicitly by go-api
// (and by operator values for query-api). The shared Secret the worker groups envFrom, and web, never carry or name it.

func helmOut(t *testing.T, off bool, args ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cmd := exec.Command("helm", append([]string{"template", "hs256", ".", "--set", "goApi.enabled=true"}, args...)...)
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

func workerDeployments(t *testing.T, rendered string) []string {
	t.Helper()
	var docs []string
	for _, document := range strings.Split(rendered, "\n---") {
		if strings.Contains(document, "\nkind: Deployment\n") && strings.Contains(document, "dev-health.io/worker-group:") {
			docs = append(docs, document)
		}
	}
	if len(docs) < 4 {
		t.Fatalf("expected the worker group Deployments, found %d", len(docs))
	}
	return docs
}

// secrets.create: the key moves out of the shared Secret into <release>-hs256; go-api reads it there; workers and web never name it.
func TestChartCreatedHS256KeyLivesInItsOwnSecret(t *testing.T) {
	out, err := helmOut(t, false, "--set", "secrets.data.JWT_SECRET_KEY=sentinel-hs256-value")
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, out)
	}
	shared, ok := docByName(t, out, "Secret", "hs256-dev-health-secrets")
	if !ok {
		t.Fatalf("no shared Secret rendered")
	}
	if strings.Contains(shared, "JWT_SECRET_KEY") || strings.Contains(shared, "sentinel-hs256-value") {
		t.Fatalf("the shared Secret carries the HS256 key:\n%s", shared)
	}
	own, ok := docByName(t, out, "Secret", "hs256-dev-health-hs256")
	if !ok || !strings.Contains(own, "JWT_SECRET_KEY: \"sentinel-hs256-value\"") {
		t.Fatalf("the hs256 Secret is missing or lacks the key (found=%v):\n%s", ok, own)
	}
	api, ok := docByName(t, out, "Deployment", "hs256-dev-health-go-api")
	if !ok || !strings.Contains(api, "name: JWT_SECRET_KEY\n              valueFrom: {secretKeyRef: {name: hs256-dev-health-hs256, key: JWT_SECRET_KEY}}") {
		t.Fatalf("go-api does not read JWT_SECRET_KEY from the hs256 Secret (found=%v):\n%s", ok, api)
	}
	for _, deployment := range workerDeployments(t, out) {
		for _, banned := range []string{"JWT_SECRET_KEY", "hs256-dev-health-hs256", "sentinel-hs256-value"} {
			if strings.Contains(deployment, banned) {
				t.Fatalf("a worker group names %s:\n%s", banned, deployment)
			}
		}
	}
	web, ok := docByName(t, out, "Deployment", "hs256-dev-health-web")
	if !ok {
		t.Fatalf("no web Deployment")
	}
	for _, banned := range []string{"JWT_SECRET_KEY", "hs256-dev-health-hs256"} {
		if strings.Contains(web, banned) {
			t.Fatalf("web names %s:\n%s", banned, web)
		}
	}
}

// secrets.create=false (production): hs256Secret.externalSecretName is required and is the only Secret go-api reads the key from.
func TestExternalHS256SecretIsRequiredAndUsed(t *testing.T) {
	base := []string{"--set", "secrets.create=false", "--set", "secrets.externalSecretName=shared-ext"}
	out, err := helmOut(t, false, base...)
	if err == nil || !strings.Contains(out, "hs256Secret.externalSecretName is required") {
		t.Fatalf("a go-api render with an external shared Secret and no hs256 Secret was not refused: err=%v\n%.300s", err, out)
	}
	out, err = helmOut(t, false, append(base, "--set", "hs256Secret.externalSecretName=hs256-ext")...)
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, out)
	}
	if _, found := docByName(t, out, "Secret", "hs256-dev-health-hs256"); found {
		t.Fatalf("the chart rendered an hs256 Secret although an external one is named")
	}
	api, _ := docByName(t, out, "Deployment", "hs256-dev-health-go-api")
	if !strings.Contains(api, "secretKeyRef: {name: hs256-ext, key: JWT_SECRET_KEY}") {
		t.Fatalf("go-api does not read the key from the external hs256 Secret:\n%s", api)
	}
	for _, deployment := range workerDeployments(t, out) {
		if strings.Contains(deployment, "hs256-ext") || strings.Contains(deployment, "JWT_SECRET_KEY") {
			t.Fatalf("a worker group reaches the hs256 Secret:\n%s", deployment)
		}
		if !strings.Contains(deployment, "name: shared-ext") {
			t.Fatalf("a worker group lost the shared Secret envFrom:\n%s", deployment)
		}
	}
}

func TestWebAuthSecretNeverFallsBackToTheMintKey(t *testing.T) {
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

func goAPIPodTemplate(t *testing.T, key string) string {
	t.Helper()
	out, err := helmOut(t, false, "--set", "secrets.data.JWT_SECRET_KEY="+key)
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, out)
	}
	api, ok := docByName(t, out, "Deployment", "hs256-dev-health-go-api")
	if !ok {
		t.Fatalf("no go-api Deployment")
	}
	start := strings.Index(api, "\n  template:\n")
	if start < 0 {
		t.Fatalf("no pod template in the go-api Deployment")
	}
	return api[start:]
}

// A key change in the chart-created hs256 Secret must roll go-api: envFrom/env never restart a pod on their own.
func TestHS256KeyChangeRollsGoAPI(t *testing.T) {
	a, b := goAPIPodTemplate(t, "key-A"), goAPIPodTemplate(t, "key-B")
	if !strings.Contains(a, "checksum/hs256:") {
		t.Fatalf("the go-api pod template carries no checksum/hs256 annotation:\n%s", a)
	}
	if a == b {
		t.Fatalf("the go-api pod template is identical for key A and key B")
	}
	if a != goAPIPodTemplate(t, "key-A") {
		t.Fatalf("the go-api pod template is not stable for the same key")
	}
	if strings.Contains(a, "key-A") {
		t.Fatalf("the key value leaked into the pod template")
	}
}

// The hs256 Secret must never alias the shared Secret the worker groups envFrom, in any spelling.
func TestHS256SecretMustDifferFromTheSharedSecret(t *testing.T) {
	for _, alias := range []string{"shared-ext", "Shared-Ext", " shared-ext "} {
		out, err := helmOut(t, false, "--set", "secrets.create=false", "--set", "secrets.externalSecretName=shared-ext",
			"--set", "hs256Secret.externalSecretName="+alias)
		if err == nil || !strings.Contains(out, "must differ") {
			t.Fatalf("hs256Secret.externalSecretName=%q aliasing the shared Secret was not refused: err=%v\n%.300s", alias, err, out)
		}
	}
}

// A user pod annotation of the same name must not displace the chart's checksum: parse the YAML, do not grep.
func TestUserAnnotationCannotOverrideTheHS256Checksum(t *testing.T) {
	annotations := func(key string) map[string]any {
		out, err := helmOut(t, false, "--set", "secrets.data.JWT_SECRET_KEY="+key, "--set", "goApi.podAnnotations.checksum/hs256=user-value", "--set", "goApi.podAnnotations.keep=me")
		if err != nil {
			t.Fatalf("render failed: %v\n%s", err, out)
		}
		api, _ := docByName(t, out, "Deployment", "hs256-dev-health-go-api")
		var parsed struct {
			Spec struct {
				Template struct {
					Metadata struct {
						Annotations map[string]any `yaml:"annotations"`
					} `yaml:"metadata"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := yaml.Unmarshal([]byte(api), &parsed); err != nil {
			t.Fatalf("parse go-api: %v\n%s", err, api)
		}
		return parsed.Spec.Template.Metadata.Annotations
	}
	a, b := annotations("key-A"), annotations("key-B")
	if a["checksum/hs256"] == "user-value" || a["checksum/hs256"] == b["checksum/hs256"] {
		t.Fatalf("the user annotation displaced the chart checksum: A=%v B=%v", a["checksum/hs256"], b["checksum/hs256"])
	}
	if a["keep"] != "me" {
		t.Fatalf("a user annotation was dropped: %v", a)
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

const requiredValuesHeading = "Required values change: separate Secrets for web and the HS256 key"

// The fail-closed web render tells the operator what to set and where to read about it; both docs carry the section.
func TestWebAuthSecretRefusalPointsAtTheDocs(t *testing.T) {
	out, err := helmOut(t, true, "--set", "web.env.BACKEND_URL=http://x")
	if err == nil {
		t.Fatalf("a web render without an auth secret succeeded")
	}
	for _, want := range []string{"web.authSecret.secretName", "docs/operate/install/production.md", requiredValuesHeading} {
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
		for _, want := range []string{"## " + requiredValuesHeading, "web.authSecret", "hs256Secret.externalSecretName", "required values change"} {
			if !strings.Contains(text, want) {
				t.Fatalf("%s lacks %q", doc, want)
			}
		}
	}
}

// web.authSecret must not name the HS256 key's Secret, the shared Secret, or the key JWT_SECRET_KEY itself.
func TestWebAuthSecretCannotPointAtTheMintKey(t *testing.T) {
	cases := map[string][]string{
		"chart-created hs256 Secret":     {"--set", "web.authSecret.secretName=hs256-dev-health-hs256"},
		"chart-created shared Secret":    {"--set", "web.authSecret.secretName=hs256-dev-health-secrets"},
		"case-folded hs256 Secret":       {"--set", "web.authSecret.secretName= HS256-Dev-Health-HS256 "},
		"key is JWT_SECRET_KEY":          {"--set", "web.authSecret.key=JWT_SECRET_KEY"},
		"external hs256 Secret":          {"--set", "secrets.create=false", "--set", "secrets.externalSecretName=shared-ext", "--set", "hs256Secret.externalSecretName=hs256-ext", "--set", "web.authSecret.secretName=hs256-ext"},
		"external shared Secret":         {"--set", "secrets.create=false", "--set", "secrets.externalSecretName=shared-ext", "--set", "hs256Secret.externalSecretName=hs256-ext", "--set", "web.authSecret.secretName=Shared-Ext"},
		"external hs256, web key is JWT": {"--set", "secrets.create=false", "--set", "secrets.externalSecretName=shared-ext", "--set", "hs256Secret.externalSecretName=hs256-ext", "--set", "web.authSecret.key=JWT_SECRET_KEY"},
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

// Class rule: nothing that web can mount or read may name the HS256 key's Secret, the shared Secret, or the key
// JWT_SECRET_KEY: web.authSecret, web.extraEnv secretKeyRef, web and web.localDevelopment secret volumes (plain and
// projected), and a literal web.env.AUTH_SECRET equal to the key.
func TestWebCannotReachTheMintKeyThroughAnySelector(t *testing.T) {
	base := []string{"--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=web-auth-ok"}
	ext := []string{"--set", "secrets.create=false", "--set", "secrets.externalSecretName=Shared-EXT", "--set", "hs256Secret.externalSecretName=HS256-Ext"}
	envJSON := func(name, key string) []string {
		return []string{"--set-json", `web.extraEnv=[{"name":"X","valueFrom":{"secretKeyRef":{"name":"` + name + `","key":"` + key + `"}}}]`}
	}
	volJSON := func(path, body string) []string {
		return []string{"--set-json", path + `=[{"name":"v","` + body + `}]`}
	}
	dev := []string{"--set", "web.localDevelopment.enabled=true", "--set", "web.env.NODE_ENV=development"}
	cases := map[string][]string{
		"authSecret external hs256, upper in values":  append(append([]string{}, ext...), "--set", "web.authSecret.secretName=hs256-ext"),
		"authSecret external shared, upper in values": append(append([]string{}, ext...), "--set", "web.authSecret.secretName=shared-ext"),
		"extraEnv hs256 Secret":                       envJSON("hs256-dev-health-hs256", "OTHER"),
		"extraEnv shared Secret, folded":              envJSON(" HS256-Dev-Health-Secrets ", "OTHER"),
		"extraEnv key JWT_SECRET_KEY":                 envJSON("some-secret", "JWT_SECRET_KEY"),
		"extraEnv external hs256, upper in values":    append(append([]string{}, ext...), envJSON("hs256-ext", "OTHER")...),
		"extraEnv external shared, upper in values":   append(append([]string{}, ext...), envJSON("shared-ext", "OTHER")...),
		"extraVolumes secret hs256":                   volJSON("web.extraVolumes", `secret":{"secretName":"hs256-dev-health-hs256"}`),
		"extraVolumes secret shared":                  volJSON("web.extraVolumes", `secret":{"secretName":"hs256-dev-health-secrets"}`),
		"extraVolumes secret item key":                volJSON("web.extraVolumes", `secret":{"secretName":"ok","items":[{"key":"JWT_SECRET_KEY","path":"k"}]}`),
		"extraVolumes projected hs256":                volJSON("web.extraVolumes", `projected":{"sources":[{"secret":{"name":"hs256-dev-health-hs256"}}]}`),
		"extraVolumes projected item key":             volJSON("web.extraVolumes", `projected":{"sources":[{"secret":{"name":"ok","items":[{"key":"JWT_SECRET_KEY","path":"k"}]}}]}`),
		"localDevelopment volumes secret hs256":       append(append([]string{}, dev...), volJSON("web.localDevelopment.extraVolumes", `secret":{"secretName":"hs256-dev-health-hs256"}`)...),
		"localDevelopment volumes projected shared":   append(append([]string{}, dev...), volJSON("web.localDevelopment.extraVolumes", `projected":{"sources":[{"secret":{"name":"hs256-dev-health-secrets"}}]}`)...),
		"literal AUTH_SECRET equals the HS256 key":    {"--set", "web.env.AUTH_SECRET=same-value", "--set", "secrets.data.JWT_SECRET_KEY=same-value"},
		"external projected hs256, upper in values":   append(append([]string{}, ext...), volJSON("web.extraVolumes", `projected":{"sources":[{"secret":{"name":"hs256-ext"}}]}`)...),
	}
	for name, args := range cases {
		out, err := helmOut(t, true, append(append([]string{}, base...), args...)...)
		if err == nil || !strings.Contains(out, "must not name the HS256 key's Secret") {
			t.Fatalf("%s: was not refused: err=%v\n%.300s", name, err, out)
		}
	}
	controls := [][]string{
		envJSON("other-secret", "OTHER"),
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
