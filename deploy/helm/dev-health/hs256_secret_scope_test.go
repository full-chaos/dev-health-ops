package devhealth_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
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
