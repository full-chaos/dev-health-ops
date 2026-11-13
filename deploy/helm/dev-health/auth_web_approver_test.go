package devhealth_test

import (
	"fmt"
	"strings"
	"testing"
)

// The web approver reference kind of POST /v1/authorize (CHAOS-11207): authService.webApprover mounts the JWKS Secret
// acr verifies web assertions with and renders the three AUTH_AUTHORIZE_WEB_APPROVER_* settings. Empty renders nothing.

const (
	webApproverMountPath = "/run/auth/web-approver"
	webApproverJWKSFile  = webApproverMountPath + "/jwks.json"
	// acr-api's live workload principal: its base64url starts with "_", so the wire form has two underscores.
	webApproverCaller = "prn__fYQz6HDTDeOd_M7zDN0fQ"
)

var webApproverEnvNames = []string{"AUTH_AUTHORIZE_WEB_APPROVER_ISSUER", "AUTH_AUTHORIZE_WEB_APPROVER_JWKS_FILE", "AUTH_AUTHORIZE_WEB_APPROVER_CALLER"}

func webApproverSets(issuer, caller, secretName string) []string {
	return []string{
		"--set-string", "authService.webApprover.issuer=" + issuer,
		"--set-string", "authService.webApprover.caller=" + caller,
		"--set-string", "authService.webApprover.jwks.secretName=" + secretName,
	}
}

func authServicePod(t *testing.T, docs []map[string]any) (container, spec any) {
	t.Helper()
	service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if service == nil {
		t.Fatal("no auth-service Deployment rendered")
	}
	spec = at(service, "spec", "template", "spec")
	containers, _ := at(spec, "containers").([]any)
	for _, c := range containers {
		if at(c, "name") == "auth-service" {
			return c, spec
		}
	}
	t.Fatal("no auth-service container rendered")
	return nil, nil
}

// Off: the default and an explicitly empty key render the same bytes, with no volume, mount or env of the kind. The
// default render is the one testdata/auth_phase/phase2.golden.json pins, which this change leaves unchanged.
func TestAuthWebApproverOffRendersNothing(t *testing.T) {
	t.Parallel()

	_, base, err := renderAuth(t, phaseArgs(2)...)
	if err != nil {
		t.Fatalf("default render: %v\n%.400s", err, base)
	}
	docs, empty, err := renderAuth(t, phaseArgs(2, webApproverSets("", "", "")...)...)
	if err != nil {
		t.Fatalf("empty key render: %v\n%.400s", err, empty)
	}
	if empty != base {
		t.Fatal("an empty authService.webApprover renders differently from the default")
	}
	container, spec := authServicePod(t, docs)
	if namedEntry(at(spec, "volumes"), "web-approver-jwks") != nil || namedEntry(at(container, "volumeMounts"), "web-approver-jwks") != nil {
		t.Fatal("the default render mounts the web approver JWKS")
	}
	for _, name := range webApproverEnvNames {
		if _, ok := envValue(container, name); ok {
			t.Errorf("the default render sets %s", name)
		}
	}
}

// On: the whole Secret directory is mounted read-only (no subPath, so a rotation reaches the pod) and the three
// settings point at it.
func TestAuthWebApproverOnMountsTheSecretAndRendersTheSettings(t *testing.T) {
	t.Parallel()

	docs, out, err := renderAuth(t, phaseArgs(2, webApproverSets("https://www.fullchaos.dev", webApproverCaller, "acr-web-assertion-jwks")...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	container, spec := authServicePod(t, docs)
	volume := namedEntry(at(spec, "volumes"), "web-approver-jwks")
	if volume == nil {
		t.Fatal("no web-approver-jwks volume")
	}
	if got := at(volume, "secret", "secretName"); got != "acr-web-assertion-jwks" {
		t.Errorf("volume secretName = %v", got)
	}
	if got, want := fmt.Sprint(at(volume, "secret", "items")), "[map[key:web-jwks.json path:jwks.json]]"; got != want {
		t.Errorf("volume items = %s, want %s", got, want)
	}
	mount := namedEntry(at(container, "volumeMounts"), "web-approver-jwks")
	if mount == nil {
		t.Fatal("no web-approver-jwks mount")
	}
	if at(mount, "mountPath") != webApproverMountPath || at(mount, "readOnly") != true || at(mount, "subPath") != nil {
		t.Errorf("mount = %v, want %s read-only without subPath", mount, webApproverMountPath)
	}
	want := map[string]string{
		"AUTH_AUTHORIZE_WEB_APPROVER_ISSUER":    "https://www.fullchaos.dev",
		"AUTH_AUTHORIZE_WEB_APPROVER_JWKS_FILE": webApproverJWKSFile,
		"AUTH_AUTHORIZE_WEB_APPROVER_CALLER":    webApproverCaller,
	}
	for name, value := range want {
		if got, ok := envValue(container, name); !ok || got != value {
			t.Errorf("%s = %q (set %v), want %q", name, got, ok, value)
		}
	}
}

// A partial key is refused at render, with one message naming exactly the missing key.
func TestAuthWebApproverPartialKeyIsRefused(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		issuer, caller, secret string
		missing                string
	}{
		"issuer missing":          {"", webApproverCaller, "acr-web-assertion-jwks", "authService.webApprover.issuer"},
		"caller missing":          {"https://www.fullchaos.dev", "", "acr-web-assertion-jwks", "authService.webApprover.caller"},
		"jwks.secretName missing": {"https://www.fullchaos.dev", webApproverCaller, "", "authService.webApprover.jwks.secretName"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, out, err := renderAuth(t, phaseArgs(2, webApproverSets(c.issuer, c.caller, c.secret)...)...)
			if err == nil {
				t.Fatal("a partial authService.webApprover rendered")
			}
			want := "authService.webApprover is partly set: missing " + c.missing + ". "
			if !strings.Contains(out, want) {
				t.Fatalf("the refusal does not name exactly %s: %.400s", c.missing, out)
			}
		})
	}
}

// The three settings have one source: authService.extraEnv refuses each name, in any case.
func TestAuthWebApproverExtraEnvIsRefused(t *testing.T) {
	t.Parallel()

	for _, name := range webApproverEnvNames {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for _, spelling := range []string{name, strings.ToLower(name)} {
				_, out, err := renderAuth(t, phaseArgs(2, "--set-json", fmt.Sprintf(`authService.extraEnv=[{"name":%q,"value":"x"}]`, spelling))...)
				if err == nil {
					t.Fatalf("authService.extraEnv %s rendered", spelling)
				}
				if !strings.Contains(out, "authService.extraEnv must not set") {
					t.Fatalf("authService.extraEnv %s: the refusal does not name extraEnv: %.300s", spelling, out)
				}
			}
		})
	}
}

// acr-api's live principal starts with "_" after prn_: it reaches the container byte for byte (no YAML or quoting loss).
func TestAuthWebApproverCallerWithALeadingUnderscoreRendersUnchanged(t *testing.T) {
	t.Parallel()

	docs, out, err := renderAuth(t, phaseArgs(2, webApproverSets("https://www.fullchaos.dev", webApproverCaller, "acr-web-assertion-jwks")...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	container, _ := authServicePod(t, docs)
	if got, _ := envValue(container, "AUTH_AUTHORIZE_WEB_APPROVER_CALLER"); got != webApproverCaller {
		t.Fatalf("AUTH_AUTHORIZE_WEB_APPROVER_CALLER = %q, want %q", got, webApproverCaller)
	}
	if !strings.Contains(out, `value: "`+webApproverCaller+`"`) {
		t.Fatal("the rendered manifest does not quote the caller as written")
	}
}
