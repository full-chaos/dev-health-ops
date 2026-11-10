package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

// goApi.acrRevoke wires go-api's acr credential revoke client: acr's internal API origin, acr's token endpoint and a projected
// ServiceAccount token with acr's workload-exchange audience. Off by default, and off renders nothing. The chart derives
// ACR_REVOKE_SUBJECT_FILE from the mount, so the path cannot drift from the volume. A plain-http origin renders only with the
// explicit opt-in AND a cluster-local Service host; goApi.extraEnv can never set an ACR_REVOKE_* name.

const (
	arBase     = "https://acr.revoke-proof.example"
	arToken    = "https://acr.revoke-proof.example/api/v1/oauth/token"
	arAudience = "dev-health-acr-token-exchange"
	arCluster  = "http://dev-health-acr.dev-health.svc.cluster.local:8080"
	arMount    = "/var/run/dev-health/acr-revoke"
	arVolume   = "acr-revoke-token"
)

var arOwnEnv = []string{"ACR_REVOKE_URL", "ACR_REVOKE_TOKEN_URL", "ACR_REVOKE_SUBJECT_FILE", "ACR_REVOKE_ALLOW_CLUSTER_HTTP"}

func arOn(extra ...string) []string {
	sets := []string{"--set", "goApi.enabled=true", "--set", "goApi.acrRevoke.enabled=true",
		"--set-string", "goApi.acrRevoke.url=" + arBase, "--set-string", "goApi.acrRevoke.tokenURL=" + arToken,
		"--set-string", "goApi.acrRevoke.audience=" + arAudience}
	return append(sets, extra...)
}

func arGoAPI(t *testing.T, args ...string) (map[string]any, map[string]any) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	dep := authDoc(docs, "Deployment", "a-dev-health-go-api")
	if dep == nil {
		t.Fatal("no go-api Deployment in the render")
	}
	return dep, sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")
}

func arValue(env map[string]any, name string) any { return at(env[name], "value") }

func TestACRRevokeOffRendersNothing(t *testing.T) {
	_, def, err := renderAuth(t, "--set", "goApi.enabled=true")
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, def)
	}
	for _, name := range append(arOwnEnv, arVolume) {
		if strings.Contains(def, name) {
			t.Errorf("the default render names %s", name)
		}
	}
	_, off, err := renderAuth(t, "--set", "goApi.enabled=true", "--set", "goApi.acrRevoke.enabled=false",
		"--set-string", "goApi.acrRevoke.url="+arBase, "--set-string", "goApi.acrRevoke.tokenURL="+arToken,
		"--set-string", "goApi.acrRevoke.audience="+arAudience)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, off)
	}
	if def != off {
		t.Fatal("goApi.acrRevoke.enabled=false changes the render")
	}
}

func TestACRRevokeOnRendersEnvVolumeAndDerivedPath(t *testing.T) {
	dep, env := arGoAPI(t, arOn()...)
	want := map[string]string{"ACR_REVOKE_URL": arBase, "ACR_REVOKE_TOKEN_URL": arToken, "ACR_REVOKE_SUBJECT_FILE": arMount + "/token"}
	for name, value := range want {
		if got := arValue(env, name); got != value {
			t.Errorf("%s = %v, want %q", name, got, value)
		}
	}
	if got := arValue(env, "ACR_REVOKE_ALLOW_CLUSTER_HTTP"); got != "false" {
		t.Errorf("ACR_REVOKE_ALLOW_CLUSTER_HTTP = %v, want \"false\": every enabled render carries all four names", got)
	}
	vols, _ := at(dep, "spec", "template", "spec", "volumes").([]any)
	if len(vols) != 1 || at(vols[0], "name") != arVolume {
		t.Fatalf("go-api pod volumes = %v, want the one %s volume", vols, arVolume)
	}
	sources, _ := at(vols[0], "projected", "sources").([]any)
	if len(sources) != 1 {
		t.Fatalf("projected sources = %v, want one serviceAccountToken", sources)
	}
	src := at(sources[0], "serviceAccountToken")
	if at(src, "audience") != arAudience || at(src, "path") != "token" || !reflect.DeepEqual(at(src, "expirationSeconds"), 3600) {
		t.Errorf("serviceAccountToken = %v, want audience %s, path token, expirationSeconds 3600", src, arAudience)
	}
	mounts, _ := at(dep, "spec", "template", "spec", "containers", 0, "volumeMounts").([]any)
	if len(mounts) != 1 || at(mounts[0], "name") != arVolume || at(mounts[0], "mountPath") != arMount || at(mounts[0], "readOnly") != true {
		t.Fatalf("go-api volumeMounts = %v, want the one read-only %s mount at %s", mounts, arVolume, arMount)
	}
	if at(mounts[0], "subPath") != nil {
		t.Error("a subPath mount never receives the kubelet's token rotation")
	}
	if at(dep, "spec", "template", "spec", "automountServiceAccountToken") == true {
		t.Error("the projected volume must not turn on the default automount")
	}
}

func TestACRRevokeExpirationIsSettable(t *testing.T) {
	dep, _ := arGoAPI(t, arOn("--set", "goApi.acrRevoke.expirationSeconds=600")...)
	if got := at(dep, "spec", "template", "spec", "volumes", 0, "projected", "sources", 0, "serviceAccountToken", "expirationSeconds"); !reflect.DeepEqual(got, 600) {
		t.Errorf("expirationSeconds = %v, want 600", got)
	}
}

func TestACRRevokeClusterHTTPNeedsTheOptInAndTheClusterHost(t *testing.T) {
	_, env := arGoAPI(t, arOn("--set-string", "goApi.acrRevoke.url="+arCluster, "--set-string", "goApi.acrRevoke.tokenURL="+arCluster+"/api/v1/oauth/token",
		"--set", "goApi.acrRevoke.allowClusterHTTP=true")...)
	if got := arValue(env, "ACR_REVOKE_ALLOW_CLUSTER_HTTP"); got != "true" {
		t.Errorf("ACR_REVOKE_ALLOW_CLUSTER_HTTP = %v, want \"true\"", got)
	}
	if got := arValue(env, "ACR_REVOKE_URL"); got != arCluster {
		t.Errorf("ACR_REVOKE_URL = %v", got)
	}
	_, env = arGoAPI(t, arOn("--set", "goApi.acrRevoke.allowClusterHTTP=false")...)
	if got := arValue(env, "ACR_REVOKE_ALLOW_CLUSTER_HTTP"); got != "false" {
		t.Errorf("allowClusterHTTP=false renders ACR_REVOKE_ALLOW_CLUSTER_HTTP = %v, want \"false\"", got)
	}
}

func TestACRRevokeClusterHostTakesValidLabelsAndATokenQuery(t *testing.T) {
	host := "http://dev-health-1.ns-2.svc.cluster.local:8080"
	_, env := arGoAPI(t, arOn("--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url="+host,
		"--set-string", "goApi.acrRevoke.tokenURL="+host+"/api/v1/oauth/token?x=1")...)
	if got := arValue(env, "ACR_REVOKE_URL"); got != host {
		t.Errorf("ACR_REVOKE_URL = %v, want %s", got, host)
	}
}

func TestACRRevokeTokenQueryMayHoldAnAtSign(t *testing.T) {
	for _, token := range []string{"https://acr.revoke-proof.example?email=a@b", "https://acr.revoke-proof.example/api/v1/oauth/token?email=a@b"} {
		_, env := arGoAPI(t, arOn("--set-string", "goApi.acrRevoke.tokenURL="+token)...)
		if got := arValue(env, "ACR_REVOKE_TOKEN_URL"); got != token {
			t.Errorf("ACR_REVOKE_TOKEN_URL = %v, want %s: an @ in the query is not user info", got, token)
		}
	}
}

func TestACRRevokeKeepsTheExistingExtraVolume(t *testing.T) {
	dep, _ := arGoAPI(t, append(arOn(), "--set-json", "goApi.extraVolumes="+extraVolumesJSON, "--set-json", "goApi.extraVolumeMounts="+extraMountsJSON)...)
	vols, _ := at(dep, "spec", "template", "spec", "volumes").([]any)
	mounts, _ := at(dep, "spec", "template", "spec", "containers", 0, "volumeMounts").([]any)
	if len(vols) != 2 || len(mounts) != 2 {
		t.Fatalf("volumes %v mounts %v, want acr-revoke-token and workload-token", vols, mounts)
	}
	names := map[any]bool{at(vols[0], "name"): true, at(vols[1], "name"): true}
	if !names[arVolume] || !names["workload-token"] {
		t.Errorf("volume names = %v", names)
	}
}

func TestACRRevokeRefusesUnusableValues(t *testing.T) {
	cases := []struct {
		name string
		sets []string
		want string
	}{
		{"no url", []string{"--set-string", "goApi.acrRevoke.url="}, "goApi.acrRevoke.url is required"},
		{"no token url", []string{"--set-string", "goApi.acrRevoke.tokenURL="}, "goApi.acrRevoke.tokenURL is required"},
		{"no audience", []string{"--set-string", "goApi.acrRevoke.audience="}, "goApi.acrRevoke.audience"},
		{"plain http url without the opt-in", []string{"--set-string", "goApi.acrRevoke.url=" + arCluster}, "goApi.acrRevoke.url"},
		{"plain http token url without the opt-in", []string{"--set-string", "goApi.acrRevoke.tokenURL=" + arCluster + "/api/v1/oauth/token"}, "goApi.acrRevoke.tokenURL"},
		{"opt-in with an off-cluster http host", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://acr.internal.test"}, "goApi.acrRevoke.url"},
		{"opt-in with a loopback http host", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://127.0.0.1:8080"}, "goApi.acrRevoke.url"},
		{"opt-in with a suffix inside a longer host", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://acr.svc.cluster.local.evil.test"}, "goApi.acrRevoke.url"},
		{"opt-in with the bare suffix", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://svc.cluster.local"}, "goApi.acrRevoke.url"},
		{"opt-in as a string", []string{"--set-string", "goApi.acrRevoke.allowClusterHTTP=true"}, "goApi.acrRevoke.allowClusterHTTP"},
		{"url with a query", []string{"--set-string", "goApi.acrRevoke.url=" + arBase + "/?x=1"}, "goApi.acrRevoke.url"},
		{"url with user info", []string{"--set-string", "goApi.acrRevoke.url=https://u@acr.revoke-proof.example"}, "goApi.acrRevoke.url"},
		{"token url with a fragment", []string{"--set-string", "goApi.acrRevoke.tokenURL=" + arToken + "#f"}, "goApi.acrRevoke.tokenURL"},
		{"other scheme", []string{"--set-string", "goApi.acrRevoke.url=ftp://acr.revoke-proof.example"}, "goApi.acrRevoke.url"},
		{"opt-in with a cluster host and another scheme", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=ftp://acr.ns.svc.cluster.local"}, "goApi.acrRevoke.url must be https"},
		{"https url without a host", []string{"--set-string", "goApi.acrRevoke.url=https:///internal"}, "goApi.acrRevoke.url must be https"},
		{"url with an empty query", []string{"--set-string", "goApi.acrRevoke.url=" + arBase + "?"}, "goApi.acrRevoke.url"},
		{"url with an empty fragment", []string{"--set-string", "goApi.acrRevoke.url=" + arBase + "#"}, "goApi.acrRevoke.url"},
		{"token url with an empty fragment", []string{"--set-string", "goApi.acrRevoke.tokenURL=" + arToken + "#"}, "goApi.acrRevoke.tokenURL"},
		{"url with empty user info", []string{"--set-string", "goApi.acrRevoke.url=https://@acr.revoke-proof.example"}, "goApi.acrRevoke.url"},
		{"token url with empty user info", []string{"--set-string", "goApi.acrRevoke.tokenURL=https://@acr.revoke-proof.example/api/v1/oauth/token"}, "goApi.acrRevoke.tokenURL"},
		{"token url with user info and a path", []string{"--set-string", "goApi.acrRevoke.tokenURL=https://u@acr.revoke-proof.example/api/v1/oauth/token"}, "goApi.acrRevoke.tokenURL"},
		{"url with user info and a path", []string{"--set-string", "goApi.acrRevoke.url=https://u@acr.revoke-proof.example/x"}, "goApi.acrRevoke.url"},
		{"opt-in, cluster url with an empty query", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=" + arCluster + "?"}, "goApi.acrRevoke.url"},
		{"opt-in, cluster url with empty user info", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://@dev-health-acr.dev-health.svc.cluster.local:8080"}, "goApi.acrRevoke.url"},
		{"opt-in, cluster host with an empty label", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://dev..svc.cluster.local"}, "goApi.acrRevoke.url"},
		{"opt-in, cluster host with an underscore", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://dev_health.svc.cluster.local"}, "goApi.acrRevoke.url"},
		{"opt-in, cluster host with a leading hyphen", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://-dev.svc.cluster.local"}, "goApi.acrRevoke.url"},
		{"opt-in, cluster host with a trailing hyphen", []string{"--set", "goApi.acrRevoke.allowClusterHTTP=true", "--set-string", "goApi.acrRevoke.url=http://dev-.svc.cluster.local"}, "goApi.acrRevoke.url"},
		{"expiration below the Kubernetes minimum", []string{"--set", "goApi.acrRevoke.expirationSeconds=599"}, "goApi.acrRevoke.expirationSeconds"},
		{"expiration above a day", []string{"--set", "goApi.acrRevoke.expirationSeconds=86401"}, "goApi.acrRevoke.expirationSeconds"},
		{"expiration as a string", []string{"--set-string", "goApi.acrRevoke.expirationSeconds=3600"}, "goApi.acrRevoke.expirationSeconds"},
		{"an extra volume with the chart's name", []string{"--set-json", `goApi.extraVolumes=[{"name":"acr-revoke-token","emptyDir":{}}]`}, "acr-revoke-token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, out, err := renderAuthRaw(t, arOn(tc.sets...)...)
			if err == nil {
				t.Fatalf("render passed; want a refusal naming %s", tc.want)
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("refusal does not name %s:\n%.600s", tc.want, out)
			}
		})
	}
}

func TestACRRevokeNamesNeverComeFromExtraEnv(t *testing.T) {
	for _, name := range arOwnEnv {
		for _, on := range []bool{true, false} {
			args := []string{"--set", "goApi.enabled=true"}
			if on {
				args = arOn()
			}
			args = append(args, "--set", "goApi.extraEnv[0].name="+name, "--set-string", "goApi.extraEnv[0].value=x")
			_, out, err := renderAuthRaw(t, args...)
			if err == nil {
				t.Errorf("acrRevoke on=%v: goApi.extraEnv %s rendered", on, name)
				continue
			}
			if !strings.Contains(out, "goApi.extraEnv must not set "+name) {
				t.Errorf("acrRevoke on=%v: refusal does not name %s:\n%.400s", on, name, out)
			}
		}
	}
	_, out, err := renderAuthRaw(t, "--set", "goApi.enabled=true", "--set", "goApi.extraEnv[0].name=acr_revoke_url", "--set-string", "goApi.extraEnv[0].value=x")
	if err == nil || !strings.Contains(out, "goApi.extraEnv must not set acr_revoke_url") {
		t.Errorf("a lower-case ACR_REVOKE_* name rendered: %v\n%.400s", err, out)
	}
}

func TestACRRevokeLeavesOtherWorkloadsUnchanged(t *testing.T) {
	base, out, err := renderAuth(t, "--set", "goApi.enabled=true")
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	with, out, err := renderAuth(t, arOn()...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if len(base) != len(with) {
		t.Fatalf("document count %d -> %d", len(base), len(with))
	}
	for i := range base {
		meta, _ := base[i]["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if base[i]["kind"] == "Deployment" && name == "a-dev-health-go-api" {
			continue
		}
		if !reflect.DeepEqual(base[i], with[i]) {
			t.Errorf("%v %s changed", base[i]["kind"], name)
		}
	}
}
