package workflows_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-12372: BuildKit base-image fetches from mirror.gcr.io fail now and then with a blob "not found" that a later try
// serves. ci/lib/registry_retry.sh retries those (and only those) three times at 10 s then 30 s. Fake docker and a fake
// sleep only; no wall time is asserted.

const (
	rrLayer   = `#6 ERROR: failed to copy: httpReadSeeker: failed open: could not fetch content descriptor sha256:f958af8e7f90ce12ddac2700a2df281beed4dcaaca981f271c930168b9f0d191 (application/vnd.oci.image.layer.v1.tar+gzip) from remote: not found`
	rrConfig  = `#5 ERROR: failed to copy: httpReadSeeker: failed open: could not fetch content descriptor sha256:2101eb697fc5b5b7f64252fe6bf7a21c4a06a51852c71c3424d44e3f824cbb52 (application/vnd.oci.image.config.v1+json) from remote: not found`
	rrProduct = `dependency failed to start: container authcorpus0123456789ab-go-api-1 is unhealthy`
)

var rrTransient = map[string]string{
	"layer":          rrLayer,
	"config":         rrConfig,
	"docker media":   strings.Replace(rrConfig, "application/vnd.oci.image.config.v1+json", "application/vnd.docker.container.image.v1+json", 1),
	"429":            `#4 ERROR: failed to resolve source metadata for mirror.gcr.io/library/golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd: unexpected status from GET request to https://mirror.gcr.io/v2/library/golang/manifests/sha256:f2c872bc: 429 Too Many Requests`,
	"503":            `#4 ERROR: failed to resolve source metadata for mirror.gcr.io/library/golang:1.27.3-alpine: unexpected status from HEAD request to https://mirror.gcr.io/v2/library/golang/manifests/1.27.3-alpine: 503 Service Unavailable`,
	"tls handshake":  `#4 ERROR: failed to do request: Head "https://mirror.gcr.io/v2/library/golang/manifests/1.27.3-alpine": net/http: TLS handshake timeout`,
	"connection rst": `#4 ERROR: failed to copy: read tcp 10.0.0.1:1234->74.125.1.1:443: read: connection reset by peer`,
}

var rrPermanent = map[string]string{
	"manifest unknown": `#4 ERROR: failed to resolve source metadata for mirror.gcr.io/library/golang:9.9: manifest unknown`,
	"manifest not found": `#4 ERROR: failed to resolve source metadata for mirror.gcr.io/library/golang:9.9: mirror.gcr.io/library/golang:9.9: not found` + "\n" +
		`manifest sha256:0000 not found`,
	"denied":       `#4 ERROR: failed to resolve source metadata for ghcr.io/full-chaos/x:1: denied`,
	"unauthorized": `#4 ERROR: failed to resolve source metadata for ghcr.io/full-chaos/x:1: unauthorized: authentication required`,
	"no platform":  `#4 ERROR: failed to resolve source metadata for mirror.gcr.io/library/golang:1: no match for platform in manifest: not found`,
	"compile":      `#12 ERROR: process "/bin/sh -c go build ./..." did not complete successfully: exit code: 1`,
	"product":      rrProduct,
}

type rrBox struct {
	dir, bin, calls, sleeps, tmp string
}

func newRRBox(t *testing.T) rrBox {
	t.Helper()
	dir := t.TempDir()
	b := rrBox{dir: dir, bin: filepath.Join(dir, "bin"), calls: filepath.Join(dir, "calls"), sleeps: filepath.Join(dir, "sleeps"), tmp: filepath.Join(dir, "tmp")}
	for _, d := range []string{b.bin, b.tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	docker := `#!/bin/sh
printf '%s\n' "$*" >> "` + b.calls + `"
case "$*" in
  *"image inspect"*) echo sha256:0123 ; exit 0 ;;
  $FAKE_MATCH)
    n=$(cat "` + b.dir + `/n" 2>/dev/null || echo 0)
    n=$((n + 1)); echo "$n" > "` + b.dir + `/n"
    if [ "$n" -le "${FAKE_FAILS:-0}" ]; then
      printf '%s\n' "$FAKE_TEXT" >&2
      exit 1
    fi ;;
esac
exit 0
`
	sleep := "#!/bin/sh\necho \"$1\" >> \"" + b.sleeps + "\"\nexit 0\n"
	for name, body := range map[string]string{"docker": docker, "sleep": sleep} {
		if err := os.WriteFile(filepath.Join(b.bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func (b rrBox) env(extra ...string) []string {
	return append([]string{"PATH=" + b.bin + ":/usr/bin:/bin", "HOME=" + b.dir, "TMPDIR=" + b.tmp, "LC_ALL=C"}, extra...)
}

func (b rrBox) lines(t *testing.T, name string) []string {
	t.Helper()
	raw, err := os.ReadFile(name)
	if err != nil {
		return nil
	}
	return strings.Fields(string(raw))
}

func (b rrBox) builds(t *testing.T) int {
	t.Helper()
	n := 0
	raw, _ := os.ReadFile(b.calls)
	for _, l := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(l, "build ") {
			n++
		}
	}
	return n
}

type rrResult struct {
	out    string
	rc     int
	builds int
	sleeps string
}

func runRRLib(t *testing.T, fails int, text string) rrResult {
	t.Helper()
	b := newRRBox(t)
	lib := filepath.Join(authProofRoot(t), "ci", "lib", "registry_retry.sh")
	cmd := exec.Command("bash", "-c", `set -euo pipefail; source "$1"; rc=0; registry_retry_run docker-build -- docker build --file F . || rc=$?; echo "RC=${rc}"`, "_", lib)
	cmd.Dir = b.dir
	cmd.Env = b.env("FAKE_MATCH=build*", "FAKE_FAILS="+strconv.Itoa(fails), "FAKE_TEXT="+text)
	out, _ := cmd.CombinedOutput()
	res := rrResult{out: string(out), builds: b.builds(t), sleeps: strings.Join(b.lines(t, b.sleeps), " ")}
	m := regexp.MustCompile(`(?m)^RC=(\d+)$`).FindStringSubmatch(res.out)
	if m == nil {
		t.Fatalf("no RC line:\n%s", res.out)
	}
	res.rc, _ = strconv.Atoi(m[1])
	if left, _ := os.ReadDir(b.tmp); len(left) != 0 {
		t.Errorf("per-try logs left behind in TMPDIR: %v", left)
	}
	left, _ := os.ReadDir(b.dir)
	for _, e := range left {
		switch e.Name() {
		case "bin", "tmp", "calls", "n", "sleeps":
		default:
			t.Errorf("a stray file in the working directory: %s", e.Name())
		}
	}
	return res
}

func TestRegistryRetryRetriesEachTransientClassThenSucceeds(t *testing.T) {
	for name, text := range rrTransient {
		t.Run(name, func(t *testing.T) {
			r := runRRLib(t, 2, text)
			if r.rc != 0 || r.builds != 3 || r.sleeps != "10 30" {
				t.Errorf("want rc 0, 3 builds, sleeps 10 30; got rc=%d builds=%d sleeps=%q\n%s", r.rc, r.builds, r.sleeps, r.out)
			}
			if n := strings.Count(r.out, text); n != 2 {
				t.Errorf("every try's output stays in the log: want the text twice, got %d\n%s", n, r.out)
			}
			if !strings.Contains(r.out, "registry-retry: name=docker-build attempt=1/3 class=") {
				t.Errorf("a retried try names its class:\n%s", r.out)
			}
		})
	}
}

func TestRegistryRetryNeverRetriesAPermanentError(t *testing.T) {
	for name, text := range rrPermanent {
		t.Run(name, func(t *testing.T) {
			r := runRRLib(t, 1, text)
			if r.rc != 1 || r.builds != 1 || r.sleeps != "" {
				t.Errorf("want rc 1, 1 build, no sleep; got rc=%d builds=%d sleeps=%q\n%s", r.rc, r.builds, r.sleeps, r.out)
			}
		})
	}
}

func TestRegistryRetryIsBoundedAtThreeTriesAndKeepsTheLastRC(t *testing.T) {
	r := runRRLib(t, 9, rrConfig)
	if r.rc != 1 || r.builds != 3 || r.sleeps != "10 30" {
		t.Errorf("want rc 1, 3 builds, sleeps 10 30 (none after the last); got rc=%d builds=%d sleeps=%q\n%s", r.rc, r.builds, r.sleeps, r.out)
	}
}

func TestRegistryRetryVetoesATransientLineBesideAPermanentOne(t *testing.T) {
	for name, perm := range rrPermanent {
		t.Run(name, func(t *testing.T) {
			r := runRRLib(t, 1, rrConfig+"\n"+perm)
			if r.builds != 1 || r.sleeps != "" {
				t.Errorf("want 1 build, no sleep; got builds=%d sleeps=%q\n%s", r.builds, r.sleeps, r.out)
			}
		})
	}
}

func TestRegistryRetryMatchesWholeLinesOnly(t *testing.T) {
	near := map[string]string{
		"prefix":       "log: " + rrConfig,
		"suffix":       rrLayer + " (see the build log)",
		"other result": strings.Replace(rrConfig, "from remote: not found", "from remote: forbidden", 1),
		"short digest": strings.Replace(rrLayer, "sha256:f958af8e7f90ce12ddac2700a2df281beed4dcaaca981f271c930168b9f0d191", "sha256:f958af8e", 1),
		"other media":  strings.Replace(rrLayer, "application/vnd.oci.image.layer.v1.tar+gzip", "text/plain", 1),
		"other host":   strings.Replace(rrTransient["429"], "mirror.gcr.io/v2", "example.com/v2", 1),
	}
	for name, text := range near {
		t.Run(name, func(t *testing.T) {
			r := runRRLib(t, 1, text)
			if r.builds != 1 || r.sleeps != "" {
				t.Errorf("a near-miss must not be retried: builds=%d sleeps=%q", r.builds, r.sleeps)
			}
		})
	}
}

func TestRegistryRetryReadsCRLFOutput(t *testing.T) {
	r := runRRLib(t, 1, rrConfig+"\r")
	if r.rc != 0 || r.builds != 2 {
		t.Errorf("a CRLF class line must be retried: rc=%d builds=%d\n%s", r.rc, r.builds, r.out)
	}
}

func TestRegistryRetryPatternGrepsIgnoreTheLocale(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), "ci", "lib", "registry_retry.sh"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, "grep -aE") {
			n++
			if !strings.Contains(line, "LC_ALL=C grep -aE") {
				t.Errorf("a pattern grep without LC_ALL=C: %s", strings.TrimSpace(line))
			}
		}
	}
	if n == 0 {
		t.Error("no pattern grep found in ci/lib/registry_retry.sh")
	}
}

func runBuildShared(t *testing.T, fails int, text string) (rc int, builds int, sleeps string, out string) {
	t.Helper()
	b := newRRBox(t)
	outDir := filepath.Join(b.dir, "shared")
	cmd := exec.Command("bash", filepath.Join(authProofRoot(t), "ci", "check_go_containers.sh"), "build-shared", outDir)
	cmd.Dir = b.dir
	cmd.Env = b.env("FAKE_MATCH=build*", "FAKE_FAILS="+strconv.Itoa(fails), "FAKE_TEXT="+text)
	raw, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); ok {
		rc = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return rc, b.builds(t), strings.Join(b.lines(t, b.sleeps), " "), string(raw)
}

// The exact failing line of run 41216797885 (go-container-build, linux/arm64).
func TestBuildSharedRetriesARegistryBlobFetchFailure(t *testing.T) {
	rc, builds, sleeps, out := runBuildShared(t, 2, rrLayer)
	if rc != 0 || builds != 8 || sleeps != "10 30" {
		t.Errorf("want rc 0, 6 targets + 2 retried builds = 8, sleeps 10 30; got rc=%d builds=%d sleeps=%q\n%s", rc, builds, sleeps, out)
	}
}

func TestBuildSharedDoesNotRetryAPermanentFailure(t *testing.T) {
	rc, builds, sleeps, out := runBuildShared(t, 1, rrPermanent["manifest unknown"])
	if rc == 0 || builds != 1 || sleeps != "" {
		t.Errorf("want a failure after 1 build, no sleep; got rc=%d builds=%d sleeps=%q\n%s", rc, builds, sleeps, out)
	}
}

// The exact failing line of run 41254379233 (auth-corpus-queue go-api): the config blob, not a layer.
func TestAuthCorpusExternalRetriesTheConfigBlobFetchUpToThreeTries(t *testing.T) {
	r := runCorpusWithFailingUp(t, "go-api", 2, rrConfig, " go-api")
	if r.err != nil || len(r.ups) != 3 || r.retryDowns != 2 {
		t.Fatalf("want 3 up calls and 2 retry downs; got err=%v ups=%q downs=%d\n%s", r.err, r.ups, r.retryDowns, r.out)
	}
	if r.retryMarker != "AUTH-CORPUS-INFRA-RETRY class=2 step=go-api" {
		t.Errorf("retry line = %q", r.retryMarker)
	}
	if strings.Count(r.out, rrConfig) != 2 {
		t.Errorf("every try's output stays in the log:\n%s", r.out)
	}
}

func TestAuthCorpusExternalRegistryClassStopsAtThreeTries(t *testing.T) {
	r := runCorpusWithFailingUp(t, "go-api", 3, rrConfig, " go-api")
	if r.err == nil || len(r.ups) != 3 {
		t.Errorf("want 3 up calls and a failed run; got err=%v ups=%q", r.err, r.ups)
	}
}

// Every build-or-pull call site in ci/*.sh goes through registry_retry_run, or is named here by file and function with
// the reason it is not in the class. A new site fails this test until it does one or the other.
var rrCensusAllowlist = map[string]string{
	"ci/auth_corpus_external.sh:up_once":          "retried by up_service: infra_class asks registry_transient, retry loop in up_service",
	"ci/check_auth_k8s.sh:build":                  "base images pulled with pull_pinned_base_image (retry) and built --pull=false",
	"ci/check_session_cutover_rehearsal.sh:build": "base images pulled with pull_pinned_base_image (retry) and built --pull=false",
}

var rrCensusLine = regexp.MustCompile(`docker (buildx )?build\b|docker compose[^#]* (--build|build)\b|compose[^#]*\$\{BUILD_FLAG\}|^\s*compose build\b`)
var rrFuncStart = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\(\) *\{`)

func TestEveryBuildSiteInCIScriptsIsRetriedOrNamed(t *testing.T) {
	root := authProofRoot(t)
	files, err := filepath.Glob(filepath.Join(root, "ci", "*.sh"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no ci/*.sh: %v", err)
	}
	seen := map[string]bool{}
	for _, f := range files {
		rel, _ := filepath.Rel(root, f)
		if strings.HasSuffix(rel, "_test.sh") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		fn := "main"
		sourcesLib := strings.Contains(string(raw), "registry_retry.sh")
		for i, line := range strings.Split(string(raw), "\n") {
			if m := rrFuncStart.FindStringSubmatch(line); m != nil {
				fn = m[1]
			} else if strings.HasPrefix(line, "}") {
				fn = "main"
			}
			if strings.HasPrefix(strings.TrimSpace(line), "#") || !rrCensusLine.MatchString(line) || strings.Contains(line, "--no-build") {
				continue
			}
			key := rel + ":" + fn
			if strings.Contains(line, "registry_retry_run") {
				if !sourcesLib {
					t.Errorf("%s:%d calls registry_retry_run but never sources ci/lib/registry_retry.sh", rel, i+1)
				}
				continue
			}
			if _, ok := rrCensusAllowlist[key]; ok {
				seen[key] = true
				continue
			}
			t.Errorf("%s:%d is a build/pull call outside registry_retry_run and not allowlisted (%s): %s", rel, i+1, key, strings.TrimSpace(line))
		}
	}
	for key := range rrCensusAllowlist {
		if !seen[key] {
			t.Errorf("allowlist entry %s matches no build call: remove it", key)
		}
	}
}
