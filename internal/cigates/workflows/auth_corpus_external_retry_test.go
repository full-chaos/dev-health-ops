package workflows_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// CHAOS-11670: `compose up --build` of ci/auth_corpus_external.sh is retried ONCE, and only when its captured output
// names an infrastructure class seen on main (venue-oracles runs 40273038441, 40283712992, 40431223738). Any other
// failure fails at once. Fake docker only; the wall time is never asserted.

var corpusInfraTexts = map[string]string{
	"1": `Error response from daemon: failed to set up container networking: driver failed programming external connectivity on endpoint authcorpus795d647c2465-go-api-1 (34aa73b875bebf89c1ad995ce6ab778de3944c9e58c498f12b2c57f53d323710): Bind for 127.0.0.1:32768 failed: port is already allocated`,
	"2": `#7 ERROR: failed to copy: httpReadSeeker: failed open: could not fetch content descriptor sha256:82e0bb27ae78502103ed383ed33346f88a83943fb7bd93977850d5890ee8fb3b (application/vnd.oci.image.layer.v1.tar+gzip) from remote: not found`,
	"3": `#18 473.4 go: github.com/klauspost/compress@v1.19.1: read "https://proxy.golang.org/github.com/klauspost/compress/@v/v1.19.1.zip": stream error: stream ID 1; INTERNAL_ERROR; received from peer`,
}

const corpusProductText = `go-api-1  | {"level":"ERROR","msg":"auth proxy: Authentication service unavailable"}
dependency failed to start: container authcorpus0123456789ab-go-api-1 is unhealthy`

// failingUpDocker puts a docker in front of the recording fake: the first FAKE_UP_FAILS `up` calls whose arguments end
// with FAKE_UP_MATCH (any when empty) print FAKE_UP_TEXT on stderr and exit 1; every other call goes to the fake.
func failingUpDocker(t *testing.T, fake fakeDocker) string {
	t.Helper()
	dir := t.TempDir()
	body := `#!/bin/sh
case "$*" in
  *" up "*"$FAKE_UP_MATCH")
    n=$(cat "` + fake.record + `.upfail" 2>/dev/null || echo 0)
    if [ "$n" -lt "${FAKE_UP_FAILS:-0}" ]; then
      echo $((n + 1)) > "` + fake.record + `.upfail"
      printf '%s\n' "$*" >> "` + fake.record + `"
      printf '%s\n' "$FAKE_UP_TEXT" >&2
      exit 1
    fi ;;
esac
exec "` + filepath.Join(fake.dir, "docker") + `" "$@"
`
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	// The registry class waits 10 s then 30 s between tries; no wall time is spent or asserted.
	if err := os.WriteFile(filepath.Join(dir, "sleep"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

type retryRun struct {
	out         string
	err         error
	ups, downs  []string
	retryDowns  int
	retryMarker string
}

func runCorpusWithFailingUp(t *testing.T, leg string, fails int, text, match string) retryRun {
	t.Helper()
	fake := newFakeDocker(t)
	fake.withCurl(t)
	front := failingUpDocker(t, fake)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), leg)
	cmd.Env = append(fake.env("FAKE_UP_FAILS="+itoa(fails), "FAKE_UP_TEXT="+text, "FAKE_UP_MATCH="+match,
		"AUTH_CORPUS_USERS_UNOWNED=1"), "PATH="+front+":"+fake.dir+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	r := retryRun{out: string(out), err: err}
	for _, call := range strings.Split(fake.calls(t), "\n") {
		switch {
		case strings.Contains(call, " up "):
			r.ups = append(r.ups, call[strings.Index(call, " up ")+4:])
		case strings.Contains(call, " down ") && !strings.Contains(call, "--volumes"):
			r.retryDowns++
		}
	}
	for _, line := range strings.Split(r.out, "\n") {
		if strings.HasPrefix(line, "AUTH-CORPUS-INFRA-RETRY ") {
			r.retryMarker = line
		}
	}
	return r
}

func itoa(n int) string { return string(rune('0' + n)) }

func TestAuthCorpusExternalUpRetriesOnceOnEachInfraClass(t *testing.T) {
	for class, text := range corpusInfraTexts {
		t.Run("class"+class, func(t *testing.T) {
			r := runCorpusWithFailingUp(t, "go-api", 1, text, " go-api")
			if r.err != nil {
				t.Fatalf("the run must continue after the retry: %v\n%s", r.err, r.out)
			}
			if len(r.ups) != 2 || r.retryDowns != 1 {
				t.Errorf("want 2 up calls and 1 retry down, got ups=%q retryDowns=%d", r.ups, r.retryDowns)
			}
			if want := "AUTH-CORPUS-INFRA-RETRY class=" + class + " step=go-api"; r.retryMarker != want {
				t.Errorf("retry line = %q, want %q", r.retryMarker, want)
			}
		})
	}
}

func TestAuthCorpusExternalUpNeverRetriesAProductFailure(t *testing.T) {
	r := runCorpusWithFailingUp(t, "go-api", 1, corpusProductText, " go-api")
	if r.err == nil {
		t.Fatalf("a product failure must fail the run:\n%s", r.out)
	}
	if len(r.ups) != 1 || r.retryDowns != 0 || r.retryMarker != "" {
		t.Errorf("want 1 up, no retry down, no retry line; got ups=%q retryDowns=%d line=%q", r.ups, r.retryDowns, r.retryMarker)
	}
}

func TestAuthCorpusExternalUpRetriesAtMostOnce(t *testing.T) {
	r := runCorpusWithFailingUp(t, "go-api", 2, corpusInfraTexts["1"], " go-api")
	if r.err == nil {
		t.Fatalf("two infra failures in a row must fail the run:\n%s", r.out)
	}
	if len(r.ups) != 2 || r.retryDowns != 1 {
		t.Errorf("want 2 up calls and 1 retry down, got ups=%q retryDowns=%d", r.ups, r.retryDowns)
	}
	if n := strings.Count(r.out, corpusInfraTexts["1"]); n != 2 {
		t.Errorf("both attempts' output must be in the log: want the class text twice, got %d:\n%s", n, r.out)
	}
}

// compose output with CRLF line ends is still a class line.
func TestAuthCorpusExternalUpRetriesWhenTheOutputHasCRLFLineEnds(t *testing.T) {
	r := runCorpusWithFailingUp(t, "go-api", 1, corpusInfraTexts["1"]+"\r", " go-api")
	if r.err != nil || len(r.ups) != 2 || r.retryMarker != "AUTH-CORPUS-INFRA-RETRY class=1 step=go-api" {
		t.Errorf("a CRLF class line must be retried once: err=%v ups=%q line=%q\n%s", r.err, r.ups, r.retryMarker, r.out)
	}
}

// A retry down removes go-api too, so the auth-service step brings go-api back before it tries auth-service again.
func TestAuthCorpusExternalAuthServiceStepRetryRestartsGoAPIFirst(t *testing.T) {
	r := runCorpusWithFailingUp(t, "acp", 1, corpusInfraTexts["2"], " auth-service")
	if want := "AUTH-CORPUS-INFRA-RETRY class=2 step=auth-service"; r.retryMarker != want {
		t.Fatalf("retry line = %q, want %q\n%s", r.retryMarker, want, r.out)
	}
	if len(r.ups) < 4 {
		t.Fatalf("want go-api, auth-service (fails), go-api, auth-service; got %q", r.ups)
	}
	for i, svc := range []string{" go-api", " auth-service", " go-api", " auth-service"} {
		if !strings.HasSuffix(r.ups[i], svc) {
			t.Errorf("up %d = %q, want it to end with %q", i, r.ups[i], svc)
		}
	}
}

// A class line next to a product line is a product failure: no retry.
func TestAuthCorpusExternalUpDoesNotRetryAClassLineBesideAProductLine(t *testing.T) {
	for class, text := range corpusInfraTexts {
		t.Run("class"+class, func(t *testing.T) {
			r := runCorpusWithFailingUp(t, "go-api", 1, text+"\n"+corpusProductText, " go-api")
			if r.err == nil {
				t.Fatalf("a product failure must fail the run:\n%s", r.out)
			}
			if len(r.ups) != 1 || r.retryDowns != 0 || r.retryMarker != "" {
				t.Errorf("want 1 up and no retry; got ups=%q retryDowns=%d line=%q", r.ups, r.retryDowns, r.retryMarker)
			}
		})
	}
}

// The class texts are matched as whole lines: the same words inside another line, or a changed host, are not a class.
func TestAuthCorpusExternalUpClassPatternsAreAnchored(t *testing.T) {
	near := map[string]string{
		"prefix":       "log: " + corpusInfraTexts["1"],
		"suffix":       corpusInfraTexts["2"] + " (see the build log)",
		"other host":   strings.Replace(corpusInfraTexts["3"], "proxy.golang.org", "proxy.example.com", 1),
		"other stream": strings.Replace(corpusInfraTexts["3"], "INTERNAL_ERROR", "CANCEL", 1),
		"other bind":   strings.Replace(corpusInfraTexts["1"], "127.0.0.1", "10.0.0.1", 1),
	}
	for name, text := range near {
		t.Run(name, func(t *testing.T) {
			r := runCorpusWithFailingUp(t, "go-api", 1, text, " go-api")
			if r.err == nil || len(r.ups) != 1 || r.retryMarker != "" {
				t.Errorf("a near-miss text must not be retried: err=%v ups=%q line=%q", r.err, r.ups, r.retryMarker)
			}
		})
	}
}

// The first attempt's whole output stays in the job log.
func TestAuthCorpusExternalUpKeepsTheFirstAttemptOutput(t *testing.T) {
	r := runCorpusWithFailingUp(t, "go-api", 1, corpusInfraTexts["2"], " go-api")
	if r.err != nil || strings.Count(r.out, corpusInfraTexts["2"]) != 1 {
		t.Errorf("want the first attempt's text once in the output, err=%v:\n%s", r.err, r.out)
	}
}

// The class and product-line patterns are byte patterns: the grep calls run with LC_ALL=C, so a letter outside a
// bracket range in the caller's locale cannot make a near-miss line a class line.
func TestAuthCorpusExternalUpClassPatternsIgnoreTheLocale(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"))
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
	if n != 2 {
		t.Errorf("want 2 pattern greps in ci/auth_corpus_external.sh, found %d", n)
	}
}
