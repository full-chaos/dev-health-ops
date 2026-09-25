package venueoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const goldenBuild = "0123456789abcdef0123456789abcdef01234567"

func writeGoldenFile(t *testing.T, dir string, file goldenFile) (path, digest string) {
	t.Helper()
	raw, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	path = filepath.Join(dir, "golden.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return path, hex.EncodeToString(sum[:])
}

func sampleRequests() []Request {
	body := `{"a":1}`
	return []Request{
		{Name: "first", Method: "GET", Path: "/x"},
		{Name: "second", Method: "POST", Path: "/y", Body: &body},
	}
}

func sampleGolden(requests []Request) goldenFile {
	file := goldenFile{Header: goldenHeader{Test: "TestSample", PythonBuild: goldenBuild, Recipe: "record it"}, Rows: map[string]goldenRows{"rows": {Rows: "a | b"}}}
	for index, request := range requests {
		entry := requestKey(request)
		entry.Status, entry.Headers, entry.Body = 200+index, map[string]string{"content-type": "application/json"}, `{"n":`+string(rune('0'+index))+`}`
		file.Requests = append(file.Requests, entry)
	}
	return file
}

func TestFrozenGoldenAnswersFromTheFile(t *testing.T) {
	requests := sampleRequests()
	path, digest := writeGoldenFile(t, t.TempDir(), sampleGolden(requests))
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}, "TestSample", false)
	if err != nil {
		t.Fatal(err)
	}
	if golden.Recording() {
		t.Fatal("a frozen golden must not record")
	}
	answers, err := golden.frozenAnswers(requests)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 2 || answers[0].Status != 200 || answers[1].Status != 201 || answers[1].Body != `{"n":1}` || answers[0].Headers["content-type"] != "application/json" {
		t.Fatalf("answers = %+v", answers)
	}
	if got, err := golden.frozenRows("rows"); err != nil || got != "a | b" {
		t.Fatalf("rows = %q, %v", got, err)
	}
	if got := golden.PythonRoot(t, "/repo"); got != "/repo" {
		t.Fatalf("frozen root = %q, want the caller's own", got)
	}
}

// Each refusal is a way a frozen golden could be trusted wrongly.
func TestFrozenGoldenRefusesWhatItCannotTrust(t *testing.T) {
	requests := sampleRequests()
	dir := t.TempDir()
	path, digest := writeGoldenFile(t, dir, sampleGolden(requests))
	cases := map[string]struct {
		spec GoldenSpec
		want string
	}{
		"missing file":     {GoldenSpec{Path: filepath.Join(dir, "none.json"), PythonBuild: goldenBuild, SHA256: digest, Recipe: "r"}, "is missing"},
		"no pinned digest": {GoldenSpec{Path: path, PythonBuild: goldenBuild, Recipe: "r"}, "pins no digest"},
		"edited by hand":   {GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: strings.Repeat("0", 64), Recipe: "r"}, "changed without its digest"},
		"another build":    {GoldenSpec{Path: path, PythonBuild: strings.Repeat("a", 40), SHA256: digest, Recipe: "r"}, "was executed on build"},
		"not a build":      {GoldenSpec{Path: path, PythonBuild: "main", SHA256: digest, Recipe: "r"}, "40-hex"},
		"no recipe":        {GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}, "recipe"},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := openGolden(testCase.spec, "TestSample", false)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %v, want it to say %q", err, testCase.want)
			}
		})
	}
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "r"}, "TestSample", false)
	if err != nil {
		t.Fatal(err)
	}
	drift := map[string]func([]Request){
		"renamed":      func(r []Request) { r[0].Name = "other" },
		"other path":   func(r []Request) { r[0].Path = "/z" },
		"other body":   func(r []Request) { changed := `{"a":2}`; r[1].Body = &changed },
		"other method": func(r []Request) { r[0].Method = "POST" },
		"body removed": func(r []Request) { r[1].Body = nil },
	}
	for name, mutate := range drift {
		t.Run("drift "+name, func(t *testing.T) {
			changed := sampleRequests()
			mutate(changed)
			if _, err := golden.frozenAnswers(changed); err == nil || !strings.Contains(err.Error(), "regenerate") {
				t.Fatalf("error = %v, want a regeneration instruction", err)
			}
		})
	}
	if _, err := golden.frozenAnswers(sampleRequests()[:1]); err == nil || !strings.Contains(err.Error(), "holds 2 answers for 1 requests") {
		t.Fatalf("a shorter request list: error = %v", err)
	}
	if _, err := golden.frozenRows("nope"); err == nil || !strings.Contains(err.Error(), `no row comparison "nope"`) {
		t.Fatalf("an unknown row comparison: error = %v", err)
	}
}

func TestRecordingWritesTheHeaderAndNeverAProof(t *testing.T) {
	spec := GoldenSpec{Path: filepath.Join(t.TempDir(), "sub", "golden.json"), PythonBuild: goldenBuild, Recipe: "record it"}
	golden, err := openGolden(spec, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if !golden.Recording() {
		t.Fatal("recording golden reports frozen")
	}
	value := golden.Rows(t, "rows", func() string { return "x | y" })
	if value != "x | y" {
		t.Fatalf("recording must return what the source computed, got %q", value)
	}
	if golden.recorded.Header.PythonBuild != goldenBuild || golden.recorded.Header.Test != "TestSample" || golden.recorded.Header.Recipe != "record it" {
		t.Fatalf("header = %+v", golden.recorded.Header)
	}
}

func TestPinnedCheckoutMustBeCleanAndAtTheBuild(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"}, args...)...)
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-q")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "a.txt")
	run("commit", "-q", "-m", "one")
	head := run("rev-parse", "HEAD")
	if err := verifyPinnedCheckout(dir, head); err != nil {
		t.Fatalf("a clean checkout at the build was refused: %v", err)
	}
	if err := verifyPinnedCheckout(dir, strings.Repeat("b", 40)); err == nil || !strings.Contains(err.Error(), "the test pins") {
		t.Fatalf("a checkout at another build was accepted: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("edited\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyPinnedCheckout(dir, head); err == nil || !strings.Contains(err.Error(), "uncommitted changes") {
		t.Fatalf("a dirty checkout was accepted: %v", err)
	}
	if err := verifyPinnedCheckout(t.TempDir(), head); err == nil || !strings.Contains(err.Error(), "not a git checkout") {
		t.Fatalf("a directory that is no checkout was accepted: %v", err)
	}
}

func TestStableUUIDIsDeterministicDistinctAndWellFormed(t *testing.T) {
	first, again, other := StableUUID("org-a"), StableUUID("org-a"), StableUUID("org-b")
	if first != again {
		t.Fatalf("StableUUID is not deterministic: %s != %s", first, again)
	}
	if first == other {
		t.Fatal("two names share a UUID")
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(first) {
		t.Fatalf("%q is not a version-5 UUID", first)
	}
}
