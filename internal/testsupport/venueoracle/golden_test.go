package venueoracle

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	file := goldenFile{Header: goldenHeader{Test: "TestSample", PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64), Recipe: "record it"}, Rows: map[string]goldenRows{"rows": {Rows: "a | b"}}}
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
	// A shorter request list is served, and the answer it leaves unused is
	// what Finish refuses; a longer one asks for more than the file holds.
	if _, err := golden.frozenAnswers(sampleRequests()[:1]); err != nil {
		t.Fatalf("a shorter request list: error = %v", err)
	}
	if err := golden.unusedAnswers(); err == nil || !strings.Contains(err.Error(), "holds 2 answers but the test asked for 1") {
		t.Fatalf("an unused frozen answer: error = %v", err)
	}
	if _, err := golden.frozenAnswers(sampleRequests()); err == nil || !strings.Contains(err.Error(), "the test asks for 2 more") {
		t.Fatalf("more requests than answers left: error = %v", err)
	}
	if _, err := golden.frozenAnswers(sampleRequests()[1:]); err != nil {
		t.Fatalf("the remaining request: error = %v", err)
	}
	if err := golden.unusedAnswers(); err != nil {
		t.Fatalf("every answer used: error = %v", err)
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
	golden.state = statePython
	value := golden.InspectRows(t, "rows", func() string { return "x | y" })
	if value != "x | y" {
		t.Fatalf("recording must return what the source computed, got %q", value)
	}
	if golden.recorded.Header.PythonBuild != goldenBuild || golden.recorded.Header.Test != "TestSample" || golden.recorded.Header.Recipe != "record it" {
		t.Fatalf("header = %+v", golden.recorded.Header)
	}
}

func TestPinnedCheckoutMustBeTheBuildItsSourceByteForByte(t *testing.T) {
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
	write := func(name, content string) {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	run("init", "-q")
	write("src/app/probe_module.py", "ROUTE = 1\n")
	write("README.md", "docs\n")
	run("add", ".")
	run("commit", "-q", "-m", "one")
	head := run("rev-parse", "HEAD")
	digest, err := verifyPinnedCheckout(dir, head)
	if err != nil || len(digest) != 64 {
		t.Fatalf("a clean checkout at the build was refused: %q %v", digest, err)
	}
	if again, err := verifyPinnedCheckout(dir, head); err != nil || again != digest {
		t.Fatalf("the producer digest is not stable: %q %q %v", digest, again, err)
	}
	if _, err := verifyPinnedCheckout(dir, strings.Repeat("b", 40)); err == nil || !strings.Contains(err.Error(), "the test pins") {
		t.Fatalf("a checkout at another build was accepted: %v", err)
	}
	// A tracked file edited.
	write("src/app/probe_module.py", "ROUTE = 2\n")
	if _, err := verifyPinnedCheckout(dir, head); err == nil || !strings.Contains(err.Error(), "uncommitted or untracked") {
		t.Fatalf("a dirty checkout was accepted: %v", err)
	}
	// The same edit hidden from git status: the planted producer drift. Only the
	// byte-for-byte comparison with the commit's blobs sees it.
	run("update-index", "--assume-unchanged", "src/app/probe_module.py")
	if _, err := verifyPinnedCheckout(dir, head); err == nil || !strings.Contains(err.Error(), "differs from the pinned commit's blob") {
		t.Fatalf("a source edit hidden by assume-unchanged was accepted: %v", err)
	}
	run("update-index", "--no-assume-unchanged", "src/app/probe_module.py")
	run("checkout", "-q", "--", "src/app/probe_module.py")
	if _, err := verifyPinnedCheckout(dir, head); err != nil {
		t.Fatalf("the restored checkout was refused: %v", err)
	}
	// An untracked source file is a route the pinned build never had.
	write("src/app/untracked_route.py", "x = 1\n")
	if _, err := verifyPinnedCheckout(dir, head); err == nil || !strings.Contains(err.Error(), "uncommitted or untracked") {
		t.Fatalf("an untracked file was accepted: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "src/app/untracked_route.py")); err != nil {
		t.Fatal(err)
	}
	// An ignored file under src (a sitecustomize.py on the Python path runs at
	// start-up) is refused, and so is any bytecode: a timestamp-validated cache
	// can run code older than the source beside it.
	write(".git/info/exclude", "sitecustomize.py\n__pycache__/\n*.pyc\n")
	write("src/app/__pycache__/m.cpython-314.pyc", "x")
	if _, err := verifyPinnedCheckout(dir, head); err == nil || !strings.Contains(err.Error(), "bytecode cache") {
		t.Fatalf("a bytecode cache was accepted: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "src/app/__pycache__")); err != nil {
		t.Fatal(err)
	}
	write("src/injected.pyc", "x")
	if _, err := verifyPinnedCheckout(dir, head); err == nil || !strings.Contains(err.Error(), "compiled Python file") {
		t.Fatalf("a compiled Python file was accepted: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "src/injected.pyc")); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyPinnedCheckout(dir, head); err != nil {
		t.Fatalf("the cleared checkout was refused: %v", err)
	}
	write("src/sitecustomize.py", "print('hook')\n")
	if _, err := verifyPinnedCheckout(dir, head); err == nil || !strings.Contains(err.Error(), "sitecustomize.py") {
		t.Fatalf("an ignored startup hook was accepted: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "src/sitecustomize.py")); err != nil {
		t.Fatal(err)
	}
	// A committed symbolic link under src: its blob is the link text, not the code it
	// resolves to, so the source it points at could change unseen.
	write("outside/real.py", "VALUE = 1\n")
	if err := os.Symlink("../../outside/real.py", filepath.Join(dir, "src/app/linked.py")); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-q", "-m", "link")
	linked := run("rev-parse", "HEAD")
	if _, err := verifyPinnedCheckout(dir, linked); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("a symbolic link under src was accepted: %v", err)
	}
	if _, err := verifyPinnedCheckout(t.TempDir(), head); err == nil || !strings.Contains(err.Error(), "not a git checkout") {
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

func TestAGoldenBelongsToOneTest(t *testing.T) {
	path, digest := writeGoldenFile(t, t.TempDir(), sampleGolden(sampleRequests()))
	spec := GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}
	if _, err := openGolden(spec, "TestSample", false); err != nil {
		t.Fatalf("the golden's own test was refused: %v", err)
	}
	if _, err := openGolden(spec, "TestAnother", false); err == nil || !strings.Contains(err.Error(), "not for") {
		t.Fatalf("a golden recorded for another test was accepted: %v", err)
	}
}

func fakeToken(claims string) string {
	encode := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
	return "Bearer " + encode(`{"alg":"HS256"}`) + "." + encode(claims) + "." + encode("signature")
}

func TestRequestHeadersAreKeptInTheKeyAndABearerTokenByItsClaims(t *testing.T) {
	sameCaller := func(claims string) Request {
		return Request{Name: "a", Method: "GET", Path: "/x", Headers: map[string]string{"Authorization": fakeToken(claims), "Content-Type": "application/json"}}
	}
	base := requestKey(sameCaller(`{"sub":"u1","role":"admin","iat":1,"exp":2,"jti":"one"}`)).HeadersSHA256
	// Another process mints the same caller's token again: issue time, expiry and id differ.
	if again := requestKey(sameCaller(`{"jti":"two","exp":9,"iat":8,"role":"admin","sub":"u1"}`)).HeadersSHA256; again != base {
		t.Fatalf("the same caller in another process changed the key:\n%s\n%s", base, again)
	}
	for name, changed := range map[string]Request{
		"another role":         sameCaller(`{"sub":"u1","role":"member","iat":1,"exp":2}`),
		"another user":         sameCaller(`{"sub":"u2","role":"admin","iat":1,"exp":2}`),
		"another content type": {Name: "a", Method: "GET", Path: "/x", Headers: map[string]string{"Authorization": fakeToken(`{"sub":"u1","role":"admin"}`), "Content-Type": "text/plain"}},
		"no authorization":     {Name: "a", Method: "GET", Path: "/x", Headers: map[string]string{"Content-Type": "application/json"}},
		"a raw authorization":  {Name: "a", Method: "GET", Path: "/x", Headers: map[string]string{"Authorization": "Bearer changed", "Content-Type": "application/json"}},
	} {
		if requestKey(changed).HeadersSHA256 == base {
			t.Errorf("%s did not change the key", name)
		}
	}
	// The token's header is part of the identity; its signature is not (a token
	// is minted per process, so a signature differs by construction).
	withHeader := func(header string) Request {
		encode := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
		token := "Bearer " + encode(header) + "." + encode(`{"sub":"u1","iat":1}`) + "." + encode("sig")
		return Request{Name: "a", Method: "GET", Path: "/x", Headers: map[string]string{"Authorization": token}}
	}
	if requestKey(withHeader(`{"alg":"HS256"}`)).HeadersSHA256 == requestKey(withHeader(`{"alg":"none"}`)).HeadersSHA256 {
		t.Error("another token header (alg) did not change the key")
	}
	otherSignature := Request{Name: "a", Method: "GET", Path: "/x", Headers: map[string]string{"Authorization": fakeToken(`{"sub":"u1","role":"admin"}`)}}
	sameSignatureless := Request{Name: "a", Method: "GET", Path: "/x", Headers: map[string]string{"Authorization": strings.TrimSuffix(fakeToken(`{"sub":"u1","role":"admin"}`), base64.RawURLEncoding.EncodeToString([]byte("signature"))) + base64.RawURLEncoding.EncodeToString([]byte("another"))}}
	if requestKey(otherSignature).HeadersSHA256 != requestKey(sameSignatureless).HeadersSHA256 {
		t.Error("a different signature over the same header and claims changed the key: tokens minted per process would never replay")
	}
	// A golden recorded for one caller refuses a request from another.
	requests := []Request{sameCaller(`{"sub":"u1","role":"admin","iat":1}`)}
	path, digest := writeGoldenFile(t, t.TempDir(), sampleGolden(requests))
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}, "TestSample", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := golden.frozenAnswers([]Request{sameCaller(`{"sub":"u1","role":"member","iat":1}`)}); err == nil || !strings.Contains(err.Error(), "regenerate") {
		t.Fatalf("header drift was accepted: %v", err)
	}
}

func TestAFrozenSnapshotNobodyComparedIsRefused(t *testing.T) {
	requests := sampleRequests()
	path, digest := writeGoldenFile(t, t.TempDir(), sampleGolden(requests))
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}, "TestSample", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := golden.unusedRows(); err == nil || !strings.Contains(err.Error(), "rows") {
		t.Fatalf("a snapshot nobody asked for was accepted: %v", err)
	}
	// Retrieved, but the test dropped the comparison: the snapshot is not used.
	if _, err := golden.frozenRows("rows"); err != nil {
		t.Fatal(err)
	}
	if err := golden.unusedRows(); err == nil || !strings.Contains(err.Error(), "rows") {
		t.Fatalf("a snapshot that was retrieved but never compared was accepted: %v", err)
	}
	golden.rowsUsed["rows"] = true // what CompareRows and InspectRows do once the test used it
	if err := golden.unusedRows(); err != nil {
		t.Fatalf("every snapshot used: %v", err)
	}
}

func TestRowsThatDifferAreAnErrorNamingBothValues(t *testing.T) {
	if err := rowsDiffer("orgs", "id=1", "id=1"); err != nil {
		t.Fatalf("equal rows were refused: %v", err)
	}
	if err := rowsDiffer("orgs", "id=1", "id=999"); err == nil || !strings.Contains(err.Error(), "id=1") || !strings.Contains(err.Error(), "id=999") {
		t.Fatalf("divergent rows were accepted: %v", err)
	}
}

func TestARecordingWritesACandidateBesideTheGoldenNeverTheGolden(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "sub", "g.json")
	newRecording := func() *Golden {
		golden, err := openGolden(GoldenSpec{Path: final, PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
		if err != nil {
			t.Fatal(err)
		}
		golden.recorded.Requests = []goldenRequest{{Name: "a"}}
		golden.recorded.Header.ProducerDigest = strings.Repeat("c", 64)
		return golden
	}
	if _, err := newRecording().writeCandidate(true); err == nil || !strings.Contains(err.Error(), "already failed") {
		t.Fatalf("a failed run wrote a candidate: %v", err)
	}
	empty, err := openGolden(GoldenSpec{Path: final, PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := empty.writeCandidate(false); err == nil || !strings.Contains(err.Error(), "no request was served") {
		t.Fatalf("an empty recording wrote a candidate: %v", err)
	}
	unverified := newRecording()
	unverified.recorded.Header.ProducerDigest = ""
	if _, err := unverified.writeCandidate(false); err == nil || !strings.Contains(err.Error(), "never verified") {
		t.Fatalf("a recording whose producer was never verified wrote a candidate: %v", err)
	}
	if _, err := os.Stat(final + GoldenCandidateSuffix); err == nil {
		t.Fatal("a candidate is on disk that no passing run wrote")
	}
	digest, err := newRecording().writeCandidate(false)
	if err != nil || len(digest) != 64 {
		t.Fatalf("a passing run did not write: %q %v", digest, err)
	}
	raw, err := os.ReadFile(final + GoldenCandidateSuffix)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != digest {
		t.Fatal("the printed digest is not the digest of the candidate written")
	}
	if _, err := os.Stat(final); err == nil {
		t.Fatal("a recording wrote the golden itself: only the record verb may, after a fresh-process replay")
	}
}

func TestTheLifecycleOrderIsEnforcedWithANamedError(t *testing.T) {
	golden := &Golden{spec: GoldenSpec{Path: "x.json"}}
	if err := golden.stepErr("Diff", statePython, stateDiffed); err == nil || !strings.Contains(err.Error(), "Diff called when the golden is opened") {
		t.Fatalf("Diff before any answer was fetched: %v", err)
	}
	if err := golden.stepErr("Finish", stateDiffed); err == nil || !strings.Contains(err.Error(), "Finish called when the golden is opened") {
		t.Fatalf("Finish before Diff: %v", err)
	}
	if err := golden.stepErr("Rows", statePython, stateDiffed); err == nil || !strings.Contains(err.Error(), "Rows called when the golden is opened") {
		t.Fatalf("Rows before any answer: %v", err)
	}
	golden.state = statePython
	if err := golden.stepErr("Python", stateOpen, statePython, stateDiffed); err != nil {
		t.Fatalf("a second Python call was refused: %v", err)
	}
	golden.afterDiff()
	if golden.state != stateDiffed {
		t.Fatalf("state %v", golden.state)
	}
	golden.state = stateFinished
	for _, call := range []string{"Python", "Rows", "Diff", "Finish"} {
		if err := golden.stepErr(call, stateOpen, statePython, stateDiffed); err == nil || !strings.Contains(err.Error(), "finished") {
			t.Fatalf("%s after Finish was accepted: %v", call, err)
		}
	}
}

// The public replay workflow a test uses, from OpenGolden to Finish, with the
// comparison done by Diff against a Go plane.
func TestThePublicFrozenWorkflowEndToEnd(t *testing.T) {
	t.Setenv(goldenUpdateEnv, "")
	t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR", t.TempDir())
	body := B64(`{"a":1}`)
	requests := []Request{
		{Name: "first", Method: "GET", Path: "/x"},
		{Name: "second", Method: "POST", Path: "/y", Body: body},
	}
	file := sampleGolden(requests)
	file.Header.Test = t.Name()
	for index := range file.Requests {
		file.Requests[index].Headers["content-length"] = "7"
	}
	path, digest := writeGoldenFile(t, t.TempDir(), file)
	goPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/x":
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"n":0}`))
		default:
			w.WriteHeader(201)
			_, _ = w.Write([]byte(`{"n":1}`))
		}
	}))
	t.Cleanup(goPlane.Close)
	golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"})
	answers := golden.Python(t, nil, requests)
	if len(answers) != 2 || answers[1].Status != 201 {
		t.Fatalf("answers = %+v", answers)
	}
	if got := golden.CompareRows(t, "rows", func() string { t.Fatal("frozen replay must not read the source database"); return "" }, "a | b"); got != "a | b" {
		t.Fatalf("rows = %q", got)
	}
	Diff(t, goPlane.URL, requests, answers, DiffOptions{Golden: golden})
	golden.Finish(t)
}

// handOut gives a golden two handed-out answers the way Python does.
func handOut(t *testing.T) (*Golden, []Request, []Response) {
	t.Helper()
	requests := sampleRequests()
	golden := &Golden{spec: GoldenSpec{Path: "x.json"}}
	answers := []Response{{Status: 200}, {Status: 201}}
	for index := range answers {
		golden.slots = append(golden.slots, answerSlot{request: requestIdentity(requests[index])})
		answers[index].slot = len(golden.slots)
	}
	return golden, requests, answers
}

func TestAnAnswerNobodyComparedIsRefusedUnlessTheTestDeclaresItInspected(t *testing.T) {
	golden, requests, answers := handOut(t)
	if err := golden.bindAnswers(requests[:1], answers[:1]); err != nil { // Diff was handed one of the two answers
		t.Fatal(err)
	}
	if err := golden.answersCompared(); err == nil || !strings.Contains(err.Error(), "neither compared") {
		t.Fatalf("an answer nobody compared was accepted: %v", err)
	}
	if err := golden.consume(answers[1:]); err != nil { // the test read the other one itself
		t.Fatal(err)
	}
	if err := golden.answersCompared(); err != nil {
		t.Fatalf("a declared inspection was refused: %v", err)
	}
}

// One answer reused for two requests hides the divergence of the second: the
// answer must be the one fetched for that request.
func TestAnAnswerAnswersTheOneRequestItWasFetchedFor(t *testing.T) {
	golden, requests, answers := handOut(t)
	if err := golden.bindAnswers(requests, []Response{answers[0], answers[0]}); err == nil || !strings.Contains(err.Error(), "belongs to another request") {
		t.Fatalf("answer 1 reused for request 2 was accepted: %v", err)
	}
	golden, requests, answers = handOut(t)
	if err := golden.bindAnswers(requests, []Response{answers[1], answers[0]}); err == nil || !strings.Contains(err.Error(), "belongs to another request") {
		t.Fatalf("answers out of order were accepted: %v", err)
	}
	golden, requests, answers = handOut(t)
	if err := golden.bindAnswers(requests, []Response{{Status: 200}, answers[1]}); err == nil || !strings.Contains(err.Error(), "did not come from golden.Python") {
		t.Fatalf("an answer that did not come from Python() was accepted: %v", err)
	}
	golden, requests, answers = handOut(t)
	if err := golden.bindAnswers(requests, answers); err != nil {
		t.Fatalf("the right answers were refused: %v", err)
	}
	if err := golden.bindAnswers(requests, answers); err == nil || !strings.Contains(err.Error(), "already used once") {
		t.Fatalf("the same answers compared twice were accepted: %v", err)
	}
	// An answer compared by Diff cannot also be declared inspected.
	if err := golden.consume(answers[:1]); err == nil || !strings.Contains(err.Error(), "already used once") {
		t.Fatalf("a compared answer was declared inspected: %v", err)
	}
}

func TestAFrozenRowSnapshotAnswersOneComparison(t *testing.T) {
	path, digest := writeGoldenFile(t, t.TempDir(), sampleGolden(sampleRequests()))
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}, "TestSample", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := golden.frozenRows("rows"); err != nil {
		t.Fatal(err)
	}
	if _, err := golden.frozenRows("rows"); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("a reused row snapshot was accepted: %v", err)
	}
}

func TestARecordingRefusesPythonFromARootItNeverVerifiedOrThatTheVenueDoesNotServe(t *testing.T) {
	golden, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	verified, other := t.TempDir(), t.TempDir()
	if err := golden.recordingRootErr(&Venue{Root: verified}); err == nil || !strings.Contains(err.Error(), "PythonRoot") {
		t.Fatalf("an unverified root was accepted: %v", err)
	}
	golden.verifiedRoot = verified
	if err := golden.recordingRootErr(&Venue{Root: verified}); err != nil {
		t.Fatalf("a venue on the verified root was refused: %v", err)
	}
	// The same directory through a symlink is the same root.
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(verified, link); err != nil {
		t.Fatal(err)
	}
	if err := golden.recordingRootErr(&Venue{Root: link}); err != nil {
		t.Fatalf("the verified root through a symlink was refused: %v", err)
	}
	// The verified checkout is not the one the venue runs Python from.
	if err := golden.recordingRootErr(&Venue{Root: other}); err == nil || !strings.Contains(err.Error(), "not from the verified checkout") {
		t.Fatalf("a venue on another root was accepted: %v", err)
	}
	if err := golden.recordingRootErr(nil); err == nil {
		t.Fatal("no venue was accepted")
	}
}

// The recording path a test takes up to the Python plane: OpenGolden in
// recording mode, PythonRoot verifying the pinned checkout, and Finish writing
// the candidate (the Python plane itself needs a venue, so its answers are put
// in the recording directly).
func TestTheRecordingPathVerifiesTheRootThenWritesOnlyACandidate(t *testing.T) {
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
	if err := os.MkdirAll(filepath.Join(dir, "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "src", "app.py"), []byte("X = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-q", "-m", "one")
	build := run("rev-parse", "HEAD")

	t.Setenv(goldenUpdateEnv, "1")
	t.Setenv(goldenPythonRootEnv, dir)
	t.Setenv("PYTHONDONTWRITEBYTECODE", "1")
	final := filepath.Join(t.TempDir(), "testdata", "g.json")
	golden := OpenGolden(t, GoldenSpec{Path: final, PythonBuild: build, Recipe: "record it"})
	if !golden.Recording() {
		t.Fatal("OpenGolden did not open a recording")
	}
	if err := golden.recordingRootErr(&Venue{Root: dir}); err == nil {
		t.Fatal("Python was allowed before the root was verified")
	}
	if got := golden.PythonRoot(t, "/the/callers/own/root"); got != dir {
		t.Fatalf("PythonRoot = %q, want the pinned checkout %q", got, dir)
	}
	if err := golden.recordingRootErr(&Venue{Root: dir}); err != nil {
		t.Fatalf("a verified root was refused: %v", err)
	}
	if len(golden.recorded.Header.ProducerDigest) != 64 {
		t.Fatalf("header producer digest = %q", golden.recorded.Header.ProducerDigest)
	}
	golden.recorded.Requests = []goldenRequest{{Name: "a", Method: "GET", Path: "/x"}}
	golden.state = stateDiffed
	golden.Finish(t)
	if _, err := os.Stat(final); err == nil {
		t.Fatal("Finish wrote the golden itself")
	}
	raw, err := os.ReadFile(final + GoldenCandidateSuffix)
	if err != nil {
		t.Fatalf("Finish wrote no candidate: %v", err)
	}
	var written goldenFile
	if err := json.Unmarshal(raw, &written); err != nil || written.Header.PythonBuild != build || len(written.Header.ProducerDigest) != 64 {
		t.Fatalf("candidate = %s (%v)", raw, err)
	}
}

func TestARecordingNeedsBytecodeWritingSwitchedOff(t *testing.T) {
	t.Setenv("PYTHONDONTWRITEBYTECODE", "")
	if err := bytecodeEnvErr(); err == nil || !strings.Contains(err.Error(), "PYTHONDONTWRITEBYTECODE=1") {
		t.Fatalf("a recording with bytecode writing on was accepted: %v", err)
	}
	t.Setenv("PYTHONDONTWRITEBYTECODE", "1")
	if err := bytecodeEnvErr(); err != nil {
		t.Fatalf("PYTHONDONTWRITEBYTECODE=1 was refused: %v", err)
	}
}
