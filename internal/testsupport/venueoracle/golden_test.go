package venueoracle

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
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
	file := goldenFile{Header: goldenHeader{Test: "TestSample", PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64)}, Rows: map[string]goldenRows{"rows": {Rows: "a | b"}}}
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
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}, "TestSample")
	if err != nil {
		t.Fatal(err)
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
		"missing file":     {GoldenSpec{Path: filepath.Join(dir, "none.json"), PythonBuild: goldenBuild, SHA256: digest}, "is missing"},
		"no pinned digest": {GoldenSpec{Path: path, PythonBuild: goldenBuild}, "pins no digest"},
		"edited by hand":   {GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: strings.Repeat("0", 64)}, "changed without its digest"},
		"another build":    {GoldenSpec{Path: path, PythonBuild: strings.Repeat("a", 40), SHA256: digest}, "was executed on build"},
		"not a build":      {GoldenSpec{Path: path, PythonBuild: "main", SHA256: digest}, "40-hex"},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := openGolden(testCase.spec, "TestSample")
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %v, want it to say %q", err, testCase.want)
			}
		})
	}
	// A header that names no producer digest was not written by this harness.
	unstamped := sampleGolden(requests)
	unstamped.Header.ProducerDigest = ""
	unstampedPath, unstampedDigest := writeGoldenFile(t, t.TempDir(), unstamped)
	if _, err := openGolden(GoldenSpec{Path: unstampedPath, PythonBuild: goldenBuild, SHA256: unstampedDigest}, "TestSample"); err == nil || !strings.Contains(err.Error(), "names no producer digest") {
		t.Fatalf("a golden without a producer digest was accepted: %v", err)
	}
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}, "TestSample")
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
			if _, err := golden.frozenAnswers(changed); err == nil || !strings.Contains(err.Error(), "the request it was recorded for") {
				t.Fatalf("error = %v, want a refusal naming the recorded request", err)
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
	spec := GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}
	if _, err := openGolden(spec, "TestSample"); err != nil {
		t.Fatalf("the golden's own test was refused: %v", err)
	}
	if _, err := openGolden(spec, "TestAnother"); err == nil || !strings.Contains(err.Error(), "not for") {
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
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}, "TestSample")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := golden.frozenAnswers([]Request{sameCaller(`{"sub":"u1","role":"member","iat":1}`)}); err == nil || !strings.Contains(err.Error(), "the request it was recorded for") {
		t.Fatalf("header drift was accepted: %v", err)
	}
}

func TestAFrozenSnapshotNobodyComparedIsRefused(t *testing.T) {
	requests := sampleRequests()
	path, digest := writeGoldenFile(t, t.TempDir(), sampleGolden(requests))
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}, "TestSample")
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
	golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest})
	answers := golden.Python(t, nil, requests)
	if len(answers) != 2 || answers[1].Status != 201 {
		t.Fatalf("answers = %+v", answers)
	}
	if got := golden.CompareRows(t, "rows", "a | b"); got != "a | b" {
		t.Fatalf("rows = %q", got)
	}
	Diff(t, goPlane.URL, requests, answers, DiffOptions{Golden: golden})
	golden.Finish(t)
}

// TestSkipDiffLetsAnAllConsumedGoldenReachFinish is the lifecycle a test that
// never calls Diff needs (a named-Python-divergence check that only inspects
// the Python plane's own answers and row state, never re-sends the same
// requests to Go for comparison): Finish refuses before SkipDiff runs (state
// is still "answers fetched", the same named error the fixed lifecycle gives
// any other out-of-order call), and succeeds once every handed-out answer and
// row snapshot was declared Consumed/InspectRows and SkipDiff ran. SkipDiff
// does not relax the "every answer used" requirement: with one answer left
// uninspected, Finish still refuses it.
func TestSkipDiffLetsAnAllConsumedGoldenReachFinish(t *testing.T) {
	t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR", t.TempDir())
	requests := sampleRequests()
	file := sampleGolden(requests)
	file.Header.Test = t.Name()
	path, digest := writeGoldenFile(t, t.TempDir(), file)
	golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest})
	answers := golden.Python(t, nil, requests)

	if err := golden.stepErr("Finish", stateDiffed); err == nil || !strings.Contains(err.Error(), "Finish called when the golden is answers fetched") {
		t.Fatalf("Finish before SkipDiff: %v", err)
	}

	golden.Consumed(t, answers[:1]...)
	golden.SkipDiff(t)
	if golden.state != stateDiffed {
		t.Fatalf("state after SkipDiff = %v, want diffed", golden.state)
	}
	if err := golden.answersCompared(); err == nil || !strings.Contains(err.Error(), "neither compared") {
		t.Fatalf("SkipDiff waived the still-uninspected second answer: %v", err)
	}
	golden.Consumed(t, answers[1:]...)
	golden.InspectRows(t, "rows")
	golden.Finish(t)
}

// handOut gives a golden two handed-out answers the way Python does.
func handOut(t *testing.T) (*Golden, []Request, []Response) {
	t.Helper()
	requests := sampleRequests()
	golden := &Golden{spec: GoldenSpec{Path: "x.json"}}
	answers := []Response{{Status: 200}, {Status: 201}}
	for index := range answers {
		golden.slots = append(golden.slots, answerSlot{request: golden.identityOf(requests[index])})
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
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}, "TestSample")
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

// A frozen answer's Allow header was stored in the order the comparison uses,
// and the projection of the compared side gives the Go plane's header that
// order too; the comparison still treats it as a set of exactly these methods.
func TestAnAllowHeaderIsProjectedInTheComparisonsOrderAndComparedAsASet(t *testing.T) {
	requests := []Request{{Name: "TRACE /health/", Method: "TRACE", Path: "/health/"}}
	golden := &Golden{spec: GoldenSpec{Path: "x.json"}}
	projectAllow := func(allow string) map[string]string {
		projected, err := golden.projectResponse(Response{Status: 405, Headers: map[string]string{"allow": allow, "content-type": "application/json"}, Body: "{}"})
		if err != nil {
			t.Fatal(err)
		}
		return projected.Headers
	}
	first, second := projectAllow("PUT, HEAD, PATCH, GET, POST, DELETE, OPTIONS"), projectAllow("GET, DELETE, PUT, OPTIONS, POST, PATCH, HEAD")
	if first["allow"] != "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT" || first["allow"] != second["allow"] {
		t.Fatalf("one set of methods is projected as %q and %q", first["allow"], second["allow"])
	}
	// Another set of methods is another answer: one method less, and one method more.
	if other := projectAllow("GET, HEAD"); other["allow"] != "GET, HEAD" || other["content-type"] != "application/json" {
		t.Fatalf("projected headers: %v", other)
	}
	if more := projectAllow("PUT, HEAD, PATCH, GET, POST, DELETE, OPTIONS, TRACE"); more["allow"] != "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT, TRACE" {
		t.Fatalf("a header with one method more is projected as %q", more["allow"])
	}
	// The frozen comparison: the Go plane's header in any order is the same answer; a method more is not.
	python := Response{Status: 405, Headers: map[string]string{"allow": first["allow"]}, Body: "{}"}
	same := func(goAllow string) bool {
		ok, _, _, _ := Compare(requests[0], python, Response{Status: 405, Headers: map[string]string{"allow": goAllow}, Body: "{}"}, DiffOptions{})
		return ok
	}
	if !same("POST, GET, PUT, DELETE, PATCH, HEAD, OPTIONS") || same("POST, GET, PUT, DELETE, PATCH, HEAD, OPTIONS, TRACE") || same("POST, GET") {
		t.Fatal("the comparison does not treat Allow as a set of exactly these methods")
	}
}
