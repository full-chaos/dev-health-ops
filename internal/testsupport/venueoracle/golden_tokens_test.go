package venueoracle

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// mintJWT is a token with the given signature text, built at test time so no
// token-shaped string is in this source.
func mintJWT(header, claims, signature string) string {
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	return enc(header) + "." + enc(claims) + "." + enc(signature)
}

// tokenSamples holds one planted token per shape, assembled at run time.
func tokenSamples() map[string]string {
	rep := strings.Repeat
	return map[string]string{
		"jwt":                      mintJWT(`{"alg":"HS256","typ":"JWT"}`, `{"sub":"u1","iat":1}`, "sig"),
		"github-token":             "gh" + "p_" + rep("a", 36),
		"github-fine-grained-pat":  "github" + "_pat_" + rep("b", 30),
		"gitlab-pat":               "gl" + "pat-" + rep("c", 24),
		"slack-token":              "xo" + "xb-" + rep("1", 14),
		"stripe-key":               "sk" + "_live_" + rep("d", 20),
		"aws-access-key":           "AK" + "IA" + rep("E", 16),
		"google-api-key":           "AI" + "za" + rep("f", 35),
		"private-key-block":        "-----BEGIN " + "RSA PRIVATE KEY-----",
		"anthropic-key":            "sk-" + "ant-" + rep("g", 24),
		"openai-key":               "sk-" + rep("h", 40),
		"linear-key":               "lin" + "_api_" + rep("i", 36),
		"customer-push-token":      "fc" + "push_" + strings.Repeat("k", 40),
		"authorization-credential": "Bear" + "er " + rep("j", 30),
	}
}

func TestEveryTokenShapeIsFoundAndNamed(t *testing.T) {
	samples := tokenSamples()
	names := TokenShapeNames()
	if len(names) != 14 || len(samples) != 14 {
		t.Fatalf("shapes = %d, samples = %d, want 14 each", len(names), len(samples))
	}
	for _, name := range names {
		sample, ok := samples[name]
		if !ok {
			t.Fatalf("no planted sample for shape %q", name)
		}
		found := strings.Join(TokenShapesIn(`{"body":"x `+sample+` y"}`), ",")
		if !strings.Contains(found, name) {
			t.Errorf("a planted %s was not found (found %q)", name, found)
		}
		if got := TokenShapesIn("a plain body with a stable id 3f2a9c10-0000-5000-8000-000000000001"); len(got) != 0 {
			t.Errorf("a stable id read as a token: %v", got)
		}
	}
}

func TestProjectionDropsSignatureAndVolatileValuesAndKeepsTheRest(t *testing.T) {
	a := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","role":"admin","iat":100,"exp":200,"jti":"x","n":3}`, "signature-one")
	b := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","role":"admin","iat":999,"exp":1099,"jti":"y","n":3}`, "signature-two")
	other := mintJWT(`{"alg":"HS256"}`, `{"sub":"u2","role":"admin","iat":100,"exp":200,"jti":"x","n":3}`, "signature-one")
	pa, pb, po := ProjectTokens(`{"t":"`+a+`"}`), ProjectTokens(`{"t":"`+b+`"}`), ProjectTokens(`{"t":"`+other+`"}`)
	if pa != pb {
		t.Fatalf("two tokens of one caller project differently:\n%s\n%s", pa, pb)
	}
	if pa == po {
		t.Fatal("a different subject projects the same")
	}
	for _, want := range []string{"sub=string:u1", "role=string:admin", "iat=number:", "exp=number:", "jti=string:", "n=number:3", "hdr.alg=string:HS256"} {
		if !strings.Contains(pa, want) {
			t.Errorf("projection %s lacks %q", pa, want)
		}
	}
	if strings.Contains(pa, "signature") || strings.Contains(pa, "|iat=number:100") {
		t.Errorf("projection keeps a signature or a volatile value: %s", pa)
	}
	if found := TokenShapesIn(pa); len(found) > 0 {
		t.Errorf("a projection still holds a token shape: %v", found)
	}
	if again := ProjectTokens(pa); again != pa {
		t.Errorf("projection is not idempotent:\n%s\n%s", pa, again)
	}
	if strings.ContainsAny(strings.TrimPrefix(strings.TrimSuffix(pa, `"}`), `{"t":"`), `"\`) {
		t.Errorf("projection breaks the surrounding JSON string: %s", pa)
	}
}

func TestTheRecorderRefusesACandidateHoldingATokenShape(t *testing.T) {
	final := filepath.Join(t.TempDir(), "g.json")
	for name, sample := range tokenSamples() {
		golden, err := openGolden(GoldenSpec{Path: final, PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
		if err != nil {
			t.Fatal(err)
		}
		golden.recorded.Header.ProducerDigest = strings.Repeat("c", 64)
		golden.recorded.Requests = []goldenRequest{{Name: "a", Body: "x " + sample}}
		if _, err := golden.writeCandidate(false); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: the recorder wrote a candidate holding it: %v", name, err)
		}
	}
	if _, err := os.Stat(final + GoldenCandidateSuffix); err == nil {
		t.Fatal("a candidate holding a token is on disk")
	}
}

func TestRecordingProjectsAJWTAndScrubsWhatTheSpecNames(t *testing.T) {
	final := filepath.Join(t.TempDir(), "g.json")
	scrub := func(text string) string { return strings.ReplaceAll(text, "opaque-session-12345", "<session>") }
	golden, err := openGolden(GoldenSpec{Path: final, PythonBuild: goldenBuild, Recipe: "record it", Scrub: scrub}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":7}`, "sig")
	got, err := golden.projectResponse(Response{Status: 200, Headers: map[string]string{"set-cookie": "session=opaque-session-12345; token=" + token}, Body: `{"access_token":"` + token + `"}`})
	if err != nil {
		t.Fatal(err)
	}
	if found := TokenShapesIn(got.Body + got.Headers["set-cookie"]); len(found) > 0 {
		t.Fatalf("projected answer holds %v", found)
	}
	if !strings.Contains(got.Headers["set-cookie"], "session=<session>;") || !strings.Contains(got.Body, "sub=string:u1") {
		t.Fatalf("projection or scrub missing: %+v", got)
	}
}

// TestDiffComparesBothPlanesAsProjected is the Go side of the projection: the
// frozen answer holds a projected token, the Go plane mints a fresh one.
func TestDiffComparesBothPlanesAsProjected(t *testing.T) {
	t.Setenv(goldenUpdateEnv, "")
	t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR", t.TempDir())
	requests := []Request{{Name: "login", Method: "GET", Path: "/x"}}
	python := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":1,"exp":2}`, "python-signature")
	file := sampleGolden(requests)
	file.Header.Test = t.Name()
	file.Requests[0].Status = 200
	file.Requests[0].Headers = map[string]string{"content-type": "application/json", "content-length": "999"}
	file.Requests[0].Body = ProjectTokens(`{"token":"` + python + `"}`)
	path, digest := writeGoldenFile(t, t.TempDir(), file)
	goPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"` + mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":123456,"exp":123457}`, "go-signature-longer") + `"}`))
	}))
	t.Cleanup(goPlane.Close)
	golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"})
	answers := golden.Python(t, nil, requests)
	golden.CompareRows(t, "rows", func() string { return "" }, "a | b")
	Diff(t, goPlane.URL, requests, answers, DiffOptions{Golden: golden})
	golden.Finish(t)
}

// goldenTokenViolations walks root for golden files (a JSON file under a
// testdata directory with a golden header) and names each that holds a token.
func goldenTokenViolations(t *testing.T, root string) (checked int, violations []string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == ".git" || name == "node_modules" || name == ".venv" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".json") || !strings.Contains(filepath.ToSlash(path), "/testdata/") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(raw), `"python_build"`) {
			return nil
		}
		checked++
		if err := tokenShapeErr(path, raw); err != nil {
			violations = append(violations, err.Error())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return checked, violations
}

func TestNoGoldenInTheRepoHoldsATokenShape(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	checked, violations := goldenTokenViolations(t, root)
	if checked == 0 {
		t.Fatal("the walk found no golden: a gate that checks nothing passes everything")
	}
	if len(violations) > 0 {
		t.Fatalf("goldens hold token shapes:\n%s", strings.Join(violations, "\n"))
	}
}

// TestTheGateFailsOnAPlantedToken is verification rule 2 for the walk above:
// the same walk over a tree with one planted token per shape reports each.
func TestTheGateFailsOnAPlantedToken(t *testing.T) {
	dir := t.TempDir()
	for name, sample := range tokenSamples() {
		sub := filepath.Join(dir, name, "testdata")
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		body := `{"header":{"python_build":"x"},"requests":[{"body":"` + sample + `"}]}`
		if err := os.WriteFile(filepath.Join(sub, "g.json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	checked, violations := goldenTokenViolations(t, dir)
	if checked != 14 || len(violations) != 14 {
		t.Fatalf("planted 14 tokens: checked %d, reported %d", checked, len(violations))
	}
}

func TestProjectionKeepsNumbersExactAndTheLifetime(t *testing.T) {
	project := func(claims string) string {
		return ProjectTokens(mintJWT(`{"alg":"HS256"}`, claims, "s"))
	}
	if project(`{"n":9007199254740992}`) == project(`{"n":9007199254740993}`) {
		t.Fatal("2^53 and 2^53+1 project equal")
	}
	if project(`{"n":1}`) == project(`{"n":1.0}`) {
		t.Fatal("1 and 1.0 project equal")
	}
	if project(`{"iat":10,"exp":70}`) != project(`{"iat":500,"exp":560}`) {
		t.Fatal("the same lifetime at another time projects differently")
	}
	if project(`{"iat":10,"exp":70}`) == project(`{"iat":10,"exp":3610}`) {
		t.Fatal("a 60 s and a 3600 s lifetime project equal")
	}
	if project(`{"iat":10,"nbf":10}`) == project(`{"iat":10,"nbf":40}`) {
		t.Fatal("a different not-before offset projects equal")
	}
}

func TestAnUndecodableTokenIsRefusedByTheRecorderAndByDiff(t *testing.T) {
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	truncated := "eyJhYmM" // decodes to a cut-off JSON object
	for name, token := range map[string]string{
		"payload not json": enc(`{"alg":"x"}`) + "." + truncated + ".sig",
		"header not json":  truncated + "." + enc(`{"a":1}`) + ".sig",
	} {
		projected := ProjectTokens(`{"t":"` + token + `"}`)
		if !strings.Contains(projected, undecodableJWT) {
			t.Fatalf("%s: projection %s does not mark the token undecodable", name, projected)
		}
		if err := tokenShapeErr("g.json", []byte(projected)); err == nil || !strings.Contains(err.Error(), "could not be decoded") {
			t.Errorf("%s: the recorder accepted it: %v", name, err)
		}
		if err := undecodableErr(Request{Name: "r"}, Response{Body: projected}); err == nil {
			t.Errorf("%s: Diff accepted it", name)
		}
	}
}

func TestATokenInsideAPackedBodyIsProjectedRefusedAndReported(t *testing.T) {
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":7}`, "sig")
	golden, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	packed := PackBody([]byte("program output " + token))
	projected, err := golden.projectResponse(Response{Body: packed})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := unpackBody(projected.Body)
	if err != nil || strings.Contains(raw, token) || !strings.Contains(raw, "sub=string:u1") {
		t.Fatalf("a packed body was not projected: %q %v", raw, err)
	}
	if _, err := golden.projectResponse(Response{Body: packedPrefix + "not-base64!"}); err == nil {
		t.Fatal("a packed body that does not unpack was accepted")
	}
	golden.recorded.Header.ProducerDigest = strings.Repeat("c", 64)
	golden.recorded.Requests = []goldenRequest{{Name: "a", Body: packed}}
	if _, err := golden.writeCandidate(false); err == nil || !strings.Contains(err.Error(), "jwt") {
		t.Fatalf("the recorder accepted a token inside a packed body: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "x", "testdata")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"header":{"python_build":"x"},"requests":[{"body":"` + packed + `"}]}`
	if err := os.WriteFile(filepath.Join(dir, "g.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if checked, violations := goldenTokenViolations(t, filepath.Dir(filepath.Dir(dir))); checked != 1 || len(violations) != 1 {
		t.Fatalf("the walk missed a token inside a packed body: %d %v", checked, violations)
	}
}

// TestDiffRefusesAnUndecodableToken runs the failing Diff in a child test
// process, because the refusal is t.Fatal: the child must fail naming the
// reason, so the call in Diff is shown wired and not only its helper.
func TestDiffRefusesAnUndecodableToken(t *testing.T) {
	if os.Getenv("VENUEORACLE_CHILD_UNDECODABLE") == "1" {
		t.Setenv(goldenUpdateEnv, "")
		t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR", t.TempDir())
		requests := []Request{{Name: "login", Method: "GET", Path: "/x"}}
		file := sampleGolden(requests)
		file.Header.Test = t.Name()
		file.Requests[0].Body = `{"token":"` + undecodableJWT + `:payload>"}`
		path, digest := writeGoldenFile(t, t.TempDir(), file)
		goPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
		t.Cleanup(goPlane.Close)
		golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"})
		answers := golden.Python(t, nil, requests)
		Diff(t, goPlane.URL, requests, answers, DiffOptions{Golden: golden})
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestDiffRefusesAnUndecodableToken$", "-test.v")
	command.Env = append(os.Environ(), "VENUEORACLE_CHILD_UNDECODABLE=1")
	out, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "not a JSON object") {
		t.Fatalf("Diff did not refuse an undecodable token: err=%v\n%s", err, out)
	}
}

// TestScrubIsTheEscapeForAFalsePositive: an identifier that merely looks like
// a credential is refused, and a per-golden Scrub that names it lets the
// recording through.
func TestScrubIsTheEscapeForAFalsePositive(t *testing.T) {
	lookalike := "basic " + strings.Repeat("a", 30)
	record := func(scrub func(string) string) error {
		golden, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it", Scrub: scrub}, "TestSample", true)
		if err != nil {
			t.Fatal(err)
		}
		projected, err := golden.projectResponse(Response{Body: "plan: " + lookalike})
		if err != nil {
			t.Fatal(err)
		}
		golden.recorded.Header.ProducerDigest = strings.Repeat("c", 64)
		golden.recorded.Requests = []goldenRequest{{Name: "a", Body: projected.Body}}
		_, err = golden.writeCandidate(false)
		return err
	}
	if err := record(nil); err == nil {
		t.Fatal("a credential lookalike was recorded without a scrub")
	}
	if err := record(func(text string) string { return strings.ReplaceAll(text, lookalike, "<plan-name>") }); err != nil {
		t.Fatalf("the scrub did not clear the lookalike: %v", err)
	}
}
