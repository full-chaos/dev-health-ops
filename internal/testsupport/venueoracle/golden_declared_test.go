package venueoracle

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The declared constants of these tests are synthetic.
const (
	declaredA = "synthetic-credential-AAAA-1234"
	declaredB = "synthetic-credential-BBBB-5678"
)

func declaredGolden(t *testing.T, recording bool, constants ...string) *Golden {
	t.Helper()
	golden, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it", CredentialConstants: constants}, t.Name(), recording)
	if err != nil {
		t.Fatal(err)
	}
	return golden
}

// placeholderOf is computed here from crypto/sha256 directly, not through the code under test: a digest of the
// wrong text would then fail every test instead of agreeing with itself.
func placeholderOf(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "<credential sha256:" + hex.EncodeToString(sum[:])[:12] + ">"
}

// A declared constant is replaced by its digest wherever it stands: a whole value, inside a header value, a
// path, free text, a packed body, a row; on both planes through one function; and the value stays compared
// (as its digest): a different constant gives a different placeholder.
func TestADeclaredConstantIsReplacedWhereverItStandsAndStaysPinned(t *testing.T) {
	g := declaredGolden(t, true, declaredA, declaredB)
	for name, text := range map[string]string{
		"whole value":       declaredA,
		"inside a header":   "Bearer " + declaredA + "; charset=utf-8",
		"inside a path":     "/v1/orgs/x/tokens/" + declaredA + "/revoke?x=1",
		"inside free text":  "the CLI said: token " + declaredA + " was refused (retry with " + declaredB + ")",
		"twice in one text": declaredA + " and again " + declaredA,
	} {
		out := g.DigestDeclared(text)
		if strings.Contains(out, declaredA) || strings.Contains(out, declaredB) {
			t.Errorf("%s: a declared constant is left raw: %q", name, out)
		}
		if !strings.Contains(out, placeholderOf(declaredA)) {
			t.Errorf("%s: the placeholder of the constant is missing: %q", name, out)
		}
	}
	if g.DigestDeclared(declaredA) == g.DigestDeclared(declaredB) {
		t.Fatal("two different constants got the same placeholder: the value is not pinned")
	}
	if g.DigestDeclared("another-credential-CCCC-9999") != "another-credential-CCCC-9999" {
		t.Fatal("an undeclared value was changed: the scrub is not exact")
	}
	if once := g.DigestDeclared("x " + declaredA); g.DigestDeclared(once) != once {
		t.Fatal("the scrub is not idempotent")
	}
	// through the harness's own projection: a recorded body (packed too), a request path and a row.
	projected, err := g.project("r", "body", "x "+declaredA+" y")
	if err != nil || strings.Contains(projected, declaredA) || !strings.Contains(projected, placeholderOf(declaredA)) {
		t.Fatalf("a body is not projected: %q %v", projected, err)
	}
	packed, err := g.project("r", "body", PackBody([]byte("x "+declaredB)))
	if err != nil {
		t.Fatal(err)
	}
	if raw, _ := unpackBody(packed); strings.Contains(raw, declaredB) || !strings.Contains(raw, placeholderOf(declaredB)) {
		t.Fatalf("a packed body is not projected: %q", raw)
	}
	if path := g.projectKeyText("/p/" + declaredA); strings.Contains(path, declaredA) || !strings.Contains(path, placeholderOf(declaredA)) {
		t.Fatalf("a request path is not projected: %q", path)
	}
	// a different token on the other plane = a different projected answer = a DIFF (still pinned).
	if g.DigestDeclared("auth="+declaredA) == g.DigestDeclared("auth="+declaredB) {
		t.Fatal("a different declared token on the other plane projects alike: the compare would pass")
	}
}

// The record verb's checks: a candidate that holds a declared constant raw (even inside a packed body) is
// refused naming the position and count, never the value; a declaration nothing needs is refused as stale.
func TestARecordingThatLeaksOrNeverSeesADeclaredConstantIsRefused(t *testing.T) {
	g := declaredGolden(t, true, declaredA, declaredB)
	leak := []byte(`{"requests":[{"name":"r","body":"` + string(PackBody([]byte("x "+declaredB))) + `"}]}`)
	err := declaredLeakErr("g.json", leak, g.spec.CredentialConstants)
	if err == nil || !strings.Contains(err.Error(), "#1 (1 times)") || strings.Contains(err.Error(), declaredB) {
		t.Fatalf("a packed raw constant was not refused by position: %v", err)
	}
	if err := declaredLeakErr("g.json", []byte(`{"requests":[{"body":"clean"}]}`), g.spec.CredentialConstants); err != nil {
		t.Fatalf("a clean candidate is refused: %v", err)
	}
	g.DigestDeclared("only " + declaredA)
	if err := g.declaredStaleErr(); err == nil || !strings.Contains(err.Error(), "#1 never occurs") || strings.Contains(err.Error(), declaredB) {
		t.Fatalf("a declaration that never occurs was not refused as stale: %v", err)
	}
	g.DigestDeclared("now " + declaredB)
	if err := g.declaredStaleErr(); err != nil {
		t.Fatalf("both constants occur and the declaration is refused: %v", err)
	}
}

// The golden's header holds the full digest of each declared constant (under a name no scanner reads as a key)
// and a frozen run refuses a test that declares other, more or fewer constants.
func TestTheHeaderPinsTheDeclaredConstantsAndAFrozenRunRefusesOthers(t *testing.T) {
	recording := declaredGolden(t, true, declaredB, declaredA)
	sumA, sumB := sha256.Sum256([]byte(declaredA)), sha256.Sum256([]byte(declaredB))
	want := []string{hex.EncodeToString(sumA[:]), hex.EncodeToString(sumB[:])}
	sort.Strings(want)
	got := recording.recorded.Header.DeclaredConstants
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] || len(got[0]) != 64 {
		t.Fatalf("the recording's header does not hold the sorted full digests: %v", got)
	}
	if err := declaredHeaderErr("g.json", want, []string{declaredB, declaredA}, "recipe"); err != nil {
		t.Fatalf("the same constants are refused: %v", err)
	}
	for name, constants := range map[string][]string{
		"another constant": {declaredA, "synthetic-credential-ZZZZ-0000"},
		"one fewer":        {declaredA},
		"one more":         {declaredA, declaredB, "synthetic-credential-ZZZZ-0000"},
		"none":             nil,
	} {
		err := declaredHeaderErr("g.json", want, constants, "recipe")
		if err == nil || strings.Contains(err.Error(), declaredA) {
			t.Errorf("%s: the changed declaration was not refused, or the refusal shows a value: %v", name, err)
		}
	}
}

// A declaration that cannot be used is refused when the golden opens: too short (it would replace ordinary
// text) and a duplicate (a constant that is a prefix of another is allowed: see the prefix test).
func TestAnUnusableDeclarationIsRefused(t *testing.T) {
	for name, constants := range map[string][]string{
		"too short": {"short"},
		"duplicate": {declaredA, declaredA},
	} {
		if _, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "r", CredentialConstants: constants}, "T", true); err == nil {
			t.Errorf("%s: the declaration was accepted", name)
		}
	}
}

// Through the candidate write itself (not the helpers): a recording that holds a declared constant raw, or
// declares one it never saw, writes no candidate; a clean recording writes one whose header holds the digests.
func TestTheCandidateWriteRefusesALeakAndAStaleDeclaration(t *testing.T) {
	record := func(constants []string, body string, seen ...string) (*Golden, error) {
		g := declaredGolden(t, true, constants...)
		g.byVerb = true
		g.recorded.Header.ProducerDigest = strings.Repeat("a", 64)
		for _, text := range seen {
			g.DigestDeclared(text)
		}
		g.recorded.Requests = []goldenRequest{{Name: "r", Method: "GET", Path: "/p", Status: 200, Body: body}}
		_, err := g.writeCandidate(false)
		return g, err
	}
	g, err := record([]string{declaredA}, "raw "+declaredA, declaredA)
	if err == nil || !strings.Contains(err.Error(), "still holds declared credential constant") || strings.Contains(err.Error(), declaredA) || exists(g.spec.Path+GoldenCandidateSuffix) {
		t.Fatalf("a candidate holding a declared constant raw was written or the refusal shows it: %v", err)
	}
	g, err = record([]string{declaredA, declaredB}, "clean", declaredA)
	if err == nil || !strings.Contains(err.Error(), "never occurs") || exists(g.spec.Path+GoldenCandidateSuffix) {
		t.Fatalf("a stale declaration still wrote a candidate: %v", err)
	}
	g, err = record([]string{declaredA}, "ok "+placeholderOf(declaredA), declaredA)
	if err != nil || !exists(g.spec.Path+GoldenCandidateSuffix) {
		t.Fatalf("a clean recording wrote no candidate: %v", err)
	}
	raw, _ := os.ReadFile(g.spec.Path + GoldenCandidateSuffix)
	if strings.Contains(string(raw), declaredA) || !strings.Contains(string(raw), "declared_constants_sha256") {
		t.Fatalf("the candidate holds the raw constant or lacks its digest header: %s", raw)
	}
}

func exists(path string) bool { _, err := os.Stat(path); return err == nil }

// A FROZEN golden opens only for the constants it was recorded with: the header's digests are checked by
// openGolden itself, not only by the helper.
func TestAFrozenGoldenOpensOnlyForItsDeclaredConstants(t *testing.T) {
	request := ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)
	entry := requestKey(request)
	entry.Body = "ABC\n"
	header := goldenHeader{Test: "TestAFrozenGoldenOpensOnlyForItsDeclaredConstants", PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64), Recipe: "record it",
		PythonEnv: "k", PythonEnvVersion: pythonEnvKeyVersion, DeclaredConstants: declaredDigests([]string{declaredA})}
	path, digest := writeGoldenFile(t, t.TempDir(), goldenFile{Header: header, Requests: []goldenRequest{entry}})
	open := func(constants ...string) error {
		_, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it", CredentialConstants: constants}, "TestAFrozenGoldenOpensOnlyForItsDeclaredConstants", false)
		return err
	}
	if err := open(declaredA); err != nil {
		t.Fatalf("the golden's own constants are refused: %v", err)
	}
	for name, constants := range map[string][]string{"another constant": {declaredB}, "none declared": nil, "one more": {declaredA, declaredB}} {
		if err := open(constants...); err == nil || !strings.Contains(err.Error(), "declared credential constant") {
			t.Errorf("%s: a frozen golden opened for constants it was not recorded with: %v", name, err)
		}
	}
}

// One constant may be a prefix of another: the longer is replaced first and each keeps its own digest.
func TestAConstantThatIsAPrefixOfAnotherKeepsItsOwnDigest(t *testing.T) {
	longer := declaredA + "-suffix"
	g := declaredGolden(t, true, declaredA, longer)
	both := g.DigestDeclared("x " + longer + " y " + declaredA + " z")
	want := "x " + placeholderOf(longer) + " y " + placeholderOf(declaredA) + " z"
	if both != want {
		t.Fatalf("the longer constant was cut by the shorter one: %q", both)
	}
}

// The header field's NAME is part of the secret-scan contract (CHAOS-7898): a 64-hex value under a name a scanner
// reads as a key (credential, key, secret, token, password, auth, api) is a hit of the scan of record; this name is
// not one, which is why the full digest may stand there.
func TestTheDeclaredDigestsHeaderNameIsNotAScannerKeyword(t *testing.T) {
	field, ok := reflect.TypeOf(goldenHeader{}).FieldByName("DeclaredConstants")
	if !ok {
		t.Fatal("the header has no DeclaredConstants field")
	}
	name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
	if name != "declared_constants_sha256" {
		t.Fatalf("the header name changed to %q: re-run the scan of record on a candidate with a full 64-hex digest under it before accepting it", name)
	}
	for _, word := range []string{"credential", "key", "secret", "token", "passw", "auth", "api", "creds", "access"} {
		if strings.Contains(strings.ToLower(name), word) {
			t.Fatalf("the header name %q holds the scanner keyword %q", name, word)
		}
	}
}

// Two declared constants whose placeholders collide are refused (about 2^-49 per pair at 12 hex digits, but a
// collision would make two credentials indistinguishable in the golden). The test narrows the width to 2 hex digits
// to find a colliding pair among a few hundred candidates, and checks that the real width refuses nothing here.
func TestTwoConstantsWithTheSamePlaceholderAreRefused(t *testing.T) {
	const width = 2
	var first, second string
	seen := map[string]string{}
	for i := 0; second == ""; i++ {
		value := fmt.Sprintf("synthetic-credential-%05d", i)
		key := constantDigest(value)[:width]
		if other, ok := seen[key]; ok {
			first, second = other, value
		}
		seen[key] = value
	}
	if err := declaredConstantsErrWidth([]string{first, second}, width); err == nil || !strings.Contains(err.Error(), "same") || strings.Contains(err.Error(), first) {
		t.Fatalf("two constants with the same placeholder were accepted or the refusal shows a value: %v", err)
	}
	if err := declaredConstantsErrWidth([]string{declaredA, declaredB}, declaredPlaceholderHex); err != nil {
		t.Fatalf("two constants with different placeholders were refused: %v", err)
	}
	if err := declaredConstantsErr([]string{declaredA, declaredB}); err != nil {
		t.Fatalf("the real width refuses an ordinary pair: %v", err)
	}
}

// The real placeholder width refuses a REAL collision: two texts whose sha256 agree on the first 12 hex digits
// (found once by cycle search over the 12-hex prefix, about 2^24 steps; the two are synthetic) and differ in full.
func TestTheRealPlaceholderWidthRefusesARealCollision(t *testing.T) {
	const first, second = "collide-1025c90ebdbf", "collide-f15a9c0305b0"
	if constantDigest(first) == constantDigest(second) || constantDigest(first)[:12] != constantDigest(second)[:12] {
		t.Fatal("the pair is not a 12-hex collision any more: find another with a cycle search over sha256 prefixes")
	}
	if err := declaredConstantsErr([]string{first, second}); err == nil || !strings.Contains(err.Error(), "same 12-hex placeholder") || strings.Contains(err.Error(), first) {
		t.Fatalf("a real 12-hex collision was accepted at the real width, or the refusal shows a value: %v", err)
	}
}

// A declared constant that is JWT-shaped is replaced by its digest BEFORE the token projection (which would turn
// it into claims and lose the declaration): the order of the stages is part of the contract.
func TestAJWTShapedDeclaredConstantIsReplacedByItsDigestNotProjected(t *testing.T) {
	// Synthetic, minted here: the claims are a plain structure, marshalled by encoding/json and encoded by mintJWT
	// (base64url); no token literal is in the source and no secret exists.
	header, _ := json.Marshal(map[string]any{"alg": "HS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{"sub": "synthetic-user", "exp": 4102444800})
	jwt := mintJWT(string(header), string(claims), "synthetic-signature")
	g := declaredGolden(t, true, jwt)
	projected, err := g.project("r", "body", "token="+jwt+";")
	if err != nil {
		t.Fatal(err)
	}
	if want := "token=" + placeholderOf(jwt) + ";"; projected != want {
		t.Fatalf("a JWT-shaped declared constant was projected to claims, not replaced by its digest: %q", projected)
	}
	if plain := ProjectTokens("token=" + jwt + ";"); plain == "token="+jwt+";" {
		t.Fatal("the synthetic JWT is not recognised as one by the projection: the test proves nothing")
	}
}

// The header's digests are sorted whatever the order of the declaration: a test that lists its constants in
// another order than their digests' must still match its golden.
func TestTheHeaderDigestsAreSortedWhateverTheDeclarationOrder(t *testing.T) {
	first, second := declaredA, declaredB
	if constantDigest(first) < constantDigest(second) {
		first, second = second, first // the declaration order is now the DESCENDING digest order
	}
	got := declaredGolden(t, true, first, second).recorded.Header.DeclaredConstants
	if len(got) != 2 || got[0] >= got[1] {
		t.Fatalf("the header digests are not in ascending order: %v", got)
	}
	if err := declaredHeaderErr("g.json", got, []string{second, first}, "r"); err != nil {
		t.Fatalf("the same constants in the other order are refused: %v", err)
	}
}

// ---- decoded forms (D4196): a declared constant is refused raw AND inside a base64 token (4 variants), URL-escaped
// (query and path) and JSON-escaped (one and two levels), each with its own fixture.

// declaredQuoted holds every character an escape changes: a quote, a slash, a plus, an equals sign, an at sign,
// and '>' / '?' (sextets 62 and 63: '+' '/' in the standard base64 alphabet, '-' '_' in the URL one).
const declaredQuoted = `synth"pass/word+x=@1>>??z>>??q`

func candidateWith(leaf string) []byte {
	raw, err := json.Marshal(map[string]any{"requests": []map[string]string{{"name": "r", "body": leaf}}})
	if err != nil {
		panic(err)
	}
	return raw
}

func leakFormsOf(t *testing.T, leaf string, constants ...string) string {
	t.Helper()
	err := declaredLeakErr("g.json", candidateWith(leaf), constants)
	if err == nil {
		return ""
	}
	for _, constant := range constants {
		if strings.Contains(err.Error(), constant) {
			t.Fatalf("the refusal shows a constant: %v", err)
		}
	}
	return err.Error()
}

// base64 variants: a Basic header value, an SMTP AUTH PLAIN payload and base64url, padded and raw, standard and URL
// alphabets: each is found, and the fixture is checked to really hold the variant's own characters.
func TestADeclaredConstantInsideABase64TokenIsRefusedInEveryVariant(t *testing.T) {
	// find a user name whose padded encoding has padding and whose URL alphabet differs from the standard one
	var user string
	for _, candidate := range []string{"u", "us", "usr", "user", "userx"} {
		std := base64.StdEncoding.EncodeToString([]byte(candidate + ":" + declaredQuoted))
		if strings.Contains(std, "=") && base64.URLEncoding.EncodeToString([]byte(candidate+":"+declaredQuoted)) != std {
			user = candidate
			break
		}
	}
	if user == "" {
		t.Fatal("no fixture user name gives a padded encoding with URL-only characters")
	}
	payload := []byte(user + ":" + declaredQuoted)
	for name, token := range map[string]string{
		"Basic header (standard, padded)": "Authorization: Basic " + base64.StdEncoding.EncodeToString(payload),
		"standard, raw":                   "x " + base64.RawStdEncoding.EncodeToString(payload) + " y",
		"URL alphabet, padded":            "token " + base64.URLEncoding.EncodeToString(payload),
		"URL alphabet, raw":               "t=" + base64.RawURLEncoding.EncodeToString(payload),
		"SMTP AUTH PLAIN":                 "AUTH PLAIN " + base64.StdEncoding.EncodeToString([]byte("\x00"+user+"\x00"+declaredQuoted)),
	} {
		if got := leakFormsOf(t, token, declaredQuoted); !strings.Contains(got, "#0 (base64)") || !strings.Contains(got, "decoded form") {
			t.Errorf("%s: a base64-encoded declared constant was not refused: %q", name, got)
		}
	}
	// the three precondition checks: the fixtures really use the variants
	std := base64.StdEncoding.EncodeToString(payload)
	if !strings.ContainsAny(std, "+/") || !strings.ContainsAny(base64.URLEncoding.EncodeToString(payload), "-_") || !strings.Contains(std, "=") {
		t.Fatalf("the fixture does not hold '+/', '-_' and padding: %q", std)
	}
}

func TestADeclaredConstantThatIsURLEscapedIsRefused(t *testing.T) {
	for name, leaf := range map[string]string{
		"query escape": "/v1/x?k=" + url.QueryEscape(declaredQuoted) + "&z=1",
		"path escape":  "/v1/tokens/" + url.PathEscape(declaredQuoted) + "/revoke",
	} {
		if got := leakFormsOf(t, leaf, declaredQuoted); !strings.Contains(got, "#0 (URL-escaped)") {
			t.Errorf("%s: a URL-escaped declared constant was not refused: %q", name, got)
		}
	}
}

func TestADeclaredConstantThatIsJSONEscapedIsRefused(t *testing.T) {
	once := `{"password":"` + jsonEscaped(declaredQuoted) + `"}`
	twice, _ := json.Marshal(map[string]string{"body": once})
	for name, leaf := range map[string]string{"one level": once, "two levels": string(twice)} {
		if got := leakFormsOf(t, leaf, declaredQuoted); !strings.Contains(got, "#0 (JSON-escaped)") {
			t.Errorf("%s: a JSON-escaped declared constant was not refused: %q", name, got)
		}
	}
}

// The way out the refusal names: the test declares the encoded text it sends as its own constant; the harness then
// replaces it (and the plain one) and the candidate passes; a candidate with neither a plain nor a decoded form passes.
func TestDeclaringTheEncodedTextClearsTheRefusal(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("user:" + declaredQuoted))
	g := declaredGolden(t, true, declaredQuoted, encoded)
	leaf := g.DigestDeclared("Authorization: Basic " + encoded)
	if got := leakFormsOf(t, leaf, declaredQuoted, encoded); got != "" {
		t.Fatalf("a declared encoded text still leaves a refusal: %q", got)
	}
	if got := leakFormsOf(t, "nothing here, only "+placeholderOf(declaredQuoted), declaredQuoted); got != "" {
		t.Fatalf("a clean candidate is refused: %q", got)
	}
}

// ---- vetter-2 hold on 8c539a0771: the URL form is decoded (any hex case, any encoder), base64 is read per alphabet
// and at every alignment, and a decoded form is also looked for inside a packed body and for every declared constant.

func TestAURLEscapedConstantIsRefusedWhateverTheEncoderSpelling(t *testing.T) {
	lower := strings.ToLower(url.QueryEscape(declaredQuoted))
	if lower == url.QueryEscape(declaredQuoted) {
		t.Fatal("the fixture has no upper-case hex to lower")
	}
	// Python's quote(safe="/") leaves '/' as it is and escapes the rest in upper case; a hand encoder may leave more.
	keepSlash := strings.ReplaceAll(url.QueryEscape(declaredQuoted), "%2F", "/")
	for name, leaf := range map[string]string{
		"lower-case hex":     "/v1/x?k=" + lower + "&z=1",
		"slash kept":         "/v1/x?k=" + keepSlash,
		"plus as a space":    "/v1/x?k=" + strings.ReplaceAll(url.QueryEscape("synth pass word"), "%20", "+"),
		"path escape, lower": "/v1/tokens/" + strings.ToLower(url.PathEscape(declaredQuoted)) + "/revoke",
	} {
		constant := declaredQuoted
		if name == "plus as a space" {
			constant = "synth pass word"
		}
		if got := leakFormsOf(t, leaf, constant); !strings.Contains(got, "#0 (URL-escaped)") {
			t.Errorf("%s: a URL-escaped declared constant was not refused: %q", name, got)
		}
	}
	if got := leakFormsOf(t, "/v1/x?k=100%&z=%zz%4", declaredQuoted); got != "" {
		t.Errorf("a text with broken escapes and no constant was refused: %q", got)
	}
}

func TestABase64TokenIsReadAtEveryAlignmentAndPerAlphabet(t *testing.T) {
	payload := []byte("user:" + declaredQuoted)
	std := base64.StdEncoding.EncodeToString(payload)
	url64 := base64.RawURLEncoding.EncodeToString(payload)
	for name, leaf := range map[string]string{
		"after one slash":           "/a/" + std,
		"after a path":              "/abc/" + std,
		"after a longer path":       "/abcd/" + std,
		"after a word":              "tokenvalue" + std,
		"url alphabet in a path":    "/callback/" + url64 + "/done",
		"url alphabet after a word": "tokenvalue" + url64,
		"std after two slashes":     "//" + std,
	} {
		if got := leakFormsOf(t, leaf, declaredQuoted); !strings.Contains(got, "#0 (base64)") {
			t.Errorf("%s: a base64 declared constant was not refused: %q", name, got)
		}
	}
	if got := leakFormsOf(t, "/callback/"+"abcdefghijklmnop"+"/done tokenvalue/abc/", declaredQuoted); got != "" {
		t.Errorf("plain path text with no constant was refused: %q", got)
	}
}

func TestEveryDeclaredConstantIsLookedForInABase64Token(t *testing.T) {
	second := declaredQuoted + "-second"
	token := "Basic " + base64.StdEncoding.EncodeToString([]byte("u:"+second))
	if got := leakFormsOf(t, token, declaredQuoted, second); !strings.Contains(got, "#1 (base64)") {
		t.Errorf("the second declared constant was not found in a base64 token: %q", got)
	}
}

func TestAnEncodedFormInsideAPackedBodyIsRefused(t *testing.T) {
	inner := "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("u:"+declaredQuoted))
	packed := PackBody([]byte(inner))
	if got := leakFormsOf(t, packed, declaredQuoted); !strings.Contains(got, "#0 (base64)") {
		t.Errorf("a Basic value inside a packed body was not refused: %q", got)
	}
}

// A token that ends one character after the encoded text (a lone trailing character carries no byte) and an escape
// that ends the text are both read.
func TestADecodedFormAtTheEdgeOfATokenOrTheTextIsRefused(t *testing.T) {
	var user string
	for _, candidate := range []string{"u", "us", "usr", "user", "userx", "userxx"} {
		if len("user"+candidate+":"+declaredQuoted)%3 == 0 {
			user = "user" + candidate
			break
		}
	}
	if user == "" {
		t.Fatal("no fixture user name makes the payload a multiple of three bytes")
	}
	raw := base64.RawStdEncoding.EncodeToString([]byte(user + ":" + declaredQuoted))
	if len(raw)%4 != 0 {
		t.Fatalf("fixture token length %d is not a multiple of four", len(raw))
	}
	if got := leakFormsOf(t, "x "+raw+"Z y", declaredQuoted); !strings.Contains(got, "#0 (base64)") {
		t.Errorf("a token with one trailing character was not read: %q", got)
	}
	const tail = "synth@pass@"
	if got := leakFormsOf(t, "/v1/x?k="+url.QueryEscape(tail), tail); !strings.Contains(got, "#0 (URL-escaped)") {
		t.Errorf("an escape that ends the text was not read: %q", got)
	}
}

// ---- lead D4213: the check compares DECODED values. Every JSON escape form of a constant is refused, and a URL or
// base64 form inside an escaped JSON text too.

func uEscaped(s string) string {
	var out strings.Builder
	for _, r := range s {
		if r >= 0x10000 {
			r -= 0x10000
			fmt.Fprintf(&out, `\u%04x\u%04x`, 0xD800+(r>>10), 0xDC00+(r&0x3ff))
			continue
		}
		fmt.Fprintf(&out, `\u%04X`, r)
	}
	return out.String()
}

func TestAJSONEscapedConstantIsRefusedInEveryEscapeForm(t *testing.T) {
	const constant = "synth/pass\"word+\U0001F511x"
	slashEscaped := strings.ReplaceAll(jsonEscaped(constant), "/", `\/`)
	for name, leaf := range map[string]string{
		`\uXXXX (upper hex)`:  `{"password":"` + uEscaped(constant) + `"}`,
		`\uXXXX (lower hex)`:  `{"password":"` + strings.ToLower(uEscaped(constant)) + `"}`,
		`escaped slash form`:  `{"password":"` + slashEscaped + `"}`,
		`mixed forms`:         `{"password":"synth\/pass"word` + `+` + `🔑x"}`,
		`fragment, not JSON`:  `password=` + slashEscaped + `;`,
		`two levels, \u form`: `{"body":"{\"password\":\"` + uEscaped(constant) + `\"}"}`,
	} {
		if got := leakFormsOf(t, leaf, constant); !strings.Contains(got, "#0 (JSON-escaped)") {
			t.Errorf("%s: a JSON-escaped declared constant was not refused: %q", name, got)
		}
	}
	// a non-hex digit is not read as a number: \u004G would be 'P' if G counted as hex
	if got := leakFormsOf(t, `synth\u004G`, "synthP"); got != "" {
		t.Errorf("an invalid escape was decoded as if it were valid: %q", got)
	}
	if got := leakFormsOf(t, `{"a":"\u00zz \q \ud83d tail"}`, constant); got != "" {
		t.Errorf("invalid escapes with no constant were refused: %q", got)
	}
}

func TestAnEncodedFormInsideAnEscapedJSONTextIsRefused(t *testing.T) {
	std := base64.StdEncoding.EncodeToString([]byte("u:" + declaredQuoted))
	inner := `{"auth":"Basic ` + std + `","url":"/x?k=` + url.QueryEscape(declaredQuoted) + `"}`
	outer, _ := json.Marshal(map[string]string{"body": inner})
	for name, leaf := range map[string]string{
		"base64 inside JSON text": inner,
		"inside two levels":       string(outer),
		"u-escaped base64 plus":   strings.ReplaceAll(inner, "+", `+`),
	} {
		got := leakFormsOf(t, leaf, declaredQuoted)
		if !strings.Contains(got, "#0 (base64)") && !strings.Contains(got, "#0 (URL-escaped)") {
			t.Errorf("%s: not refused: %q", name, got)
		}
	}
}
