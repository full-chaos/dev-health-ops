package venueoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func declaredGolden(t *testing.T, constants ...string) *Golden {
	t.Helper()
	if err := declaredConstantsErr(constants); err != nil {
		t.Fatal(err)
	}
	return &Golden{spec: GoldenSpec{Path: "g.json", PythonBuild: goldenBuild, CredentialConstants: constants}}
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
	g := declaredGolden(t, declaredA, declaredB)
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
	packed, err := g.project("r", "body", packBody([]byte("x "+declaredB)))
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

// The golden's header holds the full digest of each declared constant (under a name no scanner reads as a key)
// and a frozen run refuses a test that declares other, more or fewer constants.
func TestTheHeaderPinsTheDeclaredConstantsAndAFrozenRunRefusesOthers(t *testing.T) {
	sumA, sumB := sha256.Sum256([]byte(declaredA)), sha256.Sum256([]byte(declaredB))
	want := []string{hex.EncodeToString(sumA[:]), hex.EncodeToString(sumB[:])}
	sort.Strings(want)
	got := declaredDigests([]string{declaredB, declaredA})
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] || len(got[0]) != 64 {
		t.Fatalf("the header digests are not the sorted full digests: %v", got)
	}
	if err := declaredHeaderErr("g.json", want, []string{declaredB, declaredA}); err != nil {
		t.Fatalf("the same constants are refused: %v", err)
	}
	for name, constants := range map[string][]string{
		"another constant": {declaredA, "synthetic-credential-ZZZZ-0000"},
		"one fewer":        {declaredA},
		"one more":         {declaredA, declaredB, "synthetic-credential-ZZZZ-0000"},
		"none":             nil,
	} {
		err := declaredHeaderErr("g.json", want, constants)
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
		if _, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, CredentialConstants: constants}, "T"); err == nil || !strings.Contains(err.Error(), "CredentialConstants") {
			t.Errorf("%s: the declaration was accepted: %v", name, err)
		}
	}
}

// A FROZEN golden opens only for the constants it was recorded with: the header's digests are checked by
// openGolden itself, not only by the helper.
func TestAFrozenGoldenOpensOnlyForItsDeclaredConstants(t *testing.T) {
	request := ProgramRequest("corpus", []byte("abc"))
	entry := requestKey(request)
	entry.Body = "ABC\n"
	header := goldenHeader{Test: "TestAFrozenGoldenOpensOnlyForItsDeclaredConstants", PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64),
		DeclaredConstants: declaredDigests([]string{declaredA})}
	path, digest := writeGoldenFile(t, t.TempDir(), goldenFile{Header: header, Requests: []goldenRequest{entry}})
	open := func(constants ...string) error {
		_, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, CredentialConstants: constants}, "TestAFrozenGoldenOpensOnlyForItsDeclaredConstants")
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
	g := declaredGolden(t, declaredA, longer)
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
	g := declaredGolden(t, jwt)
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
	got := declaredDigests([]string{first, second})
	if len(got) != 2 || got[0] >= got[1] {
		t.Fatalf("the header digests are not in ascending order: %v", got)
	}
	if err := declaredHeaderErr("g.json", got, []string{second, first}); err != nil {
		t.Fatalf("the same constants in the other order are refused: %v", err)
	}
}
