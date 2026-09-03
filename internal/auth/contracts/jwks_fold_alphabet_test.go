package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"unicode"

	"github.com/full-chaos/dev-health-go/authverify"
)

// THE FOLD ALPHABET, DERIVED RATHER THAN CHOSEN.
//
// Round 3 blocked this contract on U+017F, and the first fix hand-picked U+017F
// and U+212A because they were the two the reviewer and I happened to name.
// That is the same mistake one level up: a corpus of the code points someone
// thought of. The set is now computed from unicode.SimpleFold, so it is
// whatever Go's own fold relation says it is, and it grows by itself if the
// Unicode tables change under a toolchain upgrade.
//
// It happens to come out as exactly {U+212A, U+017F} for the letters in the
// declared names today. That does NOT make the hand-picked version equivalent:
// it was right by luck, and nothing about it would have noticed a third.

// foldCycle returns every code point unicode.SimpleFold cycles through for r,
// excluding r itself.
func foldCycle(r rune) []rune {
	var out []rune
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// foldVariantsOf returns spellings of name reachable by replacing ONE letter
// with a fold-equivalent code point, plus the all-letters-folded spelling.
//
// Single substitution matters separately from the mixed form: round 3's fix was
// not load-bearing precisely because its generator only produced a MIXED
// variant, whose Kelvin K made it detectable by the wrong predicate. The shape
// that distinguishes a correct fold predicate from an ASCII-case one is a
// single non-ASCII substitution leaving the name otherwise lowercase.
func foldVariantsOf(name string) []string {
	runes := []rune(name)
	seen := map[string]bool{name: true}
	var out []string

	for i, r := range runes {
		for _, f := range foldCycle(r) {
			candidate := make([]rune, len(runes))
			copy(candidate, runes)
			candidate[i] = f
			if s := string(candidate); !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	// One mixed row: every letter taken to its first fold-equivalent.
	mixed := make([]rune, len(runes))
	for i, r := range runes {
		mixed[i] = r
		if fc := foldCycle(r); len(fc) > 0 {
			mixed[i] = fc[0]
		}
	}
	if s := string(mixed); !seen[s] {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// bindsInTheRealConsumer reports whether a document whose `keys` member is
// spelled `name` is READ AS A JWKS by the real consumer.
//
// This is the runtime, not a mirror of it. A local struct with the same tags
// would drift from dev-health-go silently, and a drifted mirror is exactly the
// artefact-substitution this package keeps re-learning. If the name binds, the
// document is complete and Keys() succeeds; if it does not, `keys` is absent
// (or the name is an unknown field under DisallowUnknownFields) and Keys()
// fails. Either way the answer is unambiguous.
func bindsInTheRealConsumer(t *testing.T, dir, name string) bool {
	t.Helper()
	document := fmt.Sprintf(
		`{%q:[{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"example-signing-key","x":%q}]}`,
		name, goodX)
	path := filepath.Join(dir, fmt.Sprintf("bind-%x.json", name))
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatalf("writing probe: %v", err)
	}
	_, err := authverify.NewEd25519JWKSVerifier(path).Keys()
	return err == nil
}

// theFoldPredicateAsTheHarnessUsesIt returns the fold predicate BY LOOKUP FROM
// narrowingPredicates, not by calling foldsToADeclaredName directly.
//
// The first version of this file called the function directly, and a mutation
// proof caught that it was vacuous: reverting the predicate ENTRY to the wrong
// ToLower form left this test green, because the test was never looking at the
// entry. It was verifying a function that happens to share a name with the
// thing the differential exempts on. That is this package's own recurring
// defect -- checking the artefact you have rather than the one the claim is
// about -- committed inside the test written to prevent it.
//
// Looking it up by name means the exemption the harness ACTUALLY applies is
// what gets compared against the runtime.
func theFoldPredicateAsTheHarnessUsesIt(t *testing.T) func(map[string]any) bool {
	t.Helper()
	const want = "member name folds to a declared name"
	for _, p := range narrowingPredicates {
		if strings.HasPrefix(p.name, want) {
			return p.holds
		}
	}
	t.Fatalf("no narrowing predicate whose name starts with %q; the fold exemption has been "+
		"renamed or removed and this test is no longer checking the harness", want)
	return nil
}

func TestTheFoldPredicateIsVerifiedAgainstTheRuntimeNotItsDescription(t *testing.T) {
	// THE CHECK ROUND 3 SHOWED WAS MISSING, and the reason it was missing is
	// worth more than the check.
	//
	// The predicate/fixture correspondence test added after round 2 passed
	// while the fold predicate was wrong, and it was RIGHT to pass: every
	// predicate did have a fixture. What no structural test can check is
	// whether a predicate says what it MEANS. `!= strings.ToLower(name)`
	// describes ASCII case variance and was standing in for Unicode folding,
	// and nothing but the runtime can tell you those differ.
	//
	// So: for every fold spelling of every declared name, ask the REAL consumer
	// whether it binds, and require the predicate to agree. A predicate that
	// mirrors a runtime behaviour is checked against that runtime.
	dir := t.TempDir()
	predicateHolds := theFoldPredicateAsTheHarnessUsesIt(t)

	var checked, folded int
	byClass := map[string]int{}

	for _, declared := range declaredMemberNames {
		for _, variant := range foldVariantsOf(declared) {
			checked++
			// Ask the predicate about a DOCUMENT, exactly as the differential
			// does, rather than about a bare string.
			predicate := predicateHolds(map[string]any{variant: []any{}})
			runtime := bindsInTheRealConsumer(t, dir, variant)

			// A variant that folds to `keys` should bind AND be exempted. One
			// that folds to some other declared name binds nothing here, so
			// only the `keys` family is a runtime comparison.
			if strings.EqualFold(declared, "keys") {
				if predicate != runtime {
					t.Errorf("PREDICATE DISAGREES WITH THE RUNTIME for %q: predicate says "+
						"narrowing=%v, the real consumer says binds=%v. The predicate is "+
						"describing something other than what the decoder does",
						variant, predicate, runtime)
				}
				folded++
			} else if !predicateHolds(map[string]any{"keys": []any{map[string]any{variant: "v"}}}) {
				t.Errorf("%q folds to the declared name %q and the predicate does not recognise it",
					variant, declared)
			}

			for _, r := range variant {
				if r > unicode.MaxASCII {
					byClass[fmt.Sprintf("U+%04X", r)]++
				}
			}
		}
	}

	if checked == 0 {
		t.Fatal("no fold variants generated; this test would pass vacuously")
	}
	if folded == 0 {
		t.Fatal("no `keys` fold variant reached the runtime comparison, so the predicate was " +
			"never actually checked against the decoder")
	}
	// Per code-point class, as ruled: a non-ASCII fold that stops being
	// generated must fail rather than quietly leave its class untested.
	if len(byClass) == 0 {
		t.Fatal("no NON-ASCII fold code point appeared in any variant; the alphabet has " +
			"collapsed to ASCII case and the class round 3 found is untested")
	}
	names := make([]string, 0, len(byClass))
	for k := range byClass {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		if byClass[n] == 0 {
			t.Errorf("fold code point %s generated zero variants", n)
		}
	}
	t.Logf("%d fold variants checked, %d compared against the runtime; non-ASCII classes: %v",
		checked, folded, func() []string {
			out := make([]string, 0, len(names))
			for _, n := range names {
				out = append(out, fmt.Sprintf("%s x%d", n, byClass[n]))
			}
			return out
		}())
}
