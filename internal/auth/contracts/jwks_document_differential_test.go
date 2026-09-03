package contracts

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
)

// A DIFFERENTIAL TEST WITH THE REAL CONSUMER AS THE ORACLE.
//
// WHY THIS EXISTS, and it is not redundant with the golden corpus. Codex round
// 1 blocked this contract because the schema was stricter than its only
// consumer in four ways nobody had declared. The test built to catch exactly
// that -- every reject fixture run through the real consumer -- was GREEN the
// whole time, because not one of the twenty reject fixtures happened to
// exercise any of the four. The instrument was right and the corpus could not
// trip it, which is a green that reports coverage it does not have.
//
// A hand-written corpus can only contain the cases someone thought of. So this
// stops choosing cases: it generates documents across the axes where the two
// sides could differ and asserts their verdicts agree, EXCEPT where the
// disagreement is a declared narrowing. Borrowed from lane-auth-wave1, who
// built the same shape against PostgreSQL as the oracle for a SQL lexer after
// their corpus missed a case for the identical reason.
//
// THE ASYMMETRY IS DELIBERATE AND LOAD-BEARING:
//
//   - schema REFUSES while the consumer ACCEPTS is permitted ONLY when the
//     document matches a declared narrowing predicate below. An undeclared one
//     fails, which is precisely the round-1 defect.
//   - schema ACCEPTS while the consumer REFUSES is permitted ONLY when the
//     schema CANNOT express the rule -- the reject_by_client category, which
//     today is exactly the repeated kid. Anything else is the contract
//     certifying a document its only reader rejects.
//
// The second clause originally read "NEVER permitted", and the first run of
// this harness failed on 51 documents because of it: every one was a repeated
// kid, which is a documented client-enforced rule, not a defect. The harness
// caught an over-strong claim in its own assertion before it caught anything
// in the schema, which is the same class of error it was built to find.
type jwksDifferentialCase struct {
	label    string
	document map[string]any
}

// narrowingPredicates name, in code, every way this schema is deliberately
// stricter than the consumer. A disagreement matching none of them is news.
//
// These are the SAME facts as the narrower_than_consumer fixtures, expressed as
// predicates over a whole document rather than as four specific files. The
// fixtures pin the exact known instances with explanations; this names the
// classes so a NEW member of a known class does not fail spuriously while a
// genuinely new class does.
var narrowingPredicates = []struct {
	name  string
	holds func(map[string]any) bool
}{
	{"member name folds to a declared name without being it (Go's json matches field names by UNICODE SIMPLE FOLDING)",
		func(d map[string]any) bool {
			return anyMemberName(d, foldsToADeclaredName)
		}},
	{"a null value somewhere (decodes to Go's zero value)",
		func(d map[string]any) bool { return anyNullValue(d) }},
	{"kid is one DOCUMENTED by a narrower_than_consumer fixture",
		func(d map[string]any) bool {
			return anyKeyField(d, "kid", func(v any) bool {
				s, ok := v.(string)
				return ok && documentedNarrowingKids()[s]
			})
		}},
	{"use is the empty string (the consumer tolerates it alongside sig)",
		func(d map[string]any) bool {
			return anyKeyField(d, "use", func(v any) bool { s, ok := v.(string); return ok && s == "" })
		}},
}

// clientEnforcedPredicates name the rules the SCHEMA CANNOT EXPRESS, where the
// consumer legitimately refuses a document the schema validates. Kept separate
// from the narrowings because they run the opposite direction and have the
// opposite remedy: a narrowing is a choice, this is a limit of JSON Schema.
var clientEnforcedPredicates = []struct {
	name  string
	holds func(map[string]any) bool
}{
	{"a repeated kid (uniqueItems compares whole items, so it cannot see this)",
		func(d map[string]any) bool {
			seen := map[string]bool{}
			for name, value := range d {
				if !strings.EqualFold(name, "keys") {
					continue
				}
				list, ok := value.([]any)
				if !ok {
					continue
				}
				for _, entry := range list {
					object, ok := entry.(map[string]any)
					if !ok {
						continue
					}
					for k, v := range object {
						if !strings.EqualFold(k, "kid") {
							continue
						}
						kid, ok := v.(string)
						if !ok {
							continue
						}
						if seen[kid] {
							return true
						}
						seen[kid] = true
					}
				}
			}
			return false
		}},
}

// declaredMemberNames are every member this schema names. A wire member that
// FOLDS to one of these binds to the same Go struct field.
var declaredMemberNames = []string{"keys", "kty", "crv", "alg", "use", "kid", "x"}

// foldsToADeclaredName reports whether a member name reaches a declared field
// by Go's matching rules WITHOUT being that name byte-for-byte.
//
// THIS PREDICATE WAS WRONG UNTIL ROUND 3, and the way it was wrong is the point.
// It asked `name != strings.ToLower(name)`, which describes ASCII case variance.
// Go's decoder does not do ASCII case variance; it does UNICODE SIMPLE FOLDING.
// `keyſ` -- with U+017F LATIN SMALL LETTER LONG S, which folds with `s` --
// is ALREADY LOWERCASE, so the old predicate returned false while the consumer
// happily bound it to Keys. The exemption did not fire on a document it was
// meant to exempt.
//
// That did not surface for a second reason: the generator only emitted ASCII
// upper and title variants, so the shape was never produced. A predicate that
// was too narrow and a generator that was blind cancelled out, and either alone
// would have failed loudly. Both are fixed.
//
// strings.EqualFold is used because it is what the decoder's matching amounts
// to, and it was checked to be EXACTLY as wide rather than wider -- ASCII
// upper, ASCII title, U+017F long s and U+212A Kelvin sign all agree between
// EqualFold and the real consumer. A predicate broader than the narrowing it
// names is round 2's finding, and this is the place it would recur.
func foldsToADeclaredName(name string) bool {
	for _, declared := range declaredMemberNames {
		if name != declared && strings.EqualFold(name, declared) {
			return true
		}
	}
	return false
}

// documentedNarrowingKids is the set of `kid` values that a
// narrower_than_consumer FIXTURE demonstrates, read from the fixtures
// themselves.
//
// THE KID EXEMPTION IS ORACLE-DERIVED AND HAS NO ALPHABET IN IT. Ruled after
// round 4. It used to be a character class -- any character outside printable
// non-space ASCII -- and that excused a whole family on the strength of three
// fixtures: tabs, newlines, interior U+3000, CJK and combining marks were all
// waved through, every one of them a narrowing the consumer accepts and nothing
// documented. It also covered cases where BOTH sides refuse, where an exemption
// can never fire and so proves nothing about the ones that can.
//
// The rule is now the oracle's, stated once:
//
//	schema refuses AND consumer refuses  -> agreement, needs nothing
//	schema refuses AND consumer ACCEPTS  -> must be a documented kid, else FAIL
//
// Keyed to the fixtures rather than to a class, so a kid the consumer accepts
// and no fixture shows fails NAMING THE INPUT instead of being absorbed by a
// character range. A class predicate exempts what nobody has looked at; a
// fixture-keyed one exempts only what somebody wrote down.
func documentedNarrowingKids() map[string]bool {
	narrowingKidsOnce.Do(func() {
		narrowingKids = map[string]bool{}
		root, err := RepoRoot(mustGetwd())
		if err != nil {
			return // the caller's own manifest load will fail loudly first
		}
		dir := filepath.Join(root, jwksFixtureDir)
		raw, err := os.ReadFile(filepath.Join(root, jwksFixtureManifestPath))
		if err != nil {
			return
		}
		var manifest jwksFixtureManifest
		if json.Unmarshal(raw, &manifest) != nil {
			return
		}
		for _, entry := range manifest.NarrowerThanConsumer {
			body, err := os.ReadFile(filepath.Join(dir, entry.File))
			if err != nil {
				continue
			}
			var document map[string]any
			if json.Unmarshal(body, &document) != nil {
				continue
			}
			// ONLY KIDS THE SCHEMA REFUSES. A narrowing fixture about `use` or
			// about member-name folding still carries an ordinary kid, and the
			// first version collected those too -- so `example-signing-key`
			// became "documented" and this predicate matched almost every
			// document, shadowing the later predicates and exempting far more
			// than the class predicate it replaced. Worse than what it fixed,
			// and caught because the correspondence test went red.
			anyKeyField(document, "kid", func(v any) bool {
				s, ok := v.(string)
				if !ok || schemaAcceptsKid(s) {
					return false
				}
				narrowingKids[s] = true
				return false
			})
		}
	})
	return narrowingKids
}

var (
	narrowingKidsOnce sync.Once
	narrowingKids     map[string]bool
)

// schemaAcceptsKid mirrors the schema's kid pattern, ^[!-~]{1,256}$.
//
// Deliberately a re-statement rather than a read of the schema file: this
// decides which kids are NARROWINGS, and reading the pattern from the artefact
// it is meant to police would make the set move with the schema. If the pattern
// changes, TestTheKidPatternIsWhatThisMirrors fails and a human reconciles them.
func schemaAcceptsKid(kid string) bool {
	if len(kid) < 1 || len([]rune(kid)) > 256 {
		return false
	}
	for _, r := range kid {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return wd
}

func anyMemberName(node any, pred func(string) bool) bool {
	switch typed := node.(type) {
	case map[string]any:
		for k, v := range typed {
			if pred(k) || anyMemberName(v, pred) {
				return true
			}
		}
	case []any:
		for _, v := range typed {
			if anyMemberName(v, pred) {
				return true
			}
		}
	}
	return false
}

func anyNullValue(node any) bool {
	switch typed := node.(type) {
	case nil:
		return true
	case map[string]any:
		for _, v := range typed {
			if anyNullValue(v) {
				return true
			}
		}
	case []any:
		for _, v := range typed {
			if anyNullValue(v) {
				return true
			}
		}
	}
	return false
}

// anyKeyField tests a field of any key entry, matching the member name
// case-INSENSITIVELY because that is how the consumer reads it -- a predicate
// that only looked for lowercase "kid" would miss the very documents the
// casing narrowing is about.
func anyKeyField(document map[string]any, field string, pred func(any) bool) bool {
	for name, value := range document {
		if !strings.EqualFold(name, "keys") {
			continue
		}
		entries, ok := value.([]any)
		if !ok {
			continue
		}
		for _, entry := range entries {
			object, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			for k, v := range object {
				if strings.EqualFold(k, field) && pred(v) {
					return true
				}
			}
		}
	}
	return false
}

const goodX = "ExamplePublicKeyForContractFixturesOnlyAAAA"

// generateJWKSDocuments walks the axes on which the schema and the consumer
// could disagree. Seeded, so a failure is reproducible from its seed alone.
//
// The cross-product matters more than any single axis: lane-auth-wave1's
// equivalent harness went red on a COMPOUND (a keyword-adjacent literal AND a
// comment in the same statement) that neither of us would have hand-written.
// Every known narrowing here is single-axis, so the pair that bites next is
// likely a combination.
// foldCursor drives the round-robin. Package-level and reset per generation so
// a run is reproducible without a seed at all -- the fold axis is enumerated,
// so there is nothing left for a seed to decide about it.
var foldCursor int

// lowercaseFoldVariantsOf keeps only spellings that remain entirely lowercase.
//
// These are the ONLY spellings that distinguish a correct Unicode-fold
// predicate from an ASCII-case one, because a spelling containing an uppercase
// or Kelvin character is caught by both. Round 3's first fix missed this and
// its mutation stayed green.
func lowercaseFoldVariantsOf(name string) []string {
	var out []string
	for _, v := range foldVariantsOf(name) {
		if v == strings.ToLower(v) && v != name {
			out = append(out, v)
		}
	}
	return out
}

func generateJWKSDocuments(t *testing.T, count int) []jwksDifferentialCase {
	t.Helper()
	foldCursor = 0
	rng := rand.New(rand.NewSource(0x4930))

	// MUTATE A VALID BASELINE rather than sampling every field independently.
	//
	// The first version did the latter and generated 2000 documents of which
	// ZERO were schema-valid -- the non-vacuity guard below caught it on the
	// first run. Independent sampling puts almost all mass deep in invalid
	// space, where both sides refuse for the first reason they hit and no
	// disagreement can surface. Disagreements live at the BOUNDARY, so start
	// from a document both sides accept and perturb one to three things.
	baseline := func() map[string]any {
		return map[string]any{"keys": []any{map[string]any{
			"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA",
			"kid": "example-signing-key", "x": goodX,
		}}}
	}

	// Tolerant on purpose: mutations COMPOSE, so by the time a second one runs
	// the keys member may have been renamed to KEYS or emptied. The first
	// version indexed d["keys"].([]any)[0] directly and panicked on exactly
	// that combination -- which is the cross-product effect this harness exists
	// to find, arriving first in the harness itself.
	keysOf := func(d map[string]any) ([]any, string) {
		for name, value := range d {
			if !strings.EqualFold(name, "keys") {
				continue
			}
			if list, ok := value.([]any); ok {
				return list, name
			}
		}
		return nil, ""
	}
	keyOf := func(d map[string]any) map[string]any {
		list, _ := keysOf(d)
		if len(list) == 0 {
			return nil // nothing to perturb; the mutation becomes a no-op
		}
		object, _ := list[0].(map[string]any)
		return object
	}
	set := func(d map[string]any, field string, value any) {
		if k := keyOf(d); k != nil {
			k[field] = value
		}
	}
	// Declared before assignment because it recurses into nested key objects;
	// a closure cannot reference itself at its own definition.
	var rename func(map[string]any, func(string) string) map[string]any
	rename = func(d map[string]any, f func(string) string) map[string]any {
		out := map[string]any{}
		for k, v := range d {
			switch typed := v.(type) {
			case []any:
				list := make([]any, 0, len(typed))
				for _, e := range typed {
					if object, ok := e.(map[string]any); ok {
						list = append(list, rename(object, f))
						continue
					}
					list = append(list, e)
				}
				out[f(k)] = list
			default:
				out[f(k)] = v
			}
		}
		return out
	}

	// KID VALUES ARE LITERALS HERE, AND THAT IS THE WHOLE POINT.
	//
	// The narrowing kids below are written out rather than read from
	// documentedNarrowingKids(). Drawing them from the fixtures was the first
	// attempt and it was VACUOUS: the generator and the exemption then share
	// one source, so deleting a fixture deletes the case AND its exemption
	// together and the red-proof stays green. Caught by running that proof.
	//
	// With literals, deleting a fixture leaves the generator still emitting
	// that kid, the consumer still accepting it, the schema still refusing it,
	// and no documented match -- so the differential FAILS NAMING THE INPUT,
	// which is the behaviour the rule exists for.
	//
	// AGREEMENTS need no fixture: both sides refuse them, so no exemption can
	// fire. NARROWINGS must each have one.
	//
	// The trade, stated: this list is enumerated, not explored, so it verifies
	// the documented classes and polices the boundary without discovering new
	// ones. Discovery is CHAOS-4958's property sweep over Unicode categories.
	kids := []any{
		// agreements -- both sides refuse
		"", " ", "  ",
		string(rune(0x3000)),                      // ideographic space alone: trims to empty
		strings.Repeat("k", 257),                  // over the 256-byte bound
		strings.Repeat(string(rune(0x00E9)), 300), // 600 bytes: over the BYTE bound
		nil,
		// narrowings -- each has a narrower_than_consumer fixture, and these
		// literals must equal the kid in that fixture
		" example-signing-key ",             // surrounding whitespace
		"example" + string(rune(0)) + "key", // embedded NUL
		string(rune(0x00E9)),                // short non-ASCII
		"a\tb",                              // embedded tab
		"a\nb",                              // embedded newline
		"a" + string(rune(0x3000)) + "b",    // interior ideographic space
		string(rune(0x4E2D)),                // CJK
		"a" + string(rune(0x0301)),          // combining mark
	}
	xs := []any{goodX[:42], goodX + "A", goodX[:42] + "=", "+/" + goodX[2:], "", nil}
	mutations := []struct {
		name  string
		apply func(map[string]any) map[string]any
	}{
		{"kid", func(d map[string]any) map[string]any { set(d, "kid", kids[rng.Intn(len(kids))]); return d }},
		{"x", func(d map[string]any) map[string]any { set(d, "x", xs[rng.Intn(len(xs))]); return d }},
		{"kty", func(d map[string]any) map[string]any {
			set(d, "kty", []any{"okp", "RSA", "EC", "", nil}[rng.Intn(5)])
			return d
		}},
		{"crv", func(d map[string]any) map[string]any {
			set(d, "crv", []any{"ed25519", "X25519", "P-256", "", nil}[rng.Intn(5)])
			return d
		}},
		{"alg", func(d map[string]any) map[string]any {
			set(d, "alg", []any{"eddsa", "ES256", "", nil}[rng.Intn(4)])
			return d
		}},
		{"use", func(d map[string]any) map[string]any {
			set(d, "use", []any{"sig", "", "enc", "SIG", nil}[rng.Intn(5)])
			return d
		}},
		{"drop-a-field", func(d map[string]any) map[string]any {
			if k := keyOf(d); k != nil {
				fields := []string{"kty", "crv", "alg", "kid", "x"}
				delete(k, fields[rng.Intn(len(fields))])
			}
			return d
		}},
		{"extra-key-member", func(d map[string]any) map[string]any {
			set(d, "x5c", []any{"Zm9v"})
			return d
		}},
		{"extra-top-member", func(d map[string]any) map[string]any {
			d["issuer"] = "https://example.invalid"
			return d
		}},
		{"empty-keys", func(d map[string]any) map[string]any {
			if _, name := keysOf(d); name != "" {
				d[name] = []any{}
			}
			return d
		}},
		{"duplicate-kid", func(d map[string]any) map[string]any {
			first := keyOf(d)
			list, name := keysOf(d)
			if first == nil || name == "" {
				return d
			}
			second := map[string]any{}
			for k, v := range first {
				second[k] = v
			}
			d[name] = append(list, second)
			return d
		}},
		{"second-distinct-key", func(d map[string]any) map[string]any {
			list, name := keysOf(d)
			if name == "" {
				return d
			}
			second := map[string]any{"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA",
				"kid": "second-signing-key", "x": goodX}
			d[name] = append(list, second)
			return d
		}},
		{"upper-case-names", func(d map[string]any) map[string]any { return rename(d, strings.ToUpper) }},
		{"unicode-folded-names-single-substitution", func(d map[string]any) map[string]any {
			// DETERMINISTIC ROUND-ROBIN, not a random draw.
			//
			// Sampling a space smaller than the sample is a waste that costs
			// coverage: lane-auth-wave1 measured their own grammar at 3267
			// distinct statements sampled 3000 times WITH REPLACEMENT, covering
			// 60% per run with the missing 40% chosen by the seed. The fold
			// spellings here are a handful, so they are enumerated rather than
			// drawn, and each one is emitted a bounded-below number of times.
			//
			// This also fixes the thin-coverage limit reported after the
			// alphabet was derived: a random draw made lowercase-only spellings
			// rare enough that the differential distinguished the correct
			// predicate from the ASCII-case one by only 2 documents, and could
			// have decayed to 0 silently. Round-robin makes that count a
			// function of the corpus size rather than of luck.
			return rename(d, func(name string) string {
				variants := lowercaseFoldVariantsOf(name)
				if len(variants) == 0 {
					return name
				}
				v := variants[foldCursor%len(variants)]
				foldCursor++
				return v
			})
		}},
		{"unicode-folded-names-any-variant", func(d map[string]any) map[string]any {
			return rename(d, func(name string) string {
				variants := foldVariantsOf(name)
				if len(variants) == 0 {
					return name
				}
				v := variants[foldCursor%len(variants)]
				foldCursor++
				return v
			})
		}},
		{"title-case-names", func(d map[string]any) map[string]any {
			return rename(d, func(s string) string {
				if s == "" {
					return s
				}
				return strings.ToUpper(s[:1]) + s[1:]
			})
		}},
	}

	cases := make([]jwksDifferentialCase, 0, count)
	for i := 0; i < count; i++ {
		document := baseline()
		applied := []string{}
		// Zero mutations is deliberate and common: the untouched baseline is the
		// positive control, and it must keep appearing in the population.
		for n := rng.Intn(4); n > 0; n-- {
			m := mutations[rng.Intn(len(mutations))]
			document = m.apply(document)
			applied = append(applied, m.name)
		}
		label := "baseline"
		if len(applied) > 0 {
			label = strings.Join(applied, "+")
		}
		cases = append(cases, jwksDifferentialCase{
			label:    fmt.Sprintf("%04d-%s", i, label),
			document: document,
		})
	}
	return cases
}

// independentlyDerivedLowercaseFolds returns every spelling of a declared name
// that is ENTIRELY LOWERCASE and still folds to it, walking unicode.SimpleFold
// directly.
//
// Directly, and not via foldVariantsOf or lowercaseFoldVariantsOf, because this
// is the expectation the generator's coverage is measured against: an
// expectation computed from the code it checks cannot fail, and an earlier
// version of this floor was exactly that.
//
// The population is {keyſ, uſe} and cannot grow -- U+212A never contributes,
// being uppercase. See the note at the floor.
func independentlyDerivedLowercaseFolds() []string {
	var out []string
	for _, declared := range declaredMemberNames {
		runes := []rune(declared)
		for i, r := range runes {
			for _, f := range foldCycle(r) {
				candidate := make([]rune, len(runes))
				copy(candidate, runes)
				candidate[i] = f
				if v := string(candidate); v == strings.ToLower(v) && v != declared {
					out = append(out, v)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

func TestTheSchemaAndTheRealConsumerAgreeExceptOnDeclaredNarrowings(t *testing.T) {
	root := testRoot(t)
	dir := t.TempDir()
	cases := generateJWKSDocuments(t, 2000)

	// Derived BEFORE the loop because the coverage counter is gated on it, and
	// derived from unicode.SimpleFold rather than from the generator's helpers
	// so the gate cannot move with the code it measures.
	expectedSpellings := independentlyDerivedLowercaseFolds()
	if len(expectedSpellings) == 0 {
		t.Fatal("no lowercase fold spelling exists for any declared name; the fold axis has " +
			"collapsed and every per-spelling assertion below is vacuous")
	}
	expectedSet := map[string]bool{}
	for _, sp := range expectedSpellings {
		expectedSet[sp] = true
	}

	var (
		agree             int
		declaredNarrower  int
		clientEnforced    int
		undeclaredCount   int
		schemaLooserCount int
		byPredicate       = map[string]int{}
		byFoldSpelling    = map[string]int{}
		schemaAccepts     int
		consumerAccepts   int
		undeclared        []string
		schemaLooser      []string
	)

	for _, c := range cases {
		raw, err := json.Marshal(c.document)
		if err != nil {
			t.Fatalf("%s: marshalling the generated document: %v", c.label, err)
		}
		path := filepath.Join(dir, c.label+".json")
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatalf("%s: %v", c.label, err)
		}

		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("%s: generated document is not valid JSON: %v", c.label, err)
		}
		schemaRefuses := Validate(root, JWKSSurface, decoded) != nil
		_, consumerErr := authverify.NewEd25519JWKSVerifier(path).Keys()
		consumerRefuses := consumerErr != nil

		// Per-SPELLING coverage, so the fold axis cannot decay one spelling at
		// a time behind an aggregate that stays healthy.
		// GATED ON THE INDEPENDENTLY DERIVED SET, NOT ON THE PREDICATE.
		//
		// This counter measures GENERATION, and its floor's message blames the
		// generator. Gating it on foldsToADeclaredName -- the predicate under
		// test -- made that message a lie in the one case it matters: revert the
		// predicate and both spellings report "appeared 0 times ... stops being
		// generated", when they were generated and the predicate stopped
		// recognising them. Right failure, wrong component named, and a reader
		// chasing round 3's recurrence would open the generator instead of the
		// predicate.
		//
		// Found by lane-auth-wave1 applying this lane's own rule to this lane's
		// own counter. Nothing was masked -- the undeclared-narrowing assertion
		// fires too -- so this is diagnosis rather than coverage.
		anyMemberName(c.document, func(name string) bool {
			if expectedSet[name] {
				byFoldSpelling[name]++
			}
			return false
		})

		if !schemaRefuses {
			schemaAccepts++
		}
		if !consumerRefuses {
			consumerAccepts++
		}

		switch {
		case schemaRefuses == consumerRefuses:
			agree++
		case !schemaRefuses && consumerRefuses:
			// Permitted only where the schema cannot express the rule at all.
			explained := ""
			for _, p := range clientEnforcedPredicates {
				if p.holds(c.document) {
					explained = p.name
					break
				}
			}
			if explained == "" {
				schemaLooserCount++
				if len(schemaLooser) < 5 {
					schemaLooser = append(schemaLooser, fmt.Sprintf("%s: %s", c.label, raw))
				}
				continue
			}
			clientEnforced++
			byPredicate[explained]++
		default:
			// Schema stricter. Permitted only if a declared narrowing explains it.
			explained := ""
			for _, p := range narrowingPredicates {
				if p.holds(c.document) {
					explained = p.name
					break
				}
			}
			if explained == "" {
				undeclaredCount++
				if len(undeclared) < 5 {
					undeclared = append(undeclared, fmt.Sprintf("%s: %s", c.label, raw))
				}
				continue
			}
			declaredNarrower++
			byPredicate[explained]++
		}
	}

	// SKIPPED MUST BE ZERO -- every generated document lands in exactly one
	// bucket, and the buckets are asserted to sum to the population.
	//
	// Adopted from lane-auth-wave1's harness, where the equivalent counter is
	// what makes their pass meaningful: their first draft treated any
	// non-42601 error as a skip, so a dead container would have "passed" 3000
	// cases having compared none of them. Nothing here can skip by
	// construction -- a marshal or write failure is t.Fatalf, not a continue --
	// but "by construction" is exactly the kind of claim this lane has been
	// wrong about twice today, so it is counted rather than reasoned.
	accounted := agree + declaredNarrower + clientEnforced + undeclaredCount + schemaLooserCount
	if accounted != len(cases) {
		t.Fatalf("accounting does not close: %d documents generated, %d classified "+
			"(agree=%d narrowing=%d client-enforced=%d undeclared=%d schema-looser=%d). "+
			"A document that reaches no bucket is a silent skip, and a harness that can skip "+
			"can report a pass having compared nothing",
			len(cases), accounted, agree, declaredNarrower, clientEnforced,
			undeclaredCount, schemaLooserCount)
	}

	// NON-VACUITY, asserted rather than hoped for. This whole test exists
	// because a green was reported over a corpus that could not produce the
	// case. A generator that emitted only rejects, or only accepts, would be
	// the same failure in a new costume.
	if schemaAccepts == 0 {
		t.Fatal("the generator produced no schema-valid document; the comparison is one-sided " +
			"and would pass over a schema that refuses everything")
	}
	if consumerAccepts == 0 {
		t.Fatal("the generator produced no consumer-valid document; the oracle never said yes, " +
			"so agreement here means nothing")
	}
	if clientEnforced == 0 {
		t.Fatal("no generated document exercised the client-enforced rule (a repeated kid), so " +
			"this harness is not watching the schema-looser direction at all")
	}
	if declaredNarrower == 0 {
		t.Fatal("no generated document exercised ANY declared narrowing. The four known " +
			"asymmetries are then untested by this harness, which is exactly the hole round 1 " +
			"found -- a policing test whose corpus cannot trip it")
	}

	// EVERY PREDICATE MUST HAVE BEEN EXERCISED, not merely the aggregate.
	//
	// Round 2 caught the context file claiming this was already true when only
	// `declaredNarrower` and `clientEnforced` were asserted. Those are sums: a
	// generator regression that stopped producing NUL kids would leave the sum
	// non-zero on the strength of the other classes, and the NUL narrowing
	// would quietly stop being tested while everything stayed green. That is
	// the round-1 defect one level up -- an aggregate that cannot distinguish
	// "all classes covered" from "some classes covered".
	for _, p := range narrowingPredicates {
		if byPredicate[p.name] == 0 {
			t.Errorf("no generated document exercised the narrowing %q; it is exempted by a "+
				"predicate and asserted by nothing, so the schema could stop being narrower "+
				"there and this harness would not notice", p.name)
		}
	}
	for _, p := range clientEnforcedPredicates {
		if byPredicate[p.name] == 0 {
			t.Errorf("no generated document exercised the client-enforced rule %q", p.name)
		}
	}

	// EVERY LOWERCASE FOLD SPELLING MUST APPEAR, and appear more than once.
	//
	// The aggregate narrowing count cannot see a single spelling disappearing:
	// with round-robin selection the others absorb it and the total barely
	// moves. This is the same aggregate-versus-per-class distinction that
	// produced round 2's finding, applied one level down -- there it was per
	// predicate, here it is per spelling within one predicate.
	//
	// The floor is set from the measured distribution rather than chosen: with
	// deterministic round-robin every spelling appears a similar number of
	// times, so a spelling falling far below its peers means the enumeration
	// stopped being exhaustive.
	// THE DISCRIMINATING POPULATION IS EXACTLY {keyſ, uſe} AND CANNOT GROW.
	// U+212A KELVIN SIGN never contributes one, because a spelling carrying it
	// is not lowercase and is therefore matched by the ASCII-case predicate too.
	// That is a permanent property of the fold relation, not a coverage gap, and
	// the two are easy to confuse: a reader seeing "floor over the discriminating
	// subset" will assume it ranges over the fold alphabet, and it cannot.
	// THE EXPECTATION IS DERIVED INDEPENDENTLY OF THE GENERATOR'S HELPER.
	//
	// The first version built this list by calling lowercaseFoldVariantsOf --
	// the very function the generator uses. A mutation that made that helper
	// stop producing a spelling ALSO removed it from the expectation, so the
	// assertion moved with the thing it was policing and the mutation stayed
	// green. An expectation computed from the code under test cannot fail.
	//
	// This walks unicode.SimpleFold directly, the lowest-level primitive, so a
	// break anywhere in foldVariantsOf or lowercaseFoldVariantsOf leaves this
	// list intact and the floor fires.
	const perSpellingFloor = 5
	for _, spelling := range expectedSpellings {
		if byFoldSpelling[spelling] < perSpellingFloor {
			t.Errorf("fold spelling %q appeared %d times, want at least %d. A spelling that stops "+
				"being generated takes its coverage with it while the aggregate narrowing count "+
				"barely moves",
				spelling, byFoldSpelling[spelling], perSpellingFloor)
		}
	}
	spellings := make([]string, 0, len(byFoldSpelling))
	for k := range byFoldSpelling {
		spellings = append(spellings, k)
	}
	sort.Strings(spellings)
	for _, sp := range spellings {
		t.Logf("  fold spelling %-10q emitted %4d times", sp, byFoldSpelling[sp])
	}

	names := make([]string, 0, len(byPredicate))
	for k := range byPredicate {
		names = append(names, k)
	}
	sort.Strings(names)
	t.Logf("%d documents, %d classified, skipped 0: %d agree, %d declared narrowings, %d client-enforced",
		len(cases), accounted, agree, declaredNarrower, clientEnforced)
	t.Logf("  schema accepted %d, consumer accepted %d", schemaAccepts, consumerAccepts)
	for _, n := range names {
		t.Logf("  narrowing exercised %4d times: %s", byPredicate[n], n)
	}

	if len(schemaLooser) > 0 {
		t.Errorf("THE SCHEMA ACCEPTED %d DOCUMENT(S) THE REAL CONSUMER REFUSES. The contract is "+
			"certifying documents its only reader rejects:\n  %s",
			schemaLooserCount, strings.Join(schemaLooser, "\n  "))
	}
	if len(undeclared) > 0 {
		t.Errorf("THE SCHEMA REFUSED %d DOCUMENT(S) THE CONSUMER ACCEPTS, matching no declared "+
			"narrowing. Either the schema is wrong, or this is a real narrowing that needs a "+
			"narrower_than_consumer fixture and a predicate -- round 1 blocked this contract for "+
			"exactly four of these:\n  %s",
			undeclaredCount, strings.Join(undeclared, "\n  "))
	}
}

func TestEveryNarrowingPredicateHasAFixtureAndViceVersa(t *testing.T) {
	// THE STRUCTURAL FIX FOR THE ROUND-2 BLOCK, which was the round-1 defect
	// committed inside its own fix.
	//
	// A narrowing needs BOTH a predicate and a fixture, and they fail in
	// opposite ways when one is missing:
	//
	//   * a predicate with no fixture is the worse half, and it is what round 2
	//     found. The predicate EXCUSES the disagreement in the differential, so
	//     the harness stays green over an asymmetry that nothing asserts. Prose
	//     alone merely fails to test a narrowing; a lone predicate actively
	//     hides it.
	//   * a fixture with no predicate makes the differential fail the moment the
	//     generator produces that shape, reporting a real narrowing as an
	//     undeclared one.
	//
	// Both are now impossible to introduce without this test going red, which
	// is the only reason to trust the category is complete. Remembering to add
	// both is what failed twice.
	manifest := loadJWKSManifest(t)
	if len(manifest.NarrowerThanConsumer) == 0 {
		t.Fatal("no narrowing fixtures; this test would pass vacuously")
	}

	matched := map[string]bool{}
	for _, entry := range manifest.NarrowerThanConsumer {
		document, ok := decodeJWKSFixture(t, entry.File).(map[string]any)
		if !ok {
			t.Errorf("%s: not a JSON object", entry.File)
			continue
		}
		hit := ""
		for _, p := range narrowingPredicates {
			if p.holds(document) {
				hit = p.name
				break
			}
		}
		if hit == "" {
			t.Errorf("%s is filed as a narrowing but matches NO predicate, so the differential "+
				"harness would report it as an UNDECLARED narrowing the moment the generator "+
				"produced its shape", entry.File)
			continue
		}
		matched[hit] = true
	}

	for _, p := range narrowingPredicates {
		if !matched[p.name] {
			t.Errorf("the predicate %q exempts a disagreement in the differential harness and no "+
				"narrower_than_consumer fixture demonstrates it. A predicate without a fixture "+
				"EXCUSES an asymmetry that nothing asserts -- this is exactly what round 2 "+
				"blocked, for `use: \"\"`", p.name)
		}
	}
}

func TestTheKidPatternIsWhatThisMirrors(t *testing.T) {
	// THE TEST THIS FILE'S COMMENT PROMISED AND DID NOT HAVE.
	//
	// schemaAcceptsKid restates the schema's kid pattern in Go, deliberately:
	// it decides which kids count as NARROWINGS, and reading the pattern from
	// the artefact it polices would make the narrowing set move with the
	// schema. The cost of that choice is a second copy of a rule, and this
	// contract exists because two copies of a rule drift.
	//
	// The comment above schemaAcceptsKid claimed a test named exactly this
	// caught such a drift. THERE WAS NO SUCH TEST -- the name appeared only in
	// the comment. Executed at the time: changing `r > 0x7e` to `r > 0x7f` left
	// the Go suite and the Python suite both green. A mitigation named in prose
	// and absent from the tree is worse than an acknowledged gap, because it
	// reads as handled.
	//
	// lane-auth-wave1's #2166 round found the same shape in their code the same
	// hour -- an index test that recreated a hand-copied definition, so a broken
	// migration with the same name would still pass. A test that measures its
	// own copy rather than the artefact.
	//
	// This compares the two by BEHAVIOUR over probe strings, not by comparing
	// pattern text, because text equality would pass for two patterns that
	// differ only in something the text does not capture.
	root := testRoot(t)
	schema, err := Validator(root, JWKSSurface)
	if err != nil {
		t.Fatalf("loading the schema: %v", err)
	}
	_ = schema

	raw, err := os.ReadFile(filepath.Join(root,
		"contracts/auth/v1/jsonschema/jwks.v1.schema.json"))
	if err != nil {
		t.Fatalf("reading the schema file: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parsing the schema: %v", err)
	}
	pattern, ok := document["properties"].(map[string]any)["keys"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)["kid"].(map[string]any)["pattern"].(string)
	if !ok || pattern == "" {
		t.Fatal("no kid pattern in the schema; this test cannot compare against nothing")
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("the schema's kid pattern does not compile in RE2: %v", err)
	}

	probes := []string{
		"example-signing-key",
		"!",                // 0x21, the low boundary
		"~",                // 0x7e, the high boundary
		" ",                // 0x20, one below
		string(rune(0x7f)), // DEL, one above -- the mutation that went undetected
		string(rune(0)),    // NUL
		"\t", "\n",
		string(rune(0x00E9)),       // é
		string(rune(0x4E2D)),       // CJK
		string(rune(0x3000)),       // ideographic space
		"a" + string(rune(0x0301)), // combining mark
		"",                         // empty
		strings.Repeat("k", 256),   // at the bound
		strings.Repeat("k", 257),   // over it
	}
	if len(probes) == 0 {
		t.Fatal("no probes; this test would pass vacuously")
	}
	for _, probe := range probes {
		want := compiled.MatchString(probe)
		got := schemaAcceptsKid(probe)
		if want != got {
			t.Errorf("schemaAcceptsKid(%q) = %v but the SCHEMA's own pattern %q says %v. The Go "+
				"mirror has drifted from the artefact it mirrors, and the narrowing set is "+
				"decided by the mirror",
				probe, got, pattern, want)
		}
	}
}
