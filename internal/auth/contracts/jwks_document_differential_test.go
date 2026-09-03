package contracts

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	{"member name is not lowercase (Go's json matches field names case-insensitively)",
		func(d map[string]any) bool {
			return anyMemberName(d, func(k string) bool { return k != strings.ToLower(k) })
		}},
	{"a null value somewhere (decodes to Go's zero value)",
		func(d map[string]any) bool { return anyNullValue(d) }},
	{"kid carries a character outside printable non-space ASCII",
		func(d map[string]any) bool {
			return anyKeyField(d, "kid", func(v any) bool {
				s, ok := v.(string)
				if !ok {
					return false
				}
				for _, r := range s {
					if r < 0x21 || r > 0x7e {
						return true
					}
				}
				return false
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
func generateJWKSDocuments(t *testing.T, count int) []jwksDifferentialCase {
	t.Helper()
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

	kids := []any{
		"", " ", "  ", " a ", "a\t", "\na", "k" + string(rune(0)) + "k",
		strings.Repeat("k", 256), strings.Repeat("k", 257), strings.Repeat("é", 8),
		"sig key", "UPPER-KID", nil,
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

func TestTheSchemaAndTheRealConsumerAgreeExceptOnDeclaredNarrowings(t *testing.T) {
	root := testRoot(t)
	dir := t.TempDir()
	cases := generateJWKSDocuments(t, 2000)

	var (
		agree            int
		declaredNarrower int
		clientEnforced   int
		byPredicate      = map[string]int{}
		schemaAccepts    int
		consumerAccepts  int
		undeclared       []string
		schemaLooser     []string
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
				if len(undeclared) < 5 {
					undeclared = append(undeclared, fmt.Sprintf("%s: %s", c.label, raw))
				}
				continue
			}
			declaredNarrower++
			byPredicate[explained]++
		}
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

	names := make([]string, 0, len(byPredicate))
	for k := range byPredicate {
		names = append(names, k)
	}
	sort.Strings(names)
	t.Logf("%d documents: %d agree, %d declared narrowings, %d client-enforced",
		len(cases), agree, declaredNarrower, clientEnforced)
	t.Logf("  schema accepted %d, consumer accepted %d", schemaAccepts, consumerAccepts)
	for _, n := range names {
		t.Logf("  narrowing exercised %4d times: %s", byPredicate[n], n)
	}

	if len(schemaLooser) > 0 {
		t.Errorf("THE SCHEMA ACCEPTED %d+ DOCUMENT(S) THE REAL CONSUMER REFUSES. The contract is "+
			"certifying documents its only reader rejects:\n  %s",
			len(schemaLooser), strings.Join(schemaLooser, "\n  "))
	}
	if len(undeclared) > 0 {
		t.Errorf("THE SCHEMA REFUSED %d+ DOCUMENT(S) THE CONSUMER ACCEPTS, matching no declared "+
			"narrowing. Either the schema is wrong, or this is a real narrowing that needs a "+
			"narrower_than_consumer fixture and a predicate -- round 1 blocked this contract for "+
			"exactly four of these:\n  %s",
			len(undeclared), strings.Join(undeclared, "\n  "))
	}
}
