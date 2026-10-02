package venueoracle

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

// CHAOS-7955: the scan of every golden in the repo ran the regexp engine over ~150 MB and took minutes under -race. Its
// parts are now byte scanners and literal prefilters; each is held EQUAL to the code it replaced, by a differential
// test over random strings drawn from the alphabets that matter and over hand-picked edge cases.

func randomText(r *rand.Rand, alphabet string, max int) string {
	n := r.Intn(max)
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(alphabet[r.Intn(len(alphabet))])
	}
	return b.String()
}

func TestJWTCandidatesEqualTheRegexp(t *testing.T) {
	r := rand.New(rand.NewSource(7955))
	alphabets := []string{"ab.", "ab1_-. ", "eyJ.a-_ \n", "aB0_-.+/=", "a."}
	for iter := 0; iter < 60000; iter++ {
		text := randomText(r, alphabets[iter%len(alphabets)], 60)
		want := jwtCandidate.FindAllString(text, -1)
		if got := jwtCandidates(text); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Fatalf("jwtCandidates(%q) = %q, the regexp finds %q", text, got, want)
		}
	}
	for _, text := range []string{"", "a", "ab", "ab.", "ab..", "ab.c.d", "ab.c.d.e.f.g.h", "a.b.c", "x ab.cd.ef y gh.ij.kl", "ab.cd.ef.gh.ij.kl.mn", "..a.b.c", "ab.cd..", "-_.-_.-_", "é.ab.cd.ef"} {
		if got, want := jwtCandidates(text), jwtCandidate.FindAllString(text, -1); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Fatalf("edge case %q: %q, the regexp finds %q", text, got, want)
		}
	}
}

func TestEncodedRunsEqualTheRegexpAndItsLimit(t *testing.T) {
	r := rand.New(rand.NewSource(7956))
	alphabets := []string{"aA0+/_-=", "ab=", "aA0+/_-= .", "a="}
	for iter := 0; iter < 60000; iter++ {
		text := randomText(r, alphabets[iter%len(alphabets)], 90)
		for _, limit := range []int{1, 2, 2000} {
			want := encodedRun.FindAllString(text, limit)
			if got := encodedRuns(text, limit); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
				t.Fatalf("encodedRuns(%q, %d) = %q, the regexp finds %q", text, limit, got, want)
			}
		}
	}
	run24 := strings.Repeat("a", 24)
	for _, text := range []string{"", strings.Repeat("a", 23), run24, run24 + "=", run24 + "==", run24 + "===", run24 + "=a", run24 + " " + run24 + "==", strings.Repeat("a+/_-", 8) + "=="} {
		if got, want := encodedRuns(text, 2000), encodedRun.FindAllString(text, 2000); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Fatalf("edge case %q: %q, the regexp finds %q", text, got, want)
		}
	}
}

// The literal prefilter and the regexp decide alike: a text that holds the shape is still reported, and a text that
// holds none of the literals is never reported (every prefiltered shape starts with, or requires, its literal).
func TestTheLiteralPrefilterNeverHidesAShape(t *testing.T) {
	samples := map[string][]string{
		"github-token":   {"x ghp_" + strings.Repeat("A", 36), "x gho_" + strings.Repeat("A", 36), "x ghu_" + strings.Repeat("A", 36), "x ghs_" + strings.Repeat("A", 36), "x ghr_" + strings.Repeat("A", 36)},
		"stripe-key":     {"key sk_live_" + strings.Repeat("a", 16), "key sk_test_" + strings.Repeat("a", 16), "key rk_live_" + strings.Repeat("a", 16), "key rk_test_" + strings.Repeat("a", 16)},
		"aws-access-key": {"id AKIA" + strings.Repeat("A", 16) + " end", "id ASIA" + strings.Repeat("A", 16) + " end"},
		"openai-key":     {"k sk-" + strings.Repeat("a", 32), "k sk-proj-" + strings.Repeat("a", 32)},
	}
	for name, texts := range samples {
		for _, text := range texts {
			ok := false
			for _, shape := range TokenShapesIn(text) {
				if shape == name {
					ok = true
				}
			}
			if !ok {
				t.Errorf("%s: the prefilter hid a shape the regexp finds in %q: %v", name, text, TokenShapesIn(text))
			}
		}
	}
	for _, shape := range tokenShapes {
		if len(shape.anyOf) == 0 {
			continue
		}
		if shape.holds("nothing of the shape in this text at all") {
			t.Errorf("%s holds a text with none of its literals", shape.name)
		}
	}
	// random differential: prefiltered holds == the bare regexp, over texts that mix the literals and the shapes' bytes
	r := rand.New(rand.NewSource(7957))
	for _, shape := range tokenShapes {
		if shape.re == nil || len(shape.anyOf) == 0 {
			continue
		}
		alphabet := "ghp_osukrAKIASsk-live_test0aZ \n"
		for iter := 0; iter < 20000; iter++ {
			text := randomText(r, alphabet, 70)
			if shape.holds(text) != shape.re.MatchString(text) {
				t.Fatalf("%s: prefiltered %v, regexp %v on %q", shape.name, shape.holds(text), shape.re.MatchString(text), text)
			}
		}
	}
}

// The case-insensitive scheme prefilter: Bearer and Basic in any case still report; text without either never does.
func TestTheAuthorizationPrefilterKeepsEveryCase(t *testing.T) {
	for _, text := range []string{"Authorization: Bearer abcdef0123456789", "AUTHORIZATION: BEARER abcdef0123456789", "x basic dXNlcjpwYXNz1", "bEaReR token1234567890abcdefghij"} {
		if !hasAuthorizationCredential(text) {
			t.Errorf("a credential was hidden by the prefilter: %q", text)
		}
	}
	if hasAuthorizationCredential("no scheme words here 0123456789abcdef") {
		t.Error("a text without a scheme word was reported")
	}
	r := rand.New(rand.NewSource(7958))
	for iter := 0; iter < 20000; iter++ {
		text := randomText(r, "bBeEaArRsSiIcC .x0123456789abcdef", 60)
		want := false
		for _, match := range authorizationScheme.FindAllStringSubmatch(text, -1) {
			if len(match[1]) >= 20 || strings.ContainsAny(match[1], "0123456789") {
				want = true
			}
		}
		if got := hasAuthorizationCredential(text); got != want {
			t.Fatalf("hasAuthorizationCredential(%q) = %v, the bare regexp says %v", text, got, want)
		}
	}
}
