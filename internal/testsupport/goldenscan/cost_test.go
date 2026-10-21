package goldenscan

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The cost of the scan (CHAOS-10424: the tree check took 531 s under -race, 600 s timeouts under load): the expression is started at
// few positions however long a run of secret characters is, a keyword the rule cannot follow with a separator starts it nowhere, and
// the tree's goldens are read by several workers. Every speed-up is held equal to the expression over the whole text.

// A 1 MB run of secret characters after a keyword is not searched position by position (the synccli local_sync golden holds three).
func TestTheExpressionIsStartedAtFewPositionsBesideALongRun(t *testing.T) {
	text := `"git_blame": "author_email=fixture@example.com` + "\x1f" + `content=` + strings.Repeat("x", 1<<20) + `"`
	spans, starts := matchSpansCounting(text)
	if starts == 0 || starts > 1000 {
		t.Fatalf("the expression was started at %d positions over a %d-byte leaf (%d match(es)); want 1 to 1000 (its keyword is followed by a separator)", starts, len(text), len(spans))
	}
}

// A keyword that no gap, quote and separator can follow opens no window: the expression is started nowhere.
func TestAKeywordNoSeparatorFollowsStartsTheExpressionNowhere(t *testing.T) {
	run := strings.Repeat("abcdefghijklmnop", 4)
	for _, text := range []string{
		`"note": "the token of the secret key is stored elsewhere and the api is documented; ` + run + `"`,
		"token" + strings.Repeat("x", 21) + "=" + run,
		"token" + strings.Repeat("'", 4) + "=" + run,
		"token\v=" + run,
		"token\x1c=" + run,
		"token\x1f=" + run,
		"token;=" + run,
	} {
		if _, starts := matchSpansCounting(text); starts != 0 {
			t.Errorf("the expression was started at %d positions in %q, whose keyword no separator follows", starts, text)
		}
	}
}

// The windowed scan equals the expression over the whole text on a soup built at the edges of the rule's bounds: gaps of 19, 20
// and 21 bytes, quote runs of 2, 3 and 4, prefixes of 49, 50 and 51 bytes, long runs of secret characters, non-ASCII bytes (U+00A0,
// café, Arabic-Indic digits, the long s and the Kelvin sign that fold to ASCII), invalid UTF-8 bytes, and the ASCII controls U+001C to
// U+001F and the vertical tab (white space to Python, not to Go's \s, which the rule is written in).
func TestTheWindowedScanEqualsTheExpressionAtTheRuleBounds(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewSource(10424))
	pieces := []string{
		"token", "api_key", "API", "Api", "aPi", "password", "passwd", "secret", "creds", "credential", "access", "auth", "KEY",
		"=", ":", "::=", ":=", "||", "=>", "?=", ",", ">", `"`, "'", " ", "\t", "\n", "\\n", "\\r", ";", "`",
		strings.Repeat("a", 19), strings.Repeat("b.", 10), strings.Repeat("c-", 11), strings.Repeat(" ", 3), strings.Repeat("'", 4),
		strings.Repeat("p_", 25), strings.Repeat("q", 49), strings.Repeat("r", 51),
		generatedValue(10431)[:15], generatedValue(10432)[:20], strings.Repeat("k9", 120), strings.Repeat("x", 300), "=0=",
		"\u00a0", "café", "\u0663\u0664", "\u017fecret", "\u212aey", "\u023a",
		"\xff", "\xc3", "\x80", "token\xe2\x80", "\x1c", "\x1d", "\x1e", "\x1f", "\v",
	}
	for iter := 0; iter < 2500; iter++ {
		var b strings.Builder
		for n := r.Intn(16); n >= 0; n-- {
			b.WriteString(pieces[r.Intn(len(pieces))])
		}
		text := b.String()
		var want [][2]int
		// The scanner runs the rule only on a text whose lower-cased form holds one of its keywords (a long s is not folded
		// there), and only where twelve secret characters stand in a row; past that, the whole expression is the oracle.
		lowered, keyword := strings.ToLower(text), false
		for _, word := range keywords {
			keyword = keyword || strings.Contains(lowered, word)
		}
		if keyword && hasSegmentRun(text) {
			for _, loc := range generic.FindAllStringIndex(text, -1) {
				want = append(want, [2]int{loc[0], loc[1]})
			}
		}
		if got := matchSpans(text); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Fatalf("the windowed scan differs from the expression over the whole text, %q: %v vs %v", text, got, want)
		}
	}
}

// The windowed scan equals the expression over the whole text at each bound of the rule, byte class by byte class: a gap of 20 and
// 21 of each gap byte, a quote run of 3 and 4 of each quote byte (and of the bytes that are not \s to Go), each separator, and a
// prefix of 50 and 51 of each prefix byte.
func TestTheWindowedScanEqualsTheExpressionAtEachBoundByte(t *testing.T) {
	secret := generatedValue(10430)
	var texts []string
	for _, c := range []string{" ", "\t", "a", "Z", "0", "_", ".", "-"} {
		for _, n := range []int{19, 20, 21} {
			texts = append(texts, "token"+c+strings.Repeat("x", n-1)+"="+secret+"\n", "token"+strings.Repeat(c, n)+"="+secret+"\n")
		}
	}
	for _, c := range []string{" ", "\t", "\n", "\f", "\r", "'", `"`, "\v", "\x1c", "\x1d", "\x1e", "\x1f"} {
		for _, n := range []int{2, 3, 4} {
			q := strings.Repeat(c, n)
			texts = append(texts, "token"+q+"="+secret+"\n", "token"+strings.Repeat("x", 20)+q+"="+secret+"\n", "api_key"+"y"+q+":"+secret+" ")
		}
	}
	for _, sep := range []string{"=", ">", ":=", "::=", ":::=", "::::=", "||", ":", "=>", "?=", ",", "?", "|", ";"} {
		texts = append(texts, "secret"+sep+secret+"\n", "auth "+sep+" "+secret+`"`)
	}
	for _, c := range []string{"a", "Z", "0", "_", ".", "-"} {
		for _, n := range []int{49, 50, 51} {
			texts = append(texts, " "+strings.Repeat(c, n)+"token="+secret+"\n", strings.Repeat(c, n)+"Key: "+secret+"\n")
		}
	}
	for _, text := range texts {
		var want [][2]int
		for _, loc := range generic.FindAllStringIndex(text, -1) {
			want = append(want, [2]int{loc[0], loc[1]})
		}
		if got := matchSpans(text); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Errorf("the windowed scan differs from the expression over the whole text, %q: %v vs %v", text, got, want)
		}
	}
}

// A credential planted at a bound of the rule, or deep beside a 1 MB run, is still refused by the tree check.
func TestCheckTreeRefusesACredentialPlantedAtTheRuleBounds(t *testing.T) {
	run := strings.Repeat("x", 1<<20)
	planted := map[string]string{
		"a/gap20.json":     "token" + "abcdefghij0123456789" + "=" + generatedValue(10424),
		"a/quote3.json":    "secret" + " \t'" + ":" + generatedValue(10425),
		"a/prefix50.json":  strings.Repeat("p", 50) + "secret=" + generatedValue(10426),
		"a/deep.json":      run + " password=" + generatedValue(10427) + " " + run,
		"a/afterrun.json":  run + "api_key=" + generatedValue(10428),
		"a/separator.json": "auth ?= " + generatedValue(10429),
	}
	var paths []string
	for path := range planted {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	read := func(path string) ([]byte, error) { return golden(t, map[string]any{"note": planted[path]}), nil }
	problems, err := CheckTree(append([]string{"a/clean.json"}, paths...), func(path string) ([]byte, error) {
		if path == "a/clean.json" {
			return golden(t, map[string]any{"note": run}), nil
		}
		return read(path)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != len(paths) {
		t.Fatalf("%d problem(s), want one per planted golden (%d): %v", len(problems), len(paths), problems)
	}
	for i, path := range paths {
		if !strings.HasPrefix(problems[i], path+":") {
			t.Errorf("problem %d is %q, want the planted golden %s", i, problems[i], path)
		}
	}
}

// CheckTree reads the goldens with several workers: two reads are in flight at once.
func TestCheckTreeReadsGoldensConcurrently(t *testing.T) {
	var mu sync.Mutex
	inFlight, most := 0, 0
	both := make(chan struct{})
	var once sync.Once
	read := func(path string) ([]byte, error) {
		mu.Lock()
		inFlight++
		if inFlight > most {
			most = inFlight
		}
		if inFlight >= 2 {
			once.Do(func() { close(both) })
		}
		mu.Unlock()
		select {
		case <-both:
		case <-time.After(5 * time.Second):
		}
		mu.Lock()
		inFlight--
		mu.Unlock()
		return []byte(`{"name": "x"}`), nil
	}
	paths := []string{"a/1.json", "a/2.json", "a/3.json", "a/4.json"}
	problems, err := CheckTree(paths, read, nil)
	if err != nil || len(problems) != 0 {
		t.Fatalf("CheckTree: %v %v", err, problems)
	}
	if most < 2 {
		t.Fatalf("CheckTree read the goldens one at a time (at most %d read in flight)", most)
	}
}

// The problems CheckTree reports keep the order of the paths whatever order the workers finish in.
func TestCheckTreeKeepsThePathOrder(t *testing.T) {
	var paths []string
	read := func(path string) ([]byte, error) {
		var n int
		fmt.Sscanf(path, "a/%d.json", &n)
		time.Sleep(time.Duration(20-n) * time.Millisecond)
		return golden(t, map[string]any{"client_secret": someUUID}), nil
	}
	for i := 0; i < 20; i++ {
		paths = append(paths, fmt.Sprintf("a/%d.json", i))
	}
	problems, err := CheckTree(paths, read, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 20 {
		t.Fatalf("%d problem(s), want one per golden: %v", len(problems), problems)
	}
	for i, p := range problems {
		if !strings.HasPrefix(p, fmt.Sprintf("a/%d.json:", i)) {
			t.Fatalf("problem %d is %q, want the golden a/%d.json in path order", i, p, i)
		}
	}
}

// CheckTree returns the first read error in the order of the paths, whatever order the workers finish in.
func TestCheckTreeReturnsTheFirstErrorInPathOrder(t *testing.T) {
	var paths []string
	for i := 0; i < 20; i++ {
		paths = append(paths, fmt.Sprintf("a/%d.json", i))
	}
	read := func(path string) ([]byte, error) {
		var n int
		fmt.Sscanf(path, "a/%d.json", &n)
		time.Sleep(time.Duration(20-n) * time.Millisecond)
		if n == 3 || n == 17 {
			return nil, fmt.Errorf("unreadable %s", path)
		}
		return []byte(`{"name": "x"}`), nil
	}
	if _, err := CheckTree(paths, read, nil); err == nil || err.Error() != "unreadable a/3.json" {
		t.Fatalf("CheckTree returned %v, want the error of a/3.json, the first in path order", err)
	}
}
