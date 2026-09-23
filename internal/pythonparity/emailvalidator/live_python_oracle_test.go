package emailvalidator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// oracleProgram runs pydantic's own validate_email (the function EmailStr
// calls) over every input. Inputs and outputs travel as code point lists,
// so a lone surrogate survives the trip.
const oracleProgram = `
import json, sys
from pydantic_core import PydanticCustomError
from pydantic.networks import validate_email

out = []
for cps in json.load(sys.stdin):
    cps = cps or []
    value = "".join(map(chr, cps))
    try:
        _, email = validate_email(value)
        out.append({"ok": True, "email": [ord(c) for c in email]})
    except PydanticCustomError as exc:
        if exc.type != "value_error" or exc.message_template != "value is not a valid email address: {reason}":
            raise
        out.append({"ok": False, "reason": exc.context["reason"]})
json.dump(out, sys.stdout)
`

type verdict struct {
	OK     bool   `json:"ok"`
	Email  []rune `json:"email"`
	Reason string `json:"reason"`
}

func handPicked() []string {
	long := func(char string, n int) string { return strings.Repeat(char, n) }
	cases := []string{
		"", " ", "@", "a@", "@b.com", "a@b", "a@b.com", "A@B.COM", "Admin@Example.COM", "info@x.com", "INFO@x.com",
		"Support@x.com", " a@b.com ", "\ta@b.com\n", "a@b.com\n", "a @b.com", "a@ b.com", "a@b .com",
		"a.b@c.com", ".a@c.com", "a.@c.com", "a..b@c.com", "a@.c.com", "a@c.com.", "a@c..com", "a@-c.com", "a@c-.com",
		"a@c.-com", "a@c-.-com", "a@ab--c.com", "a@xn--c.com", "a@XN--nxasmq6b.com", "a@xn--nxasmq6b.com", "a@xn--.com",
		"a@xn--a-.com", "a@xn--zzzz.com", "a@xn--99999999999.com", "a@c.123", "a@c.co1", "a@localhost", "a@x.localhost",
		"a@x.test", "a@x.invalid", "a@x.onion", "a@x.arpa", "a@x.local", "a@test", "\"a\"@b.com", "\"a b\"@b.com",
		"\"a\\\"b\"@b.com", "\"a\"x@b.com", "\"\"@b.com", "\"a@b.com", "a\"b@c.com", "a\\b@c.com", "a<b@c.com",
		"Name <a@b.com>", "Name<a@b.com>", "\"Na me\" <a@b.com>", "Na.me <a@b.com>", "<a@b.com>", "< a@b.com >",
		"Name <a@b.com> x", "Name <a@b.com", "Name a@b.com>", "Name <a@b.com>>", "Name <<a@b.com>", "N\u00e9 <a@b.com>",
		"a@[1.2.3.4]", "a@[1.2.3]", "a@[1.2.3.256]", "a@[01.2.3.4]", "a@[1..3.4]", "a@[1.2.3.4\n]", "a@[1234.2.3.4]",
		"a@[IPv6:::1]", "a@[IPv6:1::2::3]", "a@[IPv6:1:2:3:4:5:6:7:8:9]", "a@[IPv6:1:2]", "a@[IPv6:g::1]", "a@[IPv6:12345::1]",
		"a@[IPv6::1:2:3:4:5:6:7]", "a@[IPv6:1:2:3:4:5:6:7:]", "a@[IPv6:1:2:3:4:5:6:7]", "a@[IPv6:::1.2.3.4]", "a@[IPv6:::1.2.3]",
		"a@[IPv6:::1/64]", "a@[IPv6:::1%eth0]", "a@[IPv6:::1%]", "a@[IPv6:" + long("1", 50) + "]", "a@[IPv6:" + long("1", 120) + "]",
		"a@[x:y]", "a@[x y:z]", "a@[\u00ff:z]", "a@[abc]", "a@[]", "a@[", "a@]",
		"\u00fc@b\u00fccher.de", "a@b\u00fccher.de", "a@B\u00dcCHER.DE", "a@\u0645\u062b\u0627\u0644.\u0625\u062e\u062a\u0628\u0627\u0631",
		"a@\u0661\u06f1.com", "a@l\u00b7l.com", "a@\u00b7l.com", "a@\u0375\u03b1.com", "a@\u05d0\u05f3.com", "a@\u30fb.com",
		"a@\u30fb\u30a2.com", "a@x\u200cy.com", "a@\u0915\u094d\u200c.com", "a@x\u200dy.com", "a@\u0915\u094d\u200d.com",
		"a@\u0301x.com", "\u0301a@b.com", "a\u0301@b.com", "e\u0301@b.com", "a@e\u0301.com", "a@\u3002.com", "a@x\u3002com",
		"a@x\uff0ecom", "a@x\uff61com", "a@\uff21.com", "\uff21@b.com", "a\uff20b.com", "a\ufe6bb.com", "a@b\u00ad.com",
		"a@b\u200b.com", "a@b\u2024.com", "a@\u2488.com", "a@b\u0080.com", "a@b\ufeff.com", "a\u00a0b@c.com", "\"a\u00a0b\"@c.com",
		"a\u2028@b.com", "a\u0000@b.com", "a@b\u0000.com", "a\U0001f600@b.com", "a@b\U0001f600.com", "a@b\U000e0001.com",
		"\u05d0@b.com", "a@\u05d0\u0031.com", "a@\u05d0a.com", "a@1\u05d0.com", "a@\u0627\u0661\u06f1.com",
		long("a", 64) + "@b.com", long("a", 65) + "@b.com", long("a", 244) + "@b.com", long("a", 245) + "@b.com",
		"a@" + long("b", 63) + ".com", "a@" + long("b", 64) + ".com", "a@" + long("b.", 126) + "com", "a@" + long("b.", 127) + "com",
		long("\u00fc", 130) + "@b.com", long("\U0001d4b6", 70) + "@b.com", "a@" + long("\u00fc", 60) + ".com",
		"a@" + long("\u00fc", 30) + "." + long("\u00fc", 30) + ".com", long("\u00fc", 60) + "@" + long("b", 60) + "." + long("c", 60) + ".com",
		long("a", 2048), long("a", 2049), long("a", 2040) + "@b.com", "a" + long("\u0301", 40) + "@b.com", "a@x" + long("\u0301", 40) + ".com",
		"a@b.c\u00f6m", "a@b.xn--c", "a@\u00df.com", "a@\u03c2.com", "a@\u0130.com", "a@K.com", "a@\u212a.com",
	}
	return cases
}

// surrogateCases holds lone surrogates, which a Go string literal cannot.
func surrogateCases() [][]rune {
	s := rune(0xd800)
	return [][]rune{
		{s, '@', 'b', '.', 'c', 'o', 'm'},
		{'a', '@', s, '.', 'c', 'o', 'm'},
		{'"', s, '"', '@', 'b', '.', 'c', 'o', 'm'},
		{'a', '@', 'b', '.', 'c', 'o', 'm', s},
		{'N', s, ' ', '<', 'a', '@', 'b', '.', 'c', 'o', 'm', '>'},
		{'a', 0xdc00, '@', 'b', '.', 'c', 'o', 'm'},
	}
}

func toRunes(texts []string) [][]rune {
	out := make([][]rune, len(texts))
	for i, text := range texts {
		out[i] = []rune(text)
	}
	return out
}

// sweeps puts every code point (surrogates included) into a local part, a
// domain label and a display name.
func sweeps() [][]rune {
	var out [][]rune
	for r := rune(0); r <= 0x10ffff; r++ {
		out = append(out, []rune{'a', r, '@', 'x', '.', 'c', 'o', 'm'})
		out = append(out, append([]rune("a@x"), append([]rune{r}, []rune("y.com")...)...))
		if r < 0x30000 {
			out = append(out, append([]rune{'N', r, ' ', '<'}, []rune("a@x.com>")...))
		}
	}
	return out
}

var fuzzAlphabet = []rune("aZ09.-_@<>\" \\[]:%/\t\n+!~'" +
	"\u00fc\u00df\u0301\u094d\u0915\u200c\u200d\u05d0\u0627\u0661\u06f1\u00b7\u30fb\u3002\uff0e\u00a0\u2028\ufeff\u00ad\uff20\U0001f600")

func fuzz(n int) [][]rune {
	random := rand.New(rand.NewSource(20260923))
	alphabet := append(fuzzAlphabet, 0xd800, 0xdfff)
	tokens := [][]rune{[]rune("xn--"), []rune(".com"), []rune("IPv6:"), []rune("a@b.com"), []rune("info"), []rune("localhost")}
	out := make([][]rune, n)
	for i := range out {
		var text []rune
		for length := random.Intn(24); len(text) < length; {
			if random.Intn(6) == 0 {
				text = append(text, tokens[random.Intn(len(tokens))]...)
			} else {
				text = append(text, alphabet[random.Intn(len(alphabet))])
			}
		}
		if random.Intn(2) == 0 && !containsRune(text, '@') {
			at := random.Intn(len(text) + 1)
			text = append(text[:at], append([]rune{'@'}, text[at:]...)...)
		}
		out[i] = text
	}
	return out
}

// TestValidateEmailMatchesLivePydantic compares ValidateEmail with
// pydantic's validate_email, verdict, normalized address and reason text,
// over hand-picked cases, every code point in three positions, and a
// seeded fuzz corpus.
func TestValidateEmailMatchesLivePydantic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" && os.Getenv("DEV_HEALTH_REGENERATE_TABLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := append(append(append(toRunes(handPicked()), surrogateCases()...), sweeps()...), fuzz(200000)...)

	payload, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", oracleProgram)
	command.Stdin = bytes.NewReader(payload)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr.Bytes()))
	}
	var want []verdict
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d of %d", len(want), len(corpus))
	}
	accepted, differences := 0, 0
	reasons := map[string]bool{}
	for index, input := range corpus {
		email, reason, ok := ValidateEmail(input)
		got := verdict{OK: ok, Email: email, Reason: reason}
		expected := want[index]
		if expected.OK {
			accepted++
		} else {
			reasons[reasonClass(expected.Reason)] = true
		}
		if got.OK != expected.OK || got.Reason != expected.Reason || !equalRunes(got.Email, expected.Email) {
			differences++
			if differences <= 25 {
				t.Errorf("%s:\n  go     %s\n  python %s", codepoints(input), describe(got), describe(expected))
			}
		}
	}
	t.Logf("%d inputs (%d accepted, %d reason classes), %d differences", len(corpus), accepted, len(reasons), differences)
	if differences > 0 {
		t.Fatalf("%d differences", differences)
	}
	checkGolden(t, selectGolden(corpus, want))
	if os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1" {
		return
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "pythonparity-emailvalidator"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// selectGolden keeps a small, deterministic slice of the live answers for
// golden_test.go: every hand-picked and surrogate case; for the code point
// sweeps, every ASCII probe and the first two probes of every (sweep,
// outcome class, category, bidirectional class) cell; the first 40 inputs
// of every refusal class; every 3000th other refusal and every 1500th
// accepted input.
func selectGolden(corpus [][]rune, want []verdict) []goldenCase {
	fixed := len(handPicked()) + len(surrogateCases())
	perClass := map[string]int{}
	perCell := map[string]int{}
	var out []goldenCase
	accepted, refused := 0, 0
	for index, input := range corpus {
		keep := index < fixed
		outcome := "ok"
		if !want[index].OK {
			outcome = reasonClass(want[index].Reason)
		}
		if kind, probe, ok := sweepProbe(input); ok {
			cell := kind + "|" + outcome + "|" + pyunicodedata.Category(probe) + "|" + pyunicodedata.Bidirectional(probe)
			perCell[cell]++
			keep = keep || probe < 0x80 || perCell[cell] <= 2
		}
		if want[index].OK {
			accepted++
			keep = keep || accepted%1500 == 1
		} else {
			refused++
			perClass[outcome]++
			keep = keep || perClass[outcome] <= 40 || refused%3000 == 1
		}
		if keep {
			out = append(out, goldenCase{Input: input, OK: want[index].OK, Email: want[index].Email, Reason: want[index].Reason})
		}
	}
	return out
}

// sweepProbe recognises the three sweep shapes and returns the code point
// under test.
func sweepProbe(input []rune) (string, rune, bool) {
	text := func(from, to int) string { return string(input[from:to]) }
	switch {
	case len(input) == 8 && input[0] == 'a' && text(2, 8) == "@x.com":
		return "local", input[1], true
	case len(input) == 9 && text(0, 3) == "a@x" && text(4, 9) == "y.com":
		return "domain", input[3], true
	case len(input) == 12 && input[0] == 'N' && text(2, 12) == " <a@x.com>":
		return "display", input[1], true
	}
	return "", 0, false
}

// checkGolden compares the committed golden with the live selection, or
// rewrites it under DEV_HEALTH_REGENERATE_TABLES=1.
func checkGolden(t *testing.T, cases []goldenCase) {
	t.Helper()
	var rendered []byte
	for _, c := range cases {
		line, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		rendered = append(append(rendered, line...), '\n')
	}
	if os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1" {
		if err := os.WriteFile(goldenPath, rendered, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(committed, rendered) {
		t.Fatalf("%s differs from the live answers; regenerate with DEV_HEALTH_REGENERATE_TABLES=1", goldenPath)
	}
}

// reasonClass is the reason with every quoted or parenthesised detail
// removed, to count how many distinct refusals the corpus reached.
func reasonClass(reason string) string {
	if index := strings.IndexAny(reason, "('\":"); index > 0 {
		return reason[:index]
	}
	return reason
}

func codepoints(text []rune) string {
	parts := make([]string, len(text))
	for i, r := range text {
		parts[i] = fmt.Sprintf("%04X", r)
	}
	return strings.Join(parts, " ")
}

func describe(v verdict) string {
	if v.OK {
		return "ok " + codepoints(v.Email)
	}
	return fmt.Sprintf("refused %q", v.Reason)
}
