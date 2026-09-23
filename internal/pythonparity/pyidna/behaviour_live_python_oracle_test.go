package pyidna

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// behaviourProgram answers each call with the live idna package: the
// result (code points or ASCII) or type(exc).__name__ and str(exc).
const behaviourProgram = `
import json, sys, idna

def run(fn, arg):
    try:
        value = fn(arg)
        if isinstance(value, bytes):
            return {"ok": [b for b in value]}
        return {"ok": [ord(c) for c in value]}
    except idna.IDNAError as exc:
        return {"kind": type(exc).__name__, "message": str(exc)}

out = []
for call in json.load(sys.stdin):
    text = "".join(map(chr, call["text"] or []))
    if call["fn"] == "decode":
        text = text.encode("ascii")
    fn = {
        "remap": lambda s: idna.uts46_remap(s, std3_rules=False, transitional=False),
        "alabel": idna.alabel,
        "ulabel": idna.ulabel,
        "encode": idna.encode,
        "decode": idna.decode,
    }[call["fn"]]
    out.append(run(fn, text))
json.dump(out, sys.stdout)
`

type behaviourCall struct {
	Fn   string `json:"fn"`
	Text []rune `json:"text"`
}

type behaviourResult struct {
	OK      []rune `json:"ok"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

var kindNames = map[Kind]string{
	KindIDNA: "IDNAError", KindBidi: "IDNABidiError",
	KindInvalidCodepoint: "InvalidCodepoint", KindInvalidCodepointContext: "InvalidCodepointContext",
}

func behaviourCorpus() []behaviourCall {
	var calls []behaviourCall
	// Decode is only ever handed ASCII (email-validator passes
	// ascii_domain.encode("ascii")), so it is only asked about ASCII.
	add := func(fn string, text []rune) {
		if fn == "decode" {
			for _, r := range text {
				if r >= 0x80 {
					return
				}
			}
		}
		calls = append(calls, behaviourCall{Fn: fn, Text: text})
	}
	for r := rune(0); r <= 0x10ffff; r++ {
		add("remap", []rune{'x', r, 'y'})
		add("alabel", []rune{'x', r})
		if r < 0x30000 {
			add("alabel", []rune{r})
			add("alabel", []rune{0x05d0, r})
			add("alabel", []rune{0x0915, 0x094d, r})
		}
	}
	for _, text := range []string{
		"", ".", "..", "a.", "a..b", "xn--", "xn--a", "xn--a-", "xn--nxasmq6b", "XN--NXASMQ6B", "xn--zzzzzz", "xn--99999999999",
		"xn--mnchen-3ya", "xn--mnchen-3ya.de.", "a。b", "a．b｡c", "ab--c", "-ab", "ab-", "l·l", "·l",
		"͵α", "א׳", "・ア", "・", "١۱", "١٢", "x‌y", "क्‌",
		"ب‌ب", "́a", "é", "é", "אa", "א1", "1א", "ا١۱", "aא",
	} {
		for _, fn := range []string{"remap", "alabel", "ulabel", "encode", "decode"} {
			add(fn, []rune(text))
		}
	}
	random := rand.New(rand.NewSource(3180))
	pieces := []string{"xn--", "a", "-", ".", "0", "z", "9", "ü", "א", "́", "‌", "्", "क", "。", "·", "l"}
	for i := 0; i < 30000; i++ {
		var text []rune
		for n := random.Intn(12); n > 0; n-- {
			text = append(text, []rune(pieces[random.Intn(len(pieces))])...)
		}
		add([]string{"remap", "alabel", "ulabel", "encode", "decode"}[i%5], text)
	}
	return calls
}

// sweepProbe recognises behaviourCorpus's code point sweep shapes and
// returns the code point under test.
func sweepProbe(call behaviourCall) (string, rune, bool) {
	t := call.Text
	switch {
	case call.Fn == "remap" && len(t) == 3 && t[0] == 'x' && t[2] == 'y':
		return "x_y", t[1], true
	case call.Fn == "alabel" && len(t) == 2 && t[0] == 'x':
		return "x_", t[1], true
	case call.Fn == "alabel" && len(t) == 1:
		return "_", t[0], true
	case call.Fn == "alabel" && len(t) == 2 && t[0] == 0x05d0:
		return "alef_", t[1], true
	case call.Fn == "alabel" && len(t) == 3 && t[0] == 0x0915 && t[1] == 0x094d:
		return "virama_", t[2], true
	}
	return "", 0, false
}

func goBehaviour(call behaviourCall) behaviourResult {
	var out []rune
	var err *Error
	switch call.Fn {
	case "remap":
		out, err = UTS46Remap(call.Text, false, false)
	case "alabel":
		var encoded []byte
		encoded, err = Alabel(call.Text)
		out = asciiToRunes(encoded)
	case "ulabel":
		out, err = Ulabel(call.Text)
	case "encode":
		var encoded []byte
		encoded, err = Encode(call.Text)
		out = asciiToRunes(encoded)
	case "decode":
		out, err = Decode(call.Text)
	}
	if err != nil {
		return behaviourResult{Kind: kindNames[err.Kind], Message: err.Message}
	}
	if out == nil {
		out = []rune{}
	}
	return behaviourResult{OK: out}
}

// TestBehaviourMatchesLivePython compares UTS46Remap, Alabel, Ulabel,
// Encode and Decode with the live idna package, result or exception class
// and text, over every code point in several positions, hand-picked
// labels and a seeded fuzz corpus.
func TestBehaviourMatchesLivePython(t *testing.T) {
	regenerate := os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1"
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" && !regenerate {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	calls := behaviourCorpus()
	var want []behaviourResult
	if err := json.Unmarshal(runPython(t, root, behaviourProgram, calls), &want); err != nil {
		t.Fatal(err)
	}
	if len(want) != len(calls) {
		t.Fatalf("python answered %d of %d", len(want), len(calls))
	}
	differences := 0
	for index, call := range calls {
		got := goBehaviour(call)
		expected := want[index]
		if expected.OK == nil && expected.Kind == "" {
			expected.OK = []rune{}
		}
		if fmt.Sprint(got) != fmt.Sprint(expected) {
			differences++
			if differences <= 20 {
				t.Errorf("%s(%U):\n  go     %+v\n  python %+v", call.Fn, call.Text, got, expected)
			}
		}
	}
	t.Logf("%d calls, %d differences", len(calls), differences)
	if differences > 0 {
		t.Fatalf("%d differences", differences)
	}
	// The golden keeps, for the code point sweeps, every ASCII probe and the
	// first two probes of every (function, shape, outcome class, category,
	// bidirectional class, joining type) cell; the first 30 calls of every
	// (function, outcome class) pair; and every 2000th other call.
	perClass := map[string]int{}
	perCell := map[string]int{}
	var golden []behaviourGolden
	for index, call := range calls {
		expected := want[index]
		class := call.Fn + "|ok"
		if expected.Kind != "" {
			class = call.Fn + "|" + expected.Kind + "|" + messageClass(expected.Message)
		}
		perClass[class]++
		keep := perClass[class] <= 30 || index%2000 == 0
		if shape, probe, ok := sweepProbe(call); ok {
			cell := class + "|" + shape + "|" + pyunicodedata.Category(probe) + "|" + pyunicodedata.Bidirectional(probe) + "|" + joiningType(probe)
			perCell[cell]++
			keep = keep || probe < 0x80 || perCell[cell] <= 2
		}
		if keep {
			golden = append(golden, behaviourGolden{Call: call, Want: expected})
		}
	}
	checkGoldenLines(t, golden, regenerate)
	if regenerate {
		return
	}
	writeProof(t, "pythonparity-pyidna-behaviour")
}
