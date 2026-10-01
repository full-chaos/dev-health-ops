package pyidna_test

import (
	"encoding/json"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyidna"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// behaviourProgram answers each call with the idna package: the result (code
// points or ASCII) or type(exc).__name__ and str(exc). Its 5.6 million
// answers are frozen as block digests, one canonical line per answer:
// kind|result|message, a text field, a code point list and a text field.
const behaviourProgram = programoracle.BlocksPython + `
import sys, idna

def run(fn, arg):
    try:
        value = fn(arg)
        if isinstance(value, bytes):
            return {"ok": [b for b in value]}
        return {"ok": [ord(c) for c in value]}
    except idna.IDNAError as exc:
        return {"kind": type(exc).__name__, "message": str(exc)}

def direct(call, label):
    pos = call.get("pos", 0)
    try:
        if call["fn"] == "contextj":
            return {"ok": [1 if idna.core.valid_contextj(label, pos) else 0]}
        if call["fn"] == "contexto":
            return {"ok": [1 if idna.core.valid_contexto(label, pos) else 0]}
        idna.core.check_bidi(label)
        return {"ok": [1]}
    except idna.IDNAError as exc:
        return {"kind": type(exc).__name__, "message": str(exc)}
    except ValueError as exc:
        return {"kind": "ValueError", "message": str(exc)}

def line(result):
    return text(result.get("kind", "")) + "|" + code_points(result.get("ok", [])) + "|" + text(result.get("message", ""))

lines = []
for call in json.load(sys.stdin):
    label = "".join(map(chr, call["text"] or []))
    if call["fn"] in ("contextj", "contexto", "bidi"):
        lines.append(line(direct(call, label)))
        continue
    if call["fn"] == "decode":
        label = label.encode("ascii")
    fn = {
        "remap": lambda s: idna.uts46_remap(s, std3_rules=False, transitional=False),
        "alabel": idna.alabel,
        "ulabel": idna.ulabel,
        "encode": idna.encode,
        "decode": idna.decode,
    }[call["fn"]]
    lines.append(line(run(fn, label)))
print(json.dumps(block_digests(lines)))
`

type behaviourCall struct {
	Fn   string `json:"fn"`
	Text []rune `json:"text"`
	// Pos is the position the contextj/contexto calls ask about.
	Pos int `json:"pos,omitempty"`
}

type behaviourResult struct {
	OK      []rune `json:"ok"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

var kindNames = map[pyidna.Kind]string{
	pyidna.KindIDNA: "IDNAError", pyidna.KindBidi: "IDNABidiError",
	pyidna.KindInvalidCodepoint: "InvalidCodepoint", pyidna.KindInvalidCodepointContext: "InvalidCodepointContext",
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
		"xn--a[b", "xn--mnchen-~ya", "xn--mnchen-{ya", "xn--mnchen-\u007fya", "xn--ab_c", "xn--zz{", "xn--a:b", "xn--@", "xn--ZZ[", "xn--mnchen-3ya.", "a.b.", "A.", "\u05d0-\u05d1", "\u05d01-2",
		"\u05d0\u0661\u0662", "\u05d01\u0661", "\u05d0\u0661\u0031", "\u0627\u06f1\u0661", "\u05d0\u05b0", "\u05d0,\u05d1", "\u05d0%\u05d1",
		"a-1", "a1-", "ab\u0301", "a\u00b7", "l\u00b7", "\u0375", "\u0375\u03b1", "\u03b1\u0375", "\u05f3", "\u05d0\u05f4",
		"\u30fb\u3042", "\u30fb\u4e00", "\u30fb\u30a2", "\u0669\u06f9", "\u0660", "\u06f0", "\u0669\u0661", "\u06f9\u06f1",
		"\u0915\u200d", "\u200d", "\u200c\u0628", "\u0628\u200c", "\u0628\u064e\u200c\u0628", "\u0628\u200c\u0627",
		"\ua872\u200c\u0628", "\u0628\u200c\u0628\u064e", "\u0915\u094d\u200c\u0915",
		// 254 characters without a trailing period (one over encode's
		// limit), and 253 with one (inside it).
		strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("a", 62),
		strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("a", 61) + ".",
		strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("a", 62) + ".",
	} {
		for _, fn := range []string{"remap", "alabel", "ulabel", "encode", "decode"} {
			add(fn, []rune(text))
		}
	}
	// check_bidi, valid_contextj and valid_contexto on their own, over every
	// code point below U+30000 in the positions their rules read.
	contexto := []rune{0x00b7, 0x0375, 0x05f3, 0x05f4, 0x30fb, 0x0661, 0x06f1}
	for r := rune(0); r < 0x30000; r++ {
		calls = append(calls,
			behaviourCall{Fn: "contexto", Text: []rune{r}},
			behaviourCall{Fn: "contextj", Text: []rune{r, 0x200c, 0x0628}, Pos: 1},
			behaviourCall{Fn: "contextj", Text: []rune{0x0628, 0x200c, r}, Pos: 1},
			behaviourCall{Fn: "contextj", Text: []rune{r, 0x200d}, Pos: 1},
			behaviourCall{Fn: "bidi", Text: []rune{0x05d0, r}},
			behaviourCall{Fn: "bidi", Text: []rune{r, 0x05d0}},
			behaviourCall{Fn: "bidi", Text: []rune{'a', r, 0x05d0}},
		)
		for _, c := range contexto {
			calls = append(calls, behaviourCall{Fn: "contexto", Text: []rune{r, c, r}, Pos: 1})
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
	case call.Fn == "contexto" && len(t) == 1:
		return "_", t[0], true
	case call.Fn == "contexto" && len(t) == 3:
		return "_" + string(t[1]) + "_", t[0], true
	case call.Fn == "contextj" && len(t) == 3 && t[0] == 0x0628:
		return "beh_zwnj_", t[2], true
	case call.Fn == "contextj" && len(t) == 3:
		return "_zwnj_beh", t[0], true
	case call.Fn == "contextj" && len(t) == 2:
		return "_zwj", t[0], true
	case call.Fn == "bidi" && len(t) == 2 && t[0] == 0x05d0:
		return "alef_", t[1], true
	case call.Fn == "bidi" && len(t) == 2:
		return "_alef", t[0], true
	case call.Fn == "bidi" && len(t) == 3:
		return "a_alef", t[1], true
	}
	return "", 0, false
}

func goBehaviour(call behaviourCall) behaviourResult {
	var out []rune
	var err *pyidna.Error
	switch call.Fn {
	case "remap":
		out, err = pyidna.UTS46Remap(call.Text)
	case "alabel":
		var encoded []byte
		encoded, err = pyidna.Alabel(call.Text)
		out = pyidna.AsciiToRunes(encoded)
	case "ulabel":
		out, err = pyidna.Ulabel(call.Text)
	case "encode":
		var encoded []byte
		encoded, err = pyidna.Encode(call.Text)
		out = pyidna.AsciiToRunes(encoded)
	case "decode":
		out, err = pyidna.Decode(call.Text)
	case "contextj":
		valid, ok := pyidna.ValidContextJ(call.Text, call.Pos)
		if !ok {
			// unicodedata.name raises this ValueError inside
			// _combining_class; check_label turns it into an IDNAError.
			return behaviourResult{Kind: "ValueError", Message: "no such name"}
		}
		return behaviourResult{OK: boolRunes(valid)}
	case "contexto":
		return behaviourResult{OK: boolRunes(pyidna.ValidContextO(call.Text, call.Pos))}
	case "bidi":
		err = pyidna.CheckBidi(call.Text)
		if err == nil {
			out = []rune{1}
		}
	}
	if err != nil {
		return behaviourResult{Kind: kindNames[err.Kind], Message: err.Message}
	}
	if out == nil {
		out = []rune{}
	}
	return behaviourResult{OK: out}
}

func boolRunes(value bool) []rune {
	if value {
		return []rune{1}
	}
	return []rune{0}
}

// TestBehaviourMatchesFrozenPython compares UTS46Remap, Alabel, Ulabel,
// Encode and Decode with the idna package, result or exception class and
// text, over every code point in several positions, hand-picked labels and a
// seeded fuzz corpus.
func TestBehaviourMatchesFrozenPython(t *testing.T) {
	regenerate := os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1"
	calls := behaviourCorpus()
	payload, err := json.Marshal(calls)
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "behaviour.golden.json", programoracle.Program{Name: "behaviour", Text: behaviourProgram, Stdin: payload})[0]
	// The answers here. The block digests prove each is the Python answer, so
	// the slice cut for the golden below is a slice of the Python answers.
	want := make([]behaviourResult, len(calls))
	line := func(dst []byte, index int) []byte {
		got := goBehaviour(calls[index])
		want[index] = got
		dst = append(programoracle.AppendText(dst, got.Kind), '|')
		dst = append(programoracle.AppendCodePoints(dst, got.OK), '|')
		return programoracle.AppendText(dst, got.Message)
	}
	programoracle.RequireBlocks(t, "idna behaviour", output, len(calls), line, func(index int) string {
		return fmt.Sprintf("%s(%U): go %+v", calls[index].Fn, calls[index].Text, goBehaviour(calls[index]))
	})
	// A port that accepts MIDDLE DOT between any two letters; the rule needs
	// an l on both sides.
	middleDot := slices.IndexFunc(calls, func(call behaviourCall) bool {
		return call.Fn == "contexto" && slices.Equal(call.Text, []rune{'a', 0x00b7, 'a'})
	})
	if middleDot < 0 {
		t.Fatal("the corpus holds no contexto call for a MIDDLE DOT between two a")
	}
	programoracle.RequireFindsDefect(t, "MIDDLE DOT accepted between two a", output, middleDot, line, func(dst []byte) []byte {
		return append(dst, "|1|"...)
	})
	t.Logf("%d calls compared in blocks, 0 differences", len(calls))
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
		if call.Fn == "contextj" || call.Fn == "contexto" || call.Fn == "bidi" {
			class += fmt.Sprint(expected.OK)
		}
		if expected.Kind != "" {
			class = call.Fn + "|" + expected.Kind + "|" + messageClass(expected.Message)
		}
		perClass[class]++
		keep := perClass[class] <= 30 || index%2000 == 0
		if shape, probe, ok := sweepProbe(call); ok {
			cell := class + "|" + shape + "|" + pyunicodedata.Category(probe) + "|" + pyunicodedata.Bidirectional(probe) + "|" + pyidna.JoiningType(probe)
			perCell[cell]++
			keep = keep || probe < 0x80 || perCell[cell] <= 2
		}
		if keep {
			golden = append(golden, behaviourGolden{Call: call, Want: expected})
		}
	}
	checkGoldenLines(t, golden, regenerate)
}
