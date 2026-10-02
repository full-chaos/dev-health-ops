package pyjson_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// corpus is JSON text the api reads from stored JSON columns or bodies.
var corpus = []string{
	`{"b":1,"a":2,"b":3}`, `{"z":{"y":[1,2.0,-0.0,1e16,1e15,123456789.125,1.5e-05,0.0001,1e-4,1e-05]}}`,
	`[true,false,null,"",0,-0,00.5e1]`, `"tab\tnew\nline\u0001\u001f\u007f\u2028é\ud83d\ude00\\\"/"`,
	`123456789012345678901234567890`, `-1`, `1.0`, `1E400`, `-1e400`, `2.5E+3`, `[1e22,1e21,9007199254740993]`,
	`{"":{},"k":[]}`, `0.1`, `0.30000000000000004`, `5e-324`, `1.7976931348623157e308`,
	// Malformed: the error message and position must match json.loads.
	``, ` `, `{`, `}`, `[`, `]`, `{"a"`, `{"a":`, `{"a":1`, `{"a":1,`, `{"a":1,}`, `[1,]`, `[1 2]`, `{"a" 1}`,
	`{a:1}`, `{"a":1 "b":2}`, `"abc`, "\"a\tb\"", `"a\qb"`, `"\u12"`, `"\u12G4"`, `"\ud800"`, `"\ud83d\ude00x"`,
	`nul`, `tru`, `-`, `-a`, `01`, `1.`, `1.e5`, `1e`, `.5`, `+1`, `1 2`, `[1] x`, `NaN`, `-Infinity`, `[NaN,1]`,
	`{"a":[1,{"b":}]}`,
	// The int literal digit limit: a ValueError at 4301 digits, none for a float.
	strings.Repeat("1", 4300), strings.Repeat("1", 4301), "-" + strings.Repeat("1", 4300), "-" + strings.Repeat("1", 4301),
	"[" + strings.Repeat("1", 4301) + "]", `{"a":` + strings.Repeat("1", 4301) + `}`, `[1,[2,` + strings.Repeat("9", 4301) + `]]`,
	strings.Repeat("1", 4301) + ".0", strings.Repeat("1", 4301) + "e0", "1e" + strings.Repeat("1", 5000), "1." + strings.Repeat("1", 5000),
	"[" + strings.Repeat("1", 4301), "[" + strings.Repeat("1", 4301) + ",", `{"a":` + strings.Repeat("1", 4301),
	"[1,]" + strings.Repeat("1", 4301), "[," + strings.Repeat("1", 4301) + "]", `{"a":x,"b":` + strings.Repeat("1", 4301) + `}`,
	strings.Repeat("1", 4301) + " x", `"` + strings.Repeat("1", 4301) + `"`, "0", "-0", "-" + strings.Repeat("0", 3), `é`, `"é\u00e9"`, `[1,,2]`, `{,}`, `{"a":1,,}`, "\t[\n1\r]\n", `Infinityx`, `nullx`,
}

const pythonDumpsProgram = `
import json, sys
out = []
for text in json.loads(sys.stdin.read()):
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        out.append("JSONDecodeError:%s:%d" % (exc.msg, exc.pos))
        continue
    except ValueError as exc:
        # An integer literal past sys.get_int_max_str_digits(): a ValueError,
        # not a JSONDecodeError.
        out.append("IntLimit:%s" % exc)
        continue
    try:
        # Starlette's JSONResponse.render encodes the dump as UTF-8.
        out.append(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8").decode())
    except ValueError:
        out.append("ValueError")
print(json.dumps(out))
`

func TestMarshalMatchesFrozenPythonJSONDumps(t *testing.T) {
	input, _ := json.Marshal(corpus)
	output := frozenPython(t, "marshal.golden.json", programoracle.Program{Name: "json.dumps compact", Text: pythonDumpsProgram, Stdin: input})[0]
	var want []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &want); err != nil {
		t.Fatal(err)
	}
	for index, text := range corpus {
		gotText := "ValueError"
		value, err := pyjson.Decode([]byte(text))
		var syntax *pyjson.SyntaxError
		var limit *pyjson.IntLimitError
		switch {
		case errors.As(err, &syntax):
			gotText = fmt.Sprintf("JSONDecodeError:%s:%d", syntax.Msg, syntax.Pos)
		case errors.As(err, &limit):
			gotText = "IntLimit:" + limit.Error()
		case err != nil:
			gotText = "decode: " + err.Error()
		default:
			if got, err := pyjson.Marshal(value); err == nil {
				gotText = string(got)
			}
		}
		if gotText != want[index] {
			t.Errorf("%s:\n Go     %s\n Python %s", text, gotText, want[index])
		}
	}
}

const pythonDumpsDefaultProgram = `
import json, sys
out = []
for text in json.loads(sys.stdin.read()):
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        out.append("JSONDecodeError:%s:%d" % (exc.msg, exc.pos))
        continue
    except ValueError as exc:
        # An integer literal past sys.get_int_max_str_digits(): a ValueError,
        # not a JSONDecodeError.
        out.append("IntLimit:%s" % exc)
        continue
    try:
        # NO keyword arguments at all -- json.dumps' own bare defaults
        # (ensure_ascii=True, allow_nan=True, separators=(", ", ": ")),
        # the EXACT call every ClickHouse JSON-column writer
        # (storage/clickhouse.py's bare json.dumps(value)) actually makes,
        # distinct from Starlette's response encoding Marshal matches above.
        out.append(json.dumps(value))
    except ValueError:
        out.append("ValueError")
print(json.dumps(out))
`

// TestDumpsMatchesFrozenPythonJSONDumpsDefault pins Dumps -- the shared
// encoder every ClickHouse JSON-column writer uses (CHAOS-6310 r1 finding
// #10) -- against a frozen Python bare `json.dumps(value)` call (no keyword
// arguments at all), reusing the same corpus TestMarshalMatchesFrozenPythonJSONDumps
// proves the compact/ensure_ascii=False form against. Dumps had no live-
// oracle proof of its own before this; two mismatches from Marshal's own
// assumptions were caught building it: Python's bare default is
// ensure_ascii=TRUE (not False) and allow_nan=TRUE (not False).
func TestDumpsMatchesFrozenPythonJSONDumpsDefault(t *testing.T) {
	input, _ := json.Marshal(corpus)
	output := frozenPython(t, "dumps-default.golden.json", programoracle.Program{Name: "json.dumps bare defaults", Text: pythonDumpsDefaultProgram, Stdin: input})[0]
	var want []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &want); err != nil {
		t.Fatal(err)
	}
	for index, text := range corpus {
		gotText := "ValueError"
		value, err := pyjson.Decode([]byte(text))
		var syntax *pyjson.SyntaxError
		var limit *pyjson.IntLimitError
		switch {
		case errors.As(err, &syntax):
			gotText = fmt.Sprintf("JSONDecodeError:%s:%d", syntax.Msg, syntax.Pos)
		case errors.As(err, &limit):
			gotText = "IntLimit:" + limit.Error()
		case err != nil:
			gotText = "decode: " + err.Error()
		default:
			if got, err := pyjson.Dumps(value); err == nil {
				gotText = got
			}
		}
		if gotText != want[index] {
			t.Errorf("%s:\n Go     %s\n Python %s", text, gotText, want[index])
		}
	}
}

// bodyCorpus is request bodies as bytes (hex), for json.loads(bytes):
// encoding detection, BOMs, surrogatepass. Built from real encodings.
var bodyCorpus = func() []string {
	utf16le := func(text string, bom bool) []byte {
		var out []byte
		if bom {
			out = append(out, 0xff, 0xfe)
		}
		for _, unit := range utf16.Encode([]rune(text)) {
			out = append(out, byte(unit), byte(unit>>8))
		}
		return out
	}
	utf16be := func(text string, bom bool) []byte {
		var out []byte
		if bom {
			out = append(out, 0xfe, 0xff)
		}
		for _, unit := range utf16.Encode([]rune(text)) {
			out = append(out, byte(unit>>8), byte(unit))
		}
		return out
	}
	utf32le := func(text string, bom bool) []byte {
		var out []byte
		if bom {
			out = append(out, 0xff, 0xfe, 0, 0)
		}
		for _, r := range text {
			out = append(out, byte(r), byte(r>>8), byte(r>>16), byte(r>>24))
		}
		return out
	}
	doc := `{"a":"é😀"}`
	bodies := [][]byte{
		[]byte(doc),
		append([]byte{0xef, 0xbb, 0xbf}, doc...),
		utf16le(doc, true), utf16be(doc, true), utf16le(doc, false), utf16be(doc, false),
		utf32le(doc, true), utf32le(doc, false),
		append(utf16le(doc, true), 0x7d), // odd length
		[]byte("\"\xed\xa0\x80\""),       // encoded surrogate (surrogatepass)
		[]byte("\"\xff\""),               // invalid UTF-8
		utf16le("1", false), utf16be("1", false),
		{0xff, 0xfe, 0x22, 0x00, 0x00, 0xd8, 0x22, 0x00}, // UTF-16LE lone surrogate
		{0x7b, 0x00, 0x00, 0x00, 0x31},                   // looks UTF-32LE, truncated
		// Escaped surrogates in every order (corpus gap of the CHAOS-7531 vet): a LOW surrogate first stays a lone
		// surrogate, a high one is joined only to a low one that follows it.
		[]byte(`"\udc00\udc00"`), []byte(`"\ude00\ud83d\ude00"`), []byte(`"\ude00` + "\U0001F600" + `"`), []byte(`"\udc00\ud800"`),
		[]byte(`"\ud800\ud800\udc00"`), []byte(`"\udbff\udfff"`), []byte(`"\udbff\udbff"`), []byte(`"\ud800\udbff"`),
		[]byte(`"\udfff\udc00\ud800\udc00"`), []byte(`"\ud800\u0041"`), []byte(`"\udc00\u0041"`), []byte(`"a\udc00\udc00b"`),
	}
	out := make([]string, len(bodies))
	for index, body := range bodies {
		out[index] = hex.EncodeToString(body)
	}
	return out
}()

const pythonBodyProgram = `
import json, sys
out = []
for hexed in json.loads(sys.stdin.read()):
    raw = bytes.fromhex(hexed)
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        out.append("JSONDecodeError:%s:%d" % (exc.msg, exc.pos)); continue
    except Exception as exc:
        out.append("Error"); continue
    out.append(json.dumps(value, ensure_ascii=True, separators=(",", ":")))
print(json.dumps(out))
`

func TestDecodeBodyMatchesFrozenPythonJSONLoads(t *testing.T) {
	input, _ := json.Marshal(bodyCorpus)
	output := frozenPython(t, "decode-body.golden.json", programoracle.Program{Name: "json.loads of bytes", Text: pythonBodyProgram, Stdin: input})[0]
	var want []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &want); err != nil {
		t.Fatal(err)
	}
	for index, hexed := range bodyCorpus {
		raw, _ := hex.DecodeString(hexed)
		got := "Error"
		value, err := pyjson.Decode(raw)
		var syntax *pyjson.SyntaxError
		switch {
		case errors.As(err, &syntax):
			got = fmt.Sprintf("JSONDecodeError:%s:%d", syntax.Msg, syntax.Pos)
		case err == nil:
			got = asciiDump(value)
		}
		if got != want[index] {
			t.Errorf("%s:\n Go     %s\n Python %s", hexed, got, want[index])
		}
	}
}

// asciiDump is json.dumps(ensure_ascii=True, compact) for the oracle's
// values: every non-ASCII code point (surrogates included) escaped.
func asciiDump(value pyjson.Value) string {
	switch typed := value.(type) {
	case string:
		var builder strings.Builder
		builder.WriteByte('"')
		for _, r := range pyjson.Runes(typed) {
			switch {
			case r == '"' || r == '\\':
				builder.WriteByte('\\')
				builder.WriteRune(r)
			case r > 0xffff:
				high, low := utf16.EncodeRune(r)
				fmt.Fprintf(&builder, `\u%04x\u%04x`, high, low)
			case r < 0x20 || r >= 0x7f:
				fmt.Fprintf(&builder, `\u%04x`, r)
			default:
				builder.WriteRune(r)
			}
		}
		builder.WriteByte('"')
		return builder.String()
	case *pyjson.Object:
		parts := make([]string, 0, typed.Len())
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			parts = append(parts, asciiDump(key)+":"+asciiDump(item))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case []pyjson.Value:
		parts := make([]string, len(typed))
		for index, item := range typed {
			parts[index] = asciiDump(item)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	out, err := pyjson.Marshal(value)
	if err != nil {
		return "Error"
	}
	return string(out)
}
