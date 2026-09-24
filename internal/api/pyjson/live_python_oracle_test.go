package pyjson

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
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

func TestMarshalMatchesLivePythonJSONDumps(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonDumpsProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python json.dumps: %v", pyoracle.RunError(python, err, output))
	}
	var want []string
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatal(err)
	}
	for index, text := range corpus {
		gotText := "ValueError"
		value, err := Decode([]byte(text))
		var syntax *SyntaxError
		var limit *IntLimitError
		switch {
		case errors.As(err, &syntax):
			gotText = fmt.Sprintf("JSONDecodeError:%s:%d", syntax.Msg, syntax.Pos)
		case errors.As(err, &limit):
			gotText = "IntLimit:" + limit.Error()
		case err != nil:
			gotText = "decode: " + err.Error()
		default:
			if got, err := Marshal(value); err == nil {
				gotText = string(got)
			}
		}
		if gotText != want[index] {
			t.Errorf("%s:\n Go     %s\n Python %s", text, gotText, want[index])
		}
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-pyjson"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
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

// TestDumpsMatchesLivePythonJSONDumpsDefault pins Dumps -- the shared
// encoder every ClickHouse JSON-column writer uses (CHAOS-6310 r1 finding
// #10) -- against a live Python bare `json.dumps(value)` call (no keyword
// arguments at all), reusing the same corpus TestMarshalMatchesLivePythonJSONDumps
// proves the compact/ensure_ascii=False form against. Dumps had no live-
// oracle proof of its own before this; two mismatches from Marshal's own
// assumptions were caught building it: Python's bare default is
// ensure_ascii=TRUE (not False) and allow_nan=TRUE (not False).
func TestDumpsMatchesLivePythonJSONDumpsDefault(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonDumpsDefaultProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python json.dumps (bare defaults): %v", pyoracle.RunError(python, err, output))
	}
	var want []string
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatal(err)
	}
	for index, text := range corpus {
		gotText := "ValueError"
		value, err := Decode([]byte(text))
		var syntax *SyntaxError
		var limit *IntLimitError
		switch {
		case errors.As(err, &syntax):
			gotText = fmt.Sprintf("JSONDecodeError:%s:%d", syntax.Msg, syntax.Pos)
		case errors.As(err, &limit):
			gotText = "IntLimit:" + limit.Error()
		case err != nil:
			gotText = "decode: " + err.Error()
		default:
			if got, err := Dumps(value); err == nil {
				gotText = got
			}
		}
		if gotText != want[index] {
			t.Errorf("%s:\n Go     %s\n Python %s", text, gotText, want[index])
		}
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-pyjson-dumps"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
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

func TestDecodeBodyMatchesLivePythonJSONLoads(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	input, _ := json.Marshal(bodyCorpus)
	command := exec.Command(python, "-c", pythonBodyProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python json.loads: %v", pyoracle.RunError(python, err, output))
	}
	var want []string
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatal(err)
	}
	for index, hexed := range bodyCorpus {
		raw, _ := hex.DecodeString(hexed)
		got := "Error"
		value, err := Decode(raw)
		var syntax *SyntaxError
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
func asciiDump(value Value) string {
	switch typed := value.(type) {
	case string:
		var builder strings.Builder
		builder.WriteByte('"')
		for _, r := range Runes(typed) {
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
	case *Object:
		parts := make([]string, 0, typed.Len())
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			parts = append(parts, asciiDump(key)+":"+asciiDump(item))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case []Value:
		parts := make([]string, len(typed))
		for index, item := range typed {
			parts[index] = asciiDump(item)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	out, err := Marshal(value)
	if err != nil {
		return "Error"
	}
	return string(out)
}
