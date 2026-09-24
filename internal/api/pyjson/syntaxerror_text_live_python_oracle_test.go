package pyjson

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonErrorTextProgram runs json.loads over each body as bytes (what httpx's
// Response.json does) and reports "ok", the JSONDecodeError text, or
// "other:<ExceptionName>".
const pythonErrorTextProgram = `
import base64, json, sys
out = []
for item in json.loads(sys.stdin.read()):
    body = base64.b64decode(item)
    try:
        json.loads(body)
        out.append("ok")
    except json.JSONDecodeError as error:
        out.append("err:" + str(error))
    except Exception as error:
        out.append("other:" + type(error).__name__)
print(json.dumps(out, ensure_ascii=False))
`

func syntaxTextCorpus() [][]byte {
	valid := []string{
		`{"login":"octocat","id":1,"name":"Octo Cat"}`, `[1,2.5,-3e+2,true,false,null,"x"]`,
		`{"a":{"b":[{"c":"\u00e9\ud83d\ude00\n\t\"\\\/"}]},"d":[]}`, `  {"key" : "value" , "n" : 12 }  `,
		"{\n  \"a\": [1,\n 2],\n  \"b\": \"\u00e9\u65e5\"\n}", `"plain"`, `12`, `-0`, `0.5e-3`, `NaN`, `-Infinity`, `Infinity`,
	}
	fixed := []string{
		"", " ", "\n\n", "{", "[", "}", "]", "{\"a\"", "{\"a\":", "{\"a\":1", "{\"a\":1,", "{\"a\":1,}", "{\"a\":1 ,\n }", "[1,]", "[1,\n]", "[,]", "[,1]", "[1,,2]",
		"{,}", "{1:2}", "{\"a\" 1}", "{\"a\":1 \"b\":2}", "{\"a\":}", "{\"a\":[1,}", "[1 2]", "[1,2", "\"abc", "\"a\nb\"", "\"a\\x\"", "\"\\u12", "\"\\u12zz00\"",
		"\"\\ud800\\uzzzz\"", "\"\\ud800\\ude00\"", "\"\\ud800\\u12\"", "\"\\\\", "\"\\", "\"\\u", "\"\\u1234", "\"\\u1234\"", "\"\\ud800\\u1234", "\"\\ud800\\u12345\"",
		"tru", "nul", "fals", "-", "-x", "01", "1.", "1e", "1e+", "1.e3", ".5", "+1", "--1", "1 2", "{} x", "[] ]", "NaN1", "-Inf", "Infinit", "-Infinity1",
		"<html>", "  \n  oops", "\t<html>", "Bad gateway", "null x", "\"a\" \"b\"", "{\"a\":1}\n\n{\"b\":2}", "\xef\xbb\xbf{}", "\xef\xbb\xbf\xef\xbb\xbf{}", "\xef\xbb\xbf",
		"[\u00e9]", "{\u00e9:1}", "\"\u00e9\u65e5\"x", "\u65e5\n\u65e5", "[\"\u65e5\",\n\u65e5]", "{\"\u65e5\":\n  \u65e5}",
		"\"\xed\xa0\x80\"\n x", "[\"\xed\xa0\x80\",\n\n1 2]", "\x80", "{\"a\":\"\xff\"}", "\xff\xfe{\x00}\x00", "{\x00\"\x00", "\x00\x00\x00{", "\xed\xa0\x80", "{\x00", "\x00{",
		"[" + strings.Repeat("1,", 3) + "\"a", "1" + strings.Repeat("2", 30) + "e", "\"" + strings.Repeat("a", 300),
	}
	var corpus [][]byte
	for _, text := range append(append([]string{}, valid...), fixed...) {
		corpus = append(corpus, []byte(text))
	}
	rng := rand.New(rand.NewSource(20260924))
	alphabet := []string{"{", "}", "[", "]", ",", ":", "\"", "\\", "\\u", "\\ud800", "\\n", "\n", " ", "\t", "\x00", "\u00e9", "\u65e5", "1", "-", "e", ".", "t", "n", "N", "I", "0", "a"}
	for _, text := range valid {
		runes := []rune(text)
		for cut := 0; cut <= len(runes); cut++ {
			corpus = append(corpus, []byte(string(runes[:cut])))
		}
		for i := 0; i < 250; i++ {
			mutated := append([]rune{}, runes...)
			for n := 1 + rng.Intn(3); n > 0 && len(mutated) > 0; n-- {
				at := rng.Intn(len(mutated))
				switch rng.Intn(3) {
				case 0:
					mutated = append(mutated[:at], mutated[at+1:]...)
				case 1:
					mutated[at] = []rune(alphabet[rng.Intn(len(alphabet))])[0]
				default:
					insert := []rune(alphabet[rng.Intn(len(alphabet))])
					mutated = append(mutated[:at], append(insert, mutated[at:]...)...)
				}
			}
			corpus = append(corpus, []byte(string(mutated)))
		}
	}
	for i := 0; i < 2500; i++ {
		var b strings.Builder
		for n := 1 + rng.Intn(9); n > 0; n-- {
			b.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		corpus = append(corpus, []byte(b.String()))
	}
	return corpus
}

// TestSyntaxErrorTextMatchesLivePython compares Decode + SyntaxError.Text with
// json.loads(bytes) and str(JSONDecodeError) over the corpus: the same
// accept/reject decision, the same full text (message, line, column, char),
// and the same non-JSON failures (ValueError, UnicodeDecodeError).
func TestSyntaxErrorTextMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := syntaxTextCorpus()
	encoded := make([]string, len(corpus))
	for i, body := range corpus {
		encoded[i] = base64.StdEncoding.EncodeToString(body)
	}
	input, _ := json.Marshal(encoded)
	command := exec.Command(python, "-c", pythonErrorTextProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d for %d bodies", len(want), len(corpus))
	}
	mismatches, texts, oks, others := 0, 0, 0, 0
	for index, body := range corpus {
		got := "ok"
		if _, err := Decode(body); err != nil {
			var syntax *SyntaxError
			var limit *IntLimitError
			switch {
			case errors.As(err, &syntax):
				document, _ := DecodeBody(body)
				got = "err:" + syntax.Text(document)
			case errors.As(err, &limit):
				got = "other:ValueError"
			case errors.Is(err, ErrUnicode):
				got = "other:UnicodeDecodeError"
			default:
				got = "unexpected:" + err.Error()
			}
		}
		switch {
		case strings.HasPrefix(want[index], "err:"):
			texts++
		case want[index] == "ok":
			oks++
		default:
			others++
		}
		if got != want[index] {
			mismatches++
			if mismatches <= 12 {
				t.Errorf("Decode(%q)\n go     %q\n python %q", body, got, want[index])
			}
		}
	}
	if texts < 1000 || oks < 20 || others < 3 {
		t.Fatalf("corpus lost its spread: %d errors, %d ok, %d other", texts, oks, others)
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "pyjson-syntax-error-text"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d bodies compared (%d errors, %d ok, %d other), %d mismatches", len(corpus), texts, oks, others, mismatches)
}
