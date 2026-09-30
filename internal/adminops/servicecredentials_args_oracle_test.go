package adminops

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/service_credentials_args_oracle.py
var credentialArgsOracleProgram string

type argsLeaf struct {
	T string `json:"t"`
	V string `json:"v"`
}

type argsAnswer struct {
	Exit int                 `json:"exit"`
	NS   map[string]argsLeaf `json:"ns"`
}

// argsCorpus is every command line of up to three tokens over an alphabet chosen from the ways an option,
// its value, the positional, "--" and argparse's classification rules interact, per verb, plus a
// deterministic sample of longer ones.
func argsCorpus() [][2]any {
	tokens := []string{"ID", "--", "--scope", "x", "--service", "acr", "worker-operator", "bogus", "-scope", "--sc", "--scope=y",
		"--overlap-seconds", "5", "abc", "-1", "-h", "--help", "--db", "d", "--org", "o", "-l", "p", "--expires-at", "2099-01-01T00:00:00+00:00",
		"", "=", "--s", "--ov=7", "-lx", "--created-by-user-id", "-m", "--", "--log-level"}
	var corpus [][2]any
	add := func(verb string, args []string) { corpus = append(corpus, [2]any{verb, append([]string{}, args...)}) }
	for _, verb := range []string{"create", "list", "rotate", "revoke"} {
		add(verb, nil)
		for _, a := range tokens {
			add(verb, []string{a})
			for _, b := range tokens {
				add(verb, []string{a, b})
				for _, c := range tokens {
					add(verb, []string{a, b, c})
				}
			}
		}
	}
	generator := rand.New(rand.NewSource(20260926))
	for i := 0; i < 30000; i++ {
		verb := []string{"create", "rotate", "revoke", "list"}[generator.Intn(4)]
		length := 4 + generator.Intn(4)
		args := make([]string, length)
		for j := range args {
			args[j] = tokens[generator.Intn(len(tokens))]
		}
		add(verb, args)
	}
	return corpus
}

func leafText(value *string) string {
	if value == nil {
		return "<null>"
	}
	return *value
}

// credentialArgsPythonBuild is the build whose argparse answered the frozen
// corpus: a build that still carried the Python CLI.
const credentialArgsPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// TestCredentialArgsMatchTheFrozenPythonParser runs every command line of argsCorpus through the
// REAL argparse (build_parser().parse_args) and through parseCredentialArgs and compares whether it parses,
// whether it is help, and every value the handler reads. The parser's answers were executed once on
// credentialArgsPythonBuild and are frozen in testdata/golden/credential_args.json (the recipe regenerates
// them by execution); the corpus and the program are part of the golden's key.
func TestCredentialArgsMatchTheFrozenPythonParser(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/credential_args.json",
		PythonBuild: credentialArgsPythonBuild,
		SHA256:      "16c39e1d8d90b8682256a5252829413471dc87082e1457ddec84feb485c97c92",
		Recipe: "git worktree add --detach $DIR " + credentialArgsPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/adminops/ -test '^TestCredentialArgsMatchTheFrozenPythonParser$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)

	corpus := argsCorpus()
	var input bytes.Buffer
	for _, item := range corpus {
		raw, _ := json.Marshal(map[string]any{"verb": item[0], "args": item[1]})
		input.Write(raw)
		input.WriteByte('\n')
	}
	env := map[string]string{"PYTHONHASHSEED": "0", "DISABLE_DOTENV": "1", "OTEL_ENABLED": "false"}
	request := venueoracle.ProgramRequest("credential args corpus", credentialArgsOracleProgram, input.Bytes(), env)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(root string, _ []venueoracle.Request) []venueoracle.Response {
		python := pyoracle.Resolve(t, root)
		command := exec.Command(python, "-c", credentialArgsOracleProgram)
		command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "PYTHONPATH=" + filepath.Join(root, "src"), "PYTHONDONTWRITEBYTECODE=1"}
		for key, value := range env {
			command.Env = append(command.Env, key+"="+value)
		}
		command.Stdin = bytes.NewReader(input.Bytes())
		var stderr strings.Builder
		command.Stderr = &stderr
		output, err := command.Output()
		if err != nil {
			t.Fatalf("live python: %v\n%s", pyoracle.RunError(python, err, nil), stderr.String())
		}
		// Packed: the corpus answers are megabytes of near-identical lines.
		return []venueoracle.Response{{Status: 0, Body: venueoracle.PackBody(output)}}
	})
	golden.Consumed(t, answers...)
	reader := bufio.NewReader(strings.NewReader(venueoracle.UnpackBody(t, answers[0].Body)))
	mismatches, parsed, refused, helped := 0, 0, 0, 0
	for index, item := range corpus {
		verb, args := item[0].(string), item[1].([]string)
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			t.Fatalf("python answer %d: %v", index, err)
		}
		var want argsAnswer
		if err := json.Unmarshal(line, &want); err != nil {
			t.Fatalf("python answer %d %q: %v", index, line, err)
		}
		got, perr := parseCredentialArgs(verb, args)
		switch {
		case perr != nil:
			refused++
			if want.Exit != 2 {
				mismatches++
				if mismatches <= 25 {
					t.Errorf("%s %q: dho refuses (%s), python exits %d %v", verb, args, perr.Msg, want.Exit, want.NS)
				}
			}
		case got.help:
			helped++
			if want.Exit != 0 || want.NS != nil {
				mismatches++
				if mismatches <= 25 {
					t.Errorf("%s %q: dho prints help, python exits %d ns %v", verb, args, want.Exit, want.NS)
				}
			}
		default:
			parsed++
			if want.Exit != 0 || want.NS == nil {
				mismatches++
				if mismatches <= 25 {
					t.Errorf("%s %q: dho parses, python exits %d", verb, args, want.Exit)
				}
				continue
			}
			gotScope := "<null>"
			if len(got.scopes) > 0 {
				items := make([]any, len(got.scopes))
				for i, scope := range got.scopes {
					items[i] = scope
				}
				encoded, _ := pythonparity.MarshalPythonJSONSorted(items) // json.dumps(list)
				gotScope = string(encoded)
			}
			wantScope := want.NS["scope"].V
			if want.NS["scope"].T == "null" {
				wantScope = "<null>"
			}
			overlap := got.overlapValue.String()
			if verb != "rotate" {
				overlap = "<null>"
			}
			service := got.service
			if verb == "revoke" {
				service = "<null>"
			}
			field := func(name string) string {
				if want.NS[name].T == "null" {
					return "<null>"
				}
				return want.NS[name].V
			}
			for name, pair := range map[string][2]string{
				"service":            {service, field("service")},
				"scope":              {gotScope, wantScope},
				"expires_at":         {leafText(got.expiresAt), field("expires_at")},
				"created_by_user_id": {leafText(got.createdBy), field("created_by_user_id")},
				"overlap_seconds":    {overlap, field("overlap_seconds")},
				"credential_id":      {credentialText(got), field("credential_id")},
				"db":                 {leafText(got.db), field("db")},
			} {
				if pair[0] != pair[1] {
					mismatches++
					if mismatches <= 25 {
						t.Errorf("%s %q: %s: dho %q python %q", verb, args, name, pair[0], pair[1])
					}
				}
			}
		}
	}
	t.Logf("%d command lines: %d parse, %d refused, %d help; %d mismatches", len(corpus), parsed, refused, helped, mismatches)
	if parsed < 2000 || refused < 2000 || helped < 100 {
		t.Fatalf("the corpus measures too little: %d parse, %d refused, %d help", parsed, refused, helped)
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d command lines differ", mismatches, len(corpus))
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("python answered more lines than the %d of the corpus", len(corpus))
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

func credentialText(args credentialArgs) string {
	if !args.hasID {
		return "<null>"
	}
	return args.credentialID
}
