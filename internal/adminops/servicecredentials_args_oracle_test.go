package adminops

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
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

// TestCredentialArgsVenueOracleMatchesThePythonParser runs every command line of argsCorpus through the
// REAL argparse (build_parser().parse_args) and through parseCredentialArgs and compares whether it parses,
// whether it is help, and every value the handler reads.
func TestCredentialArgsVenueOracleMatchesThePythonParser(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", credentialArgsOracleProgram)
	command.Env = append(os.Environ(), "PYTHONHASHSEED=0", "PYTHONPATH="+filepath.Join(root, "src"), "DISABLE_DOTENV=1", "OTEL_ENABLED=false")
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = command.Wait() })

	corpus := argsCorpus()
	go func() {
		writer := bufio.NewWriterSize(stdin, 1<<20)
		for _, item := range corpus {
			raw, _ := json.Marshal(map[string]any{"verb": item[0], "args": item[1]})
			_, _ = writer.Write(raw)
			_ = writer.WriteByte('\n')
		}
		_ = writer.Flush()
		_ = stdin.Close()
	}()
	reader := bufio.NewReaderSize(stdout, 1<<20)
	mismatches, parsed, refused, helped := 0, 0, 0, 0
	for index, item := range corpus {
		verb, args := item[0].(string), item[1].([]string)
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			t.Fatalf("python answer %d: %v\n%s", index, err, stderr.String())
		}
		var want argsAnswer
		if err := json.Unmarshal(line, &want); err != nil {
			t.Fatalf("python answer %d %q: %v\n%s", index, line, err, stderr.String())
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
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
	_ = fmt.Sprint
}

func credentialText(args credentialArgs) string {
	if !args.hasID {
		return "<null>"
	}
	return args.credentialID
}
