package pgmigrate

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestDecide(t *testing.T) {
	application := Baseline{Heads: []string{"0138"}}
	cutover := Baseline{Cutover: true, Heads: []string{"0066", "0138"}}
	chain := []ChainFile{{Revision: "0139", Name: "0139_a.sql"}, {Revision: "0140", Name: "0140_b.sql"}}
	for name, testCase := range map[string]struct {
		observation Observation
		baseline    Baseline
		want        Plan
	}{
		"empty":   {Observation{}, application, Plan{State: StateEmpty, ApplicationHead: "0138", Pending: chain}},
		"foreign": {Observation{PublicRelations: 3}, application, Plan{State: StateForeign}},
		"at the head": {Observation{HasVersionTable: true, Versions: []string{"0138"}, PublicRelations: 90}, application,
			Plan{State: StateAtHead, ApplicationHead: "0138", Pending: chain}},
		"below the head": {Observation{HasVersionTable: true, Versions: []string{"0137"}}, application,
			Plan{State: StateBelowHead, Missing: []string{"0138"}}},
		"an application head under the cutover": {Observation{HasVersionTable: true, Versions: []string{"0138"}}, cutover,
			Plan{State: StateBelowHead, Missing: []string{"0066"}}},
		"the cutover head": {Observation{HasVersionTable: true, Versions: []string{"0066", "0138"}}, cutover,
			Plan{State: StateAtHead, ApplicationHead: "0138", Pending: chain}},
		"part of the chain applied": {Observation{HasVersionTable: true, Versions: []string{"0066", "0139"}}, cutover,
			Plan{State: StateAtHead, ApplicationHead: "0139", Pending: chain[1:]}},
		"the whole chain applied": {Observation{HasVersionTable: true, Versions: []string{"0140"}}, application,
			Plan{State: StateAtHead, ApplicationHead: "0140", Pending: []ChainFile{}}},
	} {
		t.Run(name, func(t *testing.T) {
			got := Decide(testCase.observation, testCase.baseline, chain)
			if len(got.Pending) == 0 && len(testCase.want.Pending) == 0 {
				got.Pending, testCase.want.Pending = nil, nil
			}
			if !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("Decide = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestBelowHeadErrorNamesTheWayOut(t *testing.T) {
	text := BelowHeadError{Cutover: true, Recorded: []string{"0138"}, Missing: []string{"0066"}}.Error()
	for _, want := range []string{"below the cutover head", "[0138]", "[0066]", "dev-hops migrate postgres", CutoverEnv + "=1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("%q does not name %q", text, want)
		}
	}
}

func TestSanitizeDump(t *testing.T) {
	dump := "--\n-- comment kept\n\\restrict abc123\nSET transaction_timeout = 0;\nSET lock_timeout = 0;\nCREATE TABLE public.t (x integer);\n\\unrestrict abc123\n"
	want := "--\n-- comment kept\nSET lock_timeout = 0;\nCREATE TABLE public.t (x integer);\n"
	if got := SanitizeDump(dump); got != want {
		t.Fatalf("SanitizeDump = %q, want %q", got, want)
	}
}

func TestCutoverAuthorized(t *testing.T) {
	for _, testCase := range []struct {
		raw     string
		present bool
		want    bool
	}{{"1", true, true}, {"", false, false}, {"", true, false}, {"true", true, false}, {"0", true, false}} {
		if got := CutoverAuthorized(testCase.raw, testCase.present); got != testCase.want {
			t.Fatalf("CutoverAuthorized(%q, %v) = %v", testCase.raw, testCase.present, got)
		}
	}
}

// Both heads load: the application head is one revision, the cutover head is
// the same revision plus 0066, and the two schemas are the same.
func TestBaselinesLoad(t *testing.T) {
	application, err := LoadBaseline(false)
	if err != nil {
		t.Fatal(err)
	}
	cutover, err := LoadBaseline(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(application.Heads) != 1 || !reflect.DeepEqual(cutover.Heads, []string{"0066", application.Heads[0]}) {
		t.Fatalf("heads: application %v, cutover %v", application.Heads, cutover.Heads)
	}
	if application.Schema != cutover.Schema {
		t.Fatal("the cutover changes data only, but the two baseline schemas differ")
	}
	for _, baseline := range []Baseline{application, cutover} {
		if strings.Contains(baseline.Schema, `\restrict`) || strings.Contains(baseline.Data, `\restrict`) {
			t.Fatalf("the %s baseline still holds a psql meta-command", Variant(baseline.Cutover))
		}
	}
}

var alembicRevision = regexp.MustCompile(`^([0-9]{4})_[a-z0-9_]+\.py$`)

// The baseline is the alembic chain's head. An alembic revision added after
// it must regenerate the baseline in the same change, or the database dho
// builds would lack it; the integration drift check proves the content, this
// proves the head without a database.
func TestBaselineHeadIsTheAlembicHead(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	directory := filepath.Join(filepath.Dir(file), "..", "..", "src", "dev_health_ops", "alembic", "versions")
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	latest := ""
	for _, entry := range entries {
		if match := alembicRevision.FindStringSubmatch(entry.Name()); match != nil && match[1] > latest {
			latest = match[1]
		}
	}
	if latest == "" {
		t.Fatalf("found no alembic revision in %s", directory)
	}
	baseline, err := LoadBaseline(false)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Heads[0] != latest {
		t.Fatalf("the alembic chain ends at %s but the baseline head is %s: regenerate the baseline", latest, baseline.Heads[0])
	}
}

func TestChainNames(t *testing.T) {
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := LoadBaseline(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range chain {
		if file.Revision <= baseline.Heads[0] {
			t.Fatalf("%s is not after the baseline head %s", file.Name, baseline.Heads[0])
		}
	}
}

// The verbs refuse arguments before touching a database, and a resolver
// failure ends the run without a connection.
func TestCommandRefusesBeforeConnecting(t *testing.T) {
	resolve := func(lookup secrets.LookupEnv, stderr io.Writer) (secrets.Value, string, bool) {
		stderr.Write([]byte(`{"error":{"code":"configuration_error"}}` + "\n"))
		return secrets.Value{}, "", false
	}
	var stdout, stderr bytes.Buffer
	env := cli.Env{Args: []string{"extra"}, Stdout: &stdout, Stderr: &stderr, Lookup: func(string) (string, bool) { return "", false }}
	if code := run(context.Background(), "upgrade", ResolveDSN(resolve), env); code != cli.ExitUsage {
		t.Fatalf("a positional argument exited %d, want %d", code, cli.ExitUsage)
	}
	stderr.Reset()
	env.Args = nil
	if code := run(context.Background(), "status", ResolveDSN(resolve), env); code != cli.ExitFailure || !strings.Contains(stderr.String(), "configuration_error") {
		t.Fatalf("a resolver failure exited %d with %q", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q", stdout.String())
	}
}
