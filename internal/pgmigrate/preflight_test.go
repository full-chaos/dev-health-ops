package pgmigrate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestClassify(t *testing.T) {
	schema := "CREATE TABLE public.alembic_version (\n);\nCREATE TABLE public.users (\n);\n"
	baseline := Baseline{Cutover: true, RiverSchema: "river", Heads: []string{"0066", "0138"}, Schema: schema}
	chain := []ChainFile{{Revision: "0139", Name: "0139_a.sql"}, {Revision: "0140", Name: "0140_b.sql"}}
	history := []HistoryEntry{{Revision: "0137", Down: "0136"}, {Revision: "0138", Down: "0137"}, {Revision: "0066", Down: "0065"}}
	known := KnownRevisions(history, baseline, chain)
	tables := []string{"alembic_version", "users"}
	production := Settings{Cutover: true, RiverSchema: "river"}
	at := func(versions ...string) Observation {
		return Observation{HasVersionTable: true, Versions: versions, PublicTables: tables}
	}
	for name, tc := range map[string]struct {
		observation Observation
		settings    Settings
		verdict     string
		reason      string
		pending     []string
		missing     []string
		exit        int
	}{
		"empty":                         {Observation{}, production, VerdictAppliesCleanly, ReasonEmptyDatabase, []string{"0139", "0140"}, nil, ExitAppliesCleanly},
		"at the baseline":               {at("0066", "0138"), production, VerdictAppliesCleanly, ReasonPendingRevs, []string{"0139", "0140"}, nil, ExitAppliesCleanly},
		"part of the chain":             {at("0066", "0139"), production, VerdictAppliesCleanly, ReasonPendingRevs, []string{"0140"}, nil, ExitAppliesCleanly},
		"at the head":                   {at("0066", "0140"), production, VerdictAtHead, ReasonUpToDate, nil, nil, cli.ExitOK},
		"the cutover missing":           {at("0138"), production, VerdictNeedsManual, ReasonCutoverMissing, nil, []string{"0066"}, cli.ExitFailure},
		"below the baseline":            {at("0066", "0137"), production, VerdictNeedsManual, ReasonBelowBaseline, nil, []string{"0138"}, cli.ExitFailure},
		"both missing":                  {at("0137"), production, VerdictNeedsManual, ReasonBelowBaseline, nil, []string{"0066", "0138"}, cli.ExitFailure},
		"a revision not known":          {at("0066", "9999"), production, VerdictNeedsManual, ReasonAheadOfBuild, nil, nil, cli.ExitFailure},
		"unknown beside baseline heads": {at("0066", "0138", "9999"), production, VerdictNeedsManual, ReasonAheadOfBuild, nil, nil, cli.ExitFailure},
		"unknown beside the head":       {at("0066", "0140", "9999"), production, VerdictNeedsManual, ReasonAheadOfBuild, nil, nil, cli.ExitFailure},
		"foreign":                       {Observation{Objects: 3}, production, VerdictNeedsManual, ReasonForeignDatabase, nil, nil, cli.ExitFailure},
		"a table gone":                  {Observation{HasVersionTable: true, Versions: []string{"0066", "0138"}, PublicTables: []string{"alembic_version"}}, production, VerdictNeedsManual, ReasonSchemaMismatch, nil, nil, cli.ExitFailure},
		"the settings differ":           {at("0066", "0140"), Settings{Cutover: false, RiverSchema: "river"}, VerdictNeedsManual, ReasonSettingsMismatch, nil, nil, cli.ExitFailure},
	} {
		t.Run(name, func(t *testing.T) {
			report := Classify(tc.observation, tc.settings, baseline, chain, known)
			if report.Verdict != tc.verdict || report.Reason != tc.reason {
				t.Fatalf("Classify = %s/%s, want %s/%s (%+v)", report.Verdict, report.Reason, tc.verdict, tc.reason, report)
			}
			wantPending := tc.pending
			if wantPending == nil {
				wantPending = []string{}
			}
			wantMissing := tc.missing
			if wantMissing == nil {
				wantMissing = []string{}
			}
			if !reflect.DeepEqual(report.Pending, wantPending) || !reflect.DeepEqual(report.Missing, wantMissing) {
				t.Fatalf("pending %v missing %v, want %v %v", report.Pending, report.Missing, wantPending, wantMissing)
			}
			if !reflect.DeepEqual(report.BuildHeads, []string{"0066", "0140"}) {
				t.Fatalf("build_heads = %v", report.BuildHeads)
			}
			if got := report.ExitCode(false); got != tc.exit {
				t.Fatalf("exit %d, want %d", got, tc.exit)
			}
		})
	}
}

// --strict turns applies_cleanly into a failure and nothing else.
func TestExitCodeStrict(t *testing.T) {
	for verdict, want := range map[string]int{
		VerdictAtHead:         cli.ExitOK,
		VerdictAppliesCleanly: cli.ExitFailure,
		VerdictNeedsManual:    cli.ExitFailure,
	} {
		if got := (PreflightReport{Verdict: verdict}).ExitCode(true); got != want {
			t.Errorf("strict %s exits %d, want %d", verdict, got, want)
		}
	}
	if got := (PreflightReport{Verdict: VerdictAppliesCleanly}).ExitCode(false); got != ExitAppliesCleanly || ExitAppliesCleanly == cli.ExitOK {
		t.Errorf("applies_cleanly exits %d without --strict, want %d (distinct from success)", got, ExitAppliesCleanly)
	}
}

// The JSON always carries every field, with arrays and never null.
func TestPreflightReportJSONShape(t *testing.T) {
	report := Classify(Observation{}, Settings{Cutover: true, RiverSchema: "river"},
		Baseline{Cutover: true, RiverSchema: "river", Heads: []string{"0066", "0138"}, Schema: "CREATE TABLE public.x ("},
		nil, map[string]bool{})
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"verdict", "reason", "recorded", "build_heads", "pending", "missing", "migrator_active"} {
		value, present := decoded[key]
		if !present || value == nil {
			t.Fatalf("%q is absent or null in %s", key, raw)
		}
	}
}

// A measurement that did not happen is exit 3, with nothing on stdout: no DSN, no
// baseline and a bad flag all end before a verdict could be printed.
func TestPreflightMeasurementFailures(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := cli.Env{Stdout: &stdout, Stderr: &stderr, Lookup: func(key string) (string, bool) {
		if key == CutoverEnv {
			return "1", true
		}
		return "", false
	}}
	unresolved := ResolveDSN(func(_ secrets.LookupEnv, stderr io.Writer) (secrets.Value, string, bool) {
		_, _ = stderr.Write([]byte(`{"error":{"code":"configuration_error"}}` + "\n"))
		return secrets.Value{}, "", false
	})
	if code := preflight(context.Background(), unresolved, env); code != ExitMeasurementFailed || stdout.Len() != 0 || !strings.Contains(stderr.String(), "configuration_error") {
		t.Fatalf("no DSN: exit %d stdout %q stderr %q, want exit 3 and no verdict", code, stdout.String(), stderr.String())
	}
	stderr.Reset()
	env.Args = []string{"extra"}
	if code := preflight(context.Background(), unresolved, env); code != cli.ExitUsage {
		t.Fatalf("a positional argument exited %d, want %d", code, cli.ExitUsage)
	}
	env.Args = []string{"--nonsense"}
	if code := preflight(context.Background(), unresolved, env); code != cli.ExitUsage {
		t.Fatalf("an unknown flag exited %d, want %d", code, cli.ExitUsage)
	}
	if ExitMeasurementFailed != 3 || ExitAppliesCleanly != 10 {
		t.Fatalf("the contract's exit codes moved: %d %d", ExitMeasurementFailed, ExitAppliesCleanly)
	}
}

// oldScriptDerivation is the head derivation the roll pre-check
// (hook-parity-check.sh, the script `preflight` replaces) ran over the Alembic
// scripts: every revision that no other revision names as its down_revision.
const oldScriptDerivation = `
import re, sys, pathlib
revs, downs = {}, set()
for f in pathlib.Path(sys.argv[1]).glob("[0-9]*.py"):
    t = f.read_text()
    r = re.search(r'^revision\s*(?::[^=]+)?=\s*["\']([^"\']+)["\']', t, re.M)
    d = re.search(r'^down_revision\s*(?::[^=]+)?=\s*(.+)$', t, re.M)
    if not r: continue
    revs[r.group(1)] = 1
    if d: downs.update(re.findall(r'["\']([^"\']+)["\']', d.group(1)))
print(" ".join(sorted(x for x in revs if x not in downs)))
`

// TestBuildHeadsAreTheOldScriptsHeads is the old-script oracle: the heads the
// roll pre-check derived from the Alembic scripts are the heads the preflight
// reports for this build. It reads the scripts, so it fails when they are gone
// rather than passing on nothing: when the Python chain is deleted, this oracle
// retires with hook-parity-check.sh.
func TestBuildHeadsAreTheOldScriptsHeads(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("python3 is required to run the old script's derivation: %v", err)
	}
	versions, err := filepath.Abs(filepath.Join("..", "..", "src", "dev_health_ops", "alembic", "versions"))
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(python, "-c", oldScriptDerivation, versions).Output()
	if err != nil {
		t.Fatalf("the old script's derivation failed: %v", err)
	}
	derived := strings.Fields(string(output))
	if len(derived) == 0 {
		t.Fatalf("the old script's derivation found no revision in %s: the measurement did not happen", versions)
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if got := Heads(baseline, chain); !reflect.DeepEqual(got, derived) {
		t.Fatalf("the old script derived the heads %v, the preflight's build_heads are %v", derived, got)
	}
}

// A chain revision the Alembic walk does not carry (a release with no Alembic script)
// is still a revision this build knows.
func TestKnownRevisionsIncludeAChainRevisionTheWalkLacks(t *testing.T) {
	baseline := Baseline{Heads: []string{"0066", "0138"}}
	chain := []ChainFile{{Revision: "0139", Name: "0139_a.sql"}, {Revision: "0999", Name: "0999_scriptless.sql"}}
	history := []HistoryEntry{{Revision: "0138", Down: "0137", RealHead: true, Head: true}, {Revision: "0066", Down: "0065", RealHead: true, Head: true}}
	known := KnownRevisions(history, baseline, chain)
	for _, revision := range []string{"0066", "0138", "0139", "0999"} {
		if !known[revision] {
			t.Errorf("%s is not known: %v", revision, known)
		}
	}
	if known["9999"] {
		t.Error("an unrelated revision is known")
	}
}
