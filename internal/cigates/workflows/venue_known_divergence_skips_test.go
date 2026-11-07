package workflows_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The venue-oracles verb refuses every skip of a venue oracle test except a known divergence of product rule 1: the
// skip message starts "KNOWN-DIVERGENCE CHAOS-<n>", carries python= and go= (both planes ran), and the checked-in
// ledger holds its ticket, test and row. The verb prints one KNOWN-DIVERGENCE skips line per run.

const knownLedgerText = "# ticket\ttest\trow\treason\nCHAOS-7\tTestVenueX\trow_a\ta stricter answer\n"

// skipEvents is a go test -json log of TestVenueX whose subtest row_a skipped with message (empty: no skip).
func skipEvents(t *testing.T, message string) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	event := func(action, test, output string) {
		e := map[string]string{"Action": action, "Package": "example/venue", "Test": test}
		if output != "" {
			e["Output"] = output
		}
		if err := enc.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
	event("run", "TestVenueX", "")
	event("run", "TestVenueX/row_a", "")
	event("output", "TestVenueX/row_a", "=== RUN   TestVenueX/row_a\n")
	buf.WriteString("a line go test wrote that is not an event\n")
	if message == "" {
		event("output", "TestVenueX/row_a", "--- PASS: TestVenueX/row_a (0.00s)\n")
		event("pass", "TestVenueX/row_a", "")
	} else {
		event("output", "TestVenueX/row_a", "--- SKIP: TestVenueX/row_a (0.00s)\n")
		event("output", "TestVenueX/row_a", "    venueoracle.go:540: "+message+"\n")
		event("output", "TestVenueX/row_a", "        detail line\n")
		event("skip", "TestVenueX/row_a", "")
	}
	event("pass", "TestVenueX", "")
	return buf.String()
}

// runVenueSkips runs ci/check_go.sh venue-oracle-skips over log and ledger for names.
func runVenueSkips(t *testing.T, log, ledger string, names ...string) (string, string, error) {
	t.Helper()
	dir := t.TempDir()
	logPath, ledgerPath := filepath.Join(dir, "events.json"), filepath.Join(dir, "known_divergences.tsv")
	if err := os.WriteFile(logPath, []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ledgerPath, []byte(ledger), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", append([]string{"ci/check_go.sh", "venue-oracle-skips", logPath, ledgerPath}, names...)...)
	cmd.Dir = repoRoot(t)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatalf("venue-oracle-skips did not run: %v\n%s", err, stderr.String())
	}
	return stdout.String(), stderr.String(), err
}

func TestVenueOracleSkipsAcceptOnlyALedgerKnownDivergence(t *testing.T) {
	for _, tc := range []struct {
		name, message, refusal string
	}{
		{name: "a plain skip", message: "the stub is not configured", refusal: "reported SKIP without a KNOWN-DIVERGENCE CHAOS-<n> message"},
		{name: "a ticket the ledger does not hold", message: "KNOWN-DIVERGENCE CHAOS-8 python=404 go=401: other", refusal: "that the ledger"},
		{name: "a known divergence without python= and go=", message: "KNOWN-DIVERGENCE CHAOS-7: no answers", refusal: "without python=<answer> go=<answer>"},
		{name: "a known divergence without go=", message: "KNOWN-DIVERGENCE CHAOS-7 python=404: one answer", refusal: "without python=<answer> go=<answer>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runVenueSkips(t, skipEvents(t, tc.message), knownLedgerText, "TestVenueX")
			if err == nil || !strings.Contains(stderr, tc.refusal) || !strings.Contains(stderr, "TestVenueX/row_a") {
				t.Fatalf("err %v, stderr %q, stdout %q: want the refusal %q", err, stderr, stdout, tc.refusal)
			}
		})
	}
	t.Run("a row the ledger does not hold", func(t *testing.T) {
		ledger := strings.Replace(knownLedgerText, "\trow_a\t", "\trow_b\t", 1)
		_, stderr, err := runVenueSkips(t, skipEvents(t, "KNOWN-DIVERGENCE CHAOS-7 python=404 go=401: x"), ledger, "TestVenueX")
		if err == nil || !strings.Contains(stderr, "that the ledger") {
			t.Fatalf("err %v, stderr %q: want the ledger refusal", err, stderr)
		}
	})
	t.Run("a ledger known divergence", func(t *testing.T) {
		stdout, stderr, err := runVenueSkips(t, skipEvents(t, "KNOWN-DIVERGENCE CHAOS-7 python=404 go=401: CHAOS-7, rule 1: x"), knownLedgerText, "TestVenueX")
		if err != nil || stderr != "" || !strings.Contains(stdout, "KNOWN CHAOS-7 TestVenueX/row_a python=404 go=401\n") ||
			!strings.Contains(stdout, "KNOWN-DIVERGENCE skips: 1 (tickets CHAOS-7)\n") {
			t.Fatalf("err %v, stderr %q, stdout %q: want the pass and the summary line", err, stderr, stdout)
		}
	})
	t.Run("no skip", func(t *testing.T) {
		stdout, stderr, err := runVenueSkips(t, skipEvents(t, ""), knownLedgerText, "TestVenueX")
		if err != nil || stderr != "" || strings.Contains(stdout, "KNOWN ") || !strings.HasSuffix(stdout, "\nKNOWN-DIVERGENCE skips: 0 (tickets none)\n") {
			t.Fatalf("err %v, stderr %q, stdout %q", err, stderr, stdout)
		}
	})
}

// The skip message venueoracle.KnownDivergenceSkip writes is the form the verb accepts: a real go test -json run of
// the venueoracle Diff cell passes with a ledger row for it, and is refused without one.
func TestVenueOracleSkipsReadTheKnownDivergenceSkipOfVenueoracle(t *testing.T) {
	cmd := exec.Command("go", "test", "-count=1", "-json", "-run", "^TestDiffSkipsADivergentKnownDivergence$", "./internal/testsupport/venueoracle")
	cmd.Dir = repoRoot(t)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go test venueoracle: %v\n%s", err, out)
	}
	ledger := "CHAOS-1\tTestDiffSkipsADivergentKnownDivergence\tgap\tthe cell's gap\n"
	stdout, stderr, err := runVenueSkips(t, string(out), ledger, "TestDiffSkipsADivergentKnownDivergence")
	if err != nil || !strings.Contains(stdout, "KNOWN CHAOS-1 TestDiffSkipsADivergentKnownDivergence/gap python=200 go=401\n") {
		t.Fatalf("err %v, stderr %q, stdout %q", err, stderr, stdout)
	}
	if _, _, err := runVenueSkips(t, string(out), knownLedgerText, "TestDiffSkipsADivergentKnownDivergence"); err == nil {
		t.Fatal("a known divergence the ledger does not hold passed")
	}
}

// The venue-oracles verb judges every discovered test's skips with venue_oracle_skips over the repository ledger and
// prints the summary line; no other skip check is left in it.
func TestVenueOraclesVerbJudgesSkipsByTheLedger(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "\ncheck_venue_oracles() {")
	if start < 0 {
		t.Fatal("check_go.sh has no check_venue_oracles")
	}
	end := strings.Index(src[start+1:], "\n}\n")
	body := src[start : start+1+end]
	for _, want := range []string{
		`local known_ledger="${ROOT}/internal/testsupport/venueoracle/testdata/known_divergences.tsv"`,
		`if ! known="$(venue_oracle_skips "${json_log}" "${known_ledger}" "${name}")"; then`,
		`| venue_oracle_known_summary`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("check_venue_oracles lacks %q", want)
		}
	}
	if strings.Contains(body, `select(.Action == "skip")`) {
		t.Error("check_venue_oracles still reads skip events itself, outside venue_oracle_skips")
	}
}

var knownLedgerRow = regexp.MustCompile(`^CHAOS-[0-9]+\tTest[A-Za-z0-9_]+\t[^\t]+\t[^\t]+$`)

// The repository ledger is well formed: ticket, test, row, reason; no row twice.
func TestKnownDivergenceLedgerRowsAreWellFormed(t *testing.T) {
	f, err := os.Open(filepath.Join(repoRoot(t), "internal", "testsupport", "venueoracle", "testdata", "known_divergences.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	seen := map[string]bool{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !knownLedgerRow.MatchString(line) {
			t.Errorf("not ticket, test, row, reason: %q", line)
			continue
		}
		key := strings.Join(strings.Split(line, "\t")[1:3], "/")
		if seen[key] {
			t.Errorf("row twice: %s", key)
		}
		seen[key] = true
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) == 0 {
		t.Error("the ledger holds no row")
	}
}
