package restprove

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func readReport(t *testing.T, path string) jsonReport {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}
	return report
}

// TestEveryConfigurationRefusalWritesAFreshReport runs execute(parseFlags())
// -- main's own path -- over every way a configuration is refused after
// -report is parsed: a flag the flag package cannot parse, an unknown
// flag, a missing required flag, an invalid -run-deadline, a malformed
// -bind and a base URL that would change the request. Each replaces a
// stale report with one naming every corpus request in not_run, prints
// the same accounting on stdout, and exits non-zero.
func TestEveryConfigurationRefusalWritesAFreshReport(t *testing.T) {
	good := []string{"-org", "org-1", "-recorded-by", "r", "-review-evidence", "e", "-dry-run",
		"-candidate-bearer-exec", `["true"]`, "-baseline-bearer-exec", `["true"]`, "-python-api-url", "http://api:8000"}
	cells := map[string][]string{
		"unparseable duration":  {"-run-deadline", "soon"},
		"unknown flag":          {"-no-such-flag"},
		"missing required flag": {"-org", ""},
		"run deadline zero":     {"-run-deadline", "0s"},
		"run deadline negative": {"-run-deadline", "-1m"},
		"malformed bind":        {"-bind", "no-equals-sign"},
		"base URL with a query": {"-python-api-url", "http://api:8000/?x=1"},
	}
	for name, bad := range cells {
		dir := t.TempDir()
		reportPath := dir + "/report.json"
		if err := os.WriteFile(reportPath, []byte(`{"outcomes":[{"operation":"stale"}],"partial":false,"exit_cause":"completed"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		args := append([]string{"-report", reportPath, "-artifact-dir", dir}, good...)
		args = append(args, bad...)
		var err error
		stdout := captureStdout(t, func() { err = execute(parseFlags(args)) })
		if err == nil {
			t.Errorf("%s: execute returned nil, want the refusal", name)
			continue
		}
		report := readReport(t, reportPath)
		if report.ExitCause != exitRefusedBeforeMeasuring || report.PartialCause != partialCauseRefusedBeforeMeasuring || !report.Partial || report.PartialError == "" {
			t.Errorf("%s: report exit_cause=%q partial_cause=%q partial=%v partial_error=%q, want a fresh refused_before_measuring report", name, report.ExitCause, report.PartialCause, report.Partial, report.PartialError)
		}
		accountFor(t, report)
		if !strings.Contains(stdout, "exit_cause="+exitRefusedBeforeMeasuring) || !strings.Contains(stdout, "never attempted: [") {
			t.Errorf("%s: stdout does not carry the accounting: %q", name, stdout)
		}
	}
}

// TestHelpIsNotARefusal: -h prints usage and exits zero, writing nothing.
func TestHelpIsNotARefusal(t *testing.T) {
	if err := execute(parseFlags([]string{"-h"})); err != nil {
		t.Fatalf("-h: %v", err)
	}
}

// TestStdoutCarriesTheAccountingWithoutReport: with no -report, a run
// refused before its first request still prints every corpus request it
// did not attempt, the partial cause and the exit cause on stdout.
func TestStdoutCarriesTheAccountingWithoutReport(t *testing.T) {
	dir := t.TempDir()
	f := flags{queryAPIURL: "http://127.0.0.1:1", pythonAPIURL: "http://127.0.0.1:1", queryAPISrc: dir + "/no-such-query-api-source",
		org: "org-1", recordedBy: "r", reviewEvidence: "e", artifactDir: dir + "/artifacts", dryRun: true,
		timeout: time.Second, runDeadline: time.Minute}
	var err error
	stdout := captureStdout(t, func() { err = execute(f, nil) })
	if err == nil {
		t.Fatal("want the refusal")
	}
	keys := corpusRequestKeys(t)
	if !strings.Contains(stdout, "partial run: "+strconv.Itoa(len(keys))+" request(s) never attempted: [") {
		t.Fatalf("stdout does not name the %d corpus requests: %q", len(keys), stdout)
	}
	for _, key := range keys {
		if !strings.Contains(stdout, key) {
			t.Fatalf("stdout does not name %q", key)
		}
	}
	if !strings.Contains(stdout, "partial_cause="+partialCauseRefusedBeforeMeasuring) || !strings.Contains(stdout, "exit_cause="+exitRefusedBeforeMeasuring) {
		t.Fatalf("stdout lacks the causes: %q", stdout)
	}
}

// TestTheRunEndingWhileItsReportIsWrittenEndsTheRun makes the report path
// a FIFO. The run reaches the end of its loop with the run live and opens
// the FIFO to write its report; only that open lets the reader's open
// return, and only then does the test end the run. The run re-reads its
// context after the write, so it writes a second report naming the end.
// The reader holds its own write end open for the whole run, so every
// report the run writes lands in ONE stream and no report depends on
// whether the reader reopened the FIFO in time; the test closes that
// write end once the run has returned, and the reader then reads to EOF.
// No step waits on a timer.
func TestTheRunEndingWhileItsReportIsWrittenEndsTheRun(t *testing.T) {
	for name, cancelDuringWrite := range map[string]bool{"run deadline": false, "signal": true} {
		t.Run(name, func(t *testing.T) {
			const build = "build123"
			candidate := httptest.NewServer(genericRESTStubHandler(t, build, nil))
			defer candidate.Close()
			baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", nil)))
			defer baseline.Close()
			dir := t.TempDir()
			fifo := dir + "/report.json"
			if err := syscall.Mkfifo(fifo, 0o600); err != nil {
				t.Fatalf("mkfifo: %v", err)
			}
			artifacts, err := goapiproof.NewArtifactStore(dir + "/artifacts")
			if err != nil {
				t.Fatal(err)
			}
			f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "r", reviewEvidence: "e",
				timeout: time.Minute, runDeadline: 3 * time.Second, dryRun: true, reportPath: fifo}
			wantErr, wantCause := context.DeadlineExceeded, exitStoppedByRunDeadline
			if cancelDuringWrite {
				wantErr, wantCause = context.Canceled, exitStoppedBySignal
			}
			ctx := newEndableContext()
			keepAlive := make(chan *os.File, 1)
			var reports []jsonReport
			var readErr error
			readerDone := make(chan struct{})
			go func() {
				defer close(readerDone)
				defer close(keepAlive)
				// Blocks until the run opens the write end: its loop is
				// over and it is writing its report.
				stream, err := os.Open(fifo)
				if err != nil {
					readErr = err
					return
				}
				defer stream.Close()
				// A reader exists, so this open returns at once. While it
				// is open the stream never reaches EOF between the run's
				// writes, and each later write of the run joins it.
				writer, err := os.OpenFile(fifo, os.O_WRONLY, 0)
				if err != nil {
					readErr = err
					return
				}
				keepAlive <- writer
				ctx.end(wantErr)
				decoder := json.NewDecoder(stream)
				for {
					var report jsonReport
					if err := decoder.Decode(&report); err != nil {
						if !errors.Is(err, io.EOF) {
							readErr = err
						}
						return
					}
					reports = append(reports, report)
				}
			}()
			var runErr error
			stdout := captureStdout(t, func() {
				runErr = runMeasurement(ctx, goapiproof.NewLegClient(0), f, staticCredentialForTest(), staticCredentialForTest(), sameProverBuildForTest(build), nil, artifacts)
			})
			// Every write of the run has gone into the pipe; closing the
			// reader's own write end lets it read the rest and reach EOF.
			if writer, ok := <-keepAlive; ok {
				_ = writer.Close()
			}
			awaitEvent(t, readerDone, "the report reader reaching the end of the stream")
			if readErr != nil {
				t.Fatalf("read the reports: %v", readErr)
			}
			if !errors.Is(runErr, wantErr) {
				t.Fatalf("err = %v, want %v: the run ended during its report write", runErr, wantErr)
			}
			if len(reports) != 2 {
				t.Fatalf("the run wrote %d report(s), want 2: the report of the run as it reached its loop's end, then the one naming the end", len(reports))
			}
			if first := reports[0].ExitCause; first == exitStoppedByRunDeadline || first == exitStoppedBySignal {
				t.Fatalf("the first report says exit_cause=%q: the run was live when it began the write", first)
			}
			if last := reports[1]; last.ExitCause != wantCause {
				t.Fatalf("the last report written says exit_cause=%q, want %q", last.ExitCause, wantCause)
			}
			if !strings.Contains(stdout, "exit_cause="+wantCause) {
				t.Fatalf("stdout does not end with exit_cause=%s", wantCause)
			}
		})
	}
}

// TestExitCauseForEveryRunEnding pins each exit cause of a run that
// reached the end of its loop and their precedence: the run context's own
// state first; a request's own timeout in an error chain is a tool error.
func TestExitCauseForEveryRunEnding(t *testing.T) {
	cells := []struct {
		name             string
		runEnded, runErr error
		legs, vac        []string
		want             string
		wantErr          bool
	}{
		{"clean", nil, nil, nil, nil, exitCompleted, false},
		{"leg never answered", nil, nil, []string{"a"}, nil, exitCompletedWithLegsThatNeverAnswered, true},
		{"vacuous declaration", nil, nil, nil, []string{"a"}, exitCompletedWithVacuousDeclarations, true},
		{"leg failure outranks vacuity", nil, nil, []string{"a"}, []string{"b"}, exitCompletedWithLegsThatNeverAnswered, true},
		{"signal", context.Canceled, errors.New("run interrupted"), []string{"a"}, nil, exitStoppedBySignal, true},
		{"run deadline", context.DeadlineExceeded, errors.New("run interrupted"), nil, nil, exitStoppedByRunDeadline, true},
		{"a request's own timeout is a tool error", nil, context.DeadlineExceeded, nil, nil, exitAbortedByToolError, true},
		{"tool error", nil, errors.New("store artifact: permission denied"), []string{"a"}, nil, exitAbortedByToolError, true},
	}
	for _, cell := range cells {
		got, err := exitCauseFor(cell.runEnded, cell.runErr, cell.legs, cell.vac)
		if got != cell.want || (err != nil) != cell.wantErr {
			t.Errorf("%s: exitCauseFor = %q, %v; want %q, err=%v", cell.name, got, err, cell.want, cell.wantErr)
		}
	}
}

// TestCommandWritesTheReportOnAFlagItCannotParse drives Command().Run --
// the dho binary's own entry point for `goapi rest-prove`, not a helper --
// with a flag the flag package cannot parse after -report: the run exits
// non-zero having written a fresh refused_before_measuring report, instead
// of exiting inside flag.Parse with nothing written.
//
// Before the S1 fold this drove main() through a re-exec'd subprocess,
// because main() called os.Exit directly. Command().Run returns an int
// instead, so the same claim is now provable in-process.
func TestCommandWritesTheReportOnAFlagItCannotParse(t *testing.T) {
	dir := t.TempDir()
	reportPath := dir + "/report.json"
	if err := os.WriteFile(reportPath, []byte(`{"outcomes":[],"exit_cause":"completed"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	code := Command().Run(context.Background(), cli.Env{
		Args:   []string{"-report", reportPath, "-run-deadline", "soon"},
		Stdout: io.Discard,
		Stderr: &stderr,
	})
	if code == cli.ExitOK {
		t.Fatalf("Command().Run = %d, want a non-zero exit: %s", code, stderr.String())
	}
	report := readReport(t, reportPath)
	if report.ExitCause != exitRefusedBeforeMeasuring || !report.Partial {
		t.Fatalf("report exit_cause=%q partial=%v, want a fresh refused_before_measuring report (stderr: %s)", report.ExitCause, report.Partial, stderr.String())
	}
	accountFor(t, report)
}
