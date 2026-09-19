package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

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
		args := append([]string{"prove", "-report", reportPath, "-artifact-dir", dir}, good...)
		args = append(args, bad...)
		resetFlagsForTest(t)
		restore := setOSArgs(t, args)
		var err error
		stdout := captureStdout(t, func() { err = execute(parseFlags()) })
		restore()
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
	resetFlagsForTest(t)
	restore := setOSArgs(t, []string{"prove", "-h"})
	defer restore()
	if err := execute(parseFlags()); err != nil {
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
// a FIFO whose reader opens only after the run deadline has passed: the
// run reaches the end of its loop with the run live, blocks writing the
// report, and the deadline passes during the write. The report the
// reader finally receives, and the error the run exits with, both name
// the run deadline.
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
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if cancelDuringWrite {
				ctx, cancel = context.WithCancel(context.Background())
				defer cancel()
			}
			reports := make(chan []byte, 2)
			readerDone := make(chan struct{})
			go func() {
				defer close(readerDone)
				// Opened only after the run has certainly reached its report.
				time.Sleep(4 * time.Second)
				if cancelDuringWrite {
					cancel()
				}
				for i := 0; i < 2; i++ {
					raw, err := os.ReadFile(fifo)
					if err != nil {
						return
					}
					reports <- raw
				}
			}()
			var runErr error
			stdout := captureStdout(t, func() {
				runErr = runMeasurement(ctx, goapiproof.NewLegClient(0), f, staticCredentialForTest(), staticCredentialForTest(), sameProverBuildForTest(build), nil, artifacts)
			})
			wantErr, wantCause := context.DeadlineExceeded, exitStoppedByRunDeadline
			if cancelDuringWrite {
				wantErr, wantCause = context.Canceled, exitStoppedBySignal
			}
			if !errors.Is(runErr, wantErr) {
				t.Fatalf("err = %v, want %v: the run ended during its report write", runErr, wantErr)
			}
			// The run returns once its last write has gone into the pipe;
			// the reader may still be receiving it.
			select {
			case <-readerDone:
			case <-time.After(10 * time.Second):
				t.Fatal("the report reader did not finish")
			}
			var last jsonReport
			for len(reports) > 0 {
				if err := json.Unmarshal(<-reports, &last); err != nil {
					t.Fatal(err)
				}
			}
			if last.ExitCause != wantCause {
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

// TestMainWritesTheReportOnAFlagItCannotParse runs this test binary as
// the command itself (main(), not a helper), with a flag the flag package
// cannot parse after -report: the process exits non-zero having written
// a fresh refused_before_measuring report, instead of exiting inside
// flag.Parse with nothing written.
func TestMainWritesTheReportOnAFlagItCannotParse(t *testing.T) {
	if os.Getenv("GO_API_REST_PROVE_RUN_MAIN") == "1" {
		os.Args = append([]string{"go-api-rest-prove"}, strings.Split(os.Getenv("GO_API_REST_PROVE_ARGS"), "\n")...)
		main()
		return
	}
	dir := t.TempDir()
	reportPath := dir + "/report.json"
	if err := os.WriteFile(reportPath, []byte(`{"outcomes":[],"exit_cause":"completed"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainWritesTheReportOnAFlagItCannotParse$")
	cmd.Env = append(os.Environ(), "GO_API_REST_PROVE_RUN_MAIN=1",
		"GO_API_REST_PROVE_ARGS="+strings.Join([]string{"-report", reportPath, "-run-deadline", "soon"}, "\n"))
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("main exited %v, want a non-zero exit: %s", err, out)
	}
	report := readReport(t, reportPath)
	if report.ExitCause != exitRefusedBeforeMeasuring || !report.Partial {
		t.Fatalf("report exit_cause=%q partial=%v, want a fresh refused_before_measuring report (output: %s)", report.ExitCause, report.Partial, out)
	}
	accountFor(t, report)
}
