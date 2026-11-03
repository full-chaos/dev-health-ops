package pushlifecyclecli

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile/reconcilejob"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const (
	canaryDSN    = "postgres://canary_login_77:canary_pw_58@db.invalid/x"
	canaryScope  = "canary:scope-61"
	canaryOrg    = "bbbbbbbb-1111-4111-8111-1111111111bb"
	canaryToken  = "cccccccc-2222-4222-8222-2222222222cc"
	canaryPrefix = "fcpush_canary"
)

type reconcileCall struct {
	schema string
	batch  int
	delay  time.Duration
}

func runReconcileVerb(t *testing.T, args []string, lookup map[string]string, report reconcilejob.Report, runErr error) (code int, out, errOut string, calls []reconcileCall) {
	t.Helper()
	oldConnect, oldReconcile := connectFn, reconcileFn
	t.Cleanup(func() { connectFn, reconcileFn = oldConnect, oldReconcile })
	connectFn = func(context.Context, string) (*pgxpool.Pool, error) { return nil, nil }
	reconcileFn = func(_ context.Context, _ *pgxpool.Pool, schema string, batch int, delay time.Duration) (reconcilejob.Report, error) {
		calls = append(calls, reconcileCall{schema, batch, delay})
		return report, runErr
	}
	var so, se bytes.Buffer
	env := cli.Env{Args: args, Stdout: &so, Stderr: &se, Lookup: func(k string) (string, bool) { v, ok := lookup[k]; return v, ok }}
	code = runReconcile(context.Background(), env)
	return code, so.String(), se.String(), calls
}

var cleanReport = reconcilejob.Report{Passes: 2, Tokens: 6, Projected: 6}

var driftReport = reconcilejob.Report{Passes: 2, Tokens: 7, Projected: 6, Transient: 1, Drift: []pushreconcile.Finding{
	{Class: pushreconcile.ClassStatus, ID: canaryToken},
	{Class: pushreconcile.ClassMissing, ID: "dddddddd-3333-4333-8333-3333333333dd"},
}}

var okLookup = map[string]string{"POSTGRES_URI": canaryDSN}

// C40: a clean store exits 0 and prints drift total=0.
func TestC40VerbCleanExitsZeroAndPrintsDriftTotal0(t *testing.T) {
	code, out, errOut, calls := runReconcileVerb(t, nil, okLookup, cleanReport, nil)
	if code != cli.ExitOK || len(calls) != 1 {
		t.Fatalf("code %d calls %d stderr %q", code, len(calls), errOut)
	}
	if !strings.Contains(out, "reconcile mode=read-only passes=2 domain_tokens=6 projected=6\n") {
		t.Fatalf("stdout %q lacks the mode line", out)
	}
	if !strings.Contains(out, "reconcile drift total=0 missing=0 extra=0 status=0 scopes=0 expiry=0 binding=0 unwireable=0 transient=0\n") {
		t.Fatalf("stdout %q lacks drift total=0", out)
	}
	if calls[0].schema != "auth" || calls[0].batch != 500 || calls[0].delay != 60*time.Second {
		t.Fatalf("defaults = %+v, want auth, 500, 60s", calls[0])
	}
}

// C40b: drift exits 1 and prints counts.
func TestC40bDriftExitsOneWithCountsOnly(t *testing.T) {
	code, out, _, _ := runReconcileVerb(t, nil, okLookup, driftReport, nil)
	if code != cli.ExitFailure {
		t.Fatalf("code %d, want %d on drift", code, cli.ExitFailure)
	}
	if !strings.Contains(out, "reconcile drift total=2 missing=1 extra=0 status=1 scopes=0 expiry=0 binding=0 unwireable=0 transient=1\n") {
		t.Fatalf("stdout %q", out)
	}
}

// C40c: -list adds one line per finding: class and token id.
func TestC40cListPrintsOnlyIdsAndClass(t *testing.T) {
	_, out, _, _ := runReconcileVerb(t, []string{"-list"}, okLookup, driftReport, nil)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("%d lines %q, want 2 count lines and 2 finding lines", len(lines), out)
	}
	if lines[2] != "status "+canaryToken || lines[3] != "missing dddddddd-3333-4333-8333-3333333333dd" {
		t.Fatalf("finding lines = %q", lines[2:])
	}
}

// C40d: nothing but counts is printed unless -list is given; no DSN, scope, organization, prefix or token id leaks.
func TestC40dOutputNeverContainsDsnScopeFingerprintOrg(t *testing.T) {
	for name, args := range map[string][]string{"counts": nil, "list": {"-list"}} {
		for _, runErr := range []error{nil, errors.New("read " + canaryDSN + " " + canaryScope + " " + canaryOrg + " " + canaryPrefix)} {
			_, out, errOut, _ := runReconcileVerb(t, args, okLookup, driftReport, runErr)
			for _, leak := range []string{canaryDSN, "canary_login_77", "canary_pw_58", canaryScope, canaryOrg, canaryPrefix} {
				if strings.Contains(out, leak) || strings.Contains(errOut, leak) {
					t.Errorf("%s (err %v): output leaks %q: %q %q", name, runErr != nil, leak, out, errOut)
				}
			}
			if name == "counts" && strings.Contains(out, canaryToken) {
				t.Errorf("counts mode prints a token id: %q", out)
			}
			if runErr == nil && name == "counts" {
				for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
					if !regexp.MustCompile(`^reconcile (mode=read-only|drift total=\d+)( [a-z_]+=\d+)+$`).MatchString(line) {
						t.Errorf("counts line %q is not a counts line", line)
					}
				}
			}
		}
	}
}

func TestC40eAReadErrorExitsOneWithOneFixedLine(t *testing.T) {
	code, out, errOut, _ := runReconcileVerb(t, nil, okLookup, reconcilejob.Report{}, errors.New("boom"))
	if code != cli.ExitFailure || out != "" {
		t.Fatalf("code %d stdout %q, want exit 1 and no stdout", code, out)
	}
	if strings.TrimSpace(errOut) != "reconcile: a read failed before the end of the run; no result" {
		t.Fatalf("stderr %q", errOut)
	}
}

func TestC40fFlagsReachTheRun(t *testing.T) {
	_, _, _, calls := runReconcileVerb(t, []string{"-batch", "7", "-confirm-delay", "0s"}, map[string]string{"POSTGRES_URI": canaryDSN, "AUTH_DATABASE_SCHEMA": "authx"}, cleanReport, nil)
	if len(calls) != 1 || calls[0] != (reconcileCall{"authx", 7, 0}) {
		t.Fatalf("calls = %+v", calls)
	}
}

// C41: the DSN variable forms.
func TestC41DsnEnvAndUnknownNameRefused(t *testing.T) {
	code, _, errOut, calls := runReconcileVerb(t, []string{"-dsn-env", "NOPE_URI"}, okLookup, cleanReport, nil)
	if code != cli.ExitFailure || len(calls) != 0 || !strings.Contains(errOut, "NOPE_URI") || strings.Contains(errOut, "postgres://") {
		t.Fatalf("code %d calls %d stderr %q", code, len(calls), errOut)
	}
	code, _, _, calls = runReconcileVerb(t, []string{"-dsn-env", "API_DATABASE_URI"}, map[string]string{"API_DATABASE_URI": canaryDSN}, cleanReport, nil)
	if code != cli.ExitOK || len(calls) != 1 {
		t.Fatalf("named variable: code %d calls %d", code, len(calls))
	}
	if code, _, _, calls = runReconcileVerb(t, nil, nil, cleanReport, nil); code != cli.ExitFailure || len(calls) != 0 {
		t.Fatalf("no DSN: code %d calls %d", code, len(calls))
	}
}

func TestC41bPoolOpenErrorHasAFixedMessage(t *testing.T) {
	oldConnect, oldReconcile := connectFn, reconcileFn
	t.Cleanup(func() { connectFn, reconcileFn = oldConnect, oldReconcile })
	connectFn = func(_ context.Context, dsn string) (*pgxpool.Pool, error) {
		return nil, errors.New("cannot parse `" + dsn + "`")
	}
	reconcileFn = func(context.Context, *pgxpool.Pool, string, int, time.Duration) (reconcilejob.Report, error) {
		t.Fatal("the reconcile must not run")
		return reconcilejob.Report{}, nil
	}
	var so, se bytes.Buffer
	env := cli.Env{Stdout: &so, Stderr: &se, Lookup: func(k string) (string, bool) { return canaryDSN, k == "POSTGRES_URI" }}
	if code := runReconcile(context.Background(), env); code != cli.ExitFailure {
		t.Fatalf("code %d", code)
	}
	for _, leak := range []string{"canary_login_77", "canary_pw_58", "postgres://"} {
		if strings.Contains(se.String(), leak) {
			t.Errorf("stderr leaks %q: %q", leak, se.String())
		}
	}
	if !strings.Contains(se.String(), "POSTGRES_URI") {
		t.Errorf("stderr does not name the variable: %q", se.String())
	}
}

// C42: bad flags are refused before any connection.
func TestC42BadFlagsRefusedBeforeConnecting(t *testing.T) {
	for _, args := range [][]string{{"extra"}, {"-batch", "0"}, {"-batch", "1001"}, {"-dsn-env", "lower"}, {"-confirm-delay", "-1s"}, {"-nope"}} {
		if code, _, _, calls := runReconcileVerb(t, args, okLookup, cleanReport, nil); code != cli.ExitUsage || len(calls) != 0 {
			t.Errorf("args %v: code %d calls %d, want usage and no run", args, code, len(calls))
		}
	}
}

func TestC42bTheGroupListsTheReconcileVerbAsReadOnly(t *testing.T) {
	var found bool
	for _, child := range Command().Children {
		if child.Name == "reconcile" {
			found = true
			if child.Run == nil || !strings.Contains(child.Summary, "read-only") {
				t.Errorf("verb %+v", child)
			}
		}
	}
	if !found {
		t.Fatal("no reconcile verb in the push-lifecycle group")
	}
}
