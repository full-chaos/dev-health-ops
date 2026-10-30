package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

// fakeEnable records the calls; plan and err are what both methods return.
type fakeEnable struct {
	plan          workload.EnablePlan
	err           error
	plans, writes []workload.EnableInput
	names         []string
}

func (f *fakeEnable) PlanEnable(_ context.Context, name string, in workload.EnableInput) (workload.EnablePlan, error) {
	f.plans = append(f.plans, in)
	f.names = append(f.names, name)
	return f.plan, f.err
}

func (f *fakeEnable) EnableWorkload(_ context.Context, name string, in workload.EnableInput) (workload.EnablePlan, error) {
	f.writes = append(f.writes, in)
	f.names = append(f.names, name)
	return f.plan, f.err
}

func TestParseEnableArgs(t *testing.T) {
	op := uuid.NewString()
	ok := map[string][]string{
		"dry run":            {"enable", "-workload", "api", "-operator", op},
		"apply":              {"enable", "-workload", "api", "-operator", op, "-apply"},
		"with the principal": {"enable", "-workload", "api", "-operator", op, "-enable-principal", "-apply"},
		"flag set to false":  {"enable", "-workload", "api", "-operator", op, "-enable-principal=false"},
	}
	for name, args := range ok {
		if _, err := parseWorkloadsArgs(args, io.Discard); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	p := mustParse(t, "enable", "-workload", "api", "-operator", op, "-enable-principal")
	if p.verb != "enable" || p.name != "api" || p.operator.String() != op || !p.enablePrincipal || p.apply {
		t.Fatalf("parsed %+v", p)
	}
	if p := mustParse(t, "enable", "-workload", "api", "-operator", op); p.enablePrincipal {
		t.Fatal("-enable-principal is on by default")
	}
	bad := map[string][]string{
		"no operator":              {"enable", "-workload", "api"},
		"empty operator":           {"enable", "-workload", "api", "-operator", ""},
		"operator not a uuid":      {"enable", "-workload", "api", "-operator", "admin"},
		"operator nil uuid":        {"enable", "-workload", "api", "-operator", uuid.Nil.String()},
		"operator upper case":      {"enable", "-workload", "api", "-operator", strings.ToUpper(op)},
		"operator braced":          {"enable", "-workload", "api", "-operator", "{" + op + "}"},
		"no workload":              {"enable", "-operator", op},
		"bad workload":             {"enable", "-workload", "", "-operator", op},
		"takes no -out":            {"enable", "-workload", "api", "-operator", op, "-out", "v"},
		"takes no -credential":     {"enable", "-workload", "api", "-operator", op, "-credential", uuid.NewString()},
		"positional argument":      {"enable", "-workload", "api", "-operator", op, "extra"},
		"flag on another verb":     {"bootstrap-disable", "-workload", "api", "-enable-principal"},
		"operator on another verb": {"bootstrap-disable", "-workload", "api", "-operator", op},
	}
	for name, args := range bad {
		if _, err := parseWorkloadsArgs(args, io.Discard); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestEnableArgumentErrorsComeBeforeTheEnvironment(t *testing.T) {
	lookup := func(string) (string, bool) {
		t.Fatal("the environment was read for a bad command line")
		return "", false
	}
	if code := executeWorkloads(t.Context(), []string{"enable", "-workload", "api"}, lookup, io.Discard, io.Discard); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
}

// TestEnablePassesTheOperatorAndTheFlag: the dry run calls only PlanEnable,
// -apply only EnableWorkload, each with the operator and the flag as given.
func TestEnablePassesTheOperatorAndTheFlag(t *testing.T) {
	op := uuid.New()
	for _, apply := range []bool{false, true} {
		for _, flag := range []bool{false, true} {
			args := []string{"enable", "-workload", "api", "-operator", op.String()}
			if flag {
				args = append(args, "-enable-principal")
			}
			if apply {
				args = append(args, "-apply")
			}
			store := &fakeEnable{}
			if code := runEnable(t.Context(), store, mustParse(t, args...), io.Discard, io.Discard); code != 0 {
				t.Fatalf("apply=%t flag=%t exit %d", apply, flag, code)
			}
			calls, other := store.plans, store.writes
			if apply {
				calls, other = store.writes, store.plans
			}
			want := workload.EnableInput{Operator: op, EnablePrincipal: flag}
			if len(calls) != 1 || len(other) != 0 || calls[0] != want || store.names[0] != "api" {
				t.Fatalf("apply=%t flag=%t: plans %v writes %v names %v", apply, flag, store.plans, store.writes, store.names)
			}
		}
	}
}

func TestEnableReportsThePlannedAndTheDoneChanges(t *testing.T) {
	id := uuid.New()
	op := uuid.NewString()
	both := workload.EnablePlan{PrincipalID: id, RowDisabled: true, PrincipalDisabled: true, EnableRow: true, EnablePrincipal: true}
	cases := map[string]struct {
		apply      bool
		plan       workload.EnablePlan
		want, deny []string
	}{
		"dry run, both": {false, both,
			[]string{"would enable: workload row", "would enable: principal " + id.String() + " (kind workload)", "no change written", "row=disabled principal=disabled"},
			[]string{"enabled: ", "minted"}},
		"apply, both": {true, both,
			[]string{"enabled: workload row", "enabled: principal " + id.String(), "no credential was minted: run bootstrap-rotate (or bootstrap-create) next", "operator=" + op},
			[]string{"would enable"}},
		"apply, row only": {true, workload.EnablePlan{PrincipalID: id, RowDisabled: true, EnableRow: true},
			[]string{"enabled: workload row", "no credential was minted"},
			[]string{"principal " + id.String()}},
		"apply, nothing": {true, workload.EnablePlan{PrincipalID: id},
			[]string{"nothing to enable (no change written)"},
			[]string{"enabled: ", "minted"}},
	}
	for name, c := range cases {
		args := []string{"enable", "-workload", "api", "-operator", op, "-enable-principal"}
		if c.apply {
			args = append(args, "-apply")
		}
		var stdout, stderr bytes.Buffer
		if code := runEnable(t.Context(), &fakeEnable{plan: c.plan}, mustParse(t, args...), &stdout, &stderr); code != 0 {
			t.Fatalf("%s: exit %d %q", name, code, stderr.String())
		}
		for _, w := range c.want {
			if !strings.Contains(stdout.String(), w) {
				t.Errorf("%s: stdout %q lacks %q", name, stdout.String(), w)
			}
		}
		for _, d := range c.deny {
			if strings.Contains(stdout.String(), d) {
				t.Errorf("%s: stdout %q holds %q", name, stdout.String(), d)
			}
		}
	}
}

// TestEnableRefusalsAreNamed: each refusal has its own class; a disabled
// principal without the flag names the flag.
func TestEnableRefusalsAreNamed(t *testing.T) {
	cases := map[error]string{
		workload.ErrNotFound:                   "(workload not registered)",
		workload.ErrOperatorNotFound:           "(the operator principal does not exist)",
		workload.ErrOperatorDisabled:           "(the operator principal is disabled)",
		workload.ErrOperatorIsSubject:          "(the operator cannot be the workload's own principal)",
		workload.ErrBootstrapPrincipalDisabled: "(the workload's principal is disabled; add -enable-principal to enable it too)",
		workload.ErrPrincipalNotWorkload:       "(the workload's principal is not of kind workload; -enable-principal refuses it)",
		workload.ErrEnableStateChanged:         "(the workload changed during the run; run again)",
		workload.ErrEnableLiveBootstrap:        "(the workload holds a live bootstrap value; revoke it first (bootstrap-disable -workload NAME -apply, or bootstrap-revoke), then enable)",
		workload.ErrStore:                      "(store unavailable)",
	}
	for refusal, want := range cases {
		for _, apply := range []bool{false, true} {
			args := []string{"enable", "-workload", "api", "-operator", uuid.NewString()}
			if apply {
				args = append(args, "-apply")
			}
			var stdout, stderr bytes.Buffer
			if code := runEnable(t.Context(), &fakeEnable{err: refusal}, mustParse(t, args...), &stdout, &stderr); code != 1 {
				t.Fatalf("%v apply=%t exit %d", refusal, apply, code)
			}
			if !strings.Contains(stderr.String(), want) || stdout.Len() != 0 {
				t.Errorf("%v apply=%t: stderr %q stdout %q, want %q", refusal, apply, stderr.String(), stdout.String(), want)
			}
		}
	}
	// Rotate is refused while the workload is disabled, so the live-value
	// refusal names revoke only (D6505).
	var stderr bytes.Buffer
	runEnable(t.Context(), &fakeEnable{err: workload.ErrEnableLiveBootstrap}, mustParse(t, "enable", "-workload", "api", "-operator", uuid.NewString()), io.Discard, &stderr)
	if strings.Contains(stderr.String(), "rotate") || strings.Contains(stderr.String(), "create") {
		t.Errorf("the live-value refusal names a verb refused while disabled: %q", stderr.String())
	}
	if errors.Is(workload.ErrPrincipalNotWorkload, workload.ErrBootstrapPrincipalDisabled) {
		t.Fatal("the two principal refusals are one error")
	}
}

func TestTheUsageNamesTheEnableVerbAndThatItMintsNothing(t *testing.T) {
	for _, want := range []string{"enable -workload N -operator ID [-enable-principal] [-apply]", "enable mints no credential"} {
		if !strings.Contains(workloadsUsage, want) {
			t.Errorf("the usage text lacks %q", want)
		}
	}
}
