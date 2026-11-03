package main

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

const bindingTestUID = "6f1c3c2e-0b8e-4d2a-9d7e-2f4a5b6c7d8e"

var bindingTuple = []string{"-trust-domain", "cluster.local", "-issuer", "https://kubernetes.default.svc", "-namespace", "dev-health", "-service-account", "api"}

func withArgs(verb string, more ...string) []string {
	return append(append([]string{verb}, bindingTuple...), more...)
}

func TestParseBindingVerbs(t *testing.T) {
	ok := map[string][]string{
		"rebind dry run":       withArgs("rebind", "-uid", bindingTestUID),
		"rebind apply":         withArgs("rebind", "-uid", bindingTestUID, "-apply"),
		"binding-disable":      withArgs("binding-disable"),
		"binding-enable apply": withArgs("binding-enable", "-apply"),
		"disable":              {"disable", "-workload", "api"},
		"disable apply":        {"disable", "-workload", "api", "-apply"},
	}
	for name, args := range ok {
		if _, err := parseWorkloadsArgs(args, io.Discard); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	bad := map[string][]string{
		"rebind without uid":            withArgs("rebind"),
		"rebind with a bad uid":         withArgs("rebind", "-uid", "x"),
		"rebind with an upper-case uid": withArgs("rebind", "-uid", strings.ToUpper(bindingTestUID)),
		"rebind with a workload":        withArgs("rebind", "-uid", bindingTestUID, "-workload", "api"),
		"rebind with an environment":    withArgs("rebind", "-uid", bindingTestUID, "-environment", "prod"),
		"rebind missing issuer":         {"rebind", "-trust-domain", "t", "-namespace", "n", "-service-account", "s", "-uid", bindingTestUID},
		"binding-disable with a uid":    withArgs("binding-disable", "-uid", bindingTestUID),
		"binding-enable without tuple":  {"binding-enable"},
		"disable without a workload":    {"disable"},
		"disable with a tuple field":    {"disable", "-workload", "api", "-namespace", "n"},
		"disable with an operator":      {"disable", "-workload", "api", "-operator", bindingTestUID},
		"disable with a bad name":       {"disable", "-workload", "Bad Name"},
	}
	for name, args := range bad {
		if _, err := parseWorkloadsArgs(args, io.Discard); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestWorkloadsUsageNamesTheNewVerbs(t *testing.T) {
	for _, verb := range []string{"rebind", "binding-disable|binding-enable", "disable -workload N"} {
		if !strings.Contains(workloadsUsage, verb) {
			t.Errorf("usage does not name %q", verb)
		}
	}
}

type fakeBindings struct {
	info    workload.BindingInfo
	lookup  error
	rebind  error
	calls   []string
	changed bool
}

func (f *fakeBindings) LookupBinding(context.Context, workload.BindingTuple) (workload.BindingInfo, error) {
	return f.info, f.lookup
}

func (f *fakeBindings) RebindBinding(_ context.Context, _ workload.BindingTuple, uid string, _ workload.Actor) (workload.BindingInfo, bool, error) {
	f.calls = append(f.calls, "rebind:"+uid)
	return f.info, f.changed, f.rebind
}

func (f *fakeBindings) DisableBinding(context.Context, uuid.UUID, workload.Actor) error {
	f.calls = append(f.calls, "disable")
	return nil
}

func (f *fakeBindings) EnableBinding(context.Context, uuid.UUID, workload.Actor) error {
	f.calls = append(f.calls, "enable")
	return nil
}

func mustParseBinding(t *testing.T, args []string) workloadsArgs {
	t.Helper()
	parsed, err := parseWorkloadsArgs(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestRunBindingDryRunWritesNothing(t *testing.T) {
	for _, args := range [][]string{withArgs("rebind", "-uid", bindingTestUID), withArgs("binding-disable"), withArgs("binding-enable")} {
		f := &fakeBindings{info: workload.BindingInfo{ID: uuid.New(), Workload: "api"}}
		var out, errb strings.Builder
		if code := runBinding(t.Context(), f, mustParseBinding(t, args), &out, &errb); code != 0 {
			t.Fatalf("%s: exit %d: %s", args[0], code, errb.String())
		}
		if len(f.calls) != 0 || !strings.Contains(out.String(), "dry run") {
			t.Errorf("%s: calls %v out %q", args[0], f.calls, out.String())
		}
	}
}

func TestRunBindingApplyCallsTheStoreAndPrintsNoUID(t *testing.T) {
	f := &fakeBindings{info: workload.BindingInfo{ID: uuid.New(), Workload: "api", Disabled: true}, changed: true}
	var out, errb strings.Builder
	if code := runBinding(t.Context(), f, mustParseBinding(t, withArgs("rebind", "-uid", bindingTestUID, "-apply")), &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if len(f.calls) != 1 || f.calls[0] != "rebind:"+bindingTestUID {
		t.Fatalf("calls %v", f.calls)
	}
	if strings.Contains(out.String(), bindingTestUID) || !strings.Contains(out.String(), "state=disabled") || !strings.Contains(out.String(), "changed=true") {
		t.Errorf("out %q", out.String())
	}
	f.calls = nil
	for verb, want := range map[string]string{"binding-disable": "disable", "binding-enable": "enable"} {
		if code := runBinding(t.Context(), f, mustParseBinding(t, withArgs(verb, "-apply")), io.Discard, &errb); code != 0 {
			t.Fatalf("%s exit %d", verb, code)
		}
		if got := f.calls[len(f.calls)-1]; got != want {
			t.Errorf("%s called %q", verb, got)
		}
	}
}

func TestRunBindingRefusals(t *testing.T) {
	cases := map[string]*fakeBindings{
		"unknown binding": {lookup: workload.ErrBindingNotFound},
		"store down":      {lookup: workload.ErrStore},
	}
	for name, f := range cases {
		var errb strings.Builder
		if code := runBinding(t.Context(), f, mustParseBinding(t, withArgs("rebind", "-uid", bindingTestUID, "-apply")), io.Discard, &errb); code != 1 {
			t.Errorf("%s: exit %d", name, code)
		}
		if len(f.calls) != 0 {
			t.Errorf("%s: wrote %v", name, f.calls)
		}
	}
	f := &fakeBindings{rebind: workload.ErrRebindStateChanged}
	var errb strings.Builder
	if code := runBinding(t.Context(), f, mustParseBinding(t, withArgs("rebind", "-uid", bindingTestUID, "-apply")), io.Discard, &errb); code != 1 || !strings.Contains(errb.String(), "changed during the run") {
		t.Errorf("state changed: %d %q", code, errb.String())
	}
	if !strings.Contains(failureClass(workload.ErrBindingNotFound), "binding not found") {
		t.Error("failureClass lacks binding not found")
	}
}

type fakeRow struct {
	info     workload.Info
	lookup   error
	disabled int
}

func (f *fakeRow) Lookup(context.Context, string) (*workload.Info, error) {
	if f.lookup != nil {
		return nil, f.lookup
	}
	return &f.info, nil
}

func (f *fakeRow) Disable(context.Context, string, workload.Actor) error { f.disabled++; return nil }

func TestRunDisable(t *testing.T) {
	f := &fakeRow{}
	var out, errb strings.Builder
	if code := runDisable(t.Context(), f, mustParseBinding(t, []string{"disable", "-workload", "api"}), &out, &errb); code != 0 || f.disabled != 0 || !strings.Contains(out.String(), "dry run") {
		t.Fatalf("dry run: %d %d %q", code, f.disabled, out.String())
	}
	out.Reset()
	if code := runDisable(t.Context(), f, mustParseBinding(t, []string{"disable", "-workload", "api", "-apply"}), &out, &errb); code != 0 || f.disabled != 1 {
		t.Fatalf("apply: %d %d", code, f.disabled)
	}
	g := &fakeRow{lookup: workload.ErrNotFound}
	if code := runDisable(t.Context(), g, mustParseBinding(t, []string{"disable", "-workload", "nope", "-apply"}), io.Discard, &errb); code != 1 || g.disabled != 0 {
		t.Fatalf("unknown: %d %d", code, g.disabled)
	}
}

func TestRebindRefusesAnEmptyOrBlankUIDAtTheParser(t *testing.T) {
	for name, uid := range map[string]string{"empty": "", "whitespace only": " ", "tab": "\t"} {
		_, err := parseWorkloadsArgs(withArgs("rebind", "-uid", uid), io.Discard)
		if err == nil {
			t.Errorf("%s uid accepted", name)
			continue
		}
		_, missing := parseWorkloadsArgs(withArgs("rebind"), io.Discard)
		if missing == nil || err.Error() != missing.Error() {
			t.Errorf("%s uid: message %q differs from the missing -uid message %v", name, err, missing)
		}
	}
}

func TestRebindEmptyUIDDryRunExitsTwoBeforeAnyStoreCall(t *testing.T) {
	lookup := func(string) (string, bool) {
		t.Fatal("the environment was read for a rebind with an empty -uid")
		return "", false
	}
	var stderr strings.Builder
	if code := executeWorkloads(t.Context(), withArgs("rebind", "-uid", ""), lookup, io.Discard, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2 (stderr %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "rebind needs -uid") {
		t.Errorf("stderr %q does not name the -uid refusal", stderr.String())
	}
}
