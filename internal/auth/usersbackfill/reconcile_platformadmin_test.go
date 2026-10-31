package usersbackfill

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

type fakeAdminTarget struct {
	none bool
	err  error
}

func (f fakeAdminTarget) NoActivePlatformAdmin(context.Context) (bool, error) { return f.none, f.err }

func TestNoActivePlatformAdminIsGatedOnlyWithTheFlag(t *testing.T) {
	for _, tc := range []struct {
		name      string
		none, set bool
		count     int
		gated     int
		prefix    string
	}{
		{"none held, flag", true, true, 1, 1, "diff."},
		{"none held, no flag", true, false, 1, 0, "info."},
		{"one held, flag", false, true, 0, 0, "diff."},
		{"one held, no flag", false, false, 0, 0, "info."},
	} {
		rep := Reconciliation{Diffs: map[string]int{}}
		if err := ReconcilePlatformAdmin(context.Background(), fakeAdminTarget{none: tc.none}, tc.set, &rep); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if rep.NoActivePlatformAdmin != tc.count || rep.GatedTotal() != tc.gated {
			t.Fatalf("%s: count %d gated %d", tc.name, rep.NoActivePlatformAdmin, rep.GatedTotal())
		}
		want := tc.prefix + DiffNoActivePlatformAdmin + "=" + map[int]string{0: "0", 1: "1"}[tc.count]
		other := "info."
		if tc.prefix == "info." {
			other = "diff."
		}
		out := strings.Join(rep.Lines(), "\n")
		if !strings.Contains(out, want) || strings.Contains(out, other+DiffNoActivePlatformAdmin+"=") {
			t.Fatalf("%s: lines\n%s", tc.name, out)
		}
	}
}

func TestNoActivePlatformAdminReadFailureNamesNoValue(t *testing.T) {
	rep := Reconciliation{Diffs: map[string]int{}}
	err := ReconcilePlatformAdmin(context.Background(), fakeAdminTarget{err: errors.New("secret-host-detail")}, true, &rep)
	if err == nil || strings.Contains(err.Error(), "secret-host-detail") {
		t.Fatalf("error %v", err)
	}
}

func TestReconcileVerbAcceptsThePlatformRolesGateFlag(t *testing.T) {
	var stderr, stdout bytes.Buffer
	code := reconcileVerb(t).Run(context.Background(), cli.Env{Args: []string{"-platform-roles-gate"},
		Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &stderr})
	if code == cli.ExitUsage || !strings.Contains(stderr.String(), "AUTH_DATABASE_URI") {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
}

func TestNoActivePlatformAdminLineIsAbsentWhenTheReadFailed(t *testing.T) {
	for _, gate := range []bool{true, false} {
		rep := Reconciliation{Diffs: map[string]int{}}
		if err := ReconcilePlatformAdmin(context.Background(), fakeAdminTarget{err: errors.New("down")}, gate, &rep); err == nil {
			t.Fatalf("gate %v: no error", gate)
		}
		if out := strings.Join(rep.Lines(), "\n"); strings.Contains(out, DiffNoActivePlatformAdmin) {
			t.Fatalf("gate %v: a failed read printed the class\n%s", gate, out)
		}
		if rep.GatedTotal() != 0 {
			t.Fatalf("gate %v: gated total %d", gate, rep.GatedTotal())
		}
	}
	rep := Reconciliation{Diffs: map[string]int{}}
	if err := ReconcilePlatformAdmin(context.Background(), fakeAdminTarget{none: true}, true, &rep); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(rep.Lines(), "\n"), "diff.no_active_platform_admin=1") {
		t.Fatal("a good read did not print the class")
	}
}

func TestNoActivePlatformAdminLineIsAbsentBeforeAnyRead(t *testing.T) {
	if out := strings.Join((Reconciliation{Diffs: map[string]int{}}).Lines(), "\n"); strings.Contains(out, DiffNoActivePlatformAdmin) {
		t.Fatalf("an unread report printed the class\n%s", out)
	}
}
