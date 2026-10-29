package usersbackfill

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// roleSink records every target and answers RoleSkipped for the ones whose
// role the store would have changed.
type roleSink struct {
	targets []identity.BackfillTarget
	differs map[uuid.UUID]bool
}

func (r *roleSink) BackfillUser(_ context.Context, t identity.BackfillTarget, _ bool) (identity.BackfillResult, error) {
	r.targets = append(r.targets, t)
	var res identity.BackfillResult
	if r.differs[t.ID] {
		if t.PlatformRolesOwned {
			res.RoleSkipped = true
		} else {
			res.Updated, res.RoleGranted = true, true
		}
	}
	return res, nil
}

func TestRunLeavesPlatformRolesToTheStoreWhileOwned(t *testing.T) {
	rows := rowsFor(4)
	for _, owned := range []bool{false, true} {
		sink := &roleSink{differs: map[uuid.UUID]bool{rows[0].ID: true, rows[2].ID: true}}
		rep, err := Run(context.Background(), pagedSource{rows: rows}, sink, Options{BatchSize: 2, PlatformRolesOwned: owned})
		if err != nil {
			t.Fatal(err)
		}
		if len(sink.targets) != 4 {
			t.Fatalf("owned=%v: %d targets, want 4", owned, len(sink.targets))
		}
		for _, target := range sink.targets {
			if target.PlatformRolesOwned != owned {
				t.Fatalf("owned=%v: target %s carries PlatformRolesOwned=%v", owned, target.ID, target.PlatformRolesOwned)
			}
		}
		wantSkipped, wantGranted := 0, 2
		if owned {
			wantSkipped, wantGranted = 2, 0
		}
		if rep.RoleSkippedOwned != wantSkipped || rep.RoleGranted != wantGranted {
			t.Fatalf("owned=%v: skipped %d granted %d, want %d and %d", owned, rep.RoleSkippedOwned, rep.RoleGranted, wantSkipped, wantGranted)
		}
		line := "platform_role_skipped_owned=" + map[bool]string{false: "0", true: "2"}[owned]
		if !strings.Contains(strings.Join(rep.Lines(), "\n"), line) {
			t.Fatalf("owned=%v: report lacks %q:\n%s", owned, line, strings.Join(rep.Lines(), "\n"))
		}
	}
}

func TestUsersBackfillOptionsReadThePlatformRolesSwitchStrictly(t *testing.T) {
	for value, want := range map[string]bool{"": false, "false": false, "true": true, " TRUE ": true} {
		opts, err := usersBackfillOptions(func(k string) (string, bool) { return value, k == "AUTH_PLATFORM_ROLES_OWNED" }, true, 7)
		if err != nil || opts.PlatformRolesOwned != want || !opts.DryRun || opts.BatchSize != 7 {
			t.Errorf("%q: %+v %v, want PlatformRolesOwned=%v", value, opts, err, want)
		}
	}
	for _, value := range []string{"yes", "1", "on"} {
		if _, err := usersBackfillOptions(func(k string) (string, bool) { return value, k == "AUTH_PLATFORM_ROLES_OWNED" }, false, 1); err == nil {
			t.Errorf("%q accepted", value)
		}
	}
}

// A malformed platform-roles switch stops users-backfill before any database
// access.
func TestUsersBackfillRefusesAMalformedPlatformRolesSwitchBeforeConnect(t *testing.T) {
	orig := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		t.Fatal("connect was reached with a malformed switch")
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	t.Cleanup(func() { connectFn = orig })
	var stderr bytes.Buffer
	code := Command().Children[0].Run(context.Background(), cli.Env{Args: []string{"-dry-run"},
		Lookup: func(k string) (string, bool) { return "maybe", k == "AUTH_PLATFORM_ROLES_OWNED" }, Stdout: &bytes.Buffer{}, Stderr: &stderr})
	if code != cli.ExitFailure || !strings.Contains(stderr.String(), "AUTH_PLATFORM_ROLES_OWNED") {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
}
