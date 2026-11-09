//go:build integration

package authschema

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// widenedUsersUpdateColumns are the public.users UPDATE columns the users write grant gained after the owner gave the
// migrator its grant options (pm D6870/D6872): a migrator given the earlier set holds no grant option on them.
var widenedUsersUpdateColumns = []string{"avatar_url", "email", "full_name", "is_active", "username"}

var gapLine = regexp.MustCompile(`(?m)^  - (.+)$`)

// refusedGapLines is the sorted gap lines of a grant-option refusal.
func refusedGapLines(err error) []string {
	var lines []string
	for _, m := range gapLine.FindAllStringSubmatch(err.Error(), -1) {
		lines = append(lines, m[1])
	}
	slices.Sort(lines)
	return lines
}

func requireNothingGranted(t *testing.T, before, after []string) {
	t.Helper()
	if onlyA, onlyB := diffLines(before, after); len(onlyA)+len(onlyB) > 0 {
		t.Errorf("the refused run left a partial grant: lost %v, gained %v", onlyA, onlyB)
	}
}

func requireRemedy(t *testing.T, env *ownerMoveEnv, err error) {
	t.Helper()
	if !errors.Is(err, ErrMigratorGrantOptionMissing) {
		t.Fatalf("error = %v, want ErrMigratorGrantOptionMissing", err)
	}
	for _, want := range []string{"auth-migrate owner-move -grants-only", env.migratorRole, env.oldRole} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q:\n%v", want, err)
		}
	}
}

func widenedUsersUpdateLines() []string {
	var want []string
	for _, column := range widenedUsersUpdateColumns {
		want = append(want, "UPDATE ("+column+") ON public.users")
	}
	return want
}

func revokeWidenedUsersUpdate(t *testing.T, ctx context.Context, env *ownerMoveEnv) {
	t.Helper()
	_, update := UsersWriteGrant()
	for _, column := range widenedUsersUpdateColumns {
		if !slices.Contains(update, column) {
			t.Fatalf("UsersWriteGrant UPDATE has no %s: the widening this cell reproduces is not on this tree", column)
		}
	}
	quoted := make([]string, len(widenedUsersUpdateColumns))
	for i, column := range widenedUsersUpdateColumns {
		quoted[i] = Quote(mustValidatedIdentifier(column))
	}
	if _, err := env.old.Exec(ctx, "REVOKE UPDATE ("+strings.Join(quoted, ", ")+") ON TABLE public.users FROM "+
		Quote(mustValidatedIdentifier(env.migratorRole))+" CASCADE"); err != nil {
		t.Fatalf("give the migrator the pre-widening grant set: %v", err)
	}
}

// A migrator given its grant options before the users write grant widened holds none on the widened columns. The
// migrate's GRANT of the users write grant then gives the runtime role nothing on them, and the posture check refuses
// exactly those columns, with no partial grant. The owner's `owner-move -grants-only` dry run covers exactly them and
// changes nothing; its apply gives the migrator the options, and the migrate passes.
func TestPreWideningMigratorGrantSetFailsThePostureOnTheWidenedColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	options := env.options(true)
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("Apply as the migrator with the current grant set: %v", err)
	}

	revokeWidenedUsersUpdate(t, ctx, env)
	quoted := make([]string, len(widenedUsersUpdateColumns))
	for i, column := range widenedUsersUpdateColumns {
		quoted[i] = Quote(mustValidatedIdentifier(column))
	}

	aclBefore := env.aclSnapshot(t, ctx)
	_, err := Apply(ctx, env.migrator, options)
	if err == nil {
		t.Fatal("the migrate passed with a migrator that holds no grant option on the widened columns")
	}
	t.Logf("migrate refusal:\n%v", err)
	requireRemedy(t, env, err)
	if refused, want := refusedGapLines(err), widenedUsersUpdateLines(); !slices.Equal(refused, want) {
		t.Fatalf("the refusal lists %v, want exactly %v", refused, want)
	}
	requireNothingGranted(t, aclBefore, env.aclSnapshot(t, ctx))

	dry := env.moveOptions(false)
	dry.GrantsOnly = true
	aclBeforeDry := env.aclSnapshot(t, ctx)
	env.move(t, ctx, dry)
	if onlyA, onlyB := diffLines(aclBeforeDry, env.aclSnapshot(t, ctx)); len(onlyA)+len(onlyB) > 0 {
		t.Errorf("the grants-only dry run changed ACLs: lost %v, gained %v", onlyA, onlyB)
	}
	tx, err := env.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var report OwnerMoveReport
	statements, err := ownerMovePublicStatements(ctx, tx, dry, env.oldRole, env.migratorRole, env.runtimeRole, &report)
	if err != nil {
		t.Fatalf("the grants-only statements: %v", err)
	}
	var usersUpdate string
	for _, statement := range statements {
		t.Logf("grants-only dry run: %s", statement)
		if strings.HasPrefix(statement, "GRANT UPDATE (") && strings.Contains(statement, `ON TABLE "public"."users" TO`) {
			usersUpdate = statement
		}
	}
	if usersUpdate == "" || !strings.HasSuffix(usersUpdate, "WITH GRANT OPTION") {
		t.Fatalf("the grants-only dry run has no GRANT UPDATE (...) ON public.users WITH GRANT OPTION: %q", usersUpdate)
	}
	for _, column := range quoted {
		if !strings.Contains(usersUpdate, column) {
			t.Errorf("the grants-only dry run's public.users UPDATE grant does not cover %s: %s", column, usersUpdate)
		}
	}
	_ = tx.Rollback(ctx)

	env.move(t, ctx, func() OwnerMoveOptions { o := env.moveOptions(true); o.GrantsOnly = true; return o }())
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("Apply after the grants-only apply: %v", err)
	}
	for _, column := range widenedUsersUpdateColumns {
		var held bool
		if err := env.admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.users', $2, 'UPDATE')`, env.runtimeRole, column).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if !held {
			t.Errorf("after the grants-only apply and the migrate the runtime role holds no UPDATE (%s)", column)
		}
	}
}

// E2: the grants pass refuses the same way, before any GRANT.
func TestGrantsPassRefusesAMigratorWithoutTheWidenedGrantOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	options := env.options(true)
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("Apply as the migrator with the current grant set: %v", err)
	}
	revokeWidenedUsersUpdate(t, ctx, env)
	aclBefore := env.aclSnapshot(t, ctx)
	_, err := Grants(ctx, env.migrator, options)
	if err == nil {
		t.Fatal("the grants pass passed with a migrator that holds no grant option on the widened columns")
	}
	requireRemedy(t, env, err)
	if refused, want := refusedGapLines(err), widenedUsersUpdateLines(); !slices.Equal(refused, want) {
		t.Fatalf("the refusal lists %v, want exactly %v", refused, want)
	}
	requireNothingGranted(t, aclBefore, env.aclSnapshot(t, ctx))
}

// E3: with the users-owned switch off the users write grant is not declared, so the missing options are no gap.
func TestGrantOptionPrecheckSkipsGrantsTheSwitchTurnsOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	revokeWidenedUsersUpdate(t, ctx, env)
	if _, err := Apply(ctx, env.migrator, env.options(false)); err != nil {
		t.Fatalf("Apply with the switch off and no option on the users write columns: %v", err)
	}
}

// E4: an absent column is DEFERRED, not a gap: the pre-check never asks PostgreSQL about a column that does not exist.
func TestGrantOptionPrecheckDefersAnAbsentColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	if _, err := env.old.Exec(ctx, `ALTER TABLE public.users DROP COLUMN sso_provider_id`); err != nil {
		t.Fatalf("build the schema before the revision that adds sso_provider_id: %v", err)
	}
	if _, err := Apply(ctx, env.migrator, env.options(true)); err != nil {
		t.Fatalf("Apply on a schema without a column of the grant set: %v", err)
	}
}
