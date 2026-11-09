//go:build integration

package authschema

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// widenedUsersUpdateColumns are the public.users UPDATE columns the users write grant gained after the owner gave the
// migrator its grant options (pm D6870/D6872): a migrator given the earlier set holds no grant option on them.
var widenedUsersUpdateColumns = []string{"avatar_url", "email", "full_name", "is_active", "username"}

var postureColumnLine = regexp.MustCompile(`(?m)^\s*- column: (\S+ on public\.users\.\S+) \(`)

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
	if _, err := env.old.Exec(ctx, fmt.Sprintf(`REVOKE UPDATE (%s) ON TABLE public.users FROM %s CASCADE`,
		strings.Join(quoted, ", "), Quote(mustValidatedIdentifier(env.migratorRole)))); err != nil {
		t.Fatalf("give the migrator the pre-widening grant set: %v", err)
	}

	aclBefore := env.aclSnapshot(t, ctx)
	_, err := Apply(ctx, env.migrator, options)
	if err == nil {
		t.Fatal("the migrate passed with a migrator that holds no grant option on the widened columns")
	}
	t.Logf("migrate refusal:\n%v", err)
	var refused []string
	for _, m := range postureColumnLine.FindAllStringSubmatch(err.Error(), -1) {
		refused = append(refused, m[1])
	}
	slices.Sort(refused)
	var want []string
	for _, column := range widenedUsersUpdateColumns {
		want = append(want, "UPDATE on public.users."+column)
	}
	if !slices.Equal(refused, want) {
		t.Fatalf("posture refused %v, want exactly %v", refused, want)
	}
	if onlyA, onlyB := diffLines(aclBefore, env.aclSnapshot(t, ctx)); len(onlyA)+len(onlyB) > 0 {
		t.Errorf("the failed migrate left a partial grant: lost %v, gained %v", onlyA, onlyB)
	}

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
