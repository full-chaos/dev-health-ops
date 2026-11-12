//go:build integration

package authschema

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

func migratorPublicRows(t *testing.T, ctx context.Context, env *ownerMoveEnv) []MigratorPublicRow {
	t.Helper()
	rows, err := env.admin.Query(ctx, MigratorPublicRowsSQL, env.migratorRole)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []MigratorPublicRow
	for rows.Next() {
		var r MigratorPublicRow
		var qt, qc, qo string
		if err := rows.Scan(&r.Table, &r.Column, &r.Privilege, &r.Grantable, &r.Grantor, &r.Owner, &qt, &qc, &qo); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// migratorRowsOutsideTheSet lists the public privileges the migrator holds that are neither in PublicPrivilegeSet nor the
// anchor privilege on a table of the set.
func migratorRowsOutsideTheSet(t *testing.T, ctx context.Context, env *ownerMoveEnv) []string {
	t.Helper()
	var outside []string
	for _, r := range MigratorRowsOutsideTheSet(migratorPublicRows(t, ctx, env)) {
		outside = append(outside, r.Key())
	}
	sort.Strings(outside)
	return outside
}

func seedMigratorLeftovers(t *testing.T, ctx context.Context, env *ownerMoveEnv) {
	t.Helper()
	migrator := Quote(mustValidatedIdentifier(env.migratorRole))
	for _, statement := range []string{
		`GRANT INSERT (is_superuser) ON public.users TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT SELECT (token_hash, user_id, org_id, family_id, expires_at, revoked_at) ON public.refresh_tokens TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT UPDATE (revoked_at) ON public.refresh_tokens TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT REFERENCES ON public.refresh_tokens TO ` + migrator,
	} {
		if _, err := env.old.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %q: %v", statement, err)
		}
	}
}

// Prod shape (CHAOS-13499): the owner once gave the migrator INSERT(users.is_superuser) and the old refresh_tokens
// privileges, with grant option. A forward owner move (here the grants-only form) must leave the migrator holding the
// set and nothing else.
func TestOwnerMoveGrantsOnlyRevokesTheMigratorsLeftoverPublicPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	seedMigratorLeftovers(t, ctx, env)
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 9 {
		t.Fatalf("setup: the migrator holds %d privileges outside the set, want 9: %v", len(got), got)
	}
	o := env.moveOptions(true)
	o.GrantsOnly = true
	report := env.move(t, ctx, o)
	if report.ExtrasRevoked != 9 || report.ExtrasKept != 0 {
		t.Errorf("report: revoked %d kept %d, want 9 and 0", report.ExtrasRevoked, report.ExtrasKept)
	}
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 0 {
		t.Errorf("the migrator holds %d public privileges outside the set after the move: %s", len(got), strings.Join(got, ", "))
	}
}

func aclOfRole(t *testing.T, ctx context.Context, env *ownerMoveEnv, role string) []string {
	t.Helper()
	var out []string
	for _, line := range env.aclSnapshot(t, ctx) {
		parts := strings.Split(line, "|")
		if len(parts) > 2 && strings.HasSuffix(parts[2], ">"+role) && strings.HasPrefix(parts[1], "public.") {
			out = append(out, line)
		}
	}
	return out
}

// After the revoke the migrator still holds the whole set, the runtime role's rows are untouched, a second run changes
// nothing, and the runtime role still reads.
func TestMigratorLeftoverRevokeKeepsTheSetAndTheRuntimeRowsAndIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	seedMigratorLeftovers(t, ctx, env)
	runtimeBefore := aclOfRole(t, ctx, env, env.runtimeRole)
	setBefore := len(migratorPublicRows(t, ctx, env)) - 9
	report := env.move(t, ctx, env.moveOptions(true))
	if report.ExtrasRevoked != 9 || report.ExtrasKept != 0 {
		t.Errorf("first run: revoked %d kept %d, want 9 and 0", report.ExtrasRevoked, report.ExtrasKept)
	}
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 0 {
		t.Errorf("outside the set after the move: %v", got)
	}
	if n := len(migratorPublicRows(t, ctx, env)); n != setBefore {
		t.Errorf("the migrator holds %d rows, want the %d of the set and the anchors", n, setBefore)
	}
	if after := aclOfRole(t, ctx, env, env.runtimeRole); strings.Join(after, "\n") != strings.Join(runtimeBefore, "\n") {
		t.Errorf("the runtime role's rows changed:\nbefore %v\nafter  %v", runtimeBefore, after)
	}
	again := env.move(t, ctx, env.moveOptions(true))
	if again.ExtrasRevoked != 0 || again.ExtrasKept != 0 {
		t.Errorf("second run: revoked %d kept %d, want 0 and 0", again.ExtrasRevoked, again.ExtrasKept)
	}
	if err := env.runtimeReadsUsers(ctx); err != nil {
		t.Errorf("the runtime role cannot read users: %v", err)
	}
}

// A fresh database (no leftovers) revokes nothing.
func TestMigratorLeftoverRevokeOnAFreshDatabaseDoesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	report := env.move(t, ctx, env.moveOptions(true))
	if report.ExtrasRevoked != 0 || report.ExtrasKept != 0 {
		t.Errorf("revoked %d kept %d, want 0 and 0", report.ExtrasRevoked, report.ExtrasKept)
	}
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 0 {
		t.Errorf("outside the set: %v", got)
	}
}

// The runtime role holds rows the migrator granted: the leftovers stay (no CASCADE, no narrowing), the run succeeds, and
// once the grants pass has removed the runtime rows the next move takes the leftovers.
func TestMigratorLeftoverRevokeKeepsWhatAnotherRoleHoldsFromTheMigrator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	seedMigratorLeftovers(t, ctx, env)
	runtime := Quote(mustValidatedIdentifier(env.runtimeRole))
	for _, statement := range []string{
		`GRANT SELECT (token_hash, user_id, org_id, family_id, expires_at, revoked_at) ON public.refresh_tokens TO ` + runtime,
		`GRANT UPDATE (revoked_at) ON public.refresh_tokens TO ` + runtime,
	} {
		if _, err := env.migrator.Exec(ctx, statement); err != nil {
			t.Fatalf("as the migrator %q: %v", statement, err)
		}
	}
	runtimeBefore := aclOfRole(t, ctx, env, env.runtimeRole)
	report := env.move(t, ctx, env.moveOptions(true))
	if report.ExtrasRevoked != 1 {
		t.Errorf("revoked %d, want only the users leftover (1)", report.ExtrasRevoked)
	}
	if report.ExtrasKept != 8 {
		t.Errorf("kept %d, want the 8 refresh_tokens leftovers", report.ExtrasKept)
	}
	if after := aclOfRole(t, ctx, env, env.runtimeRole); strings.Join(after, "\n") != strings.Join(runtimeBefore, "\n") {
		t.Errorf("the runtime role's rows changed:\nbefore %v\nafter  %v", runtimeBefore, after)
	}
	if _, err := Apply(ctx, env.migrator, env.options(true)); err == nil {
		t.Fatal("Apply without the acknowledgement was not refused: the runtime rows should be a narrowing")
	}
	options := env.options(true)
	options.AcknowledgeNarrowing = retiredRefreshTokensAckDigest
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("Apply with the acknowledgement: %v", err)
	}
	last := env.move(t, ctx, env.moveOptions(true))
	if last.ExtrasRevoked != 8 || last.ExtrasKept != 0 {
		t.Errorf("last run: revoked %d kept %d, want 8 and 0", last.ExtrasRevoked, last.ExtrasKept)
	}
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 0 {
		t.Errorf("outside the set after the last run: %v", got)
	}
}

// The census goes red when the migrator holds a privilege outside the set: each outside privilege of a database that no
// owner move has cleaned is reported, and the dry run changes nothing.
func TestMigratorCensusNamesEveryPrivilegeOutsideTheSetAndADryRunChangesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	seedMigratorLeftovers(t, ctx, env)
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 9 {
		t.Fatalf("census found %d outside the set, want 9: %v", len(got), got)
	}
	report := env.move(t, ctx, env.moveOptions(false))
	if report.ExtrasRevoked != 9 || report.Applied {
		t.Errorf("dry run: revoked %d applied %v, want 9 counted and not applied", report.ExtrasRevoked, report.Applied)
	}
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 9 {
		t.Errorf("a dry run changed the grants: %d outside the set, want 9", len(got))
	}
}

// The read-back text fcsup runs on prod is the production query with the role as a literal; run it here and save it.
func TestMigratorReadbackQueryRunsAndNamesTheLeftovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	seedMigratorLeftovers(t, ctx, env)
	text := ReadbackQueryText(env.migratorRole)
	rows, err := env.admin.Query(ctx, text)
	if err != nil {
		t.Fatalf("read-back text: %v", err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := len(migratorPublicRows(t, ctx, env)); n != want {
		t.Errorf("read-back returned %d rows, want %d", n, want)
	}
	if path := os.Getenv("READBACK_OUT"); path != "" {
		prod := ReadbackQueryText("devhealth_auth_migrator")
		if err := os.WriteFile(path, []byte(prod), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
