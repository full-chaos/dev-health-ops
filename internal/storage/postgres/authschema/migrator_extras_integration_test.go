//go:build integration

package authschema

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
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
	if len(report.ExtraLines) != 9 || !strings.HasPrefix(report.ExtraLines[0], "revoke|refresh_tokens|") || !strings.Contains(report.ExtraLines[0], "|"+env.oldRole) {
		t.Errorf("extra lines = %v, want 9 revoke lines table|privilege|column|grantable|grantor", report.ExtraLines)
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

// The read-back texts fcsup runs on prod are the production queries with the role as a literal; run them here and save them.
func TestMigratorReadbackQueriesRunAndNameTheLeftovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	seedMigratorLeftovers(t, ctx, env)
	count := func(text string) int {
		rows, err := env.admin.Query(ctx, text)
		if err != nil {
			t.Fatalf("read-back text: %v\n%s", err, text)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
			if values, err := rows.Values(); err == nil && n <= 3 {
				t.Logf("row: %v", values)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if want, got := len(migratorPublicRows(t, ctx, env)), count(ReadbackQueryText(env.migratorRole)); got != want {
		t.Errorf("read-back 1 returned %d rows, want %d", got, want)
	}
	if got := count(ReadbackPreQueryText(env.migratorRole)); got != 0 {
		t.Errorf("pre-read 2 returned %d rows on a database where the migrator granted nothing, want 0", got)
	}
	runtime := Quote(mustValidatedIdentifier(env.runtimeRole))
	if _, err := env.migrator.Exec(ctx, `GRANT SELECT (token_hash) ON public.refresh_tokens TO `+runtime); err != nil {
		t.Fatal(err)
	}
	if got := count(ReadbackPreQueryText(env.migratorRole)); got != 1 {
		t.Errorf("pre-read 2 returned %d rows after the migrator granted one column, want 1", got)
	}
	_ = count(ReadbackOtherObjectsQueryText(env.migratorRole))
	if path := os.Getenv("READBACK_OUT"); path != "" {
		const role = "devhealth_auth_migrator"
		text := "-- READBACK 1 (CHAOS-13499): every public privilege the migration login holds. Want: no refresh_tokens row, no col=is_superuser row, the rest = PublicPrivilegeSet + REFERENCES anchors.\n" +
			ReadbackQueryText(role) + ";\n\n" +
			"-- READBACK 2 (PRE-read): privileges the migration login granted to another role on a public table. Want 0 rows before the owner-move revoke.\n" +
			ReadbackPreQueryText(role) + ";\n\n" +
			"-- READBACK 3 (info only, nothing revokes these): the migration login's privileges on public sequences, functions, schema public and default ACLs.\n" +
			ReadbackOtherObjectsQueryText(role) + ";\n"
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// d1977b3e set (the LIVE hook image): internal_service_credentials read columns and the link-token purge grant, each with
// grant option plus the REFERENCES anchor. A forward move of this tip keeps them while the runtime role holds a row on the
// table (the runtime rows granted by the migrator stay, the digest-acknowledged pass still has its grantor) and takes them
// once it holds none.
func TestMigratorOlderSetRowsAreKeptWhileTheRuntimeHoldsAndRevokedWhenItDoesNot(t *testing.T) {
	const iscCols = "id, service_name, token_hash, scopes, expires_at, revoked_at"
	for _, runtimeHolds := range []bool{true, false} {
		name := "no runtime rows"
		if runtimeHolds {
			name = "runtime rows present"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			env := newOwnerMoveEnv(t, ctx, false)
			env.move(t, ctx, env.moveOptions(true))
			migrator := Quote(mustValidatedIdentifier(env.migratorRole))
			runtime := Quote(mustValidatedIdentifier(env.runtimeRole))
			tables := []string{"internal_service_credentials", "email_verification_tokens", "password_reset_tokens"}
			for _, table := range tables {
				if runtimeHoldsAnythingOn(t, ctx, env, table) {
					t.Fatalf("setup: the runtime role already holds a privilege on public.%s", table)
				}
			}
			for _, statement := range []string{
				`GRANT SELECT (` + iscCols + `) ON public.internal_service_credentials TO ` + migrator + ` WITH GRANT OPTION`,
				`GRANT REFERENCES ON public.internal_service_credentials TO ` + migrator,
				`GRANT DELETE ON public.email_verification_tokens, public.password_reset_tokens TO ` + migrator + ` WITH GRANT OPTION`,
				`GRANT SELECT (id) ON public.email_verification_tokens, public.password_reset_tokens TO ` + migrator + ` WITH GRANT OPTION`,
				`GRANT REFERENCES ON public.email_verification_tokens, public.password_reset_tokens TO ` + migrator,
			} {
				if _, err := env.old.Exec(ctx, statement); err != nil {
					t.Fatalf("seed %q: %v", statement, err)
				}
			}
			if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 13 {
				t.Fatalf("setup: %d privileges outside the set, want 13: %v", len(got), got)
			}
			if runtimeHolds {
				for _, statement := range []string{
					`GRANT SELECT (` + iscCols + `) ON public.internal_service_credentials TO ` + runtime,
					`GRANT DELETE ON public.email_verification_tokens, public.password_reset_tokens TO ` + runtime,
					`GRANT SELECT (id) ON public.email_verification_tokens, public.password_reset_tokens TO ` + runtime,
				} {
					if _, err := env.migrator.Exec(ctx, statement); err != nil {
						t.Fatalf("as the migrator %q: %v", statement, err)
					}
				}
			}
			runtimeBefore := aclOfRole(t, ctx, env, env.runtimeRole)
			report := env.move(t, ctx, env.moveOptions(true))
			wantRevoked, wantKept := 13, 0
			if runtimeHolds {
				wantRevoked, wantKept = 0, 13
			}
			if report.ExtrasRevoked != wantRevoked || report.ExtrasKept != wantKept {
				t.Errorf("revoked %d kept %d, want %d and %d: %v", report.ExtrasRevoked, report.ExtrasKept, wantRevoked, wantKept, report.ExtraLines)
			}
			if len(report.ExtraLines) != 13 {
				t.Errorf("%d extra lines, want 13: %v", len(report.ExtraLines), report.ExtraLines)
			}
			for _, line := range report.ExtraLines {
				if runtimeHolds && !strings.HasPrefix(line, "keep|") || !runtimeHolds && !strings.HasPrefix(line, "revoke|") {
					t.Errorf("line %q, want a %s line", line, map[bool]string{true: "keep", false: "revoke"}[runtimeHolds])
				}
				if runtimeHolds && !strings.Contains(line, "the runtime role holds a privilege on a table outside the set") && !strings.Contains(line, "another role holds a privilege the migrator granted") {
					t.Errorf("keep line %q names no reason", line)
				}
			}
			if after := aclOfRole(t, ctx, env, env.runtimeRole); strings.Join(after, "\n") != strings.Join(runtimeBefore, "\n") {
				t.Errorf("the runtime role's rows changed:\nbefore %v\nafter  %v", runtimeBefore, after)
			}
			if !runtimeHolds {
				if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 0 {
					t.Errorf("outside the set: %v", got)
				}
			}
		})
	}
}

// Inside the owner-move transaction, before the commit: no lock on public.users or public.refresh_tokens stronger than
// RowExclusiveLock, and a reader of public.users is not held up.
func TestOwnerMoveRevokeTakesNoStrongLockAndABlockedReaderIsNotHeld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	seedMigratorLeftovers(t, ctx, env)
	o := env.moveOptions(true)
	ran := false
	o.hookBeforeCommit = func(hookCtx context.Context) {
		ran = true
		var strong []string
		rows, err := env.admin.Query(hookCtx, `SELECT c.relname::text, l.mode FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
			WHERE c.relnamespace = 'public'::regnamespace AND c.relname IN ('users', 'refresh_tokens') AND l.pid <> pg_backend_pid() AND l.granted
			  AND l.mode NOT IN ('AccessShareLock', 'RowShareLock', 'RowExclusiveLock')`)
		if err != nil {
			t.Errorf("read pg_locks: %v", err)
			return
		}
		for rows.Next() {
			var table, mode string
			if err := rows.Scan(&table, &mode); err != nil {
				t.Error(err)
			}
			strong = append(strong, table+" "+mode)
		}
		rows.Close()
		if len(strong) > 0 {
			t.Errorf("the open owner-move transaction holds strong locks: %v", strong)
		}
		readCtx, cancelRead := context.WithTimeout(hookCtx, 10*time.Second)
		defer cancelRead()
		var n int
		if err := env.admin.QueryRow(readCtx, `SELECT count(*) FROM public.users`).Scan(&n); err != nil {
			t.Errorf("a concurrent SELECT on public.users did not return inside the open transaction: %v", err)
		}
	}
	report := env.move(t, ctx, o)
	if !ran {
		t.Fatal("the before-commit hook did not run")
	}
	if report.ExtrasRevoked != 9 {
		t.Errorf("revoked %d, want 9", report.ExtrasRevoked)
	}
}

// Restore is not changed by the revoke: the same input gives the same statements with and without leftovers, and
// nothing is revoked or counted.
func TestOwnerMoveRestoreStatementsIgnoreTheMigratorsLeftovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	statementsOf := func() ([]string, OwnerMoveReport) {
		t.Helper()
		tx, err := env.admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		o := env.reverseOptions(false)
		var report OwnerMoveReport
		statements, err := ownerMovePublicStatements(ctx, tx, o,
			Quote(mustValidatedIdentifier(o.From)), Quote(mustValidatedIdentifier(o.To)), Quote(mustValidatedIdentifier(o.RuntimeRole)), &report)
		if err != nil {
			t.Fatal(err)
		}
		return statements, report
	}
	before, _ := statementsOf()
	if len(before) == 0 {
		t.Fatal("the restore built no statement; the test compares nothing")
	}
	seedMigratorLeftovers(t, ctx, env)
	after, report := statementsOf()
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Errorf("restore statements changed with leftovers present:\nbefore %v\nafter  %v", before, after)
	}
	if report.ExtrasRevoked != 0 || report.ExtrasKept != 0 || len(report.ExtraLines) != 0 {
		t.Errorf("restore counted extras: %+v", report)
	}
}

func keepOrRevokeLines(report OwnerMoveReport, table string) (revoke, keep []string) {
	for _, line := range report.ExtraLines {
		if !strings.Contains(line, "|"+table+"|") && !strings.HasPrefix(line, "revoke|"+table+"|") && !strings.HasPrefix(line, "keep|"+table+"|") {
			continue
		}
		if strings.HasPrefix(line, "revoke|") {
			revoke = append(revoke, line)
		} else {
			keep = append(keep, line)
		}
	}
	return revoke, keep
}

// Skip rule: a leftover granted by a role that is not the table owner is kept and counted, never revoked.
func TestMigratorLeftoverGrantedByANonOwnerIsKeptAndCounted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	other, err := containers.RoleName("om_grantor", env.instance)
	if err != nil {
		t.Fatal(err)
	}
	otherQ := Quote(mustValidatedIdentifier(other))
	migrator := Quote(mustValidatedIdentifier(env.migratorRole))
	if _, err := env.admin.Exec(ctx, "CREATE ROLE "+otherQ+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(env.admin, other, t.Logf) })
	if _, err := env.old.Exec(ctx, `GRANT SELECT (user_id) ON public.refresh_tokens TO `+otherQ+` WITH GRANT OPTION`); err != nil {
		t.Fatal(err)
	}
	tx, err := env.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{"SET LOCAL ROLE " + otherQ, `GRANT SELECT (user_id) ON public.refresh_tokens TO ` + migrator} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	report := env.move(t, ctx, env.moveOptions(true))
	if report.ExtrasRevoked != 0 || report.ExtrasKept != 1 {
		t.Fatalf("revoked %d kept %d, want 0 and 1: %v", report.ExtrasRevoked, report.ExtrasKept, report.ExtraLines)
	}
	if line := report.ExtraLines[0]; !strings.HasPrefix(line, "keep|refresh_tokens|SELECT|user_id|false|"+other+"|grantor is not the table owner") {
		t.Errorf("line = %q", line)
	}
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 1 {
		t.Errorf("outside the set = %v, want the one kept row", got)
	}
}

// Skip rule: a table the migrator owns is skipped (its own implicit privileges are not leftovers).
func TestMigratorLeftoverOnATableTheMigratorOwnsIsSkippedAndCounted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	if _, err := env.admin.Exec(ctx, `ALTER TABLE public.refresh_tokens OWNER TO `+Quote(mustValidatedIdentifier(env.migratorRole))); err != nil {
		t.Fatal(err)
	}
	report := env.move(t, ctx, env.moveOptions(true))
	revoke, keep := keepOrRevokeLines(report, "refresh_tokens")
	if len(revoke) != 0 || len(keep) == 0 || report.ExtrasRevoked != 0 {
		t.Fatalf("revoked %d, refresh_tokens lines revoke %v keep %v", report.ExtrasRevoked, revoke, keep)
	}
	for _, line := range keep {
		if !strings.HasSuffix(line, "|the migrator owns the table") {
			t.Errorf("line %q names the wrong reason", line)
		}
	}
	var owner string
	if err := env.admin.QueryRow(ctx, `SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.refresh_tokens'::regclass`).Scan(&owner); err != nil || owner != env.migratorRole {
		t.Errorf("owner = %q (%v), want the migrator", owner, err)
	}
}

// Skip rule: a table that is absent has no row to list and no statement to send.
func TestMigratorLeftoverRevokeSkipsAnAbsentTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	seedMigratorLeftovers(t, ctx, env)
	if _, err := env.admin.Exec(ctx, `DROP TABLE public.refresh_tokens CASCADE`); err != nil {
		t.Fatal(err)
	}
	report := env.move(t, ctx, env.moveOptions(true))
	if report.ExtrasRevoked != 1 || report.ExtrasKept != 0 {
		t.Errorf("revoked %d kept %d, want only the users leftover: %v", report.ExtrasRevoked, report.ExtrasKept, report.ExtraLines)
	}
}

// First move of a database that holds the leftovers: the runtime role holds INSERT(users.is_superuser) from the owner, the
// move re-grants it as the migrator in the same transaction, so the migrator's leftover is kept this run (a plain
// REVOKE would be refused) and the rest goes.
func TestFirstMoveWithLeftoversAndARuntimeRowRegrantedInTheSameRunKeepsThatLeftover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	seedMigratorLeftovers(t, ctx, env)
	runtime := Quote(mustValidatedIdentifier(env.runtimeRole))
	if _, err := env.old.Exec(ctx, `GRANT INSERT (is_superuser) ON public.users TO `+runtime); err != nil {
		t.Fatal(err)
	}
	report := env.move(t, ctx, env.moveOptions(true))
	if report.ExtrasRevoked != 8 || report.ExtrasKept != 1 {
		t.Fatalf("revoked %d kept %d, want 8 and 1: %v", report.ExtrasRevoked, report.ExtrasKept, report.ExtraLines)
	}
	_, keep := keepOrRevokeLines(report, "users")
	if len(keep) != 1 || !strings.HasPrefix(keep[0], "keep|users|INSERT|is_superuser|true|") || !strings.HasSuffix(keep[0], "|another role holds a privilege the migrator granted") {
		t.Errorf("keep lines = %v", keep)
	}
	var held bool
	if err := env.admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.users', 'is_superuser', 'INSERT')`, env.runtimeRole).Scan(&held); err != nil || !held {
		t.Errorf("the runtime role lost INSERT(is_superuser) (%v, %v)", held, err)
	}
	if got := migratorRowsOutsideTheSet(t, ctx, env); len(got) != 1 || got[0] != "users|INSERT|is_superuser" {
		t.Errorf("outside the set = %v, want only users|INSERT|is_superuser", got)
	}
}
