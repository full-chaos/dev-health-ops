//go:build integration

package authschema

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// insertFixtures returns, per table, an INSERT that produces a valid row.
//
// Only INSERT needs real data. SELECT, UPDATE and DELETE are probed with
// statements that require the privilege but touch no rows (see
// probeStatements), so the fixture burden is bounded to the tables the
// manifest actually lets the runtime write.
//
// Order matters: this is dependency order, so a foreign key always has its
// parent. Written once here rather than per test, because every later
// authschema suite needs the same rows.
func insertFixtures(ids map[string]string) []struct {
	Table string
	SQL   string
	Args  []any
} {
	type fixture = struct {
		Table string
		SQL   string
		Args  []any
	}
	return []fixture{
		{"principals", `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'cap') `, []any{ids["principal"]}},
		{"organizations", `INSERT INTO auth.organizations (id, slug, name) VALUES ($1, 'cap-org', 'Cap Org')`, []any{ids["org"]}},
		{"users", `INSERT INTO auth.users (principal_id, email, email_lower) VALUES ($1, 'Cap@example.com', 'cap@example.com')`, []any{ids["principal"]}},
		{"external_identities", `INSERT INTO auth.external_identities (principal_id, provider, subject) VALUES ($1, 'cap-idp', 'cap-subject')`, []any{ids["principal"]}},
		{"memberships", `INSERT INTO auth.memberships (organization_id, principal_id) VALUES ($1, $2)`, []any{ids["org"], ids["principal"]}},
		{"platform_role_assignments", `INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ($1, 'cap.platform')`, []any{ids["principal"]}},
		{"service_accounts", `INSERT INTO auth.service_accounts (principal_id, organization_id, name) VALUES ($1, $2, 'cap-svc')`, []any{ids["svcPrincipal"], ids["org"]}},
		{"workloads", `INSERT INTO auth.workloads (principal_id, organization_id, name, trust_domain, issuer, namespace, service_account_name, audience) VALUES ($1, $2, 'cap-wl', 'td', 'iss', 'ns', 'sa', 'aud')`, []any{ids["wlPrincipal"], ids["org"]}},
		{"sessions", `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at) VALUES ($1, $2, '\xdeadbeef'::bytea, now() + interval '1 hour')`, []any{ids["session"], ids["principal"]}},
		{"refresh_credentials", `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at) VALUES ($1, $2, '\xfeedface'::bytea, now() + interval '1 day')`, []any{ids["session"], ids["principal"]}},
		{"credential_registry", `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm) VALUES ($1, 'cap-class', '\xabad1dea'::bytea, 'sha256')`, []any{ids["principal"]}},
		{"resource_grants", `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'cap.role', 'repo', 'r1')`, []any{ids["principal"], ids["org"]}},
		{"policy_revisions", `INSERT INTO auth.policy_revisions (revision, digest, document) VALUES (1, '\x01'::bytea, '{}'::jsonb)`, nil},
		{"entitlement_snapshots", `INSERT INTO auth.entitlement_snapshots (organization_id, sequence, document, digest) VALUES ($1, 1, '{}'::jsonb, '\x02'::bytea)`, []any{ids["org"]}},
		{"security_audit_events", `INSERT INTO auth.security_audit_events (event_type, outcome) VALUES ('cap.probe', 'allowed')`, nil},
		{"auth_outbox_events", `INSERT INTO auth.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key) VALUES ('principal', $1, 'cap.created', '{}'::jsonb, 'cap-idem-1')`, []any{ids["principal"]}},
	}
}

// probeStatements returns the statement that exercises one declared privilege.
//
// UPDATE and DELETE use `WHERE false`: PostgreSQL checks table privileges when
// the statement is planned, independent of how many rows match, so this proves
// the grant without needing data or a valid new value. That claim is not taken
// on trust -- TestCapabilityProbesRequireTheGrant below asserts the same
// statements FAIL on a table the manifest grants read-only.
func probeStatements(table, column string) map[string]string {
	return map[string]string{
		"SELECT": fmt.Sprintf(`SELECT count(*) FROM auth.%s`, table),
		"UPDATE": fmt.Sprintf(`UPDATE auth.%s SET %s = %s WHERE false`, table, column, column),
		"DELETE": fmt.Sprintf(`DELETE FROM auth.%s WHERE false`, table),
	}
}

// TestRuntimeCanExerciseEveryDeclaredPrivilege is the positive control,
// DERIVED FROM THE MANIFEST.
//
// The previous hand-written control proved 4 of the 55 privileges
// RuntimePosture declares, across 3 of 20 tables (lane-auth-contracts, P1,
// arithmetic re-run here). Deleting `sessions`, `refresh_credentials` or
// `auth_outbox_events` from the manifest left the whole suite GREEN -- and the
// last is not coverage debt: G-53 requires the state change and its outbox
// event to commit in ONE transaction, so a missing INSERT grant there fails
// the security write itself.
//
// WHAT THIS DOES NOT PROVE, measured rather than assumed. Removing a whole
// table from the manifest goes RED (TestEveryCreatedTableHasADeclaredPosture
// catches it, and for `sessions` the FK cascade fails this suite too).
// Removing a SINGLE PRIVILEGE from an entry stays GREEN -- verified by
// deleting `Insert` from auth_outbox_events and watching the suite pass --
// because the loop probes what the manifest declares, so a narrower manifest
// simply means fewer probes. This control therefore proves that everything
// DECLARED works; it does not prove that everything NEEDED is declared.
// Closing that second gap needs a functional test of the real flow -- a
// mutation and its outbox event committing in one transaction -- which is
// CHAOS-4885's work, not this package's. Saying so here because "55 of 55"
// invites exactly the over-reading this ticket has already been bitten by
// four times.
//
// This iterates the manifest instead. Every declared privilege is exercised,
// and the loop asserts it covered every entry, so adding a table to
// RuntimePosture without a fixture fails the build rather than silently
// widening the unproven set. That is the same shape as
// TestRuntimeSequencesCoverEveryGeneratedColumn -- which this package already
// had, for sequences, while the capability side was a second, narrower list
// standing beside the manifest it was supposed to check.
func TestRuntimeCanExerciseEveryDeclaredPrivilege(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)

	ids := map[string]string{
		"principal":    "11111111-1111-4111-8111-111111111111",
		"svcPrincipal": "22222222-2222-4222-8222-222222222222",
		"wlPrincipal":  "33333333-3333-4333-8333-333333333333",
		"org":          "44444444-4444-4444-8444-444444444444",
		"session":      "55555555-5555-4555-8555-555555555555",
	}
	// The two extra principals exist because service_accounts and workloads
	// each take a principal_id primary key, so they cannot share one row.
	for _, id := range []string{ids["svcPrincipal"], ids["wlPrincipal"]} {
		if _, err := env.runtime.Exec(ctx,
			`INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'service_account', 'cap')`, id,
		); err != nil {
			t.Fatalf("seed principal %s: %v", id, err)
		}
	}

	fixtures := insertFixtures(ids)
	insertByTable := make(map[string]int, len(fixtures))
	for index, fixture := range fixtures {
		insertByTable[fixture.Table] = index
	}

	proven := make(map[string]map[string]bool)
	record := func(table, privilege string) {
		if proven[table] == nil {
			proven[table] = make(map[string]bool)
		}
		proven[table][privilege] = true
	}

	// INSERT first, in dependency order, so later probes have rows to see.
	for _, fixture := range fixtures {
		entry, declared := postureEntry(fixture.Table)
		if !declared || !entry.Insert {
			continue
		}
		if _, err := env.runtime.Exec(ctx, fixture.SQL, fixture.Args...); err != nil {
			t.Errorf("INSERT on auth.%s was refused or invalid: %v", fixture.Table, err)
			continue
		}
		record(fixture.Table, "INSERT")
	}

	for _, entry := range RuntimePosture() {
		column := env.firstColumn(t, ctx, entry.Table)
		probes := probeStatements(entry.Table, column)
		for _, privilege := range []string{"SELECT", "UPDATE", "DELETE"} {
			if !entry.has(privilege) {
				continue
			}
			if _, err := env.runtime.Exec(ctx, probes[privilege]); err != nil {
				t.Errorf("%s on auth.%s was refused: %v", privilege, entry.Table, err)
				continue
			}
			record(entry.Table, privilege)
		}
	}

	// THE COVERAGE ASSERTION. Without this the loop above could quietly skip a
	// table and still pass, which is exactly how the previous control reached
	// 4 of 55.
	var missing []string
	declaredCount, provenCount := 0, 0
	for _, entry := range RuntimePosture() {
		for _, privilege := range entry.privileges() {
			declaredCount++
			if proven[entry.Table][privilege] {
				provenCount++
				continue
			}
			missing = append(missing, entry.Table+"."+privilege)
		}
	}
	sort.Strings(missing)
	t.Logf("manifest privileges proven against a live server: %d of %d", provenCount, declaredCount)
	if len(missing) > 0 {
		t.Fatalf(
			"%d declared privilege(s) were never exercised: %s\n"+
				"Add a fixture or a probe; an unexercised grant can be deleted from the manifest "+
				"with this suite still green.",
			len(missing), strings.Join(missing, ", "),
		)
	}
	if declaredCount == 0 {
		t.Fatal("the manifest is empty, so this control proves nothing")
	}
}

// TestCapabilityProbesRequireTheGrant is the control FOR the control.
//
// The probes above use `WHERE false` for UPDATE and DELETE, on the claim that
// PostgreSQL checks privileges at plan time regardless of matching rows. If
// that claim were wrong, every UPDATE/DELETE probe would pass whether or not
// the grant existed and the coverage number would be a fiction. So the same
// statements are run against a table the manifest grants read-only, and must
// be REFUSED.
func TestCapabilityProbesRequireTheGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthFixture(t, ctx)

	// signing_keys is SELECT-only in the manifest.
	column := env.firstColumn(t, ctx, "signing_keys")
	probes := probeStatements("signing_keys", column)

	if _, err := env.runtime.Exec(ctx, probes["SELECT"]); err != nil {
		t.Fatalf("SELECT on a SELECT-granted table was refused: %v", err)
	}
	for _, privilege := range []string{"UPDATE", "DELETE"} {
		_, err := env.runtime.Exec(ctx, probes[privilege])
		if err == nil {
			t.Fatalf(
				"%s ... WHERE false SUCCEEDED on a read-only table; the probe does not require the "+
					"grant, so every UPDATE/DELETE coverage claim in this package is vacuous", privilege,
			)
		}
		if code := asPGCode(err); code != insufficientPrivilege {
			t.Fatalf("%s -> SQLSTATE %s, want %s", privilege, code, insufficientPrivilege)
		}
		t.Logf("$ %s\n    ERROR: %s (SQLSTATE %s)", probes[privilege], pgMessage(err), asPGCode(err))
	}
}

// postureEntry looks one table up in the manifest.
func postureEntry(table string) (TablePrivilege, bool) {
	for _, entry := range RuntimePosture() {
		if entry.Table == table {
			return entry, true
		}
	}
	return TablePrivilege{}, false
}

// has reports whether an entry declares one named privilege.
func (t TablePrivilege) has(privilege string) bool {
	switch privilege {
	case "SELECT":
		return t.Select
	case "INSERT":
		return t.Insert
	case "UPDATE":
		return t.Update
	case "DELETE":
		return t.Delete
	}
	return false
}
