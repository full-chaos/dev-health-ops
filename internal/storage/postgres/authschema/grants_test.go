package authschema

import (
	"errors"
	"strings"
	"testing"
)

func postureByTable(t *testing.T) map[string]TablePrivilege {
	t.Helper()
	byTable := make(map[string]TablePrivilege)
	for _, entry := range RuntimePosture() {
		if _, duplicate := byTable[entry.Table]; duplicate {
			t.Fatalf("RuntimePosture declares %q twice", entry.Table)
		}
		byTable[entry.Table] = entry
	}
	return byTable
}

// TestAppendOnlyTablesGrantNoMutation is the load-bearing posture assertion.
//
// These four tables are the ones whose value comes entirely from being
// unalterable after the fact. An audit row the runtime can UPDATE is not
// evidence; a policy revision that can be edited stops explaining the decision
// recorded against it. The ONLY thing that makes them append-only is the
// absence of those privileges — no comment, constraint or convention in this
// schema enforces it — so their absence is asserted here directly.
func TestAppendOnlyTablesGrantNoMutation(t *testing.T) {
	byTable := postureByTable(t)
	for _, table := range []string{
		"security_audit_events",
		"policy_revisions",
		"entitlement_snapshots",
	} {
		entry, declared := byTable[table]
		if !declared {
			t.Errorf("%s has no declared posture", table)
			continue
		}
		if entry.Update || entry.Delete {
			t.Errorf(
				"%s grants UPDATE=%t DELETE=%t; it must be append-only",
				table, entry.Update, entry.Delete,
			)
		}
		if !entry.Insert || !entry.Select {
			t.Errorf("%s must still grant SELECT and INSERT (got %+v)", table, entry)
		}
	}
}

// TestReadOnlyTablesGrantNoWrite pins the tables the runtime may consult but
// must never change. signing_keys is the sharpest: a runtime that can INSERT a
// signing key can mint itself a key and publish the matching public half,
// which converts a service compromise into a token-forging capability.
func TestReadOnlyTablesGrantNoWrite(t *testing.T) {
	byTable := postureByTable(t)
	for _, table := range []string{"signing_keys", "actions", "roles", "role_actions"} {
		entry, declared := byTable[table]
		if !declared {
			t.Errorf("%s has no declared posture", table)
			continue
		}
		if entry.Insert || entry.Update || entry.Delete {
			t.Errorf(
				"%s grants INSERT=%t UPDATE=%t DELETE=%t; it must be read-only to the runtime",
				table, entry.Insert, entry.Update, entry.Delete,
			)
		}
		if !entry.Select {
			t.Errorf("%s must grant SELECT", table)
		}
	}
}

// TestEveryPostureEntryGrantsSomething: an entry with no privileges grants
// nothing while looking like coverage, which is worse than omitting the table.
func TestEveryPostureEntryGrantsSomething(t *testing.T) {
	for _, entry := range RuntimePosture() {
		if len(entry.privileges()) == 0 {
			t.Errorf("%s declares no privileges at all", entry.Table)
		}
	}
}

// TestValidateIdentifierRejectsReservedKeywords applies CHAOS-4881's round-3
// finding one layer down, where it actually matters: this package EMITS the
// schema and role names as bare identifiers, so a reserved keyword here is a
// real syntax error at migration time rather than a theoretical one.
func TestValidateIdentifierRejectsReservedKeywords(t *testing.T) {
	for _, name := range []string{"select", "from", "user", "table", "grant", "default", "check"} {
		if err := ValidateIdentifier(name); !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("ValidateIdentifier(%q) = %v, want a reserved-keyword rejection", name, err)
		}
	}
	for _, name := range []string{
		"", "Auth", "auth-schema", "auth schema", `auth"; DROP SCHEMA public; --`,
		"1auth", "_auth", strings.Repeat("a", 64),
	} {
		if err := ValidateIdentifier(name); err == nil {
			t.Errorf("ValidateIdentifier(%q) accepted a malformed identifier", name)
		}
	}
	// The accepting rows, without which every rejection above could be
	// explained by a validator that rejects everything.
	for _, name := range []string{"auth", "auth_cp", "authservice2", strings.Repeat("a", 63)} {
		if err := ValidateIdentifier(name); err != nil {
			t.Errorf("ValidateIdentifier(%q) = %v, want acceptance", name, err)
		}
	}
}

// TestOptionsValidateRefusesASharedRole pins ACP-ADR-04's central rule at the
// one place it can be violated silently: if the migration connection IS the
// runtime role, every object created is OWNED by the runtime role, which then
// holds DDL over the auth schema permanently — established by a migration run
// that reports success.
func TestOptionsValidateRefusesASharedRole(t *testing.T) {
	options := Options{Schema: "auth", RuntimeRole: "auth_runtime"}

	if err := options.Validate("auth_runtime"); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Validate accepted a migration role identical to the runtime role: %v", err)
	}
	if err := options.Validate("auth_migrator"); err != nil {
		t.Fatalf("Validate rejected distinct roles: %v", err)
	}
	// An unknown migration role (empty) must not silently pass the
	// distinctness check as if it had been verified.
	if err := options.Validate(""); err != nil {
		t.Fatalf("Validate with an unread current_user = %v, want it to defer rather than fail", err)
	}
}

// TestRuntimeSequencesCoverEveryGeneratedColumn: a bigserial column needs
// USAGE on its sequence, and an INSERT privilege alone is not enough. The
// failure appears only at the first write — for the audit table, at the first
// security event.
func TestRuntimeSequencesCoverEveryGeneratedColumn(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	all := strings.Join(func() []string {
		bodies := make([]string, 0, len(migrations))
		for _, migration := range migrations {
			bodies = append(bodies, migration.SQL)
		}
		return bodies
	}(), "\n")

	declared := make(map[string]struct{})
	for _, sequence := range runtimeSequences() {
		declared[sequence] = struct{}{}
	}

	byTable := postureByTable(t)
	for _, table := range createdTablesFromSQL(all) {
		if !strings.Contains(tableBody(all, table), "bigserial") {
			continue
		}
		entry, declaredPosture := byTable[table]
		if !declaredPosture || !entry.Insert {
			continue // the runtime cannot insert, so it needs no sequence.
		}
		expected := table + "_id_seq"
		if _, ok := declared[expected]; !ok {
			t.Errorf(
				"%s has a bigserial column and the runtime may INSERT into it, "+
					"but %q is not in runtimeSequences; the first INSERT will fail on nextval",
				table, expected,
			)
		}
	}
}

// The injection corpus that used to live here moved to identifier_test.go, onto
// NewValidatedIdentifier, when CHAOS-4917 made the constructor the security
// boundary. It is NOT duplicated here: two copies of an 18-row corpus in one
// package is the same hand-maintained-list-beside-a-source-of-truth problem this
// package exists to remove, and the copy that drifts is always the one nobody
// is looking at. The constructor calls ValidateIdentifier, so the corpus covers
// both.
