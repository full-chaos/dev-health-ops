package restorefence

import (
	"strings"
	"testing"
)

// The fence rule: a change after the target that REMOVES access must survive a
// restore. These tests pin the carried removal for each table the round named.
func TestARefreshCredentialConsumedAfterTheTargetIsCarried(t *testing.T) {
	def, ok := sectionByName("auth.refresh_credentials")
	if !ok {
		t.Fatal("auth.refresh_credentials is not a section")
	}
	carried := map[string]bool{}
	for _, c := range def.columnNames() {
		carried[c] = true
	}
	if !carried["consumed_at"] {
		t.Fatal("consumed_at is not carried: a credential rotated after the target is usable again after a restore")
	}
	if !strings.Contains(def.revoked, "consumed_at IS NOT NULL") {
		t.Error("a consumed source row is not exported")
	}
	sql := def.applySQL(testSchema(t))
	if !strings.Contains(sql, "consumed_at = COALESCE(t.consumed_at, x.consumed_at)") || !strings.Contains(sql, "revoked_at = COALESCE(t.revoked_at, x.revoked_at)") {
		t.Error("a column already set on the restored row must never be cleared or moved (consumed_at is write-once)")
	}
}

func TestDisabledPrincipalsBindingsAndOrganizationsAreCarried(t *testing.T) {
	for _, name := range []string{"auth.principals", "auth.workload_bindings", "auth.organizations"} {
		def, ok := sectionByName(name)
		if !ok {
			t.Errorf("%s is not a section", name)
			continue
		}
		if def.kind != kindRevoke || def.revoked != "s.disabled_at IS NOT NULL" || def.active != "t.disabled_at IS NULL" {
			t.Errorf("%s must copy disabled_at onto a restored row that is still enabled", name)
		}
	}
}

func TestTheSamlReplayTableOfTheLegacySchemaIsInsertedBack(t *testing.T) {
	def, ok := sectionByName("public.saml_assertion_replays")
	if !ok {
		t.Fatal("public.saml_assertion_replays is not a section")
	}
	if def.kind != kindInsert || !def.legacy {
		t.Fatal("the legacy replay table is an insert-missing section of the public schema")
	}
	sql := def.applySQL(testSchema(t))
	for _, want := range []string{"INSERT INTO public.saml_assertion_replays (provider_id, assertion_id, expires_at)", "ON CONFLICT DO NOTHING"} {
		if !strings.Contains(sql, want) {
			t.Errorf("replay apply SQL lacks %q", want)
		}
	}
	if strings.Contains(strings.ToUpper(sql), "DELETE") || strings.Contains(strings.ToUpper(sql), "UPDATE ") {
		t.Error("replay evidence is only inserted")
	}
}

func TestEverySsoFlowStateOfTheRestoredDatabaseIsInvalidated(t *testing.T) {
	sql := flowStatesSQL(testSchema(t))
	for _, want := range []string{`UPDATE "auth".sso_flow_states`, "SET consumed_at = clock_timestamp()", "WHERE consumed_at IS NULL"} {
		if !strings.Contains(sql, want) {
			t.Errorf("flow state SQL lacks %q", want)
		}
	}
	if strings.Contains(sql, ";") || strings.Contains(strings.ToUpper(sql), "DELETE") {
		t.Error("one UPDATE statement, never a DELETE")
	}
}
