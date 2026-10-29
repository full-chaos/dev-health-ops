package restorefence

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func testSchema(t *testing.T) authschema.ValidatedIdentifier {
	t.Helper()
	id, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestEverySectionNamesItsTableAndKeepsAuthAndLegacySeparate(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range sections {
		if seen[s.name] {
			t.Errorf("section %s twice", s.name)
		}
		seen[s.name] = true
		prefix := "auth."
		if s.legacy {
			prefix = "public."
		}
		if s.name != prefix+s.table {
			t.Errorf("section %q: want the name %q", s.name, prefix+s.table)
		}
		if got := s.qualified(testSchema(t)); s.legacy && !strings.HasPrefix(got, "public.") || !s.legacy && !strings.HasPrefix(got, `"auth".`) {
			t.Errorf("section %s qualifies as %s", s.name, got)
		}
	}
	for _, want := range []string{"public.refresh_tokens", "public.internal_service_credentials", "public.external_ingest_tokens", "public.pagerduty_webhook_bindings", "auth.sessions", "auth.refresh_credentials", "auth.credential_registry", "auth.signing_keys",
		"auth.principals", "auth.workload_bindings", "auth.organizations", "public.saml_assertion_replays",
		"auth.memberships", "auth.service_accounts", "auth.workloads", "auth.link_credentials", "auth.saml_assertion_uses", "auth.users", "public.provider_oauth_revocations"} {
		if !seen[want] {
			t.Errorf("section %s is missing", want)
		}
	}
}

func TestEveryApplyStatementIsGuardedAndSingle(t *testing.T) {
	for _, s := range sections {
		sql := s.applySQL(testSchema(t))
		if strings.Contains(sql, ";") {
			t.Errorf("%s: apply SQL holds more than one statement", s.name)
		}
		switch s.kind {
		case kindRevoke:
			if !strings.Contains(sql, "AND ("+s.active+")") {
				t.Errorf("%s: a revoke statement without its active guard would rewrite an already revoked row", s.name)
			}
			if strings.Contains(strings.ToUpper(sql), "INSERT") {
				t.Errorf("%s: a revoke statement must never insert a row", s.name)
			}
		case kindCounter:
			if !strings.Contains(sql, "GREATEST(t.token_version, x.token_version)") || !strings.Contains(sql, "t.is_active AND x.is_active") {
				t.Errorf("%s: the counter may only rise and a user may only be switched off", s.name)
			}
		case kindPresence:
			if !strings.Contains(sql, "DELETE FROM") || strings.Contains(strings.ToUpper(sql), "UPDATE ") {
				t.Errorf("%s: a presence section only deletes", s.name)
			}
		}
	}
}

// The carried tables and columns must exist in the schema this commit builds:
// the auth lineage for auth.*, the legacy baseline for public.*. The fence's
// SQL is run by the lead against real PostgreSQL; this catches a renamed or
// misspelled column before that.
func TestEveryCarriedTableAndColumnExistsInTheSchemaItIsReadFrom(t *testing.T) {
	migrations, err := authschema.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var authSQL strings.Builder
	for _, m := range migrations {
		authSQL.WriteString(m.SQL)
		authSQL.WriteString("\n")
	}
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal", "pgmigrate", "baseline", "head.json"))
	if err != nil {
		t.Fatal(err)
	}
	var head struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		t.Fatal(err)
	}
	for _, s := range sections {
		source, create := authSQL.String(), regexp.MustCompile(`(?s)CREATE TABLE `+s.table+`\s*\((.*?)\n\);`)
		if s.legacy {
			source = head.Schema + "\n" + legacyMigrationSQL(t, root)
			create = regexp.MustCompile(`(?s)CREATE TABLE (?:IF NOT EXISTS )?(?:public\.)?"?` + s.table + `"?\s*\((.*?)\n\)\s*;`)
		}
		m := create.FindStringSubmatch(source)
		if m == nil {
			t.Errorf("%s: no CREATE TABLE found", s.name)
			continue
		}
		for _, c := range s.columnNames() {
			inCreate := regexp.MustCompile(`(?m)^\s+"?` + c + `"?\s`).MatchString(m[1])
			added := regexp.MustCompile(`(?is)ALTER TABLE ` + s.table + `\b.*?ADD COLUMN ` + c + `\b`).MatchString(source)
			if !inCreate && !added {
				t.Errorf("%s: column %s is not declared by the schema", s.name, c)
			}
		}
		if s.touch {
			inCreate := regexp.MustCompile(`(?m)^\s+"?updated_at"?\s`).MatchString(m[1])
			if !inCreate {
				t.Errorf("%s: touch is set but the table has no updated_at", s.name)
			}
		}
	}
}

func TestAMembershipThatWasSuspendedCanBeRemovedAndNothingIsEverReactivated(t *testing.T) {
	def, ok := sectionByName("auth.memberships")
	if !ok {
		t.Fatal("auth.memberships is not a section")
	}
	sql := def.applySQL(testSchema(t))
	for _, want := range []string{"'suspended'", "'removed'", "'invited'", "'active'"} {
		if !strings.Contains(def.active+def.revoked, want) {
			t.Errorf("the membership rule does not name %s", want)
		}
	}
	if strings.Contains(def.revoked, "'active'") {
		t.Error("an active membership is not a revoked state")
	}
	if !strings.Contains(sql, "status = x.status") {
		t.Error("the membership status is not copied from the source row")
	}
}

func TestInsertAndReportSectionsHaveTheirOwnShape(t *testing.T) {
	for _, s := range sections {
		sql := strings.ToUpper(s.applySQL(testSchema(t)))
		switch s.kind {
		case kindInsert:
			if !strings.Contains(sql, "ON CONFLICT DO NOTHING") || strings.Contains(sql, "DELETE") || strings.Contains(sql, "UPDATE ") {
				t.Errorf("%s: replay evidence is only ever inserted, and an existing row is left alone", s.name)
			}
		case kindReport:
			for _, verb := range []string{"INSERT", "UPDATE ", "DELETE"} {
				if strings.Contains(sql, verb) {
					t.Errorf("%s: a report section writes nothing (found %s)", s.name, verb)
				}
			}
		}
	}
	for _, name := range []string{"auth.saml_assertion_uses"} {
		if def, _ := sectionByName(name); def.kind != kindInsert {
			t.Errorf("%s must be an insert section", name)
		}
	}
	for _, name := range []string{"auth.users", "public.provider_oauth_revocations"} {
		if def, _ := sectionByName(name); def.kind != kindReport {
			t.Errorf("%s must be a report section", name)
		}
	}
}

// The ids of what needs an operator action never carry hash or token bytes.
func TestOnlyTheReplayEvidenceColumnMayNameAHash(t *testing.T) {
	for _, s := range sections {
		for _, c := range s.columnNames() {
			if strings.Contains(c, "hash") && !(s.name == "auth.saml_assertion_uses" && c == "assertion_id_hash") {
				t.Errorf("section %s carries %s", s.name, c)
			}
		}
	}
}

// legacyMigrationSQL is the rendered SQL of the legacy migrations after the baseline: a table such as
// saml_assertion_replays is created there, not in the baseline schema.
func legacyMigrationSQL(t *testing.T, root string) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "internal", "pgmigrate", "sql", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
		b.WriteString("\n")
	}
	return b.String()
}
