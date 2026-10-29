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
	for _, want := range []string{"public.refresh_tokens", "public.internal_service_credentials", "public.external_ingest_tokens", "public.pagerduty_webhook_bindings", "auth.sessions", "auth.refresh_credentials", "auth.credential_registry", "auth.signing_keys"} {
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
			source = head.Schema
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
