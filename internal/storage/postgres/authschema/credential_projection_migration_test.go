package authschema

import (
	"regexp"
	"strings"
	"testing"
)

const credentialProjectionMigration = "credential_projection"

func credentialProjectionSQL(t *testing.T) string {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Name == credentialProjectionMigration {
			return sqlCode(m.SQL)
		}
	}
	t.Fatalf("the lineage lacks the migration named %s", credentialProjectionMigration)
	return ""
}

// The projection of external credentials is keyed by the domain and the
// domain's own reference, not by a push aggregate id, so one set of tables
// serves every domain (CHAOS-10896, CHAOS-11196).
func TestCredentialProjectionTablesAreKeyedByDomain(t *testing.T) {
	sql := credentialProjectionSQL(t)
	for table, key := range map[string]string{
		"credential_projection":        `PRIMARY KEY (domain_owner, external_reference)`,
		"credential_projection_digest": `PRIMARY KEY (domain_owner, external_reference, revision)`,
		"push_sources":                 `PRIMARY KEY (organization_id, source_id)`,
		"projection_cursor":            `stream          text        PRIMARY KEY`,
		"projection_retry":             `PRIMARY KEY (stream, outbox_id)`,
	} {
		body := regexp.MustCompile(`(?s)CREATE TABLE ` + table + ` \(.*?\n\);`).FindString(sql)
		if body == "" {
			t.Errorf("no CREATE TABLE %s", table)
			continue
		}
		if !strings.Contains(body, key) {
			t.Errorf("%s lacks %q", table, key)
		}
	}
	for _, column := range []string{"acr_class", "owner_subject", "max_seen_revision", "rotation_parent_reference", "terminal_at"} {
		if !regexp.MustCompile(`(?s)CREATE TABLE credential_projection \(.*\b` + column + `\b.*?\n\);`).MatchString(sql) {
			t.Errorf("credential_projection has no column %s", column)
		}
	}
}

// Every new table is a security table: written only inside audit.Commit.
func TestCredentialProjectionTablesAreCommitGuarded(t *testing.T) {
	sql := credentialProjectionSQL(t)
	for _, table := range []string{"credential_projection", "credential_projection_digest", "push_sources", "projection_cursor", "projection_retry"} {
		for _, trigger := range []string{
			`CREATE TRIGGER ` + table + `_commit_guard\s+BEFORE INSERT OR UPDATE OR DELETE ON ` + table + `\s+FOR EACH ROW EXECUTE FUNCTION audit_commit_guard\(\);`,
			`CREATE TRIGGER ` + table + `_truncate_guard\s+BEFORE TRUNCATE ON ` + table + `\s+FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard\(\);`,
		} {
			if !regexp.MustCompile(trigger).MatchString(sql) {
				t.Errorf("%s: missing %s", table, trigger)
			}
		}
	}
}

// The reader's read is a range scan over one aggregate type in id order; the
// partial index serves it, and the low-water mark bounds the range.
func TestCredentialProjectionServesTheReaderReadWithAPartialIndex(t *testing.T) {
	sql := credentialProjectionSQL(t)
	if !regexp.MustCompile(`CREATE INDEX auth_outbox_events_push_idx\s+ON auth_outbox_events \(id\) WHERE aggregate_type = 'external_push_credential';`).MatchString(sql) {
		t.Error("no partial index on auth_outbox_events (id) for the external_push_credential aggregate")
	}
	if !strings.Contains(sql, "CREATE INDEX projection_retry_aggregate_idx ON projection_retry (stream, aggregate_id, revision);") {
		t.Error("no retry index by aggregate and revision (the same-aggregate wait reads it)")
	}
}

// domain_revoked joins the closed revoke reason list, validated apart from the swap.
func TestCredentialProjectionWidensTheRevokedReasonList(t *testing.T) {
	sql := credentialProjectionSQL(t)
	if !strings.Contains(sql, "revoked_reason IN ('compromised', 'domain_revoked', 'owner_request')") {
		t.Error("the revoked_reason CHECK does not list domain_revoked")
	}
	if !strings.Contains(sql, ") NOT VALID;") || !strings.Contains(sql, "VALIDATE CONSTRAINT credential_registry_revoked_reason_code;") {
		t.Error("the CHECK is not added NOT VALID and validated in its own statement")
	}
	if !regexp.MustCompile(`(?s)\ALOCK TABLE principals, organizations, credential_registry IN SHARE ROW EXCLUSIVE MODE;`).MatchString(strings.TrimSpace(sql)) {
		t.Error("the first statement does not lock principals, organizations, credential_registry in that order")
	}
}
