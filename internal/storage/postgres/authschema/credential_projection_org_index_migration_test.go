package authschema

import (
	"regexp"
	"testing"
)

const credentialProjectionOrgIndexMigration = "credential_projection_org_index"

// The acr reconcile read is per organization and domain, in reference order (CHAOS-11199). The primary key leads with
// the domain and the reference, so it cannot serve the organization; this index leads with both filters and ends on
// the keyset column, so the read is one range scan with no sort.
func TestCredentialProjectionOrgIndexMigrationIsOneAdditiveIndex(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	for _, m := range migrations {
		if m.Name == credentialProjectionOrgIndexMigration {
			sql = sqlCode(m.SQL)
		}
	}
	if sql == "" {
		t.Fatalf("the lineage lacks the migration named %s", credentialProjectionOrgIndexMigration)
	}
	if !regexp.MustCompile(`(?s)\ACREATE INDEX credential_projection_org_idx\s+ON credential_projection \(domain_owner, organization_id, external_reference\);\s*\z`).MatchString(sql) {
		t.Fatalf("the migration is not exactly the one index: %q", sql)
	}
}
