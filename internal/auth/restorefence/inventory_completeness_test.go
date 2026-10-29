package restorefence

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// excluded names every table that is deliberately not a fence section, with the reason or the follow-up. A table of the
// auth schema, or a legacy public table that carries an access state, must be a section or be listed here.
var excluded = map[string]string{
	// Ruled out with a ticket (D6453 am.2, D6454, D6459).
	"auth.external_identities":       "ticket 11848: unlink is a DELETE",
	"auth.login_attempts":            "ticket 11848: lockout counters",
	"public.login_attempts":          "ticket 11848: lockout counters",
	"auth.platform_role_assignments": "ticket 11846 (ACP 4.15a/4.15b): a revoke is a DELETE the fence cannot see",
	// Not access-bearing state.
	"auth.actions":                  "not access-bearing: closed vocabulary written by the migration role",
	"auth.roles":                    "not access-bearing: closed vocabulary written by the migration role",
	"auth.role_actions":             "not access-bearing: closed vocabulary written by the migration role",
	"auth.auth_outbox_events":       "not access-bearing: append-only event stream; the revision floors (auth.aggregate_revisions) cover what consumers saw",
	"auth.security_audit_events":    "not access-bearing: append-only audit stream",
	"auth.restore_fence_state":      "not access-bearing: the fence's own state table",
	"auth.sso_flow_states":          "covered by apply, not a manifest section: apply marks every flow state consumed",
	"public.org_retention_policies": "not access-bearing: retention configuration",
	"public.saved_reports":          "not access-bearing: report definitions",
	"public.sync_configurations":    "not access-bearing: sync configuration",
	"public.billing_plans":          "not access-bearing: billing catalogue",
	"public.billing_prices":         "not access-bearing: billing catalogue",
	// Not yet ruled: an access-removing change after the target may come back. Raised to acp with the r3 findings.
	"auth.policy_revisions":           "NEEDS-RULING: an older active policy after a restore (the revision floor stops re-issue of a revision number only)",
	"auth.entitlement_snapshots":      "NEEDS-RULING: an older entitlement snapshot after a restore",
	"public.organizations":            "NEEDS-RULING: a legacy organization deactivated after T (is_active) returns active",
	"public.org_ip_allowlist":         "NEEDS-RULING: an allowlist entry deactivated after T (is_active) returns active",
	"public.integrations":             "NEEDS-RULING: provider integration flags (is_active); already named not carried in RISK-NOTES",
	"public.integration_credentials":  "NEEDS-RULING: provider credential flags (is_active); already named not carried in RISK-NOTES",
	"public.github_app_installations": "NEEDS-RULING: an installation suspended after T (suspended_at) returns unsuspended",
}

// accessStateColumns are the columns that carry an access-removing state in the legacy public schema.
var accessStateColumns = regexp.MustCompile(`(?m)^\s+"?(revoked_at|consumed_at|disabled_at|token_hash|token_version|is_active|accepted_at|locked_until|suspended_at)"?\s`)

func sectionNames() map[string]bool {
	m := map[string]bool{floorsSection: true}
	for _, s := range sections {
		m[s.name] = true
		if s.legacy {
			m["public."+s.table] = true
		} else {
			m["auth."+s.table] = true
		}
	}
	return m
}

// Round1122 P3: a revocation table the fence omits must fail a test, not be found by review.
func TestEveryAuthTableAndEveryLegacyAccessTableIsASectionOrExcluded(t *testing.T) {
	covered := sectionNames()
	var missing []string
	migrations, err := authschema.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	create := regexp.MustCompile(`CREATE TABLE (\w+)`)
	for _, m := range migrations {
		for _, g := range create.FindAllStringSubmatch(m.SQL, -1) {
			name := "auth." + g[1]
			if !covered[name] && excluded[name] == "" {
				missing = append(missing, name)
			}
		}
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
	legacy := regexp.MustCompile(`(?s)CREATE TABLE (?:IF NOT EXISTS )?(?:public\.)?"?(\w+)"? \((.*?)\n\);`)
	for _, g := range legacy.FindAllStringSubmatch(head.Schema, -1) {
		if !accessStateColumns.MatchString(g[2]) {
			continue
		}
		name := "public." + g[1]
		if !covered[name] && excluded[name] == "" {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("tables that are neither a fence section nor excluded with a reason:\n  %s", strings.Join(missing, "\n  "))
	}
	var needs []string
	for name, reason := range excluded {
		if covered[name] {
			t.Errorf("%s is both a section and excluded", name)
		}
		okReason := strings.HasPrefix(reason, "ticket ") || strings.HasPrefix(reason, "not access-bearing") ||
			strings.HasPrefix(reason, "covered by apply") || strings.HasPrefix(reason, "NEEDS-RULING")
		if !okReason {
			t.Errorf("%s: the exclusion needs a ticket or a stated reason, got %q", name, reason)
		}
		if strings.HasPrefix(reason, "NEEDS-RULING") {
			needs = append(needs, name)
		}
	}
	sort.Strings(needs)
	t.Logf("tables excluded pending an acp ruling: %s", strings.Join(needs, ", "))
}
