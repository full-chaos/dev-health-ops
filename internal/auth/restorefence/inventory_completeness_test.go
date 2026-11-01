package restorefence

import (
	"encoding/json"
	"io/fs"
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
	"auth.login_attempts":   "covered by apply from the source only (RESTORE_FENCE_SOURCE_URI; live locks, raise-only), not a manifest section: the key is an identifier hash",
	"public.login_attempts": "covered by apply from the source only (RESTORE_FENCE_SOURCE_URI; live locks, raise-only), not a manifest section: the key is an email",
	// Not access-bearing state.
	"auth.actions":                      "not access-bearing: closed vocabulary written by the migration role",
	"auth.roles":                        "not access-bearing: closed vocabulary written by the migration role",
	"auth.role_actions":                 "not access-bearing: closed vocabulary written by the migration role",
	"auth.auth_outbox_events":           "not access-bearing: append-only event stream; the revision floors (auth.aggregate_revisions) cover what consumers saw",
	"auth.security_audit_events":        "not access-bearing: append-only audit stream",
	"auth.restore_fence_state":          "not access-bearing: the fence's own state table",
	"auth.sso_flow_states":              "covered by apply, not a manifest section: apply marks every flow state consumed",
	"auth.oauth_authorization_requests": "not access-bearing: a code lives 60 s, so no restore brings one back live, and a request is decided only by its web session, which the fence covers",
	// Nothing can set oauth_grants.revoked_at yet: the writer census (writerinventory) lists one writer, Approve's
	// INSERT, so the first UPDATE fails there. A grant is also only as live as its session, which the fence covers.
	"auth.oauth_grants":             "ticket CHAOS-12254: the client revoke writes revoked_at and adds the fence section with it",
	"public.org_retention_policies": "not access-bearing: retention configuration",
	"public.saved_reports":          "not access-bearing: report definitions",
	"public.sync_configurations":    "not access-bearing: sync configuration",
	"public.billing_plans":          "not access-bearing: billing catalogue",
	"public.billing_prices":         "not access-bearing: billing catalogue",
	// Follow-up 11872 (D6459 am.1).
	"auth.policy_revisions":        "not access-bearing today: dormant, insert-only, no code reads or writes it (ADR-05/07); a reader needs a fence section (the tripwire test enforces it)",
	"auth.workload_assertion_keys": "not access-bearing today: public key metadata for the operator list, no runtime reader and no runtime GRANT (CHAOS-11208); a reader needs a fence section (the tripwire test enforces it)",
	"auth.entitlement_snapshots":   "not access-bearing today: dormant, insert-only, no code reads or writes it (ADR-05/07); a reader needs a fence section (the tripwire test enforces it)",
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
	if len(needs) > 0 {
		t.Errorf("tables excluded pending a ruling (a ticket or a reason is required): %s", strings.Join(needs, ", "))
	}
}

// Tripwire for the two dormant tables above: they are excluded from the fence only because no code reads or writes them.
// A non-test, non-migration Go file that names either table (outside the grants list) fails this test, so the change
// that adds a reader must give the table a fence section or a new, reasoned exclusion.
func TestNoProductionCodeReadsOrWritesTheDormantPolicyAndEntitlementTables(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"internal/storage/postgres/authschema/grants.go": true}
	var hits []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			n := d.Name()
			if n == ".git" || n == "node_modules" || n == "testdata" || n == "migrations" || n == ".venv" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if allowed[rel] {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		if strings.Contains(text, "policy_revisions") || strings.Contains(text, "entitlement_snapshots") {
			hits = append(hits, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) > 0 {
		t.Errorf("production code now names auth.policy_revisions or auth.entitlement_snapshots, so they are no longer dormant: classify them for the fence (a section or a reasoned exclusion) before this passes: %v", hits)
	}
}
