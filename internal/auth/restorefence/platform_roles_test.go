package restorefence

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

const platformRoleSection = "auth.platform_role_assignments"

func TestThePlatformRoleSectionIsAManifestOnlyPresenceSectionOfIDsWithNoPredicate(t *testing.T) {
	def, ok := sectionByName(platformRoleSection)
	if !ok {
		t.Fatalf("no %s section", platformRoleSection)
	}
	if def.kind != kindPresence || !def.manifestOnly || def.legacy || def.table != "platform_role_assignments" || def.suffix != "" {
		t.Fatalf("section = %+v, want a manifest-only presence section of the auth table", def)
	}
	if def.key.name != "id" || len(def.cols) != 0 {
		t.Errorf("the section carries %s + %v, want the id only", def.key.name, def.cols)
	}
	if def.revoked != "" || def.deleteGuard != "" || def.match != "" || def.matchSQL() != "x.id = t.id" {
		t.Errorf("predicates: revoked %q guard %q match %q: no predicate, the id only", def.revoked, def.deleteGuard, def.match)
	}
	if def.action != ActionRegrantPlatformRole || !def.countsAbsent {
		t.Errorf("action %q countsAbsent %v, want regrant_platform_role and the absent count", def.action, def.countsAbsent)
	}
	export := def.exportSQL(testSchema(t))
	for _, banned := range []string{"principal_id", "role_key", "granted_by", "expires_at"} {
		if strings.Contains(export, banned) {
			t.Errorf("the export carries %q: %s", banned, export)
		}
	}
	if def.runsWithoutManifest() {
		t.Error("a blind margin would delete every platform role assignment")
	}
}

func TestThePlatformRoleApplyStatementDeletesAndNeverInsertsOrUpdates(t *testing.T) {
	def, _ := sectionByName(platformRoleSection)
	sql := def.applySQL(testSchema(t))
	up := strings.ToUpper(sql)
	if !strings.Contains(sql, `DELETE FROM "auth".platform_role_assignments t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)`) {
		t.Errorf("the apply must delete the restored rows whose id the source lacks:\n%s", sql)
	}
	if strings.Contains(up, "INSERT") || strings.Contains(up, "UPDATE ") || strings.Contains(sql, ";") {
		t.Errorf("the apply must be one statement that never inserts or updates:\n%s", sql)
	}
	if !strings.Contains(sql, `(SELECT count(*) FROM m x WHERE NOT EXISTS (SELECT 1 FROM "auth".platform_role_assignments t WHERE x.id = t.id))`) {
		t.Errorf("the apply must count the manifest rows with no restored row:\n%s", sql)
	}
}

func TestRegrantPlatformRoleNamesTheManifestIDsWithNoRestoredRowAndNothingElse(t *testing.T) {
	def, _ := sectionByName(platformRoleSection)
	ids := def.idsSQL(testSchema(t))
	want := `SELECT x.id::text FROM m x WHERE NOT EXISTS (SELECT 1 FROM "auth".platform_role_assignments t WHERE x.id = t.id) ORDER BY 1`
	if !strings.Contains(ids, want) {
		t.Errorf("regrant_platform_role must name the manifest ids the restored table lacks, ids only:\n%s", ids)
	}
	for _, banned := range []string{"principal_id", "role_key", "granted_by", "expires_at"} {
		if strings.Contains(ids, banned) {
			t.Errorf("the id list names %q:\n%s", banned, ids)
		}
	}
	if followUp[ActionRegrantPlatformRole] == "" || followUp[ActionNoActivePlatformAdmin] == "" {
		t.Errorf("both new actions need a follow-up: %q / %q", followUp[ActionRegrantPlatformRole], followUp[ActionNoActivePlatformAdmin])
	}
	if ActionRegrantPlatformRole != "regrant_platform_role" || ActionNoActivePlatformAdmin != "no_active_platform_admin" {
		t.Errorf("action names: %q %q", ActionRegrantPlatformRole, ActionNoActivePlatformAdmin)
	}
}

func TestTheLockoutReadUsesTheStoreRuleInOneReadOnlyStatement(t *testing.T) {
	q := platformAdminLockoutSQL(testSchema(t))
	if !strings.Contains(q, rule.ActiveRule("r", "p")) {
		t.Errorf("the lockout read must use rule.ActiveRule, not a copy:\n%s", q)
	}
	if !strings.Contains(q, `FROM "auth".platform_role_assignments r JOIN "auth".principals p ON p.id = r.principal_id WHERE r.role_key = $1`) {
		t.Errorf("the lockout read must join the principal and filter the role:\n%s", q)
	}
	up := strings.ToUpper(q)
	for _, banned := range []string{"INSERT", "UPDATE", "DELETE", ";", "NOW()", "CLOCK_TIMESTAMP"} {
		if strings.Contains(up, banned) {
			t.Errorf("the lockout read holds %q:\n%s", banned, q)
		}
	}
}

func TestTheLockoutFiresOnlyWithZeroActiveAdminsAndAnOwnedSwitchOrARoleInUse(t *testing.T) {
	for _, c := range []struct {
		active int64
		owned  bool
		inUse  bool
		want   bool
	}{
		{0, false, false, false}, {0, true, false, true}, {0, false, true, true}, {0, true, true, true},
		{1, false, false, false}, {1, true, false, false}, {1, false, true, false}, {2, true, true, false},
	} {
		if got := lockoutFires(c.active, c.owned, c.inUse); got != c.want {
			t.Errorf("lockoutFires(%d, owned %v, inUse %v) = %v, want %v", c.active, c.owned, c.inUse, got, c.want)
		}
	}
}

func TestTheLockoutLinePrintsWithZeroIDsAndNoOtherEmptyActionDoes(t *testing.T) {
	lines := ActionLines([]OperatorAction{
		{Kind: ActionNoActivePlatformAdmin, Source: platformRoleSection, IDs: []string{}},
		{Kind: ActionRegrantPlatformRole, Source: platformRoleSection, IDs: []string{}},
		{Kind: ActionForcePasswordReset, Source: "auth.users", IDs: nil},
	})
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "operator-action no_active_platform_admin source=auth.platform_role_assignments count=0 ids= follow-up=") {
		t.Fatalf("lines = %v, want only the lockout line", lines)
	}
	if !strings.Contains(lines[0], "platform-role-bootstrap") {
		t.Errorf("the lockout line must name the break-glass verb: %s", lines[0])
	}
}

func TestOnlyThePresenceStatementWritesPlatformRoleAssignmentsInTheFence(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	write := regexp.MustCompile(`(?is)(INSERT\s+INTO|UPDATE|DELETE\s+FROM)[^"` + "`" + `]{0,40}platform_role_assignments`)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if write.Match(raw) {
			t.Errorf("%s writes platform_role_assignments by hand; only the generated presence statement may", f)
		}
	}
}

func TestEveryOtherSectionExportsWhatVersionFiveHeld(t *testing.T) {
	if ManifestVersion != 6 {
		t.Fatalf("ManifestVersion = %d, want 6: a new section is a new manifest version", ManifestVersion)
	}
	if got := sectionSetDigest(t, platformRoleSection); got != pinnedSectionSet[5] {
		t.Errorf("a section other than %s changed its export (digest %s, version 5 pinned %s)", platformRoleSection, got, pinnedSectionSet[5])
	}
}

// The lockout read must see the effect of every section (principals disabled, the platform role deletions), and the
// count of assignments that decides "in use" must be read before any section deletes one.
func TestTheLockoutReadRunsAfterEverySectionAndTheInUseCountBeforeThem(t *testing.T) {
	raw, err := os.ReadFile("apply.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	count := strings.Index(src, "platformRolesCountSQL(schema)")
	loop := strings.Index(src, "for _, s := range sections {\n\t\t\t\tif m == nil {")
	lock := strings.Index(src, "platformAdminLockout(ctx, tx, schema")
	flows := strings.Index(src, "flowStatesSQL(schema))")
	if count < 0 || loop < 0 || lock < 0 || flows < 0 {
		t.Fatalf("anchors missing: count %d loop %d lockout %d flows %d", count, loop, lock, flows)
	}
	if !(count < loop && loop < lock && lock < flows) {
		t.Errorf("order wrong: in-use count %d, section loop %d, lockout read %d, flow states %d", count, loop, lock, flows)
	}
}
