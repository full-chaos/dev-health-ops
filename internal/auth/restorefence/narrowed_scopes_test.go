package restorefence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// narrowed is the SET of one carried array column: the restored elements that the source also holds, in the restored
// order. A NULL source narrows to empty.
func narrowed(col string) string {
	return col + " = ARRAY(SELECT a FROM unnest(t." + col + ") WITH ORDINALITY e(a, o) WHERE a::text = ANY(COALESCE(x." + col + "::text[], '{}'::text[])) ORDER BY o)"
}

func held(col string) string {
	return "t." + col + "::text[] <@ COALESCE(x." + col + "::text[], '{}'::text[])"
}

// A scope narrowed after T is narrowed again: the restored set is cut to its intersection with the source. Nothing is
// ever added (a value added after T is not restored), and a swap after T leaves the empty set, named for review.
func TestCarriedScopesAreCutToTheSourceAndNeverWidened(t *testing.T) {
	for _, c := range []struct {
		name, key, revoked string
		cols               []string
		legacy             bool
	}{
		{"auth.workloads.scope", "principal_id", "", []string{"audiences", "allowed_actions"}, false},
		{"auth.credential_registry.scope", "id", "s.revoked_at IS NULL", []string{"scopes", "audiences"}, false},
	} {
		def := mustSection(t, c.name)
		if def.kind != kindRevoke || def.legacy != c.legacy || def.key.name != c.key || def.manifestOnly || def.suffix != ".scope" || def.revoked != c.revoked {
			t.Errorf("%s: want a revoke section keyed by %s, suffix .scope, export %q; got kind %v key %s manifestOnly %v suffix %q revoked %q",
				c.name, c.key, c.revoked, def.kind, def.key.name, def.manifestOnly, def.suffix, def.revoked)
		}
		if strings.Join(def.columnNames()[1:], ",") != strings.Join(c.cols, ",") {
			t.Errorf("%s carries %v, want %v", c.name, def.columnNames()[1:], c.cols)
		}
		var guard []string
		for _, col := range c.cols {
			guard = append(guard, held(col))
		}
		if want := "NOT (" + strings.Join(guard, " AND ") + ")"; def.active != want {
			t.Errorf("%s: guard %q, want %q (an unchanged row is not written)", c.name, def.active, want)
		}
		sql := def.applySQL(testSchema(t))
		for _, col := range c.cols {
			if !strings.Contains(sql, narrowed(col)) {
				t.Errorf("%s: apply SQL lacks the intersection of %s: %s", c.name, col, sql)
			}
			if strings.Contains(sql, col+" = x."+col) {
				t.Errorf("%s: %s is copied from the source (a value added after T would be granted): %s", c.name, col, sql)
			}
		}
		exp := def.exportSQL(testSchema(t))
		for _, col := range c.cols {
			if !strings.Contains(exp, "'"+col+"', s."+col+"::text") {
				t.Errorf("%s: %s must travel as the array literal text (the manifest holds scalars only): %s", c.name, col, exp)
			}
		}
		if def.action != ActionKind("review_scope_emptied") {
			t.Errorf("%s: action %q, want review_scope_emptied", c.name, def.action)
		}
		ids := def.idsSQL(testSchema(t))
		for _, col := range c.cols {
			if !strings.Contains(ids, "cardinality(t."+col+") > 0 AND NOT (t."+col+"::text[] && COALESCE(x."+col+"::text[], '{}'::text[]))") {
				t.Errorf("%s: the review must name a row whose %s is cut to empty: %s", c.name, col, ids)
			}
		}
	}
}

// C1: every carried array column is NOT NULL in the DDL; the SQL still narrows a NULL source to empty (COALESCE).
func TestEveryCarriedArrayColumnIsNotNullInTheSchema(t *testing.T) {
	migrations, err := authschema.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, m := range migrations {
		all.WriteString(m.SQL)
		all.WriteString("\n")
	}
	for _, ddl := range []string{
		`ADD COLUMN audiences text[] NOT NULL DEFAULT '{}'`,
		`ADD COLUMN allowed_actions text[] NOT NULL DEFAULT '{}'`,
		`ADD COLUMN audiences credential_token_name[] NOT NULL DEFAULT '{}'`,
		`ADD COLUMN scopes    credential_scope[] NOT NULL DEFAULT '{}'`,
	} {
		if !strings.Contains(all.String(), ddl) {
			t.Errorf("the auth lineage no longer declares %q: re-check the NULL rule of the fence's scope sections", ddl)
		}
	}
}

// C2: the carried vocabularies hold no character that the PostgreSQL array literal quotes, so a value travels as itself.
// A vocabulary change breaks this test, not a restore.
func TestTheCarriedVocabulariesNeedNoQuotingInAnArrayLiteral(t *testing.T) {
	migrations, err := authschema.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, m := range migrations {
		all.WriteString(m.SQL)
		all.WriteString("\n")
	}
	pins := []struct{ name, pattern, sample string }{
		{"workload audiences", `^[a-z][a-z0-9._:-]{0,63}$`, "dev-health.api:v1"},
		{"workload allowed_actions", `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`, "metrics.read"},
		{"credential_token_name", `^[a-z][a-z0-9._:-]{0,63}$`, "query-api"},
		{"credential_scope", `^[a-z][a-z0-9_]*(:[a-z][a-z0-9_]*)+$`, "api:access"},
	}
	for _, p := range pins {
		if !strings.Contains(all.String(), "'"+p.pattern+"'") {
			t.Errorf("%s: the DDL no longer holds the pinned pattern %s", p.name, p.pattern)
			continue
		}
		re := regexp.MustCompile(p.pattern)
		if !re.MatchString(p.sample) {
			t.Errorf("%s: the sample %q does not match its own pattern", p.name, p.sample)
		}
		for _, bad := range []string{",", "{", "}", `"`, `\`, " ", "\t", "\n"} {
			for i := 0; i <= len(p.sample); i++ {
				if v := p.sample[:i] + bad + p.sample[i:]; re.MatchString(v) {
					t.Errorf("%s: the vocabulary admits %q, which an array literal quotes", p.name, v)
				}
			}
		}
	}
}

// G5: a role lowered after T is lowered again (viewer < member < admin < owner); a role raised after T is not carried; an
// org's last owner is never demoted (named in review_membership_owner); an unknown value on either side is left.
func TestMembershipRolesOnlyGoDownAndKeepTheLastOwner(t *testing.T) {
	rank := func(e string) string {
		return "array_position(ARRAY['viewer', 'member', 'admin', 'owner']::text[], " + e + ")"
	}
	for _, c := range []struct{ name, orgCol, table string }{
		{"auth.memberships.role", "organization_id", `"auth".memberships`},
		{"public.memberships.role", "org_id", "public.memberships"},
	} {
		def := mustSection(t, c.name)
		if def.kind != kindRevoke || def.key.name != "id" || def.suffix != ".role" || def.manifestOnly || strings.Join(def.columnNames(), ",") != "id,role" {
			t.Errorf("%s: want a revoke section keyed by id carrying role; got kind %v key %s suffix %q cols %v", c.name, def.kind, def.key.name, def.suffix, def.columnNames())
		}
		sql := def.applySQL(testSchema(t))
		for _, want := range []string{
			"role = x.role",
			rank("x.role") + " < " + rank("t.role"),
			"t.role <> 'owner' OR EXISTS (SELECT 1 FROM " + c.table + " k JOIN m y ON y.id = k.id WHERE k." + c.orgCol + " = t." + c.orgCol + " AND k.id <> t.id",
			"k.role = 'owner' AND y.role = 'owner'",
		} {
			if !strings.Contains(sql, want) {
				t.Errorf("%s: apply SQL lacks %q: %s", c.name, want, sql)
			}
		}
		if def.action != ActionKind("review_membership_owner") {
			t.Errorf("%s: action %q, want review_membership_owner", c.name, def.action)
		}
		if ids := def.idsSQL(testSchema(t)); !strings.Contains(ids, "SELECT DISTINCT t."+c.orgCol+"::text") {
			t.Errorf("%s: the owner review names the org: %s", c.name, ids)
		}
	}
	if sectionIndex("auth.memberships") > sectionIndex("auth.memberships.role") || sectionIndex("public.memberships") > sectionIndex("public.memberships.role") {
		t.Error("each role section runs after the status / presence section of its table")
	}
}

// G6: a superuser flag the source removed after T stays removed; one granted after T is not carried.
func TestASuperuserTheSourceRemovedStaysRemoved(t *testing.T) {
	def := mustSection(t, "public.users.superuser")
	if def.kind != kindRevoke || !def.legacy || def.key.name != "id" || def.suffix != ".superuser" || def.manifestOnly {
		t.Errorf("want a legacy revoke section keyed by id; got kind %v legacy %v key %s suffix %q", def.kind, def.legacy, def.key.name, def.suffix)
	}
	if def.revoked != "NOT s.is_superuser" || def.active != "t.is_superuser" || strings.Join(def.columnNames(), ",") != "id,is_superuser" {
		t.Errorf("revoked %q active %q cols %v", def.revoked, def.active, def.columnNames())
	}
	if sql := def.applySQL(testSchema(t)); strings.Contains(sql, "is_superuser = true") {
		t.Errorf("the section never grants superuser: %s", sql)
	}
}

// G3: an allowlist entry is held only when the source holds it active with the SAME ip_range (the column is text in the
// DDL, so the comparison is text); a shorter source expiry is carried, a longer one is not.
func TestAnAllowlistEntryWithAnEditedRangeIsNotHeldAndItsExpiryOnlyShortens(t *testing.T) {
	def := mustSection(t, "public.org_ip_allowlist.ids")
	if strings.Join(def.columnNames(), ",") != "id,ip_range" {
		t.Errorf("the allowlist section carries %v, want id and ip_range", def.columnNames())
	}
	sql := def.applySQL(testSchema(t))
	for _, want := range []string{
		"NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id AND x.ip_range = t.ip_range)",
		"EXISTS (SELECT 1 FROM public.org_ip_allowlist k JOIN m y ON y.id = k.id AND y.ip_range = k.ip_range WHERE k.org_id = t.org_id AND k.is_active)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("allowlist retire SQL lacks %q: %s", want, sql)
		}
	}
	if ids := def.idsSQL(testSchema(t)); !strings.Contains(ids, "NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id AND x.ip_range = t.ip_range)") {
		t.Errorf("the review must use the same match: %s", ids)
	}
	exp := mustSection(t, "public.org_ip_allowlist.expiry")
	if exp.kind != kindRevoke || !exp.legacy || exp.key.name != "id" || exp.manifestOnly || strings.Join(exp.columnNames(), ",") != "id,expires_at" {
		t.Errorf("want a legacy revoke section carrying expires_at; got kind %v cols %v manifestOnly %v", exp.kind, exp.columnNames(), exp.manifestOnly)
	}
	if exp.revoked != "s.is_active AND s.expires_at IS NOT NULL" || exp.active != "(t.expires_at IS NULL OR t.expires_at > x.expires_at)" {
		t.Errorf("revoked %q active %q: the guard must hold LEAST(restored, source)", exp.revoked, exp.active)
	}
}

// G4 (and the provider credential section after it): the manifest format is version 3. A manifest of an earlier version
// (written by an earlier binary, without the newer sections) is refused with a clear message at ParseManifest and at
// Apply, before any database work.
func TestAVersionOneManifestIsRefusedWithAClearMessage(t *testing.T) {
	if ManifestVersion != 3 {
		t.Fatalf("ManifestVersion = %d, want 3", ManifestVersion)
	}
	for _, v := range []int{1, 2} {
		m := sampleManifest(t)
		m.Version = v
		if err := m.Seal(); err != nil {
			t.Fatal(err)
		}
		raw, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseManifest(raw); !errors.Is(err, ErrManifest) || !strings.Contains(err.Error(), "export it again with this auth-migrate binary") {
			t.Errorf("a version %d manifest = %v, want ErrManifest naming the re-export", v, err)
		}
		if _, err := Apply(context.Background(), nil, testSchema(t), &m, Options{}); !errors.Is(err, ErrManifest) || !strings.Contains(err.Error(), fmt.Sprintf("version %d", v)) {
			t.Errorf("Apply of a version %d manifest = %v, want ErrManifest", v, err)
		}
	}
}

// pinnedSectionSet is the digest of what a version 2 manifest holds: every section's name, columns and export query.
// A change to any of them without a ManifestVersion bump fails here.
var pinnedSectionSet = map[int]string{
	2: "1ce899a3a8cfc45c8579eb54ee13d46bc5421c765c9f46d33f5809b0c4344b9d",
	3: "PIN",
}

func sectionSetDigest(t *testing.T, skip ...string) string {
	t.Helper()
	var lines []string
	for _, s := range sections {
		if slices.Contains(skip, s.name) {
			continue
		}
		lines = append(lines, s.name+"|"+s.recordset()+"|"+s.exportSQL(testSchema(t)))
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])
}

func TestTheSectionSetIsPinnedToTheManifestVersion(t *testing.T) {
	want, ok := pinnedSectionSet[ManifestVersion]
	if !ok {
		t.Fatalf("no pinned section set for manifest version %d", ManifestVersion)
	}
	if got := sectionSetDigest(t); got != want {
		t.Errorf("the manifest sections changed (digest %s, pinned %s for version %d): bump ManifestVersion and pin the new set", got, want, ManifestVersion)
	}
}
