package restorefence

import (
	"strings"
	"testing"
)

const (
	authPasswordSection   = "auth.users.password"
	legacyPasswordSection = "public.users.password"
	authPasswordGuard     = "t.password_hash IS NOT NULL AND ((x.password_updated_at IS NOT NULL AND (t.password_updated_at IS NULL OR t.password_updated_at < x.password_updated_at)) OR t.legacy_token_version < x.legacy_token_version)"
	legacyPasswordGuard   = "t.password_hash IS NOT NULL AND t.token_version < x.token_version"
)

// A password changed after T comes back with the restore: the old (maybe compromised) hash works again. The fence clears
// the restored hash of every user whose password the source changed after T (fail closed: the user resets by the
// forgot-password mail, and both logins refuse a NULL hash) and names the user in force_password_reset. Only with a
// manifest; a user without a password is never touched.
func TestAPasswordChangedAfterTIsClearedAndNamed(t *testing.T) {
	for _, c := range []struct {
		name, key, revoked, guard, set string
		legacy                         bool
		cols                           string
	}{
		{authPasswordSection, "principal_id", "s.password_updated_at IS NOT NULL OR s.legacy_token_version > 0", authPasswordGuard,
			"SET password_hash = NULL, password_algorithm = NULL, updated_at = clock_timestamp()", false, "principal_id,password_updated_at,legacy_token_version"},
		{legacyPasswordSection, "id", "s.token_version > 0", legacyPasswordGuard,
			"SET password_hash = NULL, updated_at = clock_timestamp()", true, "id,token_version"},
	} {
		def := mustSection(t, c.name)
		if def.kind != kindRevoke || def.legacy != c.legacy || def.key.name != c.key || !def.manifestOnly || def.suffix != ".password" {
			t.Errorf("%s: want a manifest-only revoke section keyed by %s; got kind %v legacy %v key %s manifestOnly %v suffix %q",
				c.name, c.key, def.kind, def.legacy, def.key.name, def.manifestOnly, def.suffix)
		}
		// P3: exactly a timestamp and counters travel; never the hash or the algorithm.
		if got := strings.Join(def.columnNames(), ","); got != c.cols {
			t.Errorf("%s exports %s, want exactly %s", c.name, got, c.cols)
		}
		exp := def.exportSQL(testSchema(t))
		for _, bad := range []string{"password_hash", "password_algorithm"} {
			if strings.Contains(exp, bad) {
				t.Errorf("%s: the export names %s: %s", c.name, bad, exp)
			}
		}
		if def.revoked != c.revoked || def.active != c.guard {
			t.Errorf("%s: revoked %q active %q, want %q and %q", c.name, def.revoked, def.active, c.revoked, c.guard)
		}
		sql := def.applySQL(testSchema(t))
		if !strings.Contains(sql, c.set+" FROM m x") {
			t.Errorf("%s: apply SQL must clear the hash (and, for auth.users, the algorithm with it) and nothing else: %s", c.name, sql)
		}
		for _, carried := range def.columnNames()[1:] {
			if strings.Contains(sql, carried+" = x."+carried) {
				t.Errorf("%s: %s is a guard input, not a value to write: %s", c.name, carried, sql)
			}
		}
		if def.action != ActionForcePasswordReset {
			t.Errorf("%s: action %q, want force_password_reset", c.name, def.action)
		}
		if ids := def.idsSQL(testSchema(t)); !strings.Contains(ids, "WHERE ("+c.guard+") ORDER BY 1") {
			t.Errorf("%s: the action must name exactly the users whose hash it clears: %s", c.name, ids)
		}
	}
	// P2: the legacy section reads token_version before the counter section raises it to the source's.
	if sectionIndex(legacyPasswordSection) < 0 || sectionIndex(legacyPasswordSection) > sectionIndex("public.users") {
		t.Error("public.users.password must run before the public.users counter section")
	}
	if followUp[ActionForcePasswordReset] != "none" {
		t.Errorf("force_password_reset follow-up = %q, want none (the fence now clears the password)", followUp[ActionForcePasswordReset])
	}
}

// P4: the password sections are the only change of the section set: every other section exports what version 3 held.
func TestEveryOtherSectionExportsWhatVersionThreeHeld(t *testing.T) {
	if got := sectionSetDigest(t, authPasswordSection, legacyPasswordSection); got != pinnedSectionSet[3] {
		t.Errorf("a section other than the password sections changed its export (digest %s, version 3 pinned %s)", got, pinnedSectionSet[3])
	}
}
