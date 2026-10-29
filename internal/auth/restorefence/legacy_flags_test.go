package restorefence

import (
	"context"
	"regexp"
	"strings"
	"testing"
)

func mustSection(t *testing.T, name string) section {
	t.Helper()
	def, ok := sectionByName(name)
	if !ok {
		t.Fatalf("%s is not a section: the restore brings back the access the source removed after T", name)
	}
	return def
}

func sectionIndex(name string) int {
	for i, s := range sections {
		if s.name == name {
			return i
		}
	}
	return -1
}

// A legacy access flag the source switched off after T is switched off again; a flag the source switched on after T
// stays off (the fence never re-activates anything). With no manifest the section has no rows and changes nothing.
func TestTheLegacyAccessFlagsAreCarriedAndOnlySwitchedOff(t *testing.T) {
	for _, c := range []struct{ name, revoked, active, set string }{
		{"public.organizations", "NOT s.is_active", "t.is_active", "is_active = x.is_active"},
		{"public.integrations", "NOT s.is_active", "t.is_active", "is_active = x.is_active"},
		{"public.integration_credentials", "NOT s.is_active", "t.is_active", "is_active = x.is_active"},
		{"public.github_app_installations", "s.suspended_at IS NOT NULL", "t.suspended_at IS NULL", "suspended_at = COALESCE(t.suspended_at, x.suspended_at)"},
	} {
		def := mustSection(t, c.name)
		if def.kind != kindRevoke || !def.legacy || def.key.name != "id" || def.manifestOnly || def.suffix != "" {
			t.Errorf("%s: want a legacy revoke section keyed by id that also runs without a manifest; got kind %v legacy %v key %s manifestOnly %v suffix %q",
				c.name, def.kind, def.legacy, def.key.name, def.manifestOnly, def.suffix)
		}
		if def.revoked != c.revoked || def.active != c.active {
			t.Errorf("%s: revoked %q active %q, want %q and %q", c.name, def.revoked, def.active, c.revoked, c.active)
		}
		sql := def.applySQL(testSchema(t))
		if !strings.Contains(sql, c.set) {
			t.Errorf("%s: apply SQL lacks %q: %s", c.name, c.set, sql)
		}
		for _, bad := range []string{"is_active = true", "suspended_at = NULL", "DELETE"} {
			if strings.Contains(sql, bad) {
				t.Errorf("%s: apply SQL re-activates or deletes (%q): %s", c.name, bad, sql)
			}
		}
		if _, excl := excluded[c.name]; excl {
			t.Errorf("%s is a section and must leave the exclusion list", c.name)
		}
	}
}

// A legacy row the source deleted after T is switched off, never deleted, and only with a manifest: with no manifest
// nothing is known about which rows the source kept, and switching every row off is an outage.
func TestALegacyRowTheSourceDeletedIsSwitchedOffOnlyWithAManifest(t *testing.T) {
	for name, set := range map[string]string{
		"public.organizations.ids":           "SET is_active = false, updated_at = clock_timestamp()",
		"public.integrations.ids":            "SET is_active = false, updated_at = clock_timestamp()",
		"public.integration_credentials.ids": "SET is_active = false, credentials_encrypted = NULL, updated_at = clock_timestamp()",
	} {
		def := mustSection(t, name)
		if def.kind != kindRetire || !def.legacy || def.key.name != "id" || !def.manifestOnly || def.revoked != "" {
			t.Errorf("%s: want a manifest-only legacy retire section over every source id; got kind %v legacy %v key %s manifestOnly %v revoked %q",
				name, def.kind, def.legacy, def.key.name, def.manifestOnly, def.revoked)
		}
		sql := def.applySQL(testSchema(t))
		if !strings.Contains(sql, set) || !strings.Contains(sql, "NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)") || strings.Contains(sql, "DELETE") {
			t.Errorf("%s: retire SQL must switch off (%q) only rows absent at the source and never delete: %s", name, set, sql)
		}
		base := strings.TrimSuffix(name, ".ids")
		if sectionIndex(base) < 0 || sectionIndex(base) > sectionIndex(name) {
			t.Errorf("%s must run after %s", name, base)
		}
	}
}

// 0 active allowlist entries means the org accepts every address (admin/ipallowlist.go checkIPAllowed). The fence
// switches an entry off only while its org keeps an active entry the source also holds active; otherwise it leaves the
// entry and names the org in the review_ip_allowlist action.
func TestAnIPAllowlistEntryIsSwitchedOffOnlyWhileItsOrgKeepsASharedActiveEntry(t *testing.T) {
	def := mustSection(t, "public.org_ip_allowlist.ids")
	if def.kind != kindRetire || !def.legacy || def.key.name != "id" || !def.manifestOnly || def.revoked != "s.is_active" {
		t.Errorf("want a manifest-only legacy retire section over the source's ACTIVE entry ids; got kind %v legacy %v key %s manifestOnly %v revoked %q",
			def.kind, def.legacy, def.key.name, def.manifestOnly, def.revoked)
	}
	sql := def.applySQL(testSchema(t))
	for _, want := range []string{
		"UPDATE public.org_ip_allowlist t SET is_active = false, updated_at = clock_timestamp()",
		"EXISTS (SELECT 1 FROM public.org_ip_allowlist k JOIN m y ON y.id = k.id AND y.ip_range = k.ip_range WHERE k.org_id = t.org_id AND k.is_active)",
		"NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id AND x.ip_range = t.ip_range)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("allowlist retire SQL lacks %q: %s", want, sql)
		}
	}
	if def.action != ActionKind("review_ip_allowlist") {
		t.Errorf("the allowlist section names the orgs it leaves in action %q, want review_ip_allowlist", def.action)
	}
	ids := def.idsSQL(testSchema(t))
	if !strings.Contains(ids, "t.org_id::text") || !strings.Contains(ids, "NOT EXISTS (SELECT 1 FROM public.org_ip_allowlist k JOIN m y ON y.id = k.id AND y.ip_range = k.ip_range WHERE k.org_id = t.org_id AND k.is_active)") {
		t.Errorf("the review list must name the orgs whose entries the guard keeps: %s", ids)
	}
	if _, excl := excluded["public.org_ip_allowlist"]; excl {
		t.Error("public.org_ip_allowlist is a section and must leave the exclusion list")
	}
}

// The stored provider secret is cleared where the source cleared it (disconnect) or deleted the row. Only ids travel:
// no section ever exports credentials_encrypted.
func TestTheStoredProviderSecretIsClearedAndNeverCarried(t *testing.T) {
	def := mustSection(t, "public.integration_credentials.secret")
	if def.kind != kindRevoke || !def.legacy || def.key.name != "id" || len(def.cols) != 0 || def.manifestOnly {
		t.Errorf("want a legacy revoke section that carries ids only; got kind %v legacy %v key %s cols %v manifestOnly %v", def.kind, def.legacy, def.key.name, def.cols, def.manifestOnly)
	}
	if def.revoked != "s.credentials_encrypted IS NULL" || def.active != "t.credentials_encrypted IS NOT NULL" {
		t.Errorf("revoked %q active %q", def.revoked, def.active)
	}
	if sql := def.applySQL(testSchema(t)); !strings.Contains(sql, "SET credentials_encrypted = NULL, updated_at = clock_timestamp()") {
		t.Errorf("the secret section must clear the stored secret: %s", sql)
	}
	for _, s := range sections {
		if strings.Contains(s.exportSQL(testSchema(t)), "'credentials_encrypted'") {
			t.Errorf("section %s exports the stored provider secret", s.name)
		}
	}
}

// RotateBootstrap shortens the parent credential's expires_at; the restore brings the long expiry back. The fence keeps
// the earlier of the restored and the source expiry and never extends one.
func TestACredentialExpiryIsOnlyEverShortened(t *testing.T) {
	def := mustSection(t, "auth.credential_registry.expiry")
	if def.kind != kindRevoke || def.legacy || def.key.name != "id" || def.manifestOnly || def.suffix != ".expiry" {
		t.Errorf("want an auth revoke section keyed by id with the .expiry suffix; got kind %v legacy %v key %s manifestOnly %v suffix %q", def.kind, def.legacy, def.key.name, def.manifestOnly, def.suffix)
	}
	if len(def.cols) != 1 || def.cols[0] != colTS("expires_at") {
		t.Errorf("the expiry section carries expires_at only (timestamptz, overwritten under the guard); got %v", def.cols)
	}
	if def.revoked != "s.expires_at IS NOT NULL" || def.active != "(t.expires_at IS NULL OR t.expires_at > x.expires_at)" {
		t.Errorf("revoked %q active %q: the guard must hold LEAST(restored, source)", def.revoked, def.active)
	}
	sql := def.applySQL(testSchema(t))
	if !strings.Contains(sql, "expires_at = x.expires_at") || strings.Contains(sql, "GREATEST") {
		t.Errorf("expiry SQL: %s", sql)
	}
	orig := mustSection(t, "auth.credential_registry")
	if strings.Join(orig.columnNames(), ",") != "id,revoked_at,revoked_reason" {
		t.Errorf("the credential_registry revoke section must stay as it is; got %v", orig.columnNames())
	}
	if sectionIndex("auth.credential_registry") > sectionIndex("auth.credential_registry.expiry") {
		t.Error("the expiry section runs after the revoke section of the same table")
	}
}

// Q5: the "inputs" digest was a constant: a retry with other consumer inputs was a no-op. It names the floors now.
func TestTheInputsDigestNamesTheFloors(t *testing.T) {
	in := func(rev int64) []FloorInput {
		return []FloorInput{fakeInput{name: "consumer_a", floors: map[string]int64{"organization:org_a": rev}}}
	}
	a, err := resolveInputs(context.Background(), nil, Options{}, in(5))
	if err != nil {
		t.Fatal(err)
	}
	b, err := resolveInputs(context.Background(), nil, Options{}, in(9))
	if err != nil {
		t.Fatal(err)
	}
	again, _ := resolveInputs(context.Background(), nil, Options{}, in(5))
	if a.source != "inputs" || a.digest == b.digest || a.digest != again.digest {
		t.Fatalf("inputs digests %q (5), %q (9), %q (5 again): other floors must give another digest, the same floors the same", a.digest, b.digest, again.digest)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(a.digest) {
		t.Errorf("the digest %q must satisfy the restore_fence_state CHECK (64 hex)", a.digest)
	}
	blind, _ := resolveInputs(context.Background(), nil, Options{BlindMargin: 5}, nil)
	if blind.digest == a.digest {
		t.Error("a blind margin and an inputs apply share a digest")
	}
}
