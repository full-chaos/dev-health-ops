package restorefence

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const identitySection = "auth.external_identities"

// D-1 Q1/Q4: an unlinked identity is matched by id only; only the id and the principal travel.
func TestAnExternalIdentityIsAManifestOnlyPresenceSectionMatchedByID(t *testing.T) {
	def, ok := sectionByName(identitySection)
	if !ok {
		t.Fatalf("no %s section", identitySection)
	}
	if def.kind != kindPresence || !def.manifestOnly || def.legacy || def.table != "external_identities" {
		t.Fatalf("section = %+v, want a manifest-only presence section of the auth table", def)
	}
	if def.key.name != "id" || len(def.cols) != 1 || def.cols[0].name != "principal_id" {
		t.Errorf("the section carries %s + %v, want id + principal_id only", def.key.name, def.cols)
	}
	if def.match != "" || def.matchSQL() != "x.id = t.id" {
		t.Errorf("match = %q (%q), want the id only", def.match, def.matchSQL())
	}
	if def.deleteGuard != "" {
		t.Errorf("deleteGuard = %q: every restored link the source lacks is deleted", def.deleteGuard)
	}
	export := def.exportSQL(testSchema(t))
	for _, banned := range []string{"subject", "issuer", "provider", "sha256", "digest"} {
		if strings.Contains(export, banned) {
			t.Errorf("the export carries %q: %s", banned, export)
		}
	}
	apply := def.applySQL(testSchema(t))
	if !strings.Contains(apply, `DELETE FROM "auth".external_identities t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)`) || strings.Contains(apply, "INSERT") {
		t.Errorf("the apply must delete the restored links whose id the source lacks and insert nothing:\n%s", apply)
	}
	if def.action != ActionIdentityUnlinked || followUp[ActionIdentityUnlinked] != "none" {
		t.Errorf("action %q follow-up %q, want identity_unlinked / none", def.action, followUp[ActionIdentityUnlinked])
	}
	ids := def.idsSQL(testSchema(t))
	if !strings.Contains(ids, "SELECT t.id::text FROM \"auth\".external_identities t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)") {
		t.Errorf("identity_unlinked must name exactly the deleted ids, ids only:\n%s", ids)
	}
}

// D-1 Q2: a blind margin deletes nothing and lists every link as id/principal/provider, never the subject.
func TestABlindMarginListsEveryExternalIdentityWithoutItsSubject(t *testing.T) {
	def, _ := sectionByName(identitySection)
	q := blindReviewSQL(def, testSchema(t))
	if !strings.Contains(q, "t.id::text || '/' || t.principal_id::text || '/' || ") || !strings.Contains(q, "t.provider") || !strings.HasSuffix(q, "WHERE true ORDER BY 1") {
		t.Errorf("the blind list must name id/principal/provider of every link:\n%s", q)
	}
	if strings.Contains(q, "subject") || strings.Contains(q, "issuer") {
		t.Errorf("the blind list names the subject or the issuer:\n%s", q)
	}
	if def.runsWithoutManifest() {
		t.Error("a blind margin would run the delete")
	}
}

// D-1 Q3: only live source locks are read, from both tables, and the apply only raises.
func TestLockoutsAreReadLiveFromTheSourceAndOnlyRaised(t *testing.T) {
	for name, q := range map[string]string{"auth": authLocksSourceSQL(testSchema(t)), "legacy": legacyLocksSourceSQL} {
		if !strings.HasSuffix(q, "WHERE locked_until > clock_timestamp()") {
			t.Errorf("%s source read is not limited to live locks:\n%s", name, q)
		}
	}
	if !strings.Contains(authLocksSourceSQL(testSchema(t)), `FROM "auth".login_attempts`) || !strings.Contains(legacyLocksSourceSQL, "FROM public.login_attempts") {
		t.Error("the source reads must cover the auth and the legacy table")
	}
	for name, q := range map[string]string{"auth": authLocksRaiseSQL(testSchema(t)), "legacy": legacyLocksRaiseSQL} {
		for _, want := range []string{
			"attempt_count = GREATEST(t.attempt_count, EXCLUDED.attempt_count)",
			"locked_until = GREATEST(t.locked_until, EXCLUDED.locked_until)",
			"first_attempt_at = LEAST(t.first_attempt_at, EXCLUDED.first_attempt_at)",
			"WHERE t.locked_until IS NULL OR t.locked_until < EXCLUDED.locked_until OR t.attempt_count < EXCLUDED.attempt_count",
		} {
			if !strings.Contains(q, want) {
				t.Errorf("%s raise lacks %q:\n%s", name, want, q)
			}
		}
		for _, banned := range []string{"attempt_count = EXCLUDED.attempt_count", "locked_until = EXCLUDED.locked_until", "DELETE"} {
			if strings.Contains(q, banned) {
				t.Errorf("%s raise can lower a lock (%q):\n%s", name, banned, q)
			}
		}
	}
	if !strings.Contains(authLocksRaiseSQL(testSchema(t)), "ON CONFLICT (identifier_hash)") || !strings.Contains(legacyLocksRaiseSQL, "ON CONFLICT (email)") {
		t.Error("each raise must upsert by its table's unique key")
	}
}

// The lockouts ride the same source read as the revocations: a source error or another timeline refuses before any
// write, and the error never carries a key.
func TestALockoutSourceErrorCarriesNoKey(t *testing.T) {
	m := manifestWithRevocations(t)
	src := &fakeRevocationSource{err: errors.New("lock row email=" + requeueSentinel)}
	_, err := Apply(context.Background(), nil, testSchema(t), &m, Options{Source: src})
	if !errors.Is(err, ErrApply) || strings.Contains(err.Error(), requeueSentinel) {
		t.Fatalf("a failed source read = %v, want ErrApply without the source text", err)
	}
}

// The new section is the only change of the section set: every other section exports what version 4 held.
func TestEveryOtherSectionExportsWhatVersionFourHeld(t *testing.T) {
	if got := sectionSetDigest(t, identitySection); got != pinnedSectionSet[4] {
		t.Errorf("a section other than auth.external_identities changed its export (digest %s, version 4 pinned %s)", got, pinnedSectionSet[4])
	}
}
