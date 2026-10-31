package restorefence

import (
	"strings"
	"testing"
)

const queuedFromCredential = ActionKind("provider_revocation_queued_from_credential")

// A bound PagerDuty credential the fence deletes is queued for the revocation worker in the delete statement itself:
// its old token (sealed as stored, key version oauth-v1) goes to provider_oauth_revocations, attempts 0, due at once.
// No row of the same key at the source = disconnect; a row under another binding = replacement.
func TestADeletedBoundPagerDutyCredentialQueuesItsOldTokenInTheDeleteStatement(t *testing.T) {
	sql := mustSection(t, oauthSection).applySQL(testSchema(t))
	for _, want := range []string{
		"DELETE FROM public.provider_oauth_credentials t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE " + oauthHeldWant + ") RETURNING t.org_id, t.provider, t.credential_name, t.binding_id, t.token_encrypted",
		"INSERT INTO public.provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, last_error, created_at, updated_at)",
		"d.token_encrypted, 'oauth-v1', 'pending', 0, NULL, clock_timestamp(), clock_timestamp()",
		"CASE WHEN EXISTS (SELECT 1 FROM m y WHERE y.org_id = d.org_id AND y.provider = d.provider AND y.credential_name = d.credential_name) THEN 'replacement' ELSE 'disconnect' END",
		"d.provider = 'pagerduty' AND d.binding_id IS NOT NULL",
		"y.binding_id IS NOT NULL AND y.binding_id <> d.binding_id",
		"\n  FROM d WHERE (d.provider = 'pagerduty'",
		"SELECT (SELECT count(*) FROM d), (SELECT count(*) FROM q)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("the credential apply lacks %q:\n%s", want, sql)
		}
	}
}

// The queued credentials are named by their own action (follow-up none); the manual revoke names only the rest.
func TestQueuedCredentialsAreNamedApartFromTheManualRevoke(t *testing.T) {
	if got, ok := followUp[queuedFromCredential]; !ok || got != "none" {
		t.Errorf("follow-up of %s = %q (known %v), want none", queuedFromCredential, got, ok)
	}
	ids := mustSection(t, oauthSection).idsSQL(testSchema(t))
	if !strings.Contains(ids, "AND NOT (t.provider = 'pagerduty' AND t.binding_id IS NOT NULL") {
		t.Errorf("the manual revoke must leave out the queued credentials: %s", ids)
	}
}

// The preflight asks INSERT on the revocation table whenever the credential section can delete, with or without a
// revocation source: the queue write is part of the delete statement.
func TestTheCredentialSectionNeedsInsertOnTheRevocationTable(t *testing.T) {
	for _, requeue := range []bool{false, true} {
		got := mustSection(t, oauthSection).privileges(testSchema(t), requeue)
		want := []tablePrivilege{{oauthSection, "public.provider_oauth_credentials", "DELETE"}, {"public.provider_oauth_revocations", "public.provider_oauth_revocations", "INSERT"}}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("requeue %v: privileges %v, want %v", requeue, got, want)
		}
	}
	for _, s := range sections {
		if s.queues != (s.name == oauthSection) {
			t.Errorf("section %s queues = %v", s.name, s.queues)
		}
	}
}
