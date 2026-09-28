package routing

// CHAOS-7096: proof-org's flag-level refusals, unit-tested the same way
// disable's own empty-guard cases are -- no Postgres dial needed, since
// every case here is refused BEFORE runProofOrgAdd/Remove/List ever calls
// connectPostgres. Postgres-backed behavior (the actual add/remove/audit
// writes) is goapiproof.AddProofOrg/RemoveProofOrg's own package, unit-
// tested there against a real pool by that package's existing convention.

import (
	"strings"
	"testing"
)

// clearPostgresEnv keeps these tests independent of whatever POSTGRES_URI
// happens to be set in the ambient environment -- a real value there would
// let requirePostgres() pass and the test would then hang trying to dial.
func clearPostgresEnv(t *testing.T) {
	t.Helper()
	t.Setenv(postgresURIEnvVar, "")
}

func TestProofOrgAddRefusesWithNoOrgFlag(t *testing.T) {
	clearPostgresEnv(t)
	err := runProofOrgAdd([]string{"-recorded-by", "chris", "-review-evidence", "testing", "-postgres-uri", "postgres://x/y"})
	if err == nil || !strings.Contains(err.Error(), "-org is required") {
		t.Fatalf("got %v, want a refusal naming -org", err)
	}
}

func TestProofOrgAddRefusesWithNoProvenance(t *testing.T) {
	clearPostgresEnv(t)
	err := runProofOrgAdd([]string{"-org", "org-1", "-postgres-uri", "postgres://x/y"})
	if err == nil || !strings.Contains(err.Error(), "-recorded-by") {
		t.Fatalf("got %v, want a refusal naming -recorded-by/-review-evidence", err)
	}
}

func TestProofOrgAddRefusesWithNoPostgresURI(t *testing.T) {
	clearPostgresEnv(t)
	err := runProofOrgAdd([]string{"-org", "org-1", "-recorded-by", "chris", "-review-evidence", "testing"})
	if err == nil || !strings.Contains(err.Error(), "-postgres-uri") {
		t.Fatalf("got %v, want a refusal naming -postgres-uri", err)
	}
}

func TestProofOrgRemoveRefusesWithNoOrgFlag(t *testing.T) {
	clearPostgresEnv(t)
	err := runProofOrgRemove([]string{"-recorded-by", "chris", "-review-evidence", "testing", "-postgres-uri", "postgres://x/y"})
	if err == nil || !strings.Contains(err.Error(), "-org is required") {
		t.Fatalf("got %v, want a refusal naming -org", err)
	}
}

func TestProofOrgRemoveRefusesWithNoProvenance(t *testing.T) {
	clearPostgresEnv(t)
	err := runProofOrgRemove([]string{"-org", "org-1", "-postgres-uri", "postgres://x/y"})
	if err == nil || !strings.Contains(err.Error(), "-recorded-by") {
		t.Fatalf("got %v, want a refusal naming -recorded-by/-review-evidence", err)
	}
}

func TestProofOrgListRefusesWithNoPostgresURI(t *testing.T) {
	clearPostgresEnv(t)
	err := runProofOrgList(nil)
	if err == nil || !strings.Contains(err.Error(), "-postgres-uri") {
		t.Fatalf("got %v, want a refusal naming -postgres-uri", err)
	}
}

// The dispatcher itself: `proof-org` routes to these three sub-verbs and
// refuses an unknown one, the same shape splitVerb/run already give every
// top-level verb.
func TestRunProofOrgRoutesToItsSubVerbsAndRefusesAnUnknownOne(t *testing.T) {
	clearPostgresEnv(t)
	if err := runProofOrg([]string{"bogus"}); err == nil || !strings.Contains(err.Error(), `unknown proof-org sub-verb "bogus"`) {
		t.Fatalf("got %v, want a refusal naming the unknown sub-verb", err)
	}
	// "add" with no -org routes through to the real runProofOrgAdd refusal --
	// proves the dispatch actually reaches the sub-verb, not a stub.
	if err := runProofOrg([]string{"add", "-recorded-by", "chris", "-review-evidence", "testing", "-postgres-uri", "postgres://x/y"}); err == nil || !strings.Contains(err.Error(), "-org is required") {
		t.Fatalf("got %v, want add's own -org refusal", err)
	}
}
