package authorization

import (
	"slices"
	"testing"
)

func evaluatorWithRedaction(rules map[redactionKey][]string) *Evaluator {
	e := NewEvaluator()
	e.redaction = rules
	return e
}

func TestRedactionRuleOfTheMembershipRoleReachesTheAllow(t *testing.T) {
	e := evaluatorWithRedaction(map[redactionKey][]string{{"viewer", "analytics.read"}: {"contact_details", "free_text"}})
	d := e.Evaluate(orgRequest("analytics.read"), repoSnapshot("viewer"))
	if !d.Allowed || d.Obligations == nil || !slices.Equal(d.Obligations.Redaction, []string{"contact_details", "free_text"}) {
		t.Fatalf("viewer: %+v %+v, want the planted classes", d, d.Obligations)
	}
	d = e.Evaluate(orgRequest("analytics.read"), repoSnapshot("member"))
	if !d.Allowed || d.Obligations == nil || len(d.Obligations.Redaction) != 0 {
		t.Fatalf("member has no rule: %+v %+v", d, d.Obligations)
	}
	d = e.Evaluate(orgRequest("metrics.read"), repoSnapshot("viewer"))
	if !d.Allowed || d.Obligations == nil || len(d.Obligations.Redaction) != 0 {
		t.Fatalf("another action has no rule: %+v %+v", d, d.Obligations)
	}
}

func TestRedactionRuleReachesARepositoryAllowAndNeverADeny(t *testing.T) {
	e := evaluatorWithRedaction(map[redactionKey][]string{{"viewer", "acr.context.read"}: {"personal_identifiers"}})
	d := e.Evaluate(repoRequest("acme/api"), repoSnapshot("viewer"))
	if !d.Allowed || d.Obligations == nil || !slices.Equal(d.Obligations.Redaction, []string{"personal_identifiers"}) {
		t.Fatalf("repository allow: %+v %+v", d, d.Obligations)
	}
	d = e.Evaluate(repoRequest("acme/api"), repoSnapshot("viewer", revokedGrant("acme/api")))
	if d.Allowed || d.Obligations != nil {
		t.Fatalf("a deny carries no redaction: %+v %+v", d, d.Obligations)
	}
}

func TestTheShippedCatalogSetsNoRedactionYet(t *testing.T) {
	e := NewEvaluator()
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		d := e.Evaluate(orgRequest("analytics.read"), repoSnapshot(role))
		if d.Obligations == nil || len(d.Obligations.Redaction) != 0 {
			t.Errorf("%s: %+v", role, d.Obligations)
		}
	}
}
