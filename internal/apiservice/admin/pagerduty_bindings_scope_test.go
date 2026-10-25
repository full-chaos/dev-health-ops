package admin

import (
	"strings"
	"testing"
)

func TestPagerDutyBindingLookupsFilterByOrganization(t *testing.T) {
	cases := map[string]string{
		"graph":         bindingGraphSQL,
		"rotate active": bindingActiveByIDSQL,
		"rotate rows":   bindingRotateRowsSQL,
		"revoke lock":   bindingRevokeLockSQL,
	}
	for name, query := range cases {
		where := query[strings.LastIndex(query, "WHERE"):]
		if !strings.Contains(where, "org_id = $") {
			t.Errorf("%s: the WHERE clause does not filter by organisation:\n%s", name, query)
		}
	}
	if !strings.Contains(bindingGraphSQL, "i.org_id = $3") || !strings.Contains(bindingGraphSQL, "c.org_id = $3") {
		t.Errorf("graph: integration and credential joins must filter by organisation:\n%s", bindingGraphSQL)
	}
}

func TestPagerDutyBindingGraphPredicateIsNeverNull(t *testing.T) {
	predicate := bindingGraphSQL[strings.Index(bindingGraphSQL, "SELECT"):strings.Index(bindingGraphSQL, "FROM")]
	if !strings.Contains(predicate, "COALESCE(") || !strings.Contains(predicate, ", false)") {
		t.Errorf("the graph predicate can be NULL (integrations.credential_id is nullable) and the bool scan then fails; wrap it in COALESCE(..., false):\n%s", bindingGraphSQL)
	}
}
