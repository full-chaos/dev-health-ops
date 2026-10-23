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
