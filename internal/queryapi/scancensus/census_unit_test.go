package scancensus

import (
	"strings"
	"testing"
)

func TestNeutralizeKeepsSelectListTypes(t *testing.T) {
	got := Neutralize("SELECT sum(x) AS n FROM t WHERE org_id = {org_id:String} AND d >= {d:Date} %s\nLIMIT %d")[0]
	for _, want := range []string{"defaultValueOfTypeName('String')", "defaultValueOfTypeName('Date')", "LIMIT 1"} {
		if !strings.Contains(got, want) {
			t.Errorf("neutralized statement lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "%s") || strings.Contains(got, "{") {
		t.Errorf("neutralized statement keeps a placeholder:\n%s", got)
	}
}

func TestNeutralizeKeepsALiteralPercent(t *testing.T) {
	if got := Neutralize("SELECT 1 FROM t WHERE p LIKE '100%%'")[0]; !strings.Contains(got, "'100%'") {
		t.Errorf("a literal percent was lost: %s", got)
	}
}

func TestSelectListSplices(t *testing.T) {
	cases := []struct {
		statement string
		want      bool
	}{
		{"SELECT a, %s AS value FROM t", true},
		{"SELECT a, coalesce(%s, '') FROM t", true},
		{"SELECT a, sum(b) FROM t WHERE x = 1 %s", false},
		{"SELECT a, (SELECT count() FROM u WHERE k = 1 AND %s) AS n FROM t", false},
		{"SELECT a, (%s) AS n FROM t", true},
		{"SELECT a, 'FROM %s' FROM t", true},
	}
	for _, c := range cases {
		if got := SelectListSplices(c.statement); got != c.want {
			t.Errorf("SelectListSplices(%q) = %v, want %v", c.statement, got, c.want)
		}
	}
}
