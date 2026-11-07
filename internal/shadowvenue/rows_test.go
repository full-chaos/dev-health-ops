package shadowvenue

import (
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

const testRowsHeader = "row\tmethod\troute\torg\tdestructive\text\tprincipal\texpect\tpath\tbody\n"

func mustRows(t *testing.T, raw string) []Row {
	t.Helper()
	rows, err := ParseRows([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// The embedded table names every go-api shadow row of the embedded contract exactly once, and its controls are
// shadow rows too: a row the contract adds, or one it drops, refuses the table.
func TestDefaultRowsEqualTheContractShadowRows(t *testing.T) {
	rows := mustRows(t, string(DefaultRows))
	if err := CheckRows(rows, authv1.GoAPIEndpointProfiles); err != nil {
		t.Fatal(err)
	}
	controls, plain := 0, 0
	for _, row := range rows {
		if row.Control() {
			controls++
		} else {
			plain++
		}
	}
	if controls != 2 || plain != 95 {
		t.Fatalf("controls=%d rows=%d, want 2 and 95", controls, plain)
	}
}

func TestCheckRowsRefusesAMissingAnExtraAndATwiceRow(t *testing.T) {
	all := mustRows(t, string(DefaultRows))
	var dropped []Row
	for _, row := range all {
		if row.ID != "58" {
			dropped = append(dropped, row)
		}
	}
	if err := CheckRows(dropped, authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "GET /api/v1/admin/teams") {
		t.Fatalf("a dropped row: %v", err)
	}
	extra := append([]Row{}, all...)
	extra = append(extra, Row{ID: "65", Method: "GET", Route: "/api/v1/admin/nothing", Org: "A", Ext: "-", Principal: "-", Expect: "match", Path: "/api/v1/admin/nothing", Body: "-"})
	if err := CheckRows(extra, authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "/api/v1/admin/nothing") {
		t.Fatalf("an extra row: %v", err)
	}
	twice := append([]Row{}, all...)
	dup := all[len(all)-1]
	dup.ID = "65"
	twice = append(twice, dup)
	if err := CheckRows(twice, authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("a row twice: %v", err)
	}
	control := append([]Row{}, all...)
	control[0].Route = "/api/v1/admin/nothing"
	if err := CheckRows(control, authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "control") {
		t.Fatalf("a control that is no shadow row: %v", err)
	}
}

func TestParseRowsRefusesEachMalformedLine(t *testing.T) {
	good := "1\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n"
	if _, err := ParseRows([]byte(testRowsHeader + good)); err != nil {
		t.Fatalf("good line refused: %v", err)
	}
	cases := map[string]string{
		"no header":             good,
		"wrong header":          strings.Replace(testRowsHeader, "expect", "want", 1) + good,
		"nine columns":          testRowsHeader + "1\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\n",
		"bad id":                testRowsHeader + strings.Replace(good, "1\t", "x1\t", 1),
		"bad method":            testRowsHeader + strings.Replace(good, "GET", "FETCH", 1),
		"route not /api/v1":     testRowsHeader + strings.Replace(good, "\t/api/v1/admin/teams\tA", "\t/admin/teams\tA", 1),
		"bad destructive":       testRowsHeader + strings.Replace(good, "\tno\t", "\tmaybe\t", 1),
		"row expects mismatch":  testRowsHeader + strings.Replace(good, "match", "mismatch", 1),
		"row names a principal": testRowsHeader + strings.Replace(good, "\t-\tmatch", "\tmember-A\tmatch", 1),
		"control expects match": testRowsHeader + "C1\tGET\t/api/v1/admin/teams\tA\tno\t-\tmember-A\tmatch\t/api/v1/admin/teams\t-\n",
		"control no principal":  testRowsHeader + "C1\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmismatch\t/api/v1/admin/teams\t-\n",
		"control destructive":   testRowsHeader + "C1\tGET\t/api/v1/admin/teams\tA\tyes\t-\tmember-A\tmismatch\t/api/v1/admin/teams\t-\n",
		"id twice":              testRowsHeader + good + good,
		"no rows":               testRowsHeader,
		"path not absolute":     testRowsHeader + strings.Replace(good, "\t/api/v1/admin/teams\t-", "\tapi/v1/admin/teams\t-", 1),
		"empty org":             testRowsHeader + strings.Replace(good, "\tA\t", "\t\t", 1),
		"ext upper case":        testRowsHeader + strings.Replace(good, "\tno\t-\t", "\tno\tStripe\t", 1),
	}
	for name, raw := range cases {
		if _, err := ParseRows([]byte(raw)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
