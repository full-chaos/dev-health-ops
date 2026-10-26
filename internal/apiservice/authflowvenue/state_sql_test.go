package authflowvenue_test

import (
	"strings"
	"testing"
)

var authStateTables = []string{"auth.users", "auth.principals", "auth.refresh_credentials"}

var publicStateTables = []string{"public.users", "public.refresh_tokens"}

func TestStateSQLNamesNoAuthTableOnLegGoAPI(t *testing.T) {
	sql, err := stateSQLForLeg("go-api")
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range publicStateTables {
		if !strings.Contains(sql, table) {
			t.Errorf("leg go-api state SQL does not name %s", table)
		}
	}
	if strings.Contains(sql, "auth.") {
		t.Errorf("leg go-api runs without auth-service, so its state SQL must name no auth table:\n%s", sql)
	}
}

func TestStateSQLNamesEveryTableOnLegAcp(t *testing.T) {
	sql, err := stateSQLForLeg("acp")
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range append(append([]string{}, publicStateTables...), authStateTables...) {
		if !strings.Contains(sql, table) {
			t.Errorf("leg acp state SQL does not name %s", table)
		}
	}
}

func TestStateSQLRefusesAnUnknownLeg(t *testing.T) {
	for _, leg := range []string{"", "other"} {
		if _, err := stateSQLForLeg(leg); err == nil {
			t.Errorf("leg %q: want an error", leg)
		}
	}
}
