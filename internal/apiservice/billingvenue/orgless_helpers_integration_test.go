//go:build integration

package billingvenue

import (
	"strings"
	"testing"
)

func TestDropEmptyOrgCheckoutsRemovesExactlyTheRefusedOnes(t *testing.T) {
	empty := "POST /v1/checkout/sessions | form=cancel_url=/no&client_reference_id=&line_items[0][price]=price_team_cfg"
	org := "POST /v1/checkout/sessions | form=cancel_url=/no&client_reference_id=abc&line_items[0][price]=price_team_cfg"
	portal := "POST /v1/billing_portal/sessions | form=customer=cus_A"
	calls := []string{org, empty, portal, org}
	if got := dropEmptyOrgCheckouts(t, calls, 0); strings.Join(got, "\n") != strings.Join(calls, "\n") {
		t.Fatalf("with no refusal the calls change: %q", got)
	}
	if got := dropEmptyOrgCheckouts(t, calls, 1); strings.Join(got, "\n") != strings.Join([]string{org, portal, org}, "\n") {
		t.Fatalf("one refusal drops %q", got)
	}
}

func TestShiftSessionNumbersMovesOnlyNumbersPastTheRefusedOnes(t *testing.T) {
	body := `{"session_id":"cs_venue_6","url":"https://checkout.venue.test/cs_venue_6"}`
	if got := shiftSessionNumbers(body, 0); got != body {
		t.Fatalf("no refusal changes the body: %s", got)
	}
	want := `{"session_id":"cs_venue_5","url":"https://checkout.venue.test/cs_venue_5"}`
	if got := shiftSessionNumbers(body, 1); got != want {
		t.Fatalf("shifted body %s, want %s", got, want)
	}
}
