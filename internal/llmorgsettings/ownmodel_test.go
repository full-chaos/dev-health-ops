package llmorgsettings

import (
	"net/http"
	"testing"
)

// A saved config an org admin turned off is no usable own model: the platform
// model only with platform_model_fallback, else own_model_turned_off. The flag
// and a closed gate are decided first.
func TestDecideTreatsATurnedOffOwnModelAsNoOwnModel(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   Inputs
		want Decision
	}{
		{name: "turned off, no grant", in: Inputs{Saved: true, TurnedOff: true, ServiceModelOn: true}, want: Decision{Kind: Refuse, Code: CodeOwnModelTurnedOff, Status: http.StatusForbidden}},
		{name: "turned off with the grant", in: Inputs{Saved: true, TurnedOff: true, ServiceModelOn: true, Fallback: true}, want: Decision{Kind: Platform}},
		{name: "turned off with acr_service_model off", in: Inputs{Saved: true, TurnedOff: true, Fallback: true}, want: Decision{Kind: Refuse, Code: CodeAIFeaturesTurnedOff, Status: http.StatusForbidden}},
		{name: "turned off behind a closed gate", in: Inputs{Saved: true, TurnedOff: true, GateClosed: true, ServiceModelOn: true, Fallback: true}, want: Decision{Kind: Refuse, Code: CodeOwnModelGateClosed, Status: http.StatusForbidden}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Decide(tc.in); got != tc.want {
				t.Fatalf("Decide = %+v, want %+v", got, tc.want)
			}
		})
	}
}
