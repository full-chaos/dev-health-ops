package goapiproof

import (
	"reflect"
	"testing"
)

// CHAOS-9802: the MCP class proof ceremony is retired (D6024). Enabling a class root takes no receipt, a receipt
// carries no class provenance, and no run has a doc-route reference mode. These pins keep the surface from coming back.
func TestTheRetiredClassProofSurfaceStaysRetired(t *testing.T) {
	for _, field := range []string{"AllowExcluded", "Ledger"} {
		if _, has := reflect.TypeOf(EnableRequest{}).FieldByName(field); has {
			t.Errorf("EnableRequest has %s again: enable takes no proof and no ledger limit", field)
		}
	}
	for _, field := range []string{"Proven", "NamedLimit"} {
		if _, has := reflect.TypeOf(EnableOutcome{}).FieldByName(field); has {
			t.Errorf("EnableOutcome has %s again", field)
		}
	}
	if _, has := reflect.TypeOf(ReceiptProvenance{}).FieldByName("MCPClass"); has {
		t.Error("ReceiptProvenance carries class provenance again")
	}
	if _, has := reflect.TypeOf(MCPClassRootStatus{}).FieldByName("Proven"); has {
		t.Error("MCPClassRootStatus reports a proven state again")
	}
	for _, field := range []string{"DocRouteReference", "DocRouteURL"} {
		if _, has := reflect.TypeOf(Config{}).FieldByName(field); has {
			t.Errorf("Config has %s again", field)
		}
	}
}
