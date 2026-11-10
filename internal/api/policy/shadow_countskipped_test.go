package policy

import (
	"context"
	"slices"
	"testing"
)

// CHAOS-13074: a service that has no row to observe (an unmapped GraphQL root, an MCP op with no user bearer, an op
// over the observe cap) counts one skipped result with its own reason from the closed list.
func TestShadowCountSkippedCountsOneSkippedWithAClosedReason(t *testing.T) {
	rec := recordShadow(t)
	pdp := newTestPDP(t, allowAll)
	sh, _ := newShadowFor(t, pdp.client(t), nil)
	for _, reason := range []string{ShadowReasonNoUserBearer, ShadowReasonCap, ShadowReasonUnmapped, ShadowReasonOpRefused, ShadowReasonHS256, "", "other", "CAP"} {
		sh.CountSkipped(context.Background(), reason)
	}
	want := []string{"skipped/no_user_bearer", "skipped/cap", "skipped/unmapped", "skipped/op_refused", "skipped/hs256", "skipped/none", "skipped/none", "skipped/none"}
	if got := rec.resultReasons(); !slices.Equal(got, want) {
		t.Fatalf("result/reason %v, want %v", got, want)
	}
	if got := ShadowReasons(); !slices.Equal(got, []string{"none", "hs256", "envelope", "no_user_bearer", "cap", "unmapped", "op_refused", "never_decided", "high_risk_off"}) {
		t.Fatalf("closed reasons %v", got)
	}
}

func TestShadowCountSkippedOnANilShadowCountsNothing(t *testing.T) {
	rec := recordShadow(t)
	var sh *Shadow
	sh.CountSkipped(context.Background(), ShadowReasonCap)
	if got := rec.resultReasons(); len(got) != 0 {
		t.Fatalf("a nil shadow counted %v", got)
	}
}
