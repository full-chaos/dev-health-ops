package principal

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A process with no envelope key holds a nil *Verifier. Verify on it refuses every token, counted as
// outcome=not_configured, and never panics.
func TestVerifyOnANilVerifierRefusesAsNotConfigured(t *testing.T) {
	var v *Verifier
	verify := func(token string) (claims *Claims, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic: %v", p)
			}
		}()
		return v.Verify(context.Background(), token)
	}
	for _, token := range []string{"", "bm90.YSB0b2tlbg.c2ln", "eyJhbGciOiJFZERTQSIsImtpZCI6ImsifQ.e30.c2ln"} {
		claims, err := verify(token)
		if err == nil || claims != nil {
			t.Fatalf("Verify(%q) on a nil verifier = %v, %v; want a refusal", token, claims, err)
		}
		if got := err.Error(); len(got) >= 6 && got[:6] == "panic:" {
			t.Fatalf("Verify(%q) on a nil verifier: %s", token, got)
		}
	}
	var rm metricdata.ResourceMetrics
	if err := sharedTestMetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if dp := findCounterDataPoint(t, rm, "devhealth_query_api_envelope_verify_total", "outcome", "not_configured"); dp.Value != 3 {
		t.Fatalf("outcome=not_configured = %d, want 3", dp.Value)
	}
}
