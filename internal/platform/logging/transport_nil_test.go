package logging

import (
	"fmt"
	"net/url"
	"testing"
)

// A typed-nil *url.Error in the chain is found by errors.As with a nil target: TransportFailure must not dereference it
// (CHAOS-7933 r1).
func TestTransportFailureSurvivesATypedNilURLError(t *testing.T) {
	var nilURLError *url.Error
	for name, err := range map[string]error{"bare": error(nilURLError), "wrapped": fmt.Errorf("call: %w", error(nilURLError))} {
		got := TransportFailure(err)
		if got == nil || got.Error() != "exchange request failed: protocol" {
			t.Fatalf("%s: TransportFailure = %v, want the class-only text with the default operation", name, got)
		}
	}
}
