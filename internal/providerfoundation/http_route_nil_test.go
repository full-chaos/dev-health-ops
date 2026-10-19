package providerfoundation

import (
	"errors"
	"testing"
)

// DoRoute with a nil context is exactly Do with a nil context: a nil client
// answers ErrCredentialInvalid for both, and neither attaches a route.
func TestDoRouteWithANilContextIsDo(t *testing.T) {
	var client *HTTPClient
	_, doErr := client.Do(nil, "GET", "/x", nil)                                                 //nolint:staticcheck // the nil context is the case under test
	_, routeErr := client.DoRoute(nil, "GET", "/x", "/x", nil)                                   //nolint:staticcheck // the nil context is the case under test
	_, redirectErr := client.DoUnauthenticatedRoute(nil, "GET", "/x", "https://store.example/x") //nolint:staticcheck // the nil context is the case under test
	if !errors.Is(doErr, ErrCredentialInvalid) || !errors.Is(routeErr, ErrCredentialInvalid) || !errors.Is(redirectErr, ErrCredentialInvalid) {
		t.Fatalf("Do=%v DoRoute=%v DoUnauthenticatedRoute=%v, want ErrCredentialInvalid from each", doErr, routeErr, redirectErr)
	}
	if routeTemplate(nil) != "" { //nolint:staticcheck // the nil context is the case under test
		t.Fatal("a nil context carries a route")
	}
}
