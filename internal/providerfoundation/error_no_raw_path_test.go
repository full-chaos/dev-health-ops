package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// rawPathStrings returns every string held by value (fields of structs behind
// pointers and interfaces, to a fixed depth), named by field path.
func rawPathStrings(value reflect.Value, name string, depth int, out map[string]string) {
	if depth > 4 || !value.IsValid() {
		return
	}
	switch value.Kind() {
	case reflect.String:
		out[name] = value.String()
	case reflect.Pointer, reflect.Interface:
		if !value.IsNil() {
			rawPathStrings(value.Elem(), name, depth+1, out)
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			rawPathStrings(value.Field(index), name+"."+value.Type().Field(index).Name, depth+1, out)
		}
	}
}

func rawPathProbeClient(t *testing.T, provider string) (*HTTPClient, *bodyDoer) {
	t.Helper()
	doer := &bodyDoer{status: http.StatusInternalServerError, body: `{"message":"boom"}`}
	client, err := NewHTTPClient(provider, "https://api.example.test", fakehttp.Client(doer), func(*http.Request) error { return nil },
		RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client, doer
}

// A classified provider failure keeps the route template and nothing of the
// request path: no owner, repository or sha, and no query value.
func TestProviderErrorHoldsNoRawRequestPath(t *testing.T) {
	t.Parallel()
	const route = "/repos/{owner}/{repo}/commits/{sha}"
	client, doer := rawPathProbeClient(t, "github")
	_, err := client.DoRoute(context.Background(), http.MethodGet, route, "/repos/acme/secret/commits/deadbeef?q=secretq", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("err = %v, want *ProviderError", err)
	}
	if doer.path != "/repos/acme/secret/commits/deadbeef" {
		t.Fatalf("fixture sent %q, not the probe path", doer.path)
	}
	if providerErr.Route != route || !strings.Contains(providerErr.Error(), "route="+route) {
		t.Fatalf("Route=%q Error()=%q, want the template", providerErr.Route, providerErr.Error())
	}
	held := map[string]string{}
	rawPathStrings(reflect.ValueOf(providerErr), "ProviderError", 0, held)
	for name, text := range held {
		for _, raw := range []string{"acme", "secret", "deadbeef", "secretq"} {
			if strings.Contains(text, raw) {
				t.Errorf("%s = %q holds raw request path text %q", name, text, raw)
			}
		}
	}
}

// The budget dimension is read from the request path while classifying; it
// must survive dropping the path from the typed error.
func TestRateLimitedProviderErrorKeepsDimensionWithoutRawPath(t *testing.T) {
	t.Parallel()
	client, doer := rawPathProbeClient(t, "github")
	doer.status = http.StatusTooManyRequests
	_, err := client.DoRoute(context.Background(), http.MethodGet, "/search/issues", "/search/issues?q=secretq", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited {
		t.Fatalf("err = %v, want a rate-limited *ProviderError", err)
	}
	if providerErr.Dimension != "search" {
		t.Fatalf("Dimension = %q, want search", providerErr.Dimension)
	}
	held := map[string]string{}
	rawPathStrings(reflect.ValueOf(providerErr), "ProviderError", 0, held)
	for name, text := range held {
		if strings.Contains(text, "secretq") {
			t.Errorf("%s = %q holds a query value", name, text)
		}
	}
}
