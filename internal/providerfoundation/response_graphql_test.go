package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const graphQLRouteForTests = "/graphql"

func requireGraphQLFailure(t *testing.T, err error, route, reason string, sentinel error) {
	t.Helper()
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Route != route || providerErr.Reason != reason {
		t.Fatalf("error=%v, want ProviderError route=%q reason=%q", err, route, reason)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error=%v lost %v", err, sentinel)
	}
	if !providerErr.AfterResponse() || providerErr.Class != ErrorPermanent || providerErr.Retryable() {
		t.Fatalf("error=%v: a GraphQL errors body in a 2xx is a permanent post-response failure", err)
	}
	if route != "" && !strings.Contains(err.Error(), " route="+route+" reason="+reason) {
		t.Fatalf("error text %q lacks route=%s reason=%s", err.Error(), route, reason)
	}
	if strings.Contains(err.Error(), "acme-secret") {
		t.Fatalf("error text %q carries provider message text", err.Error())
	}
}

func TestResponseGraphQLCarriesRouteAndKeepsSentinel(t *testing.T) {
	routed := withResponseRoute("/graphql", &http.Response{Body: http.NoBody})
	for _, sentinel := range []error{ErrGraphQLResponse, ErrGraphQLComplexity, ErrNormalizationInvalid} {
		requireGraphQLFailure(t, ResponseGraphQL(routed, sentinel), "/graphql", ReasonGraphQLErrors, sentinel)
	}
	unrouted := ResponseGraphQL(&http.Response{Body: http.NoBody}, ErrGraphQLResponse)
	var providerErr *ProviderError
	if !errors.As(unrouted, &providerErr) || providerErr.Route != "" || providerErr.Reason != ReasonGraphQLErrors || !errors.Is(unrouted, ErrGraphQLResponse) {
		t.Fatalf("error=%v: a response with no route keeps reason and sentinel and prints no route", unrouted)
	}
}

func TestGraphQLShapeFailureCarriesRouteAndKeepsSentinel(t *testing.T) {
	for _, sentinel := range []error{ErrGraphQLResponse, ErrGraphQLComplexity} {
		err := GraphQLShapeFailure("/graphql", sentinel)
		requireGraphQLFailure(t, err, "/graphql", ReasonBodyShape, sentinel)
	}
}

func TestLinearGraphQLPagesErrorsCarryRoute(t *testing.T) {
	rows := map[string]struct {
		body     string
		sentinel error
	}{
		"complexity": {`{"errors":[{"message":"Query is too complex","extensions":{"code":"COMPLEXITY_LIMIT"}}]}`, ErrGraphQLComplexity},
		"plain":      {`{"errors":[{"message":"acme-secret failed"}]}`, ErrGraphQLResponse},
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			doer := &paginationDoer{responses: []paginationResponse{{body: row.body}}}
			client := paginationClient(t, "linear", "https://api.linear.app", fakehttp.Client(doer))
			_, err := CollectLinearGraphQLPages(context.Background(), client, LinearPageOptions{
				Query: "query Issues { issues { nodes { id } } }", ConnectionPath: []string{"issues"}, PerPage: 50, MaxPages: 1,
			})
			requireGraphQLFailure(t, err, graphQLRouteForTests, ReasonGraphQLErrors, row.sentinel)
			if len(doer.requests) != 1 {
				t.Fatalf("requests=%d, want 1", len(doer.requests))
			}
		})
	}
}
