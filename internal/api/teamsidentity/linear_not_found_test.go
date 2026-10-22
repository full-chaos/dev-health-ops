package teamsidentity

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type linearAnswerDoer struct {
	status int
	body   string
}

func (d linearAnswerDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: d.status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(d.body)), Request: request}, nil
}

// TestLinearNotFoundIsA404 pins CHAOS-10593: Linear reports a missing entity
// as a GraphQL error (in a 200 body, or on a 4xx), which used to reach the
// route as a plain error and answer 500. It now reaches the shared
// not-found mapping for team discover and member discover alike, and the
// upstream message is never echoed.
func TestLinearNotFoundIsA404(t *testing.T) {
	const planted = "PLANTED-UPSTREAM-TEXT-10593"
	notFoundBodies := map[string]string{
		"entity not found message": `{"errors":[{"message":"Entity not found: Team ` + planted + `"}]}`,
		"code NOT_FOUND":           `{"errors":[{"message":"` + planted + `","extensions":{"code":"NOT_FOUND"}}]}`,
		"type not found":           `{"errors":[{"message":"` + planted + `","extensions":{"type":"not found"}}]}`,
	}
	routes := map[string]func(context.Context) error{
		"discover": func(ctx context.Context) error { _, err := discoverLinear(ctx, linearTestCredential()); return err },
		"members": func(ctx context.Context) error {
			_, err := discoverMembersLinear(ctx, secrets.NewValue("lin_api_test"), "ENG")
			return err
		},
	}
	for routeName, run := range routes {
		for _, status := range []int{200, 400} {
			for bodyName, body := range notFoundBodies {
				t.Run(routeName+"/"+bodyName+"/"+http.StatusText(status), func(t *testing.T) {
					withDiscoveryClient(t, fakehttp.Client(linearAnswerDoer{status: status, body: body}))
					err := run(context.Background())
					if err == nil {
						t.Fatal("want an error")
					}
					if strings.Contains(err.Error(), planted) {
						t.Errorf("error text echoes upstream text: %v", err)
					}
					rec := answerProviderFailure(err)
					want := `{"detail":"` + providerNotFoundDetail + `"}`
					if rec.Code != http.StatusNotFound || rec.Body.String() != want {
						t.Errorf("status = %d body = %s, want 404 %s (err: %v)", rec.Code, rec.Body.String(), want, err)
					}
				})
			}
		}
	}
}

// TestOtherLinearGraphQLErrorsStayA500 guards the other side.
func TestOtherLinearGraphQLErrorsStayA500(t *testing.T) {
	for _, status := range []int{200, 400} {
		withDiscoveryClient(t, fakehttp.Client(linearAnswerDoer{status: status, body: `{"errors":[{"message":"Rate limit","extensions":{"code":"RATELIMITED"}}]}`}))
		err := func() error { _, err := discoverLinear(context.Background(), linearTestCredential()); return err }()
		if err == nil {
			t.Fatalf("status %d: want an error", status)
		}
		if rec := answerProviderFailure(err); rec.Code != http.StatusInternalServerError {
			t.Errorf("status %d: answered %d, want 500", status, rec.Code)
		}
	}
}
