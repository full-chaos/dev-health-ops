package providersync

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type gitLabCommitListingTransportFailureDoer struct {
	inner   *gitLabPullRequestDoer
	failure error
}

func (doer gitLabCommitListingTransportFailureDoer) Do(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, "/commits") {
		return nil, doer.failure
	}
	return doer.inner.Do(request)
}

type noopReservation struct{}

func (noopReservation) Release(context.Context) error { return nil }

// gitLabCommitListingBudget grants the requests the unit makes before the
// commits listing and refuses the listing itself.
type gitLabCommitListingBudget struct {
	inner   *gitLabPullRequestDoer
	failure error
}

func (budget gitLabCommitListingBudget) Acquire(context.Context, providerfoundation.BudgetKey) (providerfoundation.Reservation, error) {
	if len(budget.inner.requests) >= 4 {
		return nil, budget.failure
	}
	return noopReservation{}, nil
}

func gitLabOneMergeRequestResponses() []gitLabPullRequestResponse {
	return []gitLabPullRequestResponse{
		{body: gitLabRepositoryFixture},
		{body: `[{"iid":7,"title":"Add API","state":"opened","author":{"username":"author"},"created_at":"2026-07-15T10:00:00Z","updated_at":"2026-07-20T10:00:00Z"}]`},
		{body: `{}`},
		{body: `[]`},
	}
}

// The client asserts the unit's lease and takes its budget before every
// request, so a lost lease or a refused budget reaches the route as the
// listing's error. Neither may be read as "this merge request has no links".
func TestGitLabCommitListingControlFailuresFailTheUnitAndOrdinaryOnesDoNot(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		inject    func(client *providerfoundation.HTTPClient, inner *gitLabPullRequestDoer)
		wantError error
	}{
		{"lease_lost", func(client *providerfoundation.HTTPClient, inner *gitLabPullRequestDoer) {
			client.Lease = providerfoundation.LeaseGuardFunc(func(context.Context) error {
				if len(inner.requests) >= 4 {
					return providerfoundation.ErrLeaseLost
				}
				return nil
			})
		}, providerfoundation.ErrLeaseLost},
		{"lease_lost_joined_with_another_error", func(client *providerfoundation.HTTPClient, inner *gitLabPullRequestDoer) {
			client.Lease = providerfoundation.LeaseGuardFunc(func(context.Context) error {
				if len(inner.requests) >= 4 {
					return errors.Join(providerfoundation.ErrLeaseLost, errors.New("release failed"))
				}
				return nil
			})
		}, providerfoundation.ErrLeaseLost},
		{"budget_contended", func(client *providerfoundation.HTTPClient, inner *gitLabPullRequestDoer) {
			client.Budget = gitLabCommitListingBudget{inner: inner, failure: providerfoundation.ErrBudgetContended}
		}, providerfoundation.ErrBudgetContended},
		{"budget_unavailable", func(client *providerfoundation.HTTPClient, inner *gitLabPullRequestDoer) {
			client.Budget = gitLabCommitListingBudget{inner: inner, failure: providerfoundation.ErrBudgetUnavailable}
		}, providerfoundation.ErrBudgetUnavailable},
		{"ordinary_transport_error", nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &gitLabPullRequestDoer{t: t, responses: gitLabOneMergeRequestResponses()}
			var doer providerfoundation.HTTPDoer = inner
			if test.inject == nil {
				doer = gitLabCommitListingTransportFailureDoer{inner: inner, failure: errors.New("connection reset")}
			}
			client := gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example")
			if test.inject != nil {
				test.inject(client, inner)
			}
			batch, err := (GitLabPullRequestRouteHandler{}).Collect(
				context.Background(), nativeTestClaim("gitlab", "prs"), providerfoundation.Credential{}, client, normalizedAt,
			)
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) || batch.Watermark != nil || len(batch.Effects) != 0 {
					t.Fatalf("err=%v effects=%d watermark=%v: the failure of the commits listing must fail the unit with no watermark", err, len(batch.Effects), batch.Watermark)
				}
				return
			}
			if err != nil || len(batch.Effects) != 4 || len(batch.Effects[3].Rows) != 0 {
				t.Fatalf("err=%v effects=%d: an ordinary listing failure leaves the merge request without links and the unit completes", err, len(batch.Effects))
			}
		})
	}
}

// A malformed item of the commits listing is a payload fault of that merge
// request, not of the unit: the merge request goes without links this pass and
// the others are unaffected.
func TestGitLabCommitListingMalformedItemLeavesOnlyThatMergeRequestWithoutLinks(t *testing.T) {
	for name, body := range map[string]string{
		"missing_id":    `[{"short_id":"abc"}]`,
		"not_an_object": `["abc"]`,
	} {
		t.Run(name, func(t *testing.T) {
			responses := append(gitLabOneMergeRequestResponses(), gitLabPullRequestResponse{body: body})
			doer := &gitLabPullRequestDoer{t: t, responses: responses}
			batch, err := (GitLabPullRequestRouteHandler{}).Collect(
				context.Background(), nativeTestClaim("gitlab", "prs"), providerfoundation.Credential{},
				gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
			)
			if err != nil || len(batch.Effects) != 4 || len(batch.Effects[0].Rows) != 1 || len(batch.Effects[3].Rows) != 0 {
				t.Fatalf("err=%v effects=%d: the merge request row must be written and its links left out", err, len(batch.Effects))
			}
		})
	}
}
