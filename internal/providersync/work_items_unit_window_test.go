package providersync

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// refusedWindowDoer fails the test on any provider request: a unit with a
// malformed window must fail before provider I/O.
type refusedWindowDoer struct{ t *testing.T }

func (doer refusedWindowDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Errorf("a unit with a refused window issued a provider request: %s", request.URL.Path)
	return nil, errors.New("provider request after a refused window")
}

// workItemsWindowRoute runs one provider's real work-items route. With
// refuse set, the provider double fails the test on any request; without it,
// the double answers as in the provider's own route test.
type workItemsWindowRoute struct {
	provider string
	claim    func() Claim
	collect  func(t *testing.T, claim Claim, refuse bool) (CompleteRouteBatch, error)
}

func workItemsWindowRoutes() []workItemsWindowRoute {
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	doerFor := func(t *testing.T, refuse bool, real providerfoundation.HTTPDoer) providerfoundation.HTTPDoer {
		if refuse {
			return fakehttp.Client(refusedWindowDoer{t: t})
		}
		return fakehttp.Client(real)
	}
	return []workItemsWindowRoute{
		{
			provider: "github",
			claim: func() Claim {
				claim := githubWorkItemsRESTClaim()
				claim.DatasetOptions["fetch_milestones"] = false
				return claim
			},
			collect: func(t *testing.T, claim Claim, refuse bool) (CompleteRouteBatch, error) {
				fixtures := githubWorkItemsRESTFixtures()
				delete(fixtures, "/repos/acme/api/milestones")
				real := &githubWorkItemsRouteDoer{
					t:              t,
					rest:           &githubWorkItemsRESTDoer{t: t, replies: fixtures},
					graphqlReplies: []string{`{"data":{"repository":{"pr0":{"number":52,"comments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"timelineItems":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`},
				}
				return (GitHubWorkItemsRouteHandler{
					Projects:                      GitHubProjectV2Fetcher{},
					ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{},
				}).Collect(
					context.Background(), claim,
					providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
					gitHubPullRequestClient(t, doerFor(t, refuse, real), "https://api.github.com"), normalizedAt,
				)
			},
		},
		{
			provider: "gitlab",
			claim:    func() Claim { return nativeTestClaim("gitlab", "work-items") },
			collect: func(t *testing.T, claim Claim, refuse bool) (CompleteRouteBatch, error) {
				real := &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}
				return (GitLabWorkItemsRouteHandler{
					StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
				}).Collect(
					context.Background(), claim,
					providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
					gitLabWorkItemsClient(t, doerFor(t, refuse, real)), normalizedAt,
				)
			},
		},
		{
			provider: "jira",
			claim:    jiraAtlassianClaim,
			collect: func(t *testing.T, claim Claim, refuse bool) (CompleteRouteBatch, error) {
				return jiraAtlassianCompleteHandler(t).Collect(
					context.Background(), claim, providerfoundation.Credential{},
					jiraWorkItemsTestClient(t, doerFor(t, refuse, &jiraAtlassianDoer{t: t}), lease), normalizedAt,
				)
			},
		},
		{
			provider: "linear",
			claim:    linearFamilyClaim,
			collect: func(t *testing.T, claim Claim, refuse bool) (CompleteRouteBatch, error) {
				real := &linearWorkItemsDoer{responses: []string{
					linearFamilyEmptyCyclesResponse(),
					linearLifecycleIssueResponse("ENG-1", "ENG"),
				}}
				return (LinearWorkItemFamilyRouteHandler{Direct: linearFamilyDirectHandler()}).Collect(
					context.Background(), claim,
					providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
					linearWorkItemsClient(t, doerFor(t, refuse, real)), normalizedAt,
				)
			},
		},
	}
}

// A work-items unit whose window is malformed fails the unit, for each of the
// four providers, before any provider request: `since` after `before`, and a
// window of more than githubWorkItemDerivedMaxBackfillDays days. The same
// route with the window of the provider's own route test completes, so the
// refusal is the window's and nothing else's.
func TestWorkItemsRoutesRefuseAMalformedUnitWindowBeforeProviderIO(t *testing.T) {
	for _, route := range workItemsWindowRoutes() {
		t.Run(route.provider, func(t *testing.T) {
			if _, err := route.collect(t, route.claim(), false); err != nil {
				t.Fatalf("control: the unit with a valid window failed: %v", err)
			}

			inverted := route.claim()
			since := inverted.BeforeAt.AddDate(0, 0, 2)
			inverted.SinceAt = &since
			if batch, err := route.collect(t, inverted, true); !errors.Is(err, ErrInvalidConfiguration) ||
				len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("since after before: error=%v effects=%d watermark=%v want ErrInvalidConfiguration and no batch",
					err, len(batch.Effects), batch.Watermark)
			}

			tooWide := route.claim()
			wideSince := tooWide.BeforeAt.AddDate(0, 0, -githubWorkItemDerivedMaxBackfillDays-1)
			tooWide.SinceAt = &wideSince
			if batch, err := route.collect(t, tooWide, true); !errors.Is(err, ErrInvalidConfiguration) ||
				len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("window over the bound: error=%v effects=%d watermark=%v want ErrInvalidConfiguration and no batch",
					err, len(batch.Effects), batch.Watermark)
			}
		})
	}
}

// The bound itself: a window of exactly the maximum number of days is valid
// and one more day is not.
func TestWorkItemsUnitWindowDaysBound(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "work-items")
	before := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	claim.BeforeAt = &before
	atBound := before.AddDate(0, 0, -githubWorkItemDerivedMaxBackfillDays)
	claim.SinceAt = &atBound
	days, err := workItemsUnitWindowDays(claim, normalizedAt)
	if err != nil || len(days) != githubWorkItemDerivedMaxBackfillDays {
		t.Fatalf("window at the bound: days=%d error=%v want %d days", len(days), err, githubWorkItemDerivedMaxBackfillDays)
	}
	if !days[0].Equal(atBound) || !days[len(days)-1].Equal(before.AddDate(0, 0, -1)) {
		t.Fatalf("days=%s..%s want %s..%s", days[0], days[len(days)-1], atBound, before.AddDate(0, 0, -1))
	}
	overBound := atBound.AddDate(0, 0, -1)
	claim.SinceAt = &overBound
	if _, err := workItemsUnitWindowDays(claim, normalizedAt); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("window one day over the bound: error=%v want ErrInvalidConfiguration", err)
	}
	sameDay := before.Add(-time.Hour)
	claim.SinceAt = &sameDay
	if days, err := workItemsUnitWindowDays(claim, normalizedAt); err != nil || len(days) != 1 {
		t.Fatalf("one-day window: days=%d error=%v", len(days), err)
	}
}
