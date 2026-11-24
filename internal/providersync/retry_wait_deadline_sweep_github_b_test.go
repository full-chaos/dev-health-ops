package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// sweepG2Bind hands the test to a happy-path doer built before the test is
// known: the sweep wraps every doer in a sweepInjectDoer, so the doer inside
// is found there. Called first in every collect, before any request.
func sweepG2Bind(t *testing.T, doer HTTPDoerForSweep) HTTPDoerForSweep {
	t.Helper()
	inner := doer
	if inject, ok := doer.(*sweepInjectDoer); ok {
		inner = inject.inner
	}
	switch fixture := inner.(type) {
	case *githubTeamCatalogFixtureDoer:
		fixture.t = t
	case *githubWorkItemsRouteDoer:
		fixture.t = t
		fixture.rest.t = t
	case *githubWorkItemsRESTDoer:
		fixture.t = t
	case *githubTestsRouteDoer:
		fixture.t = t
	}
	return doer
}

var sweepG2Now = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

func sweepG2TeamCatalog(strict bool) retrySweepRoute {
	name := "github/team_catalog"
	if strict {
		name += "/strict"
	}
	return retrySweepRoute{
		name: name,
		newDoer: func() HTTPDoerForSweep {
			return &githubTeamCatalogFixtureDoer{byPath: map[string]string{
				"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"},{"slug":"data","name":"Data"}]`,
				"/orgs/acme/teams/platform/repos":   `[{"name":"api"},{"name":"web"}]`,
				"/orgs/acme/teams/platform/members": `[{"login":"octocat"},{"login":"monalisa"}]`,
				"/orgs/acme/teams/data/repos":       `[{"name":"etl"}]`,
				"/orgs/acme/teams/data/members":     `[{"login":"octocat"}]`,
				"/users/octocat":                    `{"login":"octocat","email":"octocat@example.com"}`,
				"/users/monalisa":                   `{"login":"monalisa","email":"mona@example.com"}`,
			}}
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			sweepG2Bind(t, doer)
			handler := GitHubTeamCatalogRouteHandler{
				Client: sweepClient(t, "github", "https://api.github.com", doer, wait), OrgName: "acme",
				Now: func() time.Time { return sweepG2Now }, ResolveEmail: true, Strict: strict,
			}
			rows, _, err := handler.Collect(ctx, "org-1", true, true)
			// The catalog has no batch: the rows stand in for the effects.
			batch := CompleteRouteBatch{Effects: make([]EffectBatch, len(rows.Teams)+len(rows.Memberships)+len(rows.RepoOwnership))}
			return batch, err
		},
	}
}

func sweepG2WorkItemsClaim() Claim {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["fetch_milestones"] = false
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	return claim
}

func sweepG2WorkItemsDoer() HTTPDoerForSweep {
	fixtures := githubWorkItemsRESTFixtures()
	delete(fixtures, "/repos/acme/api/milestones")
	return &githubWorkItemsRouteDoer{
		rest:           &githubWorkItemsRESTDoer{replies: fixtures},
		graphqlReplies: []string{`{"data":{"repository":{"pr0":{"number":52,"comments":{"nodes":[{"databaseId":9007199254740993,"body":"social","createdAt":"2026-07-23T00:00:00Z","author":{"login":"reviewer"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}},"timelineItems":{"nodes":[{"__typename":"ClosedEvent","createdAt":"2026-07-24T00:00:00Z","actor":{"login":"closer"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`},
	}
}

func sweepG2Tests(name, dataset string, chunked bool) retrySweepRoute {
	return retrySweepRoute{
		name: name,
		newDoer: func() HTTPDoerForSweep {
			return &githubTestsRouteDoer{archive: githubTestsZip(new(testing.T), map[string]string{
				"junit.xml": githubTestsJUnitFixture, "lcov.info": githubTestsLCOVFixture,
			})}
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			sweepG2Bind(t, doer)
			client := sweepClient(t, "github", "https://api.github.com", doer, wait)
			claim := nativeTestClaim("github", dataset)
			if !chunked {
				return (GitHubTestsRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG2Now)
			}
			var out CompleteRouteBatch
			err := (GitHubTestsRouteHandler{}).CollectChunks(
				ctx, claim, providerfoundation.Credential{}, client, sweepG2Now, "",
				func(emission ChunkRouteEmission) error {
					out.Effects = append(out.Effects, emission.Batch.Effects...)
					if emission.Final {
						out.Watermark = emission.Batch.Watermark
					}
					return nil
				},
			)
			return out, err
		},
	}
}

func init() {
	registerRetrySweepRoute(retrySweepRoute{
		name:    "github/security",
		newDoer: func() HTTPDoerForSweep { return &gitHubSecurityDoer{} },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubSecurityRouteHandler{}).Collect(
				ctx, nativeTestClaim("github", "security"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), sweepG2Now,
			)
		},
	})
	registerRetrySweepRoute(sweepG2TeamCatalog(false))
	registerRetrySweepRoute(sweepG2TeamCatalog(true))
	registerRetrySweepRoute(retrySweepRoute{
		name: "github/work_items_rest",
		newDoer: func() HTTPDoerForSweep {
			return &githubWorkItemsRESTDoer{replies: githubWorkItemsRESTFixtures()}
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			sweepG2Bind(t, doer)
			_, err := (GitHubWorkItemsRESTCollector{}).Collect(
				ctx, githubWorkItemsRESTClaim(),
				sweepClient(t, "github", "https://api.github.com", doer, wait),
				time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC),
			)
			return CompleteRouteBatch{}, err
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name:    "github/work_items",
		newDoer: sweepG2WorkItemsDoer,
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			sweepG2Bind(t, doer)
			claim := sweepG2WorkItemsClaim()
			projects := &githubWorkItemsRouteProjectPolicy{result: GitHubProjectV2FetchResult{
				Evidence: FetchEvidence{Provider: "github", Dataset: "projects-v2", Requests: 1, Pages: 1},
				Usage: GitHubProjectV2Usage{
					Transport: "graphql", RouteFamily: "work_item_prs",
					Dimension: BudgetGraphQLCost, RequestCount: 1,
				},
				Targets:   1,
				Snapshots: []githubProjectV2BoardSnapshot{{ProjectScopeID: "ghprojv2:acme#3"}},
			}}
			handler := GitHubWorkItemsRouteHandler{
				Projects: projects, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{},
				StoredTransitions: githubProjectV2NoStoredTransitions{},
			}
			return handler.Collect(
				ctx, claim, providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
				sweepClient(t, "github", "https://api.github.com", doer, wait),
				time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC),
			)
		},
	})
	registerRetrySweepRoute(sweepG2Tests("github/tests", "tests", false))
	registerRetrySweepRoute(sweepG2Tests("github/cicd", "cicd", false))
	registerRetrySweepRoute(sweepG2Tests("github/tests/chunked", "tests", true))
}
