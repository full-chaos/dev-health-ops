package providersync

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestFetchPageShapeFailuresCarryRouteAsBodyShape(t *testing.T) {
	ctx := context.Background()
	rows := map[string]struct {
		provider, body string
		sentinel       error
		run            func(*providerfoundation.HTTPClient) error
	}{
		"social comments null": {"github", `{"data":{"repository":{"pr0":{"number":7,"comments":null}}}}`, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubWorkItemPRSocialFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{7}, 10, nil, 0, nil, &GitHubWorkItemPRSocialFetchResult{})
			return err
		}},
		"social timeline items null": {"github", `{"data":{"repository":{"pr0":{"number":7,"timelineItems":null}}}}`, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubWorkItemPRSocialFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{7}, 0, nil, 10, nil, &GitHubWorkItemPRSocialFetchResult{})
			return err
		}},
		"pr reviews next page without a cursor": {"github", `{"data":{"repository":{"pr0":{"number":1,"reviews":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":""}}}}}}`, providerfoundation.ErrPaginationInvalid, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubPullRequestReviewFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{1}, nil, &GitHubPullRequestReviewFetchResult{})
			return err
		}},
		"pr reviews repeated cursor": {"github", `{"data":{"repository":{"pr0":{"number":1,"reviews":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}}}}}`, providerfoundation.ErrPaginationInvalid, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubPullRequestReviewFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{1}, map[int]string{1: "c1"}, &GitHubPullRequestReviewFetchResult{})
			return err
		}},
		"projects v2 changes node null": {"github", `{"data":{"node":null}}`, providerfoundation.ErrPaginationInvalid, func(c *providerfoundation.HTTPClient) error {
			var out gitHubProjectV2ChangesEnvelope
			return fetchGitHubProjectV2GraphQL(ctx, c, "query", map[string]any{"itemId": "acme-secret"}, &out, &FetchEvidence{})
		}},
		"projects v2 field values missing page info": {"github", `{"data":{"node":{"fieldValues":{"nodes":[]}}}}`, providerfoundation.ErrPaginationInvalid, func(c *providerfoundation.HTTPClient) error {
			var out gitHubProjectV2FieldValuesEnvelope
			return fetchGitHubProjectV2GraphQL(ctx, c, "query", map[string]any{"itemId": "acme-secret"}, &out, &FetchEvidence{})
		}},
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			err := row.run(fixedBodyClient(t, row.provider, row.body))
			requireRouteFailure(t, err, graphQLRoute, providerfoundation.ReasonBodyShape)
			if !errors.Is(err, row.sentinel) {
				t.Fatalf("error=%v lost %v", err, row.sentinel)
			}
		})
	}
}
