package providersync

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const graphQLErrorsBody = `{"errors":[{"message":"acme-secret denied"}]}`
const graphQLComplexityBody = `{"errors":[{"message":"Query complexity exceeded for acme-secret"}]}`

func TestGraphQLErrorsArraysInA2xxCarryRouteAndKeepSentinel(t *testing.T) {
	ctx := context.Background()
	rows := map[string]struct {
		provider, body string
		sentinel       error
		run            func(*providerfoundation.HTTPClient) error
	}{
		"github pr reviews": {"github", graphQLErrorsBody, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubPullRequestReviewFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{1}, nil, &GitHubPullRequestReviewFetchResult{})
			return err
		}},
		"github project v2 items": {"github", graphQLErrorsBody, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			return fetchGitHubProjectV2GraphQL(ctx, c, "query", map[string]any{}, &gitHubProjectV2ItemsEnvelope{}, &FetchEvidence{})
		}},
		"github project v2 changes": {"github", graphQLErrorsBody, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			return fetchGitHubProjectV2GraphQL(ctx, c, "query", map[string]any{}, &gitHubProjectV2ChangesEnvelope{}, &FetchEvidence{})
		}},
		"github project v2 field values": {"github", graphQLErrorsBody, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			return fetchGitHubProjectV2GraphQL(ctx, c, "query", map[string]any{}, &gitHubProjectV2FieldValuesEnvelope{}, &FetchEvidence{})
		}},
		"github work item social": {"github", graphQLErrorsBody, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubWorkItemPRSocialFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{1}, 10, nil, 10, nil, &GitHubWorkItemPRSocialFetchResult{})
			return err
		}},
		"gitlab files blobs": {"gitlab", graphQLErrorsBody, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := fetchGitLabGraphQLBlobs(ctx, c, "acme-secret/api", "main", []string{"a.go"}, "rawBlob")
			return err
		}},
		"linear reference catalog plain": {"linear", graphQLErrorsBody, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, _, _, err := collectLinearReferenceConnection(ctx, c, "query", linearReferenceConnectionTeams, linearReferenceConnectionVariables{}, 10, 1)
			return err
		}},
		"linear reference catalog complexity": {"linear", graphQLComplexityBody, providerfoundation.ErrGraphQLComplexity, func(c *providerfoundation.HTTPClient) error {
			_, _, _, err := collectLinearReferenceConnection(ctx, c, "query", linearReferenceConnectionTeams, linearReferenceConnectionVariables{}, 10, 1)
			return err
		}},
		"github blame (route already carried)": {"github", graphQLErrorsBody, providerfoundation.ErrNormalizationInvalid, func(c *providerfoundation.HTTPClient) error {
			_, err := fetchGitHubBlame(ctx, c, "acme-secret", "api", "main", "a.go")
			return err
		}},
		"github files blobs (route already carried)": {"github", graphQLErrorsBody, providerfoundation.ErrNormalizationInvalid, func(c *providerfoundation.HTTPClient) error {
			_, err := fetchGitHubFileContents(ctx, c, "acme-secret", "api", "tree-1", []string{"a.go"}, nil)
			return err
		}},
		"jira worklog (route already carried)": {"jira", graphQLErrorsBody, providerfoundation.ErrNormalizationInvalid, func(c *providerfoundation.HTTPClient) error {
			_, _, err := collectJiraGraphQLWorklogs(ctx, c, "ACME-SECRET-1", 1, "cloud")
			return err
		}},
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			err := row.run(fixedBodyClient(t, row.provider, row.body))
			requireRouteFailure(t, err, graphQLRoute, providerfoundation.ReasonGraphQLErrors)
			if !errors.Is(err, row.sentinel) {
				t.Fatalf("error=%v lost %v", err, row.sentinel)
			}
		})
	}
}
