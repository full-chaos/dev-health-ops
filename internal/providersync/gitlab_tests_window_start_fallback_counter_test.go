package providersync

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const gitLabWindowFallbackFamily = "dev_health_provider_gitlab_tests_window_start_fallback_total"

func gitLabWindowFallbackSample(t *testing.T, metrics *providerfoundation.Metrics, dataset string) string {
	t.Helper()
	var exposition strings.Builder
	if err := metrics.WritePrometheus(&exposition); err != nil {
		t.Fatal(err)
	}
	prefix := gitLabWindowFallbackFamily + `{provider="gitlab",dataset="` + dataset + `"} `
	for _, line := range strings.Split(exposition.String(), "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}

// gitLabWindowFallbackWalk runs one invocation from raw and stops after the first non-final emission when stopFirst is set.
func gitLabWindowFallbackWalk(t *testing.T, client *providerfoundation.HTTPClient, claim Claim, raw string, stopFirst bool) string {
	t.Helper()
	stop := errors.New("yield")
	last := raw
	err := GitLabTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), raw, func(emission ChunkRouteEmission) error {
			last = emission.CursorAfter
			if stopFirst && !emission.Final {
				return stop
			}
			return nil
		})
	if err != nil && !errors.Is(err, stop) {
		t.Fatalf("walk err=%v", err)
	}
	return last
}

func gitLabWindowFallbackClient(t *testing.T, rawDate string, withDate bool) *providerfoundation.HTTPClient {
	t.Helper()
	fake := &gitLabEntryFake{gitLabResumeFake: &gitLabResumeFake{t: t, pipelines: gitLabResumeFixture()}, rawDate: rawDate}
	if withDate {
		fake.date, fake.step = time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), time.Second
	}
	client := gitLabRepositoryClient(t, fake, "https://gitlab.example")
	client.Metrics = providerfoundation.NewMetrics()
	return client
}

func TestGitLabTestsWindowStartFallbackIsCountedPerDataset(t *testing.T) {
	for _, dataset := range []string{"tests", "cicd"} {
		for _, tc := range []struct{ name, rawDate string }{
			{"no Date header", ""},
			{"a Date that does not parse", "yesterday-ish"},
			{"a Date 30 s after the epoch", time.Unix(30, 0).UTC().Format(http.TimeFormat)},
			{"a Date 60 s after the epoch", time.Unix(60, 0).UTC().Format(http.TimeFormat)},
		} {
			t.Run(dataset+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				client := gitLabWindowFallbackClient(t, tc.rawDate, false)
				claim := nativeTestClaim("gitlab", dataset)
				beforeAt := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
				claim.BeforeAt = &beforeAt
				gitLabWindowFallbackWalk(t, client, claim, "", false)
				if got := gitLabWindowFallbackSample(t, client.Metrics, dataset); got != "1" {
					t.Fatalf("%s counter=%q, want 1", dataset, got)
				}
				other := map[string]string{"tests": "cicd", "cicd": "tests"}[dataset]
				if got := gitLabWindowFallbackSample(t, client.Metrics, other); got != "" {
					t.Fatalf("%s counter=%q, want no sample", other, got)
				}
			})
		}
	}
}

func TestGitLabTestsWindowStartWithAUsableDateIsNotCounted(t *testing.T) {
	t.Parallel()
	client := gitLabWindowFallbackClient(t, "", true)
	gitLabWindowFallbackWalk(t, client, gitLabEntryClaim(time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)), "", false)
	if got := gitLabWindowFallbackSample(t, client.Metrics, "tests"); got != "" {
		t.Fatalf("counter=%q with a usable Date, want no sample", got)
	}
}

func TestGitLabTestsWindowStartFallbackIsCountedOncePerWalkNotPerChunk(t *testing.T) {
	t.Parallel()
	client := gitLabWindowFallbackClient(t, "", false)
	claim := gitLabEntryClaim(time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC))
	cursor := gitLabWindowFallbackWalk(t, client, claim, "", true)
	if cursor == "" {
		t.Fatal("the first invocation left no resume cursor")
	}
	gitLabWindowFallbackWalk(t, client, claim, cursor, false)
	if got := gitLabWindowFallbackSample(t, client.Metrics, "tests"); got != "1" {
		t.Fatalf("counter=%q after a resume, want 1", got)
	}
}
