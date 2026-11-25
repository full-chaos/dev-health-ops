package workerservice

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// A RetryAfter of the largest Duration snoozes for the rest of the episode, no more:
// the clamp comes before any time sum.
func TestTeamAutoImportSaturatedRetryAfterSnoozesForTheRestOfTheEpisode(t *testing.T) {
	for _, cell := range []struct {
		name       string
		createdAgo time.Duration
		want       time.Duration
	}{
		{"a fresh job", time.Second, teamCatalogRateLimitEpisode - time.Second},
		{"half of the episode spent", teamCatalogRateLimitEpisode / 2, teamCatalogRateLimitEpisode / 2},
		{"one second left", teamCatalogRateLimitEpisode - time.Second, time.Second},
	} {
		t.Run(cell.name, func(t *testing.T) {
			err, observer := runTeamCatalogSnoozeCase(t, rateLimited(time.Duration(math.MaxInt64)), cell.createdAgo, false)
			var snooze *river.JobSnoozeError
			if !errors.As(err, &snooze) || snooze.Duration != cell.want {
				t.Fatalf("err = %v, want a snooze of %s", err, cell.want)
			}
			wantTeamCatalogOutcome(t, observer, jobruntime.TeamCatalogOutcomeRateLimitDeferred)
		})
	}
}
