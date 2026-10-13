package fixturescli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// recommendationsWindowDays is the window the nightly recommendations_daily_fanout schedule runs
// with (scheduler/fixed/producers.go); the fixtures load uses the same one so its run is covered
// by, and covers, the schedule's own occurrence.
const recommendationsWindowDays = 14

// jobsContractRoot (a var so tests can point at the repository root) is where every operator verb reads the job contract from.
var jobsContractRoot = "contracts/jobs/v1"

// recommendationsBackstopGeneration is the generation of the manual trigger-backstop verb for
// "--all-teams --window 14" (workersctl manualBackstopTriggerGeneration), so one load and one
// operator run of the same day are the same logical run.
func recommendationsBackstopGeneration(org, day string) string {
	return fmt.Sprintf("manual-trigger:recommendations:%s:%s:all-teams:window:%d", org, day, recommendationsWindowDays)
}

// RecommendationsBackstopResult is what the trigger did.
type RecommendationsBackstopResult struct {
	Day    string `json:"day"`
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

// TriggerRecommendationsBackstop starts the recommendations backstop for every team of org, the
// way `metrics remaining trigger-backstop --family recommendations --all-teams --window 14` does,
// through the same remaining.PostgresStore entry point. The compute itself is the worker's. A run
// that is already covered or in flight is a success: the rows are, or are about to be, there.
func TriggerRecommendationsBackstop(ctx context.Context, pool *pgxpool.Pool, registry *jobruntime.Registry, logger *slog.Logger, org string, now time.Time) (RecommendationsBackstopResult, error) {
	day := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -1).Format("2006-01-02")
	result := RecommendationsBackstopResult{Day: day}
	store, err := remaining.NewPostgresStore(pool)
	if err != nil {
		return result, fmt.Errorf("remaining store: %w", err)
	}
	publisher, err := remaining.NewPostgresPublisher(pool, registry)
	if err != nil {
		return result, fmt.Errorf("remaining publisher: %w", err)
	}
	generation := recommendationsBackstopGeneration(org, day)
	logger.Debug("recommendations backstop trigger", "org_id", org, "day", day, "window_days", recommendationsWindowDays, "generation", generation)
	outcome, err := store.StartManualRecommendationsTriggerRun(ctx, org, day, generation, nil, recommendationsWindowDays, publisher)
	result.RunID = outcome.RunID
	switch {
	case err == nil && outcome.AlreadyRan:
		result.Status = "already_ran"
	case err == nil:
		result.Status = "started"
	case errors.Is(err, remaining.ErrDayAlreadyCovered):
		result.Status, err = "already_covered", nil
	case errors.Is(err, remaining.ErrDayInProgress):
		result.Status, err = "in_progress", nil
	default:
		result.Status = "error"
	}
	logger.Info("recommendations backstop triggered", "org_id", org, "day", day, "status", result.Status, "run_id", result.RunID)
	return result, err
}
