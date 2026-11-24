package workerservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

func teamCatalogSnoozeNow() time.Time { return time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC) }

// runTeamCatalogSnoozeCase runs one post-sync GitHub team auto-import whose collector answers collectErr (or a clean
// result), for a job created createdAgo before now; zero createdAgo with zeroCreatedAt leaves JobCreatedAt unset.
func runTeamCatalogSnoozeCase(t *testing.T, collectErr error, createdAgo time.Duration, zeroCreatedAt bool) (error, *fakeTeamCatalogObserver) {
	t.Helper()
	observer := &fakeTeamCatalogObserver{}
	collector := &linearCollectorSpy{err: collectErr}
	if collectErr == nil {
		collector.result = providersync.TeamCatalogResult{TeamsWritten: 2}
	}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "github", nil },
		native:          carriedForTest(map[string]providersync.TeamCatalogCollector{"github": collector}),
		clients:         fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true, Members: true}},
		observer:        observer,
		now:             teamCatalogSnoozeNow,
	}
	reference := syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun}
	if !zeroCreatedAt {
		reference.JobCreatedAt = teamCatalogSnoozeNow().Add(-createdAgo)
	}
	return dispatcher.TeamAutoImport(context.Background(), reference), observer
}

// rateLimited is the client's answer since CHAOS-14546 for a 429 whose wait is past the deadline.
func rateLimited(retryAfter time.Duration) error {
	return errors.Join(context.DeadlineExceeded,
		&providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited, StatusCode: 429, RetryAfter: retryAfter})
}

func wantTeamCatalogOutcome(t *testing.T, observer *fakeTeamCatalogObserver, want jobruntime.TeamCatalogOutcome) {
	t.Helper()
	if len(observer.dispatches) != 1 || observer.dispatches[0].outcome != want {
		t.Fatalf("outcomes = %+v, want %s", observer.dispatches, want)
	}
}

// A post-sync team auto-import whose GitHub team catalog read is rate limited snoozes the job attempt-neutral, no
// earlier than RetryAfter, so the catalog is read again soon. RED at main 0c0732a87cae (CI leg #7016): nil and
// native_failed_nonfatal.
func TestTeamAutoImportSnoozesOnARateLimitedCatalogRead(t *testing.T) {
	err, observer := runTeamCatalogSnoozeCase(t, rateLimited(90*time.Second), time.Minute, false)
	var snooze *river.JobSnoozeError
	if !errors.As(err, &snooze) || snooze.Duration != 90*time.Second {
		t.Fatalf("err = %v (outcomes %+v): want a job snooze of RetryAfter 1m30s", err, observer.dispatches)
	}
	wantTeamCatalogOutcome(t, observer, jobruntime.TeamCatalogOutcomeRateLimitDeferred)
}

// The snooze cases: no usable RetryAfter waits the floor; a wait past the episode end is clamped to it; at or past the
// end, or with no job CreatedAt, the job ends nonfatal as before.
func TestTeamAutoImportRateLimitSnoozeIsBoundedByTheEpisode(t *testing.T) {
	for _, cell := range []struct {
		name       string
		retryAfter time.Duration
		createdAgo time.Duration
		zero       bool
		want       time.Duration // 0 = no snooze, nonfatal
	}{
		{"no RetryAfter waits the floor", 0, time.Minute, false, teamCatalogRateLimitFloor},
		{"a short RetryAfter waits the floor", 5 * time.Second, time.Minute, false, teamCatalogRateLimitFloor},
		{"a wait past the episode end is clamped to it", time.Hour, teamCatalogRateLimitEpisode - 10*time.Minute, false, 10 * time.Minute},
		{"at the episode end the job ends nonfatal", 90 * time.Second, teamCatalogRateLimitEpisode, false, 0},
		{"past the episode end the job ends nonfatal", 90 * time.Second, 3 * time.Hour, false, 0},
		{"a job with no CreatedAt ends nonfatal", 90 * time.Second, 0, true, 0},
	} {
		t.Run(cell.name, func(t *testing.T) {
			err, observer := runTeamCatalogSnoozeCase(t, rateLimited(cell.retryAfter), cell.createdAgo, cell.zero)
			var snooze *river.JobSnoozeError
			if cell.want == 0 {
				if err != nil {
					t.Fatalf("err = %v, want nil (nonfatal)", err)
				}
				wantTeamCatalogOutcome(t, observer, jobruntime.TeamCatalogOutcomeNativeFailedNonfatal)
				return
			}
			if !errors.As(err, &snooze) || snooze.Duration != cell.want {
				t.Fatalf("err = %v, want a snooze of %s", err, cell.want)
			}
			wantTeamCatalogOutcome(t, observer, jobruntime.TeamCatalogOutcomeRateLimitDeferred)
		})
	}
}

// Only a classified rate limit snoozes: a 5xx provider answer, a plain deadline, and an unclassified error keep main's
// nonfatal nil.
func TestTeamAutoImportOtherCollectorErrorsStayNonfatal(t *testing.T) {
	for name, collectErr := range map[string]error{
		"5xx":          &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient, StatusCode: 503},
		"deadline":     context.DeadlineExceeded,
		"unclassified": errors.New("github team catalog walk failed"),
	} {
		t.Run(name, func(t *testing.T) {
			err, observer := runTeamCatalogSnoozeCase(t, collectErr, time.Minute, false)
			if err != nil {
				t.Fatalf("err = %v, want nil (nonfatal)", err)
			}
			wantTeamCatalogOutcome(t, observer, jobruntime.TeamCatalogOutcomeNativeFailedNonfatal)
		})
	}
}

// Control: a clean post-sync import completes the job and records the native outcome, as main does.
func TestTeamAutoImportCleanGitHubCatalogCompletes(t *testing.T) {
	err, observer := runTeamCatalogSnoozeCase(t, nil, time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	wantTeamCatalogOutcome(t, observer, jobruntime.TeamCatalogOutcomeNative)
}
