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

// A post-sync team auto-import whose GitHub team catalog read is rate limited with the wait past the deadline (the
// client's answer since CHAOS-14546: errors.Join(context.DeadlineExceeded, the rate_limited ProviderError with its
// RetryAfter)) must snooze the job attempt-neutral, no earlier than RetryAfter, so the catalog is read again soon.
// RED at main: the dispatcher records native_failed_nonfatal and returns nil, and the job completes.
func TestTeamAutoImportSnoozesOnARateLimitedCatalogRead(t *testing.T) {
	limited := &providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited, StatusCode: 429, RetryAfter: 90 * time.Second}
	observer := &fakeTeamCatalogObserver{}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "github", nil },
		native:          carriedForTest(map[string]providersync.TeamCatalogCollector{"github": &linearCollectorSpy{err: errors.Join(context.DeadlineExceeded, limited)}}),
		clients:         fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true, Members: true}},
		observer:        observer,
	}
	err := dispatcher.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun})
	var snooze *river.JobSnoozeError
	if !errors.As(err, &snooze) || snooze.Duration < limited.RetryAfter {
		t.Fatalf("err = %v (outcomes %+v): want a job snooze of at least RetryAfter %s", err, observer.dispatches, limited.RetryAfter)
	}
}

// Control: a clean post-sync import completes the job and records the native outcome, as main does.
func TestTeamAutoImportCleanGitHubCatalogCompletes(t *testing.T) {
	observer := &fakeTeamCatalogObserver{}
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "github", nil },
		native:          carriedForTest(map[string]providersync.TeamCatalogCollector{"github": &linearCollectorSpy{result: providersync.TeamCatalogResult{TeamsWritten: 2}}}),
		clients:         fakeAutoimportClientResolver{integrationID: "integration-1"},
		selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true, Members: true}},
		observer:        observer,
	}
	if err := dispatcher.TeamAutoImport(context.Background(), syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if len(observer.dispatches) != 1 || observer.dispatches[0].outcome != jobruntime.TeamCatalogOutcomeNative {
		t.Fatalf("outcomes = %+v, want native", observer.dispatches)
	}
}
