package providerunit

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func providerClassError(class providerfoundation.ErrorClass) error {
	return &providerfoundation.ProviderError{Class: class, StatusCode: 500, Path: "/repos/acme/api"}
}

// Every class of the closed vocabulary has a stated try cap; 0 = registry ceiling.
func TestRetryMaxAttemptsPerCauseClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"permanent", providerClassError(providerfoundation.ErrorPermanent), 1},
		{"conflict", providerClassError(providerfoundation.ErrorConflict), 2},
		{"transient", providerClassError(providerfoundation.ErrorTransient), 0},
		{"cancelled", providerClassError(providerfoundation.ErrorCancelled), 0},
		{"rate limited", providerClassError(providerfoundation.ErrorRateLimited), 0},
		{"budget contended", providerfoundation.ErrBudgetContended, 0},
		{"unknown error", errors.New("boom"), 2},
		{"class outside the vocabulary", providerClassError(providerfoundation.ErrorClass("made_up")), 2},
	}
	for _, testCase := range cases {
		if got := retryMaxAttempts(testCase.err); got != testCase.want {
			t.Errorf("%s: retryMaxAttempts = %d, want %d", testCase.name, got, testCase.want)
		}
	}
	for class := range providerErrorClasses {
		switch class {
		case providerfoundation.ErrorPermanent, providerfoundation.ErrorConflict,
			providerfoundation.ErrorTransient, providerfoundation.ErrorCancelled,
			providerfoundation.ErrorRateLimited,
			providerfoundation.ErrorAuthentication, providerfoundation.ErrorNotFound:
		default:
			t.Errorf("provider error class %q has no stated retry decision", class)
		}
	}
}

func runUnitAttempt(t *testing.T, err error, attempt int) (*classRepository, error) {
	t.Helper()
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(githubFilesUnit())}
	var builds atomic.Int32
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now },
		BuildExecutor: githubRouteExecutor(now, collectErrorHandler{err: err},
			doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unexpected") }), &builds),
	}
	execution := providerExecution(repository.unit, now, attempt)
	execution.Definition.MaxAttempts = 5
	return repository, handler.Work(context.Background(), execution)
}

func isPermanentReturn(err error) bool {
	return err != nil && strings.Contains(err.Error(), "category: "+string(jobruntime.CategoryPermanent))
}

func TestCappedClassEndsAtItsCapUnderItsCauseClass(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		attempt int
		class   string
	}{
		{"permanent on try 1", providerClassError(providerfoundation.ErrorPermanent), 1, "provider_permanent"},
		{"conflict on try 2", providerClassError(providerfoundation.ErrorConflict), 2, "provider_conflict"},
		{"unclassified on try 2", errors.New("boom"), 2, providersync.CauseClassUnclassified},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			repository, err := runUnitAttempt(t, testCase.err, testCase.attempt)
			if repository.status != "failed" || repository.causeClass != testCase.class ||
				repository.lastFailCategory != GitHubFilesInventoryFailureCategory {
				t.Fatalf("status=%s category=%q class=%q, want failed/%s/%s", repository.status,
					repository.lastFailCategory, repository.causeClass,
					GitHubFilesInventoryFailureCategory, testCase.class)
			}
			if !isPermanentReturn(err) {
				t.Fatalf("Work() = %v, want a permanent return so River does not retry", err)
			}
		})
	}
}

func TestCappedClassRetriesBeforeItsCapAndUncappedClassReachesTheCeiling(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		attempt int
		retry   bool
	}{
		{"conflict on try 1", providerClassError(providerfoundation.ErrorConflict), 1, true},
		{"unclassified on try 1", errors.New("boom"), 1, true},
		{"transient on try 4", providerClassError(providerfoundation.ErrorTransient), 4, true},
		{"transient on try 5", providerClassError(providerfoundation.ErrorTransient), 5, false},
		{"cancelled on try 4", providerClassError(providerfoundation.ErrorCancelled), 4, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			repository, err := runUnitAttempt(t, testCase.err, testCase.attempt)
			if err == nil || isPermanentReturn(err) {
				t.Fatalf("Work() = %v, want a retryable return", err)
			}
			if failed := repository.status == "failed"; failed == testCase.retry {
				t.Fatalf("status=%s, want failed=%v", repository.status, !testCase.retry)
			}
		})
	}
}
