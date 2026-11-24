package workerservice

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type rosterLimitDoer struct {
	teamsStatus  int
	rosterStatus int
	rosterBody   string
	userStatus   int
	headers      http.Header
}

func (d rosterLimitDoer) Do(request *http.Request) (*http.Response, error) {
	status, body, headers := http.StatusOK, `[]`, http.Header{"Content-Type": {"application/json"}}
	limited := func(code int) {
		status, body = code, `{"message":"limited"}`
		for key, values := range d.headers {
			headers[key] = values
		}
	}
	switch {
	case request.URL.Path == "/orgs/acme/teams":
		if d.teamsStatus != 0 {
			limited(d.teamsStatus)
		} else {
			body = `[{"slug":"platform","name":"Platform"}]`
		}
	case request.URL.Path == "/orgs/acme/teams/platform/members":
		if d.rosterStatus != 0 {
			limited(d.rosterStatus)
		} else {
			body = d.rosterBody
		}
	case strings.HasPrefix(request.URL.Path, "/users/"):
		if d.userStatus != 0 {
			limited(d.userStatus)
		} else {
			body = `{"login":"octocat"}`
		}
	}
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// rosterSinkConn answers every write with an error, so a walk that gets past the skipped roster ends in a plain error
// and not a panic; reads answer zero counts.
type rosterSinkConn struct{ zeroCountConn }

var errRosterSink = errors.New("synthetic sink refused")

func (rosterSinkConn) Exec(context.Context, string, ...any) error { return errRosterSink }
func (rosterSinkConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errRosterSink
}
func (rosterSinkConn) AsyncInsert(context.Context, string, bool, ...any) error { return errRosterSink }

type rosterLimitClients struct {
	client *providerfoundation.HTTPClient
}

func (r rosterLimitClients) ResolveClient(context.Context, string, string, string) (providerfoundation.Credential, *providerfoundation.HTTPClient, string, error) {
	return providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}, r.client, "integration-1", nil
}

// WANT: a classified rate limit on a team roster page or a member email lookup in the post-sync auto-import
// (Strict=false) reaches the dispatcher and snoozes the job, like a rate limited team list. The 500 control stays nonfatal.
func TestTeamAutoImportRosterRateLimitSnoozes(t *testing.T) {
	reset := http.Header{"X-Ratelimit-Remaining": {"0"}, "X-Ratelimit-Reset": {"4102444800"}}
	for _, cell := range []struct {
		name       string
		doer       rosterLimitDoer
		email      bool
		wantSnooze bool
	}{
		{"roster 429 Retry-After 120", rosterLimitDoer{rosterStatus: 429, headers: http.Header{"Retry-After": {"120"}}}, false, true},
		{"roster 403 ratelimit remaining 0", rosterLimitDoer{rosterStatus: 403, headers: reset}, false, true},
		{"email lookup 429", rosterLimitDoer{rosterBody: `[{"login":"octocat"}]`, userStatus: 429, headers: http.Header{"Retry-After": {"120"}}}, true, true},
		{"control: team list 500", rosterLimitDoer{teamsStatus: 500}, false, false},
		{"control: roster 500", rosterLimitDoer{rosterStatus: 500}, false, false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(cell.doer),
				func(*http.Request) error { return nil },
				providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
			if err != nil {
				t.Fatal(err)
			}
			observer := &fakeTeamCatalogObserver{}
			collector := providersync.GitHubTeamCatalogCollector{
				Client: providersync.GitHubTeamCatalogRouteHandler{ResolveEmail: cell.email},
				Sink:   providersync.GitHubTeamCatalogClickHouseEffects{Conn: rosterSinkConn{}},
			}
			dispatcher := &nativeTeamAutoimportDispatcher{
				resolveProvider: func(context.Context, string, string) (string, error) { return "github", nil },
				native:          carriedForTest(map[string]providersync.TeamCatalogCollector{"github": collector}),
				clients:         rosterLimitClients{client: client},
				selections:      fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true, Members: true}},
				observer:        observer,
				now:             teamCatalogSnoozeNow,
			}
			ctx := syncdispatchruntime.WithJobCreatedAt(context.Background(), teamCatalogSnoozeNow().Add(-time.Minute))
			var gotErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("panic after the roster was skipped (walk went on to the sink): %v", r)
						gotErr = errors.New("panic")
					}
				}()
				gotErr = dispatcher.TeamAutoImport(ctx, syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun})
			}()
			var snooze *river.JobSnoozeError
			snoozed := errors.As(gotErr, &snooze)
			t.Logf("CELL %q: err=%v snoozed=%v outcomes=%+v", cell.name, gotErr, snoozed, observer.dispatches)
			if cell.wantSnooze {
				if !snoozed {
					t.Errorf("RED: want a job snooze, got err=%v outcomes=%+v", gotErr, observer.dispatches)
				} else {
					wantTeamCatalogOutcome(t, observer, jobruntime.TeamCatalogOutcomeRateLimitDeferred)
				}
				return
			}
			if gotErr != nil || len(observer.dispatches) != 1 || observer.dispatches[0].outcome != jobruntime.TeamCatalogOutcomeNativeFailedNonfatal {
				t.Errorf("control: want nil + native_failed_nonfatal, got err=%v outcomes=%+v", gotErr, observer.dispatches)
			}
		})
	}
}
