package workerservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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
	limitedBody  string
}

func (d rosterLimitDoer) Do(request *http.Request) (*http.Response, error) {
	status, body, headers := http.StatusOK, `[]`, http.Header{"Content-Type": {"application/json"}}
	limited := func(code int) {
		status, body = code, `{"message":"limited"}`
		if d.limitedBody != "" {
			body = d.limitedBody
		}
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

// rosterSinkConn answers every write with an error, so a walk that gets past a skipped roster ends in a plain error
// and not a panic; reads answer zero counts. It counts the writes it refuses: a run that was snoozed before any
// catalog write counts none.
type rosterSinkConn struct {
	zeroCountConn
	writes *int
}

var errRosterSink = errors.New("synthetic sink refused")

func (c rosterSinkConn) Exec(context.Context, string, ...any) error {
	*c.writes++
	return errRosterSink
}
func (c rosterSinkConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	*c.writes++
	return nil, errRosterSink
}
func (c rosterSinkConn) AsyncInsert(context.Context, string, bool, ...any) error {
	*c.writes++
	return errRosterSink
}

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
		wantWrites string // "none": no catalog write attempted; "some": the walk went on to the sink
	}{
		{"roster 429 Retry-After 120", rosterLimitDoer{rosterStatus: 429, headers: http.Header{"Retry-After": {"120"}}}, false, true, "none"},
		{"roster 403 ratelimit remaining 0", rosterLimitDoer{rosterStatus: 403, headers: reset}, false, true, "none"},
		{"email lookup 429", rosterLimitDoer{rosterBody: `[{"login":"octocat"}]`, userStatus: 429, headers: http.Header{"Retry-After": {"120"}}}, true, true, "none"},
		{"control: team list 500", rosterLimitDoer{teamsStatus: 500}, false, false, "none"},
		{"control: roster 500", rosterLimitDoer{rosterStatus: 500}, false, false, "some"},
		{"control: roster 403 permission, no rate-limit header", rosterLimitDoer{rosterStatus: 403, limitedBody: `{"message":"Resource not accessible by integration"}`}, false, false, "some"},
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
			writes := 0
			collector := providersync.GitHubTeamCatalogCollector{
				Client: providersync.GitHubTeamCatalogRouteHandler{ResolveEmail: cell.email},
				Sink:   providersync.GitHubTeamCatalogClickHouseEffects{Conn: rosterSinkConn{writes: &writes}},
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
			t.Logf("CELL %q: err=%v snoozed=%v outcomes=%+v sink writes=%d", cell.name, gotErr, snoozed, observer.dispatches, writes)
			if cell.wantWrites == "none" && writes != 0 || cell.wantWrites == "some" && writes == 0 {
				t.Errorf("sink writes = %d, want %s (a snooze or an early error ends before any catalog write; a skip walks on to the sink)", writes, cell.wantWrites)
			}
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

// A snoozed post-sync auto-import writes one Info line with the chosen delay and no error text.
func TestTeamAutoImportRosterRateLimitSnoozeWritesOneInfoLineWithTheDelay(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com",
		fakehttp.Client(rosterLimitDoer{rosterStatus: 429, headers: http.Header{"Retry-After": {"120"}}}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	dispatcher := &nativeTeamAutoimportDispatcher{
		resolveProvider: func(context.Context, string, string) (string, error) { return "github", nil },
		native: carriedForTest(map[string]providersync.TeamCatalogCollector{"github": providersync.GitHubTeamCatalogCollector{
			Client: providersync.GitHubTeamCatalogRouteHandler{},
			Sink:   providersync.GitHubTeamCatalogClickHouseEffects{Conn: rosterSinkConn{writes: &writes}},
		}}),
		clients:    rosterLimitClients{client: client},
		selections: fakeAutoimportSelectionsResolver{selections: providersync.TeamCatalogSelections{Teams: true, Members: true}},
		observer:   &fakeTeamCatalogObserver{},
		now:        teamCatalogSnoozeNow,
	}
	ctx := syncdispatchruntime.WithJobCreatedAt(context.Background(), teamCatalogSnoozeNow().Add(-time.Minute))
	gotErr := dispatcher.TeamAutoImport(ctx, syncdispatchruntime.DomainReference{OrganizationID: testOrg, SyncRunID: testRun})
	var snooze *river.JobSnoozeError
	if !errors.As(gotErr, &snooze) {
		t.Fatalf("err=%v, want a job snooze", gotErr)
	}
	var lines []map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var record map[string]any
		if len(raw) > 0 && json.Unmarshal(raw, &record) == nil && record["msg"] == "team_catalog_rate_limit_snoozed" {
			lines = append(lines, record)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("%d team_catalog_rate_limit_snoozed lines in %q, want 1", len(lines), logs.String())
	}
	line := lines[0]
	if line["level"] != "INFO" || line["provider"] != "github" || line["delay_ms"] != float64(120000) {
		t.Fatalf("line = %v, want level INFO, provider github, delay_ms 120000", line)
	}
	for key := range line {
		if key == "error" || key == "err" || key == "cause" {
			t.Fatalf("line carries an error operand %q: %v", key, line)
		}
	}
}
