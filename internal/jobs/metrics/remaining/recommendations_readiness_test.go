package remaining

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// timeoutError is a net.Error that is NOT a context deadline, which is what a
// driver-level read timeout actually looks like.
type timeoutError struct{}

func (timeoutError) Error() string { return "i/o timeout" }
func (timeoutError) Timeout() bool { return true }

func (timeoutError) Temporary() bool { return true }

// TestReadinessErrorsAreClassifiedByRemedyNotByTypeName pins each branch of the
// classifier against the remedy an operator would actually apply.
//
// The classes are not type names, so the test cannot be written by reading the
// implementation back. Each case names the failure a real deployment produces
// and asserts the class an operator needs to see.
func TestReadinessErrorsAreClassifiedByRemedyNotByTypeName(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "a statement timeout is transient and must not read as a migration fault",
			err:  context.DeadlineExceeded,
			want: jobruntime.RecommendationsReadinessFailOpenTimeout,
		},
		{
			name: "an orderly worker shutdown is not an incident",
			err:  context.Canceled,
			want: jobruntime.RecommendationsReadinessFailOpenCanceled,
		},
		{
			name: "a SQLSTATE means the server answered and rejected: unfinished migration",
			err:  &pgconn.PgError{Code: "42P01", Message: `relation "daily_metrics_runs" does not exist`},
			want: jobruntime.RecommendationsReadinessFailOpenQuery,
		},
		{
			name: "a driver read timeout carries no SQLSTATE and is infrastructure",
			err:  timeoutError{},
			want: jobruntime.RecommendationsReadinessFailOpenConnection,
		},
		{
			name: "an unrecognised error still lands in the closed set, never its own series",
			err:  errors.New("something the driver has not seen before"),
			want: jobruntime.RecommendationsReadinessFailOpenOther,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := classifyReadinessError(testCase.err); got != testCase.want {
				t.Errorf("classified as %q, want %q", got, testCase.want)
			}
		})
	}
}

// TestClassificationSurvivesWrapping is the case the table above cannot reach.
//
// Errors arrive from pgx WRAPPED, never bare. A classifier written against bare
// sentinels passes its own table and then mis-labels every error in production
// — the failure mode is not "wrong class" but "everything is `other`", which
// reads as an unclassifiable driver rather than as a broken classifier.
func TestClassificationSurvivesWrapping(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "wrapped cancellation",
			err:  fmt.Errorf("querying readiness: %w", context.Canceled),
			want: jobruntime.RecommendationsReadinessFailOpenCanceled,
		},
		{
			name: "doubly wrapped SQLSTATE",
			err: fmt.Errorf("gate: %w",
				fmt.Errorf("scan: %w", &pgconn.PgError{Code: "42703"})),
			want: jobruntime.RecommendationsReadinessFailOpenQuery,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := classifyReadinessError(testCase.err); got != testCase.want {
				t.Errorf("classified as %q, want %q — wrapping must not erase the class",
					got, testCase.want)
			}
		})
	}
}

// TestContextErrorsWinOverTransportErrors pins the ORDER, which is the part a
// refactor silently breaks.
//
// A cancelled context surfaces through the driver as an error that ALSO
// satisfies net.Error. Classified transport-first, an ordinary worker shutdown
// would be reported as a connection fault — the alert would fire on every
// deploy, and the class that exists to be ignorable would become the noisy one.
func TestContextErrorsWinOverTransportErrors(t *testing.T) {
	// Satisfies both net.Error and errors.Is(context.Canceled), exactly as a
	// cancelled query does on the way out of the pool.
	both := cancelledTransportError{}

	if got := classifyReadinessError(both); got != jobruntime.RecommendationsReadinessFailOpenCanceled {
		t.Errorf("classified as %q, want %q — the context check must precede the "+
			"transport check, or every shutdown reads as a connection fault",
			got, jobruntime.RecommendationsReadinessFailOpenCanceled)
	}
}

type cancelledTransportError struct{}

func (cancelledTransportError) Error() string   { return "read tcp: operation was canceled" }
func (cancelledTransportError) Timeout() bool   { return false }
func (cancelledTransportError) Temporary() bool { return false }
func (cancelledTransportError) Unwrap() error   { return context.Canceled }

var _ net.Error = cancelledTransportError{}

// TestEveryClassifiedClassIsAcceptedByTheCollector closes the loop between the
// two halves, which are in different packages and can drift apart.
//
// classifyReadinessError could return a perfectly reasonable string that the
// collector then REJECTS as unknown — the gate would classify correctly and the
// observation would still be dropped. Neither package's own tests can see that.
func TestEveryClassifiedClassIsAcceptedByTheCollector(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	for _, sample := range []error{
		context.DeadlineExceeded,
		context.Canceled,
		&pgconn.PgError{Code: "42P01"},
		timeoutError{},
		errors.New("unrecognised"),
		nil,
	} {
		class := classifyReadinessError(sample)
		if err := collector.ObserveRecommendationsReadinessFailOpen(class); err != nil {
			t.Errorf("the gate produced class %q, which the collector rejects: %v — "+
				"the two sets must not drift", class, err)
		}
	}
}

// sentinelObserver records what the gate reported, for the one path that must
// report nothing at all.
type sentinelObserver struct {
	failOpen []string
	skipped  int
}

func (observer *sentinelObserver) RecommendationsReadinessFailOpen(class string) {
	observer.failOpen = append(observer.failOpen, class)
}
func (observer *sentinelObserver) RecommendationsReadinessSkipped() { observer.skipped++ }

type sentinelLogger struct{}

func (sentinelLogger) Error(string, ...any) {}
func (sentinelLogger) Warn(string, ...any)  {}

// TestTheSingleTenantSentinelNeverReachesTheQuery pins the short-circuit.
//
// daily_metrics_runs.org_id is typed uuid, so "default" is unrepresentable
// there: the query cannot match, and a CAST of it raises rather than returning
// no rows. Reaching the database at all would turn every single-tenant
// evaluation into a fail-open, which is countable but wrong -- the gate would
// report a permanent error rate for a deployment that has no fault.
func TestTheSingleTenantSentinelNeverReachesTheQuery(t *testing.T) {
	observer := &sentinelObserver{}

	// A nil pool: reaching it panics, which is the point. The short-circuit
	// must happen before any query is attempted.
	if !DailyMetricsReady(context.Background(), nil, "default",
		time.Now().UTC(), observer, sentinelLogger{}) {
		t.Error("the single-tenant sentinel must proceed")
	}
	if len(observer.failOpen) != 0 || observer.skipped != 0 {
		t.Errorf("the sentinel path must observe nothing, got failOpen=%v skipped=%d",
			observer.failOpen, observer.skipped)
	}
}
