package workerservice

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsTransientDialError(t *testing.T) {
	t.Parallel()
	pg := func(code string) error { return fmt.Errorf("connect: %w", &pgconn.PgError{Code: code}) }
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"refused", fmt.Errorf("dial: %w", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), true},
		{"reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, true},
		{"epipe", syscall.EPIPE, true},
		{"no_route", syscall.EHOSTUNREACH, true},
		{"net_unreachable", syscall.ENETUNREACH, true},
		{"eof", io.EOF, true},
		{"unexpected_eof", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{"dns_nxdomain", &net.DNSError{Err: "no such host", IsNotFound: true}, true},
		{"pg_starting_up", pg("57P03"), true},
		{"pg_admin_shutdown", pg("57P01"), true},
		{"pg_too_many_connections", pg("53300"), true},
		{"pgbouncer_protocol_08P01", pg("08P01"), true},
		{"pg_connection_exception", pg("08006"), true},
		{"auth_28P01", pg("28P01"), false},
		{"auth_28000", pg("28000"), false},
		{"permission_42501", pg("42501"), false},
		{"other_server", pg("XX000"), false},
		{"tls_unknown_authority", x509.UnknownAuthorityError{}, false},
		{"tls_alert", tls.AlertError(42), false},
		{"config_parse", errors.New("cannot parse dsn"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isTransientDialError(tc.err); got != tc.want {
			t.Errorf("%s: isTransientDialError = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDependencyCheckFailedMarksTransientThroughTheRegistry(t *testing.T) {
	t.Parallel()
	registry := health.NewRegistry(time.Second)
	for name, err := range map[string]error{
		"refused": syscall.ECONNREFUSED, "auth": &pgconn.PgError{Code: "28P01"}, "plain": errors.New("x"),
	} {
		if regErr := registry.RegisterRequired(name, func(context.Context) error { return dependencyCheckFailed(err) }); regErr != nil {
			t.Fatal(regErr)
		}
	}
	got := map[string]health.CheckStatus{}
	for _, status := range registry.CheckRequired(context.Background()).Checks {
		got[status.Name] = status
	}
	if s := got["refused"]; !s.Failed || s.TimedOut || !s.Transient || s.Cause != health.CauseUnreachable {
		t.Errorf("refused status = %+v, want failed transient unreachable, not timed out", s)
	}
	for _, name := range []string{"auth", "plain"} {
		if s := got[name]; !s.Failed || s.Transient || s.Cause != "error" {
			t.Errorf("%s status = %+v, want failed, not transient, cause error", name, s)
		}
	}
}

func TestPreclaimReadinessRetryableCoversTransient(t *testing.T) {
	t.Parallel()
	transient := health.Readiness{Failed: []string{"a"}, Checks: []health.CheckStatus{{Name: "a", Failed: true, Transient: true}}}
	mixed := health.Readiness{Failed: []string{"a", "b"}, Checks: []health.CheckStatus{
		{Name: "a", Failed: true, Transient: true}, {Name: "b", Failed: true}}}
	if !preclaimReadinessRetryable(transient) {
		t.Error("transient failure must be retryable")
	}
	if preclaimReadinessRetryable(mixed) {
		t.Error("a genuine failure beside a transient one must stay non-retryable")
	}
}

type presenceFake struct {
	errs      []error
	starts    int
	lifecycle workerPresenceLifecycle
}

func (p *presenceFake) Start(context.Context) error {
	p.starts++
	if p.starts <= len(p.errs) {
		return p.errs[p.starts-1]
	}
	return nil
}
func (*presenceFake) BeginDrain(context.Context) error { return nil }
func (*presenceFake) Shutdown(context.Context) error   { return nil }
func (*presenceFake) Errors() <-chan error             { return nil }

func TestWorkerProcessPresenceStartRetriesTransientDial(t *testing.T) {
	t.Parallel()
	presence := &presenceFake{errs: []error{syscall.ECONNREFUSED, io.EOF}}
	clock := &preclaimFakeClock{}
	component := workerProcessComponent{presence: presence, startBudget: time.Minute, now: clock.now, sleep: clock.sleep}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v", err)
	}
	if presence.starts != 3 {
		t.Fatalf("presence starts = %d, want 3", presence.starts)
	}
}

func TestWorkerProcessPresenceStartPermanentErrorExitsAtOnce(t *testing.T) {
	t.Parallel()
	presence := &presenceFake{errs: []error{&pgconn.PgError{Code: "28P01"}}}
	clock := &preclaimFakeClock{}
	component := workerProcessComponent{presence: presence, startBudget: time.Minute, now: clock.now, sleep: clock.sleep}
	if err := component.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil, want the auth error")
	}
	if presence.starts != 1 {
		t.Fatalf("presence starts = %d, want 1", presence.starts)
	}
}

func TestStartupRetryJitterBoundsAndCap(t *testing.T) {
	t.Parallel()
	for range 200 {
		got := startupRetryJitter(10 * time.Second)
		if got < 8*time.Second || got > 12*time.Second {
			t.Fatalf("jitter(10s) = %v, outside +/-20%%", got)
		}
	}
	clock := &preclaimFakeClock{}
	err := startupRetryBudget(context.Background(), 5*time.Second, clock.now, clock.sleep,
		func(d time.Duration) time.Duration { return d * 2 },
		func(context.Context) error { return syscall.ECONNREFUSED },
		isTransientDialError,
		func(int, time.Duration, time.Duration, error) {},
		func(int, time.Duration, error, bool) {})
	if err == nil {
		t.Fatal("want budget exhausted error")
	}
	var total time.Duration
	for _, w := range clock.recordedWaits() {
		total += w
	}
	if total > 5*time.Second {
		t.Fatalf("total wait %v exceeds the 5s budget", total)
	}
}
