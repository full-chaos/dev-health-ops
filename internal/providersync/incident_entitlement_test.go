package providersync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/jackc/pgx/v5"
)

type canonicalIncidentFeatureQueryerFunc func(context.Context, string, ...any) pgx.Row

func (query canonicalIncidentFeatureQueryerFunc) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return query(ctx, sql, args...)
}

type erroringRow struct{ err error }

func (row erroringRow) Scan(...any) error { return row.err }

// A refusal has three distinct causes and providerunit treats them
// differently: a construction defect stays ErrInvalidConfiguration, a state
// that could not be READ is ErrIncidentEntitlementUnavailable (retryable), and
// only a decided-closed state is ErrIncidentEntitlementDisabled (terminal as
// feature_disabled). Collapsing the second into the third would let a
// domain-Postgres blip durably terminalize healthy units (codex round 1).
func TestPostgresIncidentEntitlementSeparatesUnavailableFromDisabled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if err := (PostgresIncidentEntitlement{}).Require(ctx, "7f2d4c2a-8f6f-4e57-9a8e-2c1d8e1f0a11"); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil pool err=%v", err)
	}
	if err := (PostgresIncidentEntitlement{}).require(ctx, canonicalIncidentFeatureQueryerFunc(func(context.Context, string, ...any) pgx.Row {
		t.Fatal("a malformed organization id must never reach the store")
		return nil
	}), "org-acme"); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("malformed org err=%v", err)
	}
	storeFault := errors.New("FATAL: sorry, too many clients already")
	err := (PostgresIncidentEntitlement{}).require(ctx, canonicalIncidentFeatureQueryerFunc(func(context.Context, string, ...any) pgx.Row {
		return erroringRow{err: storeFault}
	}), "7f2d4c2a-8f6f-4e57-9a8e-2c1d8e1f0a11")
	if !errors.Is(err, ErrIncidentEntitlementUnavailable) || !errors.Is(err, storeFault) ||
		errors.Is(err, ErrIncidentEntitlementDisabled) {
		t.Fatalf("store fault err=%v", err)
	}
	// No rows: the feature is not registered, which IS a decided-closed state.
	err = (PostgresIncidentEntitlement{}).require(ctx, canonicalIncidentFeatureQueryerFunc(func(context.Context, string, ...any) pgx.Row {
		return erroringRow{err: pgx.ErrNoRows}
	}), "7f2d4c2a-8f6f-4e57-9a8e-2c1d8e1f0a11")
	if !errors.Is(err, ErrIncidentEntitlementDisabled) || errors.Is(err, ErrIncidentEntitlementUnavailable) {
		t.Fatalf("unregistered feature err=%v", err)
	}
}

// Every refusal emits exactly one structured log line with the tenant and unit
// identity and the seam; an unevaluable entitlement logs at ERROR and is NOT
// counted as a refusal. Not parallel: it swaps the process default logger.
func TestIncidentEntitlementRefusalEmitsOneStructuredLogLine(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	metrics := providerfoundation.NewMetrics()
	claim := nativeTestClaim("pagerduty", "schedules")
	err := requireIncidentEntitlement(context.Background(), incidentEntitlementFunc(func(context.Context, string) error {
		return ErrIncidentEntitlementDisabled
	}), metrics, claim, IncidentEntitlementSeamWrite)
	if !errors.Is(err, ErrIncidentEntitlementDisabled) {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("lines=%d output=%s", len(lines), output.String())
	}
	for _, want := range []string{
		`"level":"WARN"`, `"msg":"` + incidentEntitlementRefusedEvent + `"`,
		`"org_id":"org-acme"`, `"provider":"pagerduty"`, `"dataset":"schedules"`,
		`"seam":"write"`, `"sync_run_id":"` + claim.SyncRunID + `"`, `"unit_id":"` + claim.ID + `"`,
	} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("missing %s in %s", want, lines[0])
		}
	}
	if strings.Contains(lines[0], claim.CredentialID) {
		t.Fatalf("credential id leaked into the refusal line: %s", lines[0])
	}
	assertIncidentEntitlementRefusalCounted(t, metrics, "pagerduty", "schedules", "write", 1)

	output.Reset()
	err = requireIncidentEntitlement(context.Background(), incidentEntitlementFunc(func(context.Context, string) error {
		return ErrIncidentEntitlementUnavailable
	}), metrics, claim, IncidentEntitlementSeamCollect)
	if !errors.Is(err, ErrIncidentEntitlementUnavailable) {
		t.Fatal(err)
	}
	line := strings.TrimSpace(output.String())
	if strings.Count(line, "\n") != 0 || !strings.Contains(line, `"level":"ERROR"`) ||
		!strings.Contains(line, `"msg":"`+incidentEntitlementUnavailableEvent+`"`) ||
		!strings.Contains(line, `"seam":"collect"`) {
		t.Fatalf("unavailable line=%s", line)
	}
	assertIncidentEntitlementRefusalCounted(t, metrics, "pagerduty", "schedules", "collect", 0)
}
