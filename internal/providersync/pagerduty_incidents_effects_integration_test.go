//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestPagerDutyIncidentFamilyEffectsUseMigratedClickHouseAndExactReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	// Apply the production migration chain. This test must exercise the exact
	// canonical tables, not a relaxed test-only schema.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	normalizedAt := time.Date(2026, 7, 19, 19, 0, 0, 123456000, time.UTC)
	claim := nativeTestClaim("pagerduty", "incidents")
	incident, err := normalizePagerDutyIncident(claim, "acme", pagerDutyIncidentPayload{
		ID: "PI1", Title: pagerDutyStringPtr("Database outage"), Status: pagerDutyStringPtr("triggered"),
		CreatedAt: pagerDutyStringPtr("2026-07-17T12:00:00Z"), UpdatedAt: pagerDutyStringPtr("2026-07-17T12:01:00Z"),
	}, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	alertClaim := nativeTestClaim("pagerduty", "incident-alerts")
	alert, err := normalizePagerDutyAlert(alertClaim, "acme", pagerDutyAlertPayload{
		ID: "PA1", Summary: pagerDutyStringPtr("Disk alert"), Status: pagerDutyStringPtr("triggered"), Severity: pagerDutyStringPtr("critical"),
		CreatedAt: pagerDutyStringPtr("2026-07-17T12:02:00Z"), UpdatedAt: pagerDutyStringPtr("2026-07-17T12:03:00Z"),
	}, incident.ID, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	logClaim := nativeTestClaim("pagerduty", "incident-log-entries")
	entry, err := normalizePagerDutyLogEntry(logClaim, "acme", pagerDutyLogEntryPayload{
		ID: "PL1", Type: pagerDutyStringPtr("status_change"), Summary: pagerDutyStringPtr("Triggered"), CreatedAt: pagerDutyStringPtr("2026-07-17T12:04:00Z"),
	}, incident.ID, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	noteClaim := nativeTestClaim("pagerduty", "incident-notes")
	note, err := normalizePagerDutyNote(noteClaim, "acme", pagerDutyNotePayload{
		ID: "PN1", Content: pagerDutyStringPtr("Investigating"), CreatedAt: pagerDutyStringPtr("2026-07-17T12:05:00Z"),
	}, incident.ID, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}

	sink := PagerDutyIncidentFamilyClickHouseEffects{
		Conn: conn, ProviderInstanceID: "Acme",
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	cases := []struct {
		name   string
		claim  Claim
		effect EffectBatch
		table  string
	}{
		{name: "incidents", claim: claim, effect: mustPagerDutyIncidentEffect(t, incident), table: "operational_incidents"},
		{name: "alerts", claim: alertClaim, effect: mustPagerDutyAlertEffect(t, alert), table: "operational_alerts"},
		{name: "log_entries", claim: logClaim, effect: mustPagerDutyLogEntryEffect(t, entry), table: "operational_incident_timeline_events"},
		{name: "notes", claim: noteClaim, effect: mustPagerDutyNoteEffect(t, note), table: "operational_incident_notes"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if inspection, err := sink.InspectEffect(ctx, testCase.claim, testCase.effect); err != nil || inspection != EffectAbsent {
				t.Fatalf("before write inspection=%s error=%v", inspection, err)
			}
			if err := sink.WriteEffect(ctx, testCase.claim, testCase.effect); err != nil {
				t.Fatal(err)
			}
			if inspection, err := sink.InspectEffect(ctx, testCase.claim, testCase.effect); err != nil || inspection != EffectExact {
				t.Fatalf("after write inspection=%s error=%v", inspection, err)
			}
			if err := sink.WriteEffect(ctx, testCase.claim, testCase.effect); err != nil {
				t.Fatalf("replay write: %v", err)
			}
			if inspection, err := sink.InspectEffect(ctx, testCase.claim, testCase.effect); err != nil || inspection != EffectExact {
				t.Fatalf("after replay inspection=%s error=%v", inspection, err)
			}
			var count uint64
			if err := conn.QueryRow(ctx, "SELECT count() FROM "+testCase.table+" FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ?", testCase.claim.OrgID, "pagerduty", "acme").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("rows=%d", count)
			}
		})
	}

	foreign := claim
	foreign.OrgID = "org-other"
	foreignEffect := mustPagerDutyIncidentEffect(t, incident)
	if err := sink.WriteEffect(ctx, foreign, foreignEffect); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		// The row itself still carries org-acme, so the scope fence must reject it
		// before ClickHouse can mutate the other tenant.
		t.Fatalf("foreign tenant error=%v", err)
	}
}

func mustPagerDutyIncidentEffect(t *testing.T, row pagerDutyIncidentRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("operational_incidents", EffectReadbackRequired, []pagerDutyIncidentRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func mustPagerDutyAlertEffect(t *testing.T, row pagerDutyAlertRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("operational_alerts", EffectReadbackRequired, []pagerDutyAlertRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func mustPagerDutyLogEntryEffect(t *testing.T, row pagerDutyLogEntryRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("operational_incident_timeline_events", EffectReadbackRequired, []pagerDutyLogEntryRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func mustPagerDutyNoteEffect(t *testing.T, row pagerDutyNoteRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("operational_incident_notes", EffectReadbackRequired, []pagerDutyNoteRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
