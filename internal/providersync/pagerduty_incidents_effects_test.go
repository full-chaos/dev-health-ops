package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestPagerDutyIncidentFamilyEffectsValidateEachTypedDestinationAndScope(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "incidents")
	normalizedAt := time.Date(2026, 7, 19, 19, 0, 0, 0, time.UTC)
	incident, err := normalizePagerDutyIncident(claim, "acme", pagerDutyIncidentPayload{
		ID: "PI1", Title: pagerDutyStringPtr("Incident"), Status: pagerDutyStringPtr("triggered"),
		CreatedAt: pagerDutyStringPtr("2026-07-17T12:00:00Z"), UpdatedAt: pagerDutyStringPtr("2026-07-17T12:01:00Z"),
	}, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyIncidentRows(claim, "acme", []pagerDutyIncidentRow{incident}); err != nil {
		t.Fatal(err)
	}
	alertClaim := claim
	alertClaim.Dataset = "incident-alerts"
	alert, err := normalizePagerDutyAlert(alertClaim, "acme", pagerDutyAlertPayload{
		ID: "PA1", Summary: pagerDutyStringPtr("Alert"), Status: pagerDutyStringPtr("triggered"),
		CreatedAt: pagerDutyStringPtr("2026-07-17T12:02:00Z"),
	}, incident.ID, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyAlertRows(alertClaim, "acme", []pagerDutyAlertRow{alert}); err != nil {
		t.Fatal(err)
	}
	logClaim := claim
	logClaim.Dataset = "incident-log-entries"
	entry, err := normalizePagerDutyLogEntry(logClaim, "acme", pagerDutyLogEntryPayload{
		ID: "PL1", Type: pagerDutyStringPtr("status_change"), Summary: pagerDutyStringPtr("Triggered"),
		CreatedAt: pagerDutyStringPtr("2026-07-17T12:03:00Z"),
	}, incident.ID, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyLogEntryRows(logClaim, "acme", []pagerDutyLogEntryRow{entry}); err != nil {
		t.Fatal(err)
	}
	noteClaim := claim
	noteClaim.Dataset = "incident-notes"
	note, err := normalizePagerDutyNote(noteClaim, "acme", pagerDutyNotePayload{
		ID: "PN1", Content: pagerDutyStringPtr("Investigating"), CreatedAt: pagerDutyStringPtr("2026-07-17T12:04:00Z"),
	}, incident.ID, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyNoteRows(noteClaim, "acme", []pagerDutyNoteRow{note}); err != nil {
		t.Fatal(err)
	}

	foreign := incident
	foreign.OrgID = "org-other"
	if err := fillPagerDutyIncidentOrdering(&foreign); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyIncidentRows(claim, "acme", []pagerDutyIncidentRow{foreign}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant error=%v", err)
	}
	mixed := incident
	mixed.ProviderInstanceID = "other"
	if err := fillPagerDutyIncidentOrdering(&mixed); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyIncidentRows(claim, "acme", []pagerDutyIncidentRow{mixed}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign instance error=%v", err)
	}
	tampered := incident
	tampered.Title = "tampered"
	if err := validatePagerDutyIncidentRows(claim, "acme", []pagerDutyIncidentRow{tampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("ordering tamper error=%v", err)
	}
	contractTampered := incident
	contractTampered.OrderingContract = 3
	if err := validatePagerDutyIncidentRows(claim, "acme", []pagerDutyIncidentRow{contractTampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("ordering contract tamper error=%v", err)
	}
	if err := validatePagerDutyIncidentRows(claim, "acme", []pagerDutyIncidentRow{incident, incident}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate error=%v", err)
	}
}

func TestPagerDutyIncidentFamilyEffectsRejectWrongRequestBeforeSinkAccess(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "incidents")
	sink := PagerDutyIncidentFamilyClickHouseEffects{}
	if err := sink.WriteEffect(nil, claim, EffectBatch{Destination: "operational_incidents"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil context error=%v", err)
	}
	if err := sink.WriteEffect(context.Background(), claim, EffectBatch{Destination: "wrong"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong destination error=%v", err)
	}
	if _, err := sink.InspectEffect(context.Background(), claim, EffectBatch{Destination: "operational_incidents"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid readback error=%v", err)
	}
}
