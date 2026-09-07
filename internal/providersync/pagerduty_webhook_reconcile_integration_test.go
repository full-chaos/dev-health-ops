//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The unit golden test proves the native reconciler BUILDS the rows Python
// wrote. This proves ClickHouse ACCEPTS them: the frozen goldens are replayed
// against the production migration chain, and every write is confirmed by the
// sinks' own readback comparison rather than by a count.
//
// EffectExact is the strong assertion available here. It re-reads the row by
// id from the migrated table and compares it field-by-field against what was
// sent, so a column that silently truncated, coerced, or landed in the wrong
// position fails -- which a SELECT count() would not catch. It also covers the
// deployed ordering contract: the sinks write the contract-1 column list, and
// the readback re-derives the ordering fields rather than reading columns the
// deployed schema does not have.
//
// operational_incident_responders is the reason this test exists at all. It is
// the one destination with no pull-sync producer, so it is the one whose
// column list, values order and scan targets have never been exercised against
// a real ClickHouse table by any other test.
func TestPagerDutyWebhookGoldensCommitToMigratedClickHouse(t *testing.T) {
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
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	incidentFamily := PagerDutyIncidentFamilyClickHouseEffects{
		Conn: conn, Lease: lease, ProviderInstanceID: "acme",
		Entitlement: allowIncidentEntitlement,
	}
	services := PagerDutyServicesClickHouseEffects{
		Conn: conn, Lease: lease, ProviderInstanceID: "acme",
		Entitlement: allowIncidentEntitlement,
	}
	users := PagerDutyUsersClickHouseEffects{
		Conn: conn, Lease: lease, ProviderInstanceID: "acme",
		Entitlement: allowIncidentEntitlement,
	}
	responders := PagerDutyWebhookRespondersClickHouseEffects{
		Conn: conn, Lease: lease, ProviderInstanceID: "acme",
		Entitlement: allowIncidentEntitlement,
	}

	goldens := loadPagerDutyGoldens(t)
	if len(goldens) != 18 {
		t.Fatalf("golden count = %d, want 18", len(goldens))
	}
	// Every incident-family golden describes the SAME incident (PINC1) and
	// every service golden the same service, because that is how the Python
	// fixtures were captured: one entity observed through seventeen event
	// types. The canonical id depends only on (org, provider, instance,
	// family, external_id), so replaying them into one table would have each
	// case land on the row the previous case wrote and race it on version.
	// Truncating between cases keeps each golden an independent observation,
	// which is what the goldens actually assert.
	tables := []string{
		"operational_incidents", "operational_incident_notes",
		"operational_incident_timeline_events", "operational_incident_responders",
		"operational_services", "operational_users",
	}
	covered := map[string]bool{}
	for _, golden := range goldens {
		t.Run(golden.Case, func(t *testing.T) {
			for _, table := range tables {
				if err := conn.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
					t.Fatal(err)
				}
			}
			committed := []committedWebhookEffect{}
			sinks := PagerDutyWebhookSinks{
				IncidentFamily: &inspectingWebhookSink{sink: incidentFamily, committed: &committed},
				Services:       &inspectingWebhookSink{sink: services, committed: &committed},
				Users:          &inspectingWebhookSink{sink: users, committed: &committed},
				Responders:     &inspectingWebhookSink{sink: responders, committed: &committed},
			}
			outcome, err := ReconcilePagerDutyWebhook(
				ctx, pagerDutyWebhookTestClaim(golden.OrgID), golden.ProviderInstanceID,
				pagerDutyGoldenEvent(t, golden), sinks,
				&stubIncidentHydrator{body: json.RawMessage(`{
					"id": "PINC9",
					"type": "incident",
					"title": "Hydrated from the REST API",
					"status": "triggered",
					"created_at": "2026-07-17T11:59:00Z",
					"html_url": "https://acme.pagerduty.com/incidents/PINC9"
				}`)},
			)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if len(committed) != len(golden.Rows) || outcome.Rows() != len(golden.Rows) {
				t.Fatalf("committed %d effect(s), outcome %d, golden %d",
					len(committed), outcome.Rows(), len(golden.Rows))
			}
			for index, write := range committed {
				covered[write.effect.Destination] = true
				if write.effect.Destination != golden.Rows[index].Table {
					t.Fatalf("write %d destination = %q, golden = %q",
						index, write.effect.Destination, golden.Rows[index].Table)
				}
				inspection, err := write.inspect(ctx)
				if err != nil || inspection != EffectExact {
					t.Fatalf("write %d (%s) readback = %s, error = %v",
						index, write.effect.Destination, inspection, err)
				}
			}
		})
	}
	// A golden set that stopped exercising a destination would leave this test
	// silently proving less than its name claims.
	for _, destination := range tables {
		if !covered[destination] {
			t.Fatalf("no golden committed to %s", destination)
		}
	}
}

type committedWebhookEffect struct {
	effect  EffectBatch
	inspect func(context.Context) (EffectInspection, error)
}

// inspectingWebhookSink writes through the real sink and remembers how to ask
// that same sink to read the row back, so the assertion is the production
// readback path rather than a hand-written SELECT that could disagree with it.
type inspectingWebhookSink struct {
	sink interface {
		WriteEffect(context.Context, Claim, EffectBatch) error
		InspectEffect(context.Context, Claim, EffectBatch) (EffectInspection, error)
	}
	committed *[]committedWebhookEffect
}

func (recorder *inspectingWebhookSink) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	// Absent before the write is what makes EffectExact after it meaningful:
	// without it a row left behind by an earlier case could pass the readback
	// while this write did nothing.
	if inspection, err := recorder.sink.InspectEffect(ctx, claim, effect); err != nil {
		return err
	} else if inspection != EffectAbsent {
		return errPagerDutyWebhookRowAlreadyPresent
	}
	if err := recorder.sink.WriteEffect(ctx, claim, effect); err != nil {
		return err
	}
	*recorder.committed = append(*recorder.committed, committedWebhookEffect{
		effect: effect,
		inspect: func(ctx context.Context) (EffectInspection, error) {
			return recorder.sink.InspectEffect(ctx, claim, effect)
		},
	})
	return nil
}

var errPagerDutyWebhookRowAlreadyPresent = errors.New(
	"a row for this golden was already in ClickHouse before the write",
)
