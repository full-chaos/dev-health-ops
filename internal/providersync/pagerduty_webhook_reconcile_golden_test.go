package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// The frozen Python webhook path. testdata/pagerduty_webhook_goldens/*.json
// were captured from reconcile_pagerduty_webhook BEFORE it was deleted (see
// that directory's capture.py); this test is the reason they exist. It fails
// if the native reconciler writes a different table, a different number of
// rows, or a single different column value for any of the eighteen cases.
//
// The goldens carry the DEPLOYED column contract (OPERATIONAL_ORDERING_CONTRACT
// = 1), so the four ordering columns are absent from them exactly as they are
// absent from the deployed tables. Their internal consistency is enforced
// separately, by the sinks' validate*Rows re-derivation.

var pagerDutyGoldenOrderingColumns = map[string]bool{
	"source_revision": true, "source_conflict_key": true,
	"ingest_revision": true, "ordering_contract": true,
}

type pagerDutyGoldenRow struct {
	Table   string                     `json:"table"`
	Columns map[string]json.RawMessage `json:"columns"`
}

type pagerDutyGolden struct {
	Case               string               `json:"case"`
	EventType          string               `json:"event_type"`
	EventID            string               `json:"event_id"`
	OrgID              string               `json:"org_id"`
	ProviderInstanceID string               `json:"provider_instance_id"`
	OccurredAt         time.Time            `json:"occurred_at"`
	ReceivedAt         time.Time            `json:"received_at"`
	HydratedViaREST    bool                 `json:"hydrated_via_rest"`
	Payload            json.RawMessage      `json:"payload"`
	Rows               []pagerDutyGoldenRow `json:"rows"`
}

// recordingWebhookSink keeps every committed batch in call order so the test
// asserts write ORDER too: Python appends the user row before the responder
// row that references it, and a reader following user_id depends on that.
type recordingWebhookSink struct {
	written *[]pagerDutyGoldenWrite
	failOn  string
	err     error
}

type pagerDutyGoldenWrite struct {
	destination string
	dataset     string
	rows        []json.RawMessage
}

func (sink recordingWebhookSink) WriteEffect(
	_ context.Context, claim Claim, effect EffectBatch,
) error {
	if sink.failOn != "" && sink.failOn == effect.Destination {
		return sink.err
	}
	*sink.written = append(*sink.written, pagerDutyGoldenWrite{
		destination: effect.Destination, dataset: claim.Dataset, rows: effect.Rows,
	})
	return nil
}

type stubIncidentHydrator struct {
	calls []string
	body  json.RawMessage
	err   error
}

func (hydrator *stubIncidentHydrator) HydrateIncident(
	_ context.Context, incidentID string,
) (json.RawMessage, error) {
	hydrator.calls = append(hydrator.calls, incidentID)
	if hydrator.err != nil {
		return nil, hydrator.err
	}
	return hydrator.body, nil
}

// pagerDutyWebhookTestClaim is the synthetic claim internal/jobs/pagerduty
// mints from the locked binding graph: real integration/source/credential
// ids, a deterministic unit id derived from the receipt, and a lease the
// receipt store fences.
func pagerDutyWebhookTestClaim(orgID string) Claim {
	return Claim{
		Unit: Unit{
			ID:               "11111111-1111-4111-8111-111111111111",
			SyncRunID:        "22222222-2222-4222-8222-222222222222",
			OrgID:            orgID,
			IntegrationID:    "33333333-3333-4333-8333-333333333333",
			SourceID:         "44444444-4444-4444-8444-444444444444",
			SourceExternalID: "acme",
			SourceName:       "acme",
			Provider:         "pagerduty",
			Mode:             "incremental",
			CredentialID:     "55555555-5555-4555-8555-555555555555",
			AuthSource:       "integration_credential",
		},
		Owner:          "66666666-6666-4666-8666-666666666666",
		Attempt:        1,
		LeaseExpiresAt: time.Date(2026, 7, 17, 13, 0, 0, 0, time.UTC),
	}
}

func loadPagerDutyGoldens(t *testing.T) []pagerDutyGolden {
	t.Helper()
	directory := filepath.Join("testdata", "pagerduty_webhook_goldens")
	entries, err := filepath.Glob(filepath.Join(directory, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatalf("no goldens in %s", directory)
	}
	sort.Strings(entries)
	goldens := make([]pagerDutyGolden, 0, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(entry)
		if err != nil {
			t.Fatal(err)
		}
		var golden pagerDutyGolden
		if err := json.Unmarshal(data, &golden); err != nil {
			t.Fatalf("%s: %v", entry, err)
		}
		goldens = append(goldens, golden)
	}
	return goldens
}

func pagerDutyGoldenEvent(t *testing.T, golden pagerDutyGolden) PagerDutyWebhookEvent {
	t.Helper()
	var envelope struct {
		Event struct {
			Data json.RawMessage `json:"data"`
		} `json:"event"`
	}
	if err := json.Unmarshal(golden.Payload, &envelope); err != nil {
		t.Fatal(err)
	}
	return PagerDutyWebhookEvent{
		EventID:    golden.EventID,
		EventType:  golden.EventType,
		OccurredAt: golden.OccurredAt,
		ReceivedAt: golden.ReceivedAt,
		Data:       envelope.Event.Data,
	}
}

// pagerDutyGoldenComparable renders a written row as the deployed contract's
// column map, so the comparison is against what ClickHouse would actually
// store rather than against Go struct internals.
func pagerDutyGoldenComparable(t *testing.T, row json.RawMessage) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(row, &decoded); err != nil {
		t.Fatal(err)
	}
	comparable := make(map[string]any, len(decoded))
	for name, value := range decoded {
		if pagerDutyGoldenOrderingColumns[name] {
			continue
		}
		comparable[name] = pagerDutyGoldenScalar(value)
	}
	return comparable
}

// pagerDutyGoldenScalar collapses the two encodings the same instant can have
// on either side of the comparison ("2026-07-17T12:00:00Z" from Python,
// RFC3339Nano from Go's time.Time) onto one string.
func pagerDutyGoldenScalar(value any) any {
	text, ok := value.(string)
	if !ok {
		return value
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return text
	}
	return parsed.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
}

func TestPagerDutyWebhookReconcileMatchesFrozenPythonGoldens(t *testing.T) {
	goldens := loadPagerDutyGoldens(t)
	if len(goldens) != 18 {
		t.Fatalf("golden count = %d, want 18 (one per event type plus hydration and tombstone)", len(goldens))
	}
	for _, golden := range goldens {
		t.Run(golden.Case, func(t *testing.T) {
			written := []pagerDutyGoldenWrite{}
			sink := recordingWebhookSink{written: &written}
			hydrator := &stubIncidentHydrator{body: json.RawMessage(`{
				"id": "PINC9",
				"type": "incident",
				"title": "Hydrated from the REST API",
				"status": "triggered",
				"created_at": "2026-07-17T11:59:00Z",
				"html_url": "https://acme.pagerduty.com/incidents/PINC9"
			}`)}
			outcome, err := ReconcilePagerDutyWebhook(
				context.Background(),
				pagerDutyWebhookTestClaim(golden.OrgID),
				golden.ProviderInstanceID,
				pagerDutyGoldenEvent(t, golden),
				PagerDutyWebhookSinks{IncidentFamily: sink, Services: sink, Users: sink, Responders: sink},
				hydrator,
			)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if outcome.Hydrated != golden.HydratedViaREST {
				t.Fatalf("hydrated = %v, golden = %v", outcome.Hydrated, golden.HydratedViaREST)
			}
			if got, want := len(hydrator.calls) > 0, golden.HydratedViaREST; got != want {
				t.Fatalf("hydrator calls = %v, want called = %v", hydrator.calls, want)
			}
			if len(written) != len(golden.Rows) {
				t.Fatalf("wrote %d row(s), golden has %d", len(written), len(golden.Rows))
			}
			if outcome.Rows() != len(golden.Rows) {
				t.Fatalf("outcome.Rows() = %d, want %d", outcome.Rows(), len(golden.Rows))
			}
			for index, write := range written {
				expected := golden.Rows[index]
				if write.destination != expected.Table {
					t.Fatalf("write %d destination = %q, golden = %q", index, write.destination, expected.Table)
				}
				if len(write.rows) != 1 {
					t.Fatalf("write %d carried %d rows, want 1", index, len(write.rows))
				}
				actual := pagerDutyGoldenComparable(t, write.rows[0])
				for column, raw := range expected.Columns {
					var value any
					if err := json.Unmarshal(raw, &value); err != nil {
						t.Fatal(err)
					}
					want := pagerDutyGoldenScalar(value)
					got, present := actual[column]
					if !present {
						t.Fatalf("write %d (%s): column %q missing from the native row", index, expected.Table, column)
					}
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("write %d (%s) column %q = %v, golden = %v", index, expected.Table, column, got, want)
					}
				}
				for column := range actual {
					if _, present := expected.Columns[column]; !present {
						t.Fatalf("write %d (%s): native row has extra column %q", index, expected.Table, column)
					}
				}
			}
		})
	}
}

// Every case must reach a sink whose dataset the effect sinks accept. A
// dataset/destination pair the sink would reject at runtime is a defect the
// recording sink cannot see, so assert the pairing directly.
func TestPagerDutyWebhookReconcileUsesDatasetsTheSinksAccept(t *testing.T) {
	allowed := map[string]string{
		"operational_incidents":                "incidents",
		"operational_incident_responders":      "incidents",
		"operational_incident_notes":           "incident-notes",
		"operational_incident_timeline_events": "incident-log-entries",
		"operational_services":                 "services",
		"operational_users":                    "users",
	}
	for _, golden := range loadPagerDutyGoldens(t) {
		written := []pagerDutyGoldenWrite{}
		sink := recordingWebhookSink{written: &written}
		if _, err := ReconcilePagerDutyWebhook(
			context.Background(), pagerDutyWebhookTestClaim(golden.OrgID),
			golden.ProviderInstanceID, pagerDutyGoldenEvent(t, golden),
			PagerDutyWebhookSinks{IncidentFamily: sink, Services: sink, Users: sink, Responders: sink},
			&stubIncidentHydrator{body: json.RawMessage(`{"id":"PINC9","title":"t","status":"triggered","created_at":"2026-07-17T11:59:00Z"}`)},
		); err != nil {
			t.Fatalf("%s: %v", golden.Case, err)
		}
		for _, write := range written {
			want, known := allowed[write.destination]
			if !known {
				t.Fatalf("%s: unexpected destination %q", golden.Case, write.destination)
			}
			if write.dataset != want {
				t.Fatalf("%s: %s written under dataset %q, want %q",
					golden.Case, write.destination, write.dataset, want)
			}
		}
	}
}

func TestPagerDutyWebhookReconcileRejectsUnsupportedAndMalformedEvents(t *testing.T) {
	claim := pagerDutyWebhookTestClaim("org-4105")
	written := []pagerDutyGoldenWrite{}
	sinks := PagerDutyWebhookSinks{
		IncidentFamily: recordingWebhookSink{written: &written},
		Services:       recordingWebhookSink{written: &written},
		Users:          recordingWebhookSink{written: &written},
		Responders:     recordingWebhookSink{written: &written},
	}
	base := PagerDutyWebhookEvent{
		EventID:    "evt-1",
		OccurredAt: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 7, 17, 12, 0, 5, 0, time.UTC),
		Data:       json.RawMessage(`{"id":"PINC1","title":"t","status":"triggered","created_at":"2026-07-17T11:58:00Z"}`),
	}

	// pagey.ping is a real PagerDuty event type the Python reconciler never
	// handled: its match had no arm for it, so it raised. Terminal here too.
	unsupported := base
	unsupported.EventType = "pagey.ping"
	if _, err := ReconcilePagerDutyWebhook(
		context.Background(), claim, "acme", unsupported, sinks, nil,
	); !errors.Is(err, ErrPagerDutyWebhookUnsupported) {
		t.Fatalf("unsupported event error = %v", err)
	}

	// An incident with no id cannot be normalized and cannot be hydrated
	// into one either.
	malformed := base
	malformed.EventType = "incident.triggered"
	malformed.Data = json.RawMessage(`{"title":"t","status":"triggered","created_at":"2026-07-17T11:58:00Z"}`)
	if _, err := ReconcilePagerDutyWebhook(
		context.Background(), claim, "acme", malformed, sinks, nil,
	); !errors.Is(err, ErrPagerDutyWebhookMalformed) {
		t.Fatalf("malformed event error = %v", err)
	}

	// A missing provider instance is a configuration fault, not a payload
	// fault, but it must still refuse rather than write an unscoped row.
	if _, err := ReconcilePagerDutyWebhook(
		context.Background(), claim, "  ", base, sinks, nil,
	); !errors.Is(err, ErrPagerDutyWebhookMalformed) {
		t.Fatalf("blank provider instance error = %v", err)
	}

	if len(written) != 0 {
		t.Fatalf("a rejected event wrote %d row(s)", len(written))
	}
}

// A hydration failure is the one incident-path error that MUST stay
// retryable: the REST call can succeed on the next delivery, and classifying
// it as malformed would dead-letter a recoverable event.
func TestPagerDutyWebhookReconcileKeepsHydrationFailureRetryable(t *testing.T) {
	written := []pagerDutyGoldenWrite{}
	sink := recordingWebhookSink{written: &written}
	rateLimited := errors.New("pagerduty rate limited")
	_, err := ReconcilePagerDutyWebhook(
		context.Background(), pagerDutyWebhookTestClaim("org-4105"), "acme",
		PagerDutyWebhookEvent{
			EventID: "evt-1", EventType: "incident.triggered",
			OccurredAt: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
			ReceivedAt: time.Date(2026, 7, 17, 12, 0, 5, 0, time.UTC),
			Data:       json.RawMessage(`{"id":"PINC9"}`),
		},
		PagerDutyWebhookSinks{IncidentFamily: sink, Services: sink, Users: sink, Responders: sink},
		&stubIncidentHydrator{err: rateLimited},
	)
	if !errors.Is(err, rateLimited) {
		t.Fatalf("hydration failure error = %v", err)
	}
	if errors.Is(err, ErrPagerDutyWebhookMalformed) || errors.Is(err, ErrPagerDutyWebhookUnsupported) {
		t.Fatalf("hydration failure was classified terminal: %v", err)
	}
	if len(written) != 0 {
		t.Fatalf("a failed hydration wrote %d row(s)", len(written))
	}
}

// A responder event whose user row fails to commit must not go on to write
// the responder row that references it: a responder pointing at a user_id
// with no user row is exactly the dangling reference the ordered write
// prevents.
func TestPagerDutyWebhookReconcileStopsAfterAFailedDependencyWrite(t *testing.T) {
	written := []pagerDutyGoldenWrite{}
	userFailure := errors.New("clickhouse unavailable")
	sinks := PagerDutyWebhookSinks{
		IncidentFamily: recordingWebhookSink{written: &written},
		Services:       recordingWebhookSink{written: &written},
		Responders:     recordingWebhookSink{written: &written},
		Users: recordingWebhookSink{
			written: &written, failOn: "operational_users", err: userFailure,
		},
	}
	_, err := ReconcilePagerDutyWebhook(
		context.Background(), pagerDutyWebhookTestClaim("org-4105"), "acme",
		PagerDutyWebhookEvent{
			EventID: "evt-1", EventType: "incident.responder.added",
			OccurredAt: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
			ReceivedAt: time.Date(2026, 7, 17, 12, 0, 5, 0, time.UTC),
			Data: json.RawMessage(`{"id":"PRESP1","name":"Ada","role":"responder",
				"user":{"id":"PUSER1","name":"Ada","email":"ada@example.com"},
				"incident":{"id":"PINC1","title":"t","status":"triggered","created_at":"2026-07-17T11:58:00Z"}}`),
		},
		sinks, nil,
	)
	if !errors.Is(err, userFailure) {
		t.Fatalf("error = %v, want the user write failure", err)
	}
	for _, write := range written {
		if write.destination == "operational_incident_responders" {
			t.Fatal("responder row was written after its user row failed")
		}
	}
}

// The responder row's identity comes from the WEBHOOK event id, not the
// responder assignment id -- Python's choice, preserved because changing it
// would re-key every row already in the table.
func TestPagerDutyResponderRowIsKeyedOnTheWebhookEventID(t *testing.T) {
	written := []pagerDutyGoldenWrite{}
	sink := recordingWebhookSink{written: &written}
	if _, err := ReconcilePagerDutyWebhook(
		context.Background(), pagerDutyWebhookTestClaim("org-4105"), "acme",
		PagerDutyWebhookEvent{
			EventID: "evt-responder-1", EventType: "incident.responder.replied",
			OccurredAt: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
			ReceivedAt: time.Date(2026, 7, 17, 12, 0, 5, 0, time.UTC),
			Data: json.RawMessage(`{"id":"PRESP7","name":"Grace","role":"observer",
				"incident":{"id":"PINC1","title":"t","status":"triggered","created_at":"2026-07-17T11:58:00Z"}}`),
		},
		PagerDutyWebhookSinks{IncidentFamily: sink, Services: sink, Users: sink, Responders: sink}, nil,
	); err != nil {
		t.Fatal(err)
	}
	var responder pagerDutyResponderRow
	found := false
	for _, write := range written {
		if write.destination != "operational_incident_responders" {
			continue
		}
		if err := json.Unmarshal(write.rows[0], &responder); err != nil {
			t.Fatal(err)
		}
		found = true
	}
	if !found {
		t.Fatal("no responder row was written")
	}
	if responder.ExternalID != "evt-responder-1" {
		t.Fatalf("external_id = %q, want the webhook event id", responder.ExternalID)
	}
	if responder.ResponderAssignmentID == nil || *responder.ResponderAssignmentID != "PRESP7" {
		t.Fatalf("responder_assignment_id = %v, want PRESP7", responder.ResponderAssignmentID)
	}
	if responder.UserID != nil {
		t.Fatalf("user_id = %v, want nil when the payload carries no user", responder.UserID)
	}
}

// The responder destination must satisfy the same tenant/ordering validation
// every other destination does, including the tamper check.
func TestPagerDutyResponderRowValidationMatchesTheFamilyContract(t *testing.T) {
	claim, _ := pagerDutyWebhookClaim(pagerDutyWebhookTestClaim("org-4105"), "incidents")
	occurredAt := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	row := pagerDutyResponderRow{
		OrgID: "org-4105", Provider: "pagerduty", ProviderInstanceID: "acme",
		SourceEntityType: "responder", ExternalID: "evt-1", SourceVersionAt: occurredAt,
		ObservedAt: occurredAt, LastSynced: occurredAt, IncidentID: "incident-id",
	}
	if err := fillPagerDutyResponderOrdering(&row); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyResponderRows(claim, "acme", []pagerDutyResponderRow{row}); err != nil {
		t.Fatal(err)
	}
	foreign := row
	foreign.OrgID = "org-other"
	if err := fillPagerDutyResponderOrdering(&foreign); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyResponderRows(claim, "acme", []pagerDutyResponderRow{foreign}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant error = %v", err)
	}
	tampered := row
	name := "tampered"
	tampered.ResponderName = &name
	if err := validatePagerDutyResponderRows(claim, "acme", []pagerDutyResponderRow{tampered}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("ordering tamper error = %v", err)
	}
	duplicate := []pagerDutyResponderRow{row, row}
	if err := validatePagerDutyResponderRows(claim, "acme", duplicate); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate id error = %v", err)
	}
}

// The deployed tables do not have the ordering columns (CH migration 067 has
// not run), so the responder column list must omit them exactly as its four
// siblings do -- the same property
// TestPagerDutyServicesReadbackDoesNotClaimV2ColumnsFromActiveLegacySchema
// pins for services.
func TestPagerDutyResponderColumnsOmitTheV2OrderingColumns(t *testing.T) {
	for _, column := range strings.Split(pagerDutyResponderColumns, ",") {
		if pagerDutyGoldenOrderingColumns[column] {
			t.Fatalf("responder columns claim %q, which the deployed schema does not have", column)
		}
	}
	columns := strings.Split(pagerDutyResponderColumns, ",")
	values := pagerDutyResponderValues(pagerDutyResponderRow{})
	if len(columns) != len(values) {
		t.Fatalf("responder columns = %d, values = %d", len(columns), len(values))
	}
	scans := pagerDutyResponderScanValues(&pagerDutyResponderRow{})
	if len(columns) != len(scans) {
		t.Fatalf("responder columns = %d, scan targets = %d", len(columns), len(scans))
	}
}

var _ = uuid.Nil

// The regression codex r1 found with a live probe. The first draft of this
// change widened PagerDutyIncidentFamilyClickHouseEffects to accept
// operational_incident_responders under the "incidents" dataset, which meant
// an ORDINARY PULL claim could write responder rows through the sink the pull
// route already holds. Nothing exploited it -- the pull route emits no
// responder effect -- but the sink's one-dataset-one-table invariant had been
// traded away for a webhook-only need.
//
// Responders now have their own sink type, so this test pins BOTH halves:
// the family sink refuses the destination outright, and the responder sink
// refuses everything else.
func TestPagerDutyIncidentFamilySinkStillRefusesResponderEffects(t *testing.T) {
	claim, err := pagerDutyWebhookClaim(pagerDutyWebhookTestClaim("org-4105"), "incidents")
	if err != nil {
		t.Fatal(err)
	}
	occurredAt := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	row := pagerDutyResponderRow{
		OrgID: "org-4105", Provider: "pagerduty", ProviderInstanceID: "acme",
		SourceEntityType: "responder", ExternalID: "evt-1", SourceVersionAt: occurredAt,
		ObservedAt: occurredAt, LastSynced: occurredAt, IncidentID: "incident-id",
	}
	if err := fillPagerDutyResponderOrdering(&row); err != nil {
		t.Fatal(err)
	}
	effect, err := effectBatchFromValues(
		"operational_incident_responders", EffectReadbackRequired, []pagerDutyResponderRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	family := PagerDutyIncidentFamilyClickHouseEffects{
		Conn: refusingClickHouseConn{t: t}, ProviderInstanceID: "acme",
		Entitlement: allowIncidentEntitlement,
		Lease:       providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if err := family.WriteEffect(context.Background(), claim, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("family sink accepted a responder effect: %v", err)
	}
	if _, err := family.InspectEffect(context.Background(), claim, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("family sink inspected a responder effect: %v", err)
	}

	// And the responder sink is not a general-purpose one: it takes its own
	// destination and nothing else.
	responders := PagerDutyWebhookRespondersClickHouseEffects{
		Conn: refusingClickHouseConn{t: t}, ProviderInstanceID: "acme",
		Entitlement: allowIncidentEntitlement,
		Lease:       providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	incidentEffect, err := effectBatchFromValues(
		"operational_incidents", EffectReadbackRequired, []pagerDutyIncidentRow{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := responders.WriteEffect(context.Background(), claim, incidentEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("responder sink accepted an incident effect: %v", err)
	}
	wrongDataset := claim
	wrongDataset.Dataset = "incident-notes"
	if err := responders.WriteEffect(context.Background(), wrongDataset, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("responder sink accepted the wrong dataset: %v", err)
	}
}

// refusingClickHouseConn fails the test if the sink ever reaches ClickHouse.
// A validation test that silently depended on a nil connection would pass for
// the wrong reason, so the rejection must happen before any statement.
type refusingClickHouseConn struct {
	driver.Conn
	t *testing.T
}

func (conn refusingClickHouseConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	conn.t.Fatal("sink reached ClickHouse for an effect it should have refused")
	return nil, nil
}
