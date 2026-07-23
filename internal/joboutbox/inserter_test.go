package joboutbox

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/riverqueue/river/rivertype"
)

type staticRegistry struct {
	descriptors map[string]jobruntime.Descriptor
}

func (registry staticRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := registry.descriptors[kind]
	return descriptor, ok
}

func testDescriptor() jobruntime.Descriptor {
	return jobruntime.Descriptor{
		Kind:              jobcontract.KindHeartbeat,
		CurrentVersion:    1,
		SupportedVersions: []int{1},
		Queue:             "heartbeat",
		Priority:          2,
		MaxAttempts:       1,
		Route:             "river",
	}
}

func testRow(t *testing.T) Row {
	t.Helper()
	envelope := jobcontract.Envelope{
		ContractVersion: 1,
		CorrelationID:   "relay-test-1",
		IdempotencyKey:  "heartbeat:2026-07-21T12:00:00Z",
		Domain: jobcontract.DomainLink{
			Type: "schedule_occurrence",
			ID:   "00000000-0000-4000-8000-000000000001",
		},
		Payload: jobcontract.HeartbeatPayload{ScheduledFor: "2026-07-21T12:00:00Z"},
	}
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatal(err)
	}
	claimedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	expiresAt := claimedAt.Add(time.Minute)
	return Row{
		ID:              "00000000-0000-4000-8000-000000000010",
		DedupeKey:       envelope.IdempotencyKey,
		JobKind:         jobcontract.KindHeartbeat,
		ContractVersion: 1,
		Args:            encoded,
		PayloadHash:     canonicalHash(encoded),
		Queue:           "heartbeat",
		Priority:        2,
		MaxAttempts:     1,
		ScheduledAt:     claimedAt,
		Status:          statusClaimed,
		ClaimToken:      "00000000-0000-4000-8000-000000000011",
		ClaimedAt:       &claimedAt,
		ClaimExpiresAt:  &expiresAt,
		AttemptCount:    1,
		NextAttemptAt:   claimedAt,
		CreatedAt:       claimedAt,
		UpdatedAt:       claimedAt,
	}
}

func TestPrepareRowRejectsUnknownVersionPolicyAndHashWithoutValues(t *testing.T) {
	registry := staticRegistry{descriptors: map[string]jobruntime.Descriptor{
		jobcontract.KindHeartbeat: testDescriptor(),
	}}
	tests := []struct {
		name   string
		mutate func(*Row)
		want   error
	}{
		{name: "unknown kind", mutate: func(row *Row) { row.JobKind = "secret.unknown" }, want: ErrContractRejected},
		{name: "unknown version", mutate: func(row *Row) { row.ContractVersion = 99 }, want: ErrContractRejected},
		{name: "hash mismatch", mutate: func(row *Row) { row.PayloadHash = "sha256:" + strings.Repeat("0", 64) }, want: ErrContractRejected},
		{name: "dedupe mismatch", mutate: func(row *Row) { row.DedupeKey = "different:key" }, want: ErrContractRejected},
		{name: "queue drift", mutate: func(row *Row) { row.Queue = "credential-secret" }, want: ErrPolicyRejected},
		{name: "priority drift", mutate: func(row *Row) { row.Priority = 4 }, want: ErrPolicyRejected},
		{name: "attempt drift", mutate: func(row *Row) { row.MaxAttempts = 2 }, want: ErrPolicyRejected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := testRow(t)
			test.mutate(&row)
			_, _, err := prepareRow(registry, row)
			if !errors.Is(err, test.want) {
				t.Fatalf("prepareRow() error = %v, want %v", err, test.want)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), row.DedupeKey) {
				t.Fatalf("error leaked stored values: %v", err)
			}
		})
	}
}

func TestPrepareRowEmitsCanonicalArgsWithUniqueIdempotencyOnly(t *testing.T) {
	row := testRow(t)
	registry := staticRegistry{descriptors: map[string]jobruntime.Descriptor{row.JobKind: testDescriptor()}}
	descriptor, args, err := prepareRow(registry, row)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Queue != row.Queue || args.Kind() != row.JobKind || args.IdempotencyKey != row.DedupeKey {
		t.Fatalf("unexpected prepared policy/args: %#v %#v", descriptor, args)
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "worker_outbox_id") {
		t.Fatal("outbox metadata leaked into encoded_args")
	}
}

func TestPrepareRowRejectsNonExecutableMigrationRoutes(t *testing.T) {
	for _, route := range []string{"celery", "coexistence_disabled", ""} {
		t.Run(route, func(t *testing.T) {
			row := testRow(t)
			descriptor := testDescriptor()
			descriptor.Route = route
			registry := staticRegistry{descriptors: map[string]jobruntime.Descriptor{row.JobKind: descriptor}}

			_, _, err := prepareRow(registry, row)
			if !errors.Is(err, ErrPolicyRejected) {
				t.Fatalf("prepareRow() error = %v, want %v", err, ErrPolicyRejected)
			}
			if err.Error() != ErrPolicyRejected.Error() {
				t.Fatalf("prepareRow() returned an unbounded policy error: %v", err)
			}
		})
	}
}

func TestPrepareRowAcceptsExecutableMigrationRoutes(t *testing.T) {
	for _, route := range []string{"shadow", "river_canary", "river"} {
		t.Run(route, func(t *testing.T) {
			row := testRow(t)
			descriptor := testDescriptor()
			descriptor.Route = route
			registry := staticRegistry{descriptors: map[string]jobruntime.Descriptor{row.JobKind: descriptor}}

			if _, _, err := prepareRow(registry, row); err != nil {
				t.Fatalf("prepareRow() error = %v", err)
			}
		})
	}
}

func TestPrepareRowRelaysProviderUnitCanaryAsIDOnlyTenantEnvelope(t *testing.T) {
	organizationID := "00000000-0000-4000-8000-000000000024"
	unitID := "00000000-0000-4000-8000-000000000021"
	envelope := jobcontract.Envelope{
		ContractVersion: 1,
		OrganizationID:  &organizationID,
		CorrelationID:   "sync-run:00000000-0000-4000-8000-000000000020",
		IdempotencyKey:  "sync.provider_unit:" + unitID,
		Domain: jobcontract.DomainLink{
			Type: "sync_run_unit",
			ID:   unitID,
		},
		Payload: jobcontract.ProviderUnitPayload{UnitID: unitID},
	}
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := Row{
		ID:              "00000000-0000-4000-8000-000000000022",
		DedupeKey:       envelope.IdempotencyKey,
		JobKind:         jobcontract.KindSyncProviderUnit,
		ContractVersion: 1,
		Args:            encoded,
		PayloadHash:     canonicalHash(encoded),
		Queue:           "sync",
		Priority:        2,
		MaxAttempts:     5,
		ScheduledAt:     now,
		Status:          statusClaimed,
		ClaimToken:      "00000000-0000-4000-8000-000000000023",
		AttemptCount:    1,
		NextAttemptAt:   now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	descriptor := jobruntime.Descriptor{
		Kind:              jobcontract.KindSyncProviderUnit,
		CurrentVersion:    1,
		SupportedVersions: []int{1},
		Queue:             "sync",
		Priority:          2,
		MaxAttempts:       5,
		Route:             "river_canary",
	}
	registry := staticRegistry{descriptors: map[string]jobruntime.Descriptor{
		row.JobKind: descriptor,
	}}

	preparedDescriptor, args, err := prepareRow(registry, row)
	if err != nil {
		t.Fatal(err)
	}
	if preparedDescriptor.Route != "river_canary" || args.Kind() != row.JobKind ||
		args.OrganizationID == nil || *args.OrganizationID != organizationID ||
		args.Domain.Type != "sync_run_unit" || args.Domain.ID != unitID {
		t.Fatalf("unexpected provider-unit relay: descriptor=%+v args=%+v", preparedDescriptor, args)
	}
	var payload map[string]string
	if err := json.Unmarshal(args.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) != 1 || payload["unit_id"] != unitID {
		t.Fatalf("provider-unit payload is not ID-only: %#v", payload)
	}
	relayed, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"credential", "callable", "config"} {
		if strings.Contains(strings.ToLower(string(relayed)), forbidden) {
			t.Fatalf("provider-unit relay leaked forbidden field %q", forbidden)
		}
	}
	canonical, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalHash(canonical) != row.PayloadHash {
		t.Fatalf("provider-unit relay hash drifted: got=%s want=%s", canonicalHash(canonical), row.PayloadHash)
	}
}

func TestVerifyInsertResultRejectsDuplicateIdentityMismatch(t *testing.T) {
	row := testRow(t)
	metadata, err := json.Marshal(relayMetadata{
		WorkerOutboxID:  "00000000-0000-4000-8000-000000000099",
		PayloadHash:     row.PayloadHash,
		ContractVersion: row.ContractVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := &rivertype.JobInsertResult{
		UniqueSkippedAsDuplicate: true,
		Job: &rivertype.JobRow{
			ID:          42,
			Kind:        row.JobKind,
			Queue:       row.Queue,
			Priority:    row.Priority,
			MaxAttempts: row.MaxAttempts,
			EncodedArgs: row.Args,
			Metadata:    metadata,
		},
	}
	if err := verifyInsertResult(result, row, testDescriptor()); !errors.Is(err, ErrContractRejected) {
		t.Fatalf("verifyInsertResult() error = %v", err)
	}
}

func TestFailureEvidenceIsBoundedAndValueFree(t *testing.T) {
	for _, kind := range []failureKind{failureContract, failurePolicy, failureRiver} {
		code, detail, _ := failureEvidence(kind)
		if len(code) > 64 || len(detail) > 256 || strings.Contains(detail, "postgres://") {
			t.Fatalf("unsafe failure evidence: %q %q", code, detail)
		}
	}
}

func TestRelayBackoffIsBounded(t *testing.T) {
	relay := &Relay{config: DefaultRelayConfig()}
	if got := relay.backoff(1); got != 5*time.Second {
		t.Fatalf("attempt 1 backoff = %s", got)
	}
	if got := relay.backoff(100); got != 5*time.Minute {
		t.Fatalf("bounded backoff = %s", got)
	}
}
