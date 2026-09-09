package syncreconciler

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

const testUnitID = "e784daf9-592c-5fca-b6f7-81d2c9de940e"

func TestNextProviderUnitReclaimKeyWalksTheGeneration(t *testing.T) {
	base := jobcontract.KindSyncProviderUnit + ":" + testUnitID
	for _, testCase := range []struct {
		name string
		key  string
		want string
	}{
		{"base key is generation zero", base, base + "/reclaim/1"},
		{"first reclaim advances", base + "/reclaim/1", base + "/reclaim/2"},
		{"second reclaim advances", base + "/reclaim/2", base + "/reclaim/3"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := nextProviderUnitReclaimKey(testCase.key, testUnitID)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("got %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestNextProviderUnitReclaimKeyStopsAtTheBound(t *testing.T) {
	base := jobcontract.KindSyncProviderUnit + ":" + testUnitID
	spent := base + "/reclaim/" + strconv.Itoa(maxProviderUnitReclaims)
	if _, err := nextProviderUnitReclaimKey(spent, testUnitID); !errors.Is(err, ErrReclaimBudgetSpent) {
		t.Fatalf("generation %d: err = %v, want ErrReclaimBudgetSpent -- an unbounded rearm "+
			"loop is the failure this bound exists to prevent", maxProviderUnitReclaims, err)
	}
}

// A key that names a DIFFERENT unit must never yield a generation of THIS one.
// Deriving n+1 from it would mint a delivery whose envelope points at another
// domain row -- the one mistake a repair cannot recover from, because the
// resulting job looks entirely well-formed.
func TestNextProviderUnitReclaimKeyRefusesForeignAndMalformedKeys(t *testing.T) {
	base := jobcontract.KindSyncProviderUnit + ":" + testUnitID
	otherUnit := "e784daf9-592c-5fca-b6f7-81d2c9de9401"
	for _, testCase := range []struct {
		name string
		key  string
	}{
		{"another unit's base key", jobcontract.KindSyncProviderUnit + ":" + otherUnit},
		{"another unit's reclaim key", jobcontract.KindSyncProviderUnit + ":" + otherUnit + "/reclaim/1"},
		{"a different job kind", "metrics.daily_finalize:" + testUnitID},
		{"a non-numeric generation", base + "/reclaim/one"},
		{"a zero generation", base + "/reclaim/0"},
		{"a negative generation", base + "/reclaim/-1"},
		{"a trailing separator", base + "/reclaim/"},
		{"an unrelated suffix", base + "-retry"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := nextProviderUnitReclaimKey(testCase.key, testUnitID)
			if err == nil {
				t.Fatalf("accepted %q and produced %q", testCase.key, got)
			}
			if errors.Is(err, ErrReclaimBudgetSpent) {
				t.Fatalf("%q reported a spent budget; it is malformed, not exhausted, and the "+
					"two refusals are counted separately on purpose", testCase.key)
			}
		})
	}
}

// The reclaim infix has to survive jobcontract's own identifier rule, because
// the replacement key becomes an envelope idempotency_key. safeIDPattern is
// ^[A-Za-z0-9][A-Za-z0-9._:/-]*$ -- '/' is admitted, '#' (the first separator
// this reached for) is not, and a key jobcontract rejects would be accepted
// here and refused by the relay forever.
func TestProviderUnitReclaimKeyIsALegalEnvelopeIdentifier(t *testing.T) {
	base := jobcontract.KindSyncProviderUnit + ":" + testUnitID
	key, err := nextProviderUnitReclaimKey(base, testUnitID)
	if err != nil {
		t.Fatal(err)
	}
	organization := "70d529e0-3c06-4597-8480-794fd02328b6"
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organization,
		CorrelationID:   "sync-run:1410329c-51aa-5895-89c7-b36442da361e",
		IdempotencyKey:  key,
		Domain:          jobcontract.DomainLink{Type: "sync_run_unit", ID: testUnitID},
		Payload:         jobcontract.ProviderUnitPayload{UnitID: testUnitID},
	}
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatalf("jobcontract refuses the reclaim key %q: %v", key, err)
	}
	decoded, err := jobcontract.Decode(jobcontract.KindSyncProviderUnit, encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.IdempotencyKey != key {
		t.Fatalf("idempotency key did not round-trip: %q -> %q", key, decoded.IdempotencyKey)
	}
	// 256 is the dedupe_key column's width and jobcontract's own bound.
	if len(key) > 256 {
		t.Fatalf("reclaim key is %d bytes, past the dedupe_key column's 256", len(key))
	}
}

func TestReclaimEnvelopeRebindsOnlyTheIdempotencyKey(t *testing.T) {
	organization := "70d529e0-3c06-4597-8480-794fd02328b6"
	base := jobcontract.KindSyncProviderUnit + ":" + testUnitID
	source := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organization,
		CorrelationID:   "sync-run:1410329c-51aa-5895-89c7-b36442da361e",
		IdempotencyKey:  base,
		TraceParent:     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Domain:          jobcontract.DomainLink{Type: "sync_run_unit", ID: testUnitID},
		Payload:         jobcontract.ProviderUnitPayload{UnitID: testUnitID},
	}
	encoded, err := jobcontract.MarshalCanonical(source)
	if err != nil {
		t.Fatal(err)
	}
	replacement, hash, err := reclaimEnvelope(string(encoded), base+"/reclaim/1")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := jobcontract.Decode(jobcontract.KindSyncProviderUnit, []byte(replacement))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.IdempotencyKey != base+"/reclaim/1" {
		t.Fatalf("idempotency key = %q", decoded.IdempotencyKey)
	}
	// Everything else is the SAME delivery, for the same unit, in the same
	// trace. A replacement that dropped the traceparent would silently detach
	// the recovery from the investigation that produced it.
	if decoded.TraceParent != source.TraceParent {
		t.Errorf("trace_parent = %q, want %q", decoded.TraceParent, source.TraceParent)
	}
	if decoded.CorrelationID != source.CorrelationID {
		t.Errorf("correlation_id = %q, want %q", decoded.CorrelationID, source.CorrelationID)
	}
	if decoded.Domain != source.Domain {
		t.Errorf("domain = %+v, want %+v", decoded.Domain, source.Domain)
	}
	if decoded.OrganizationID == nil || *decoded.OrganizationID != organization {
		t.Errorf("organization_id = %v, want %q", decoded.OrganizationID, organization)
	}
	if !strings.HasPrefix(hash, "sha256:") || len(hash) != 71 {
		t.Errorf("payload hash %q does not satisfy ck_worker_job_outbox_payload_hash", hash)
	}
}

func TestReclaimEnvelopeRefusesArgsItCannotDecode(t *testing.T) {
	if _, _, err := reclaimEnvelope(`{"not":"an envelope"}`, "sync.provider_unit:x/reclaim/1"); err == nil {
		t.Fatal("accepted args jobcontract cannot decode; the relay would refuse the resulting " +
			"row forever, which is a strand replacing a strand")
	}
}

func TestNewOrphanedUnitRepairRefusesIncompleteConfiguration(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		schema string
	}{
		// Every arm passes nil pools, so each also proves the nil-pool
		// refusal; the schema arms additionally prove the identifier guard is
		// evaluated rather than short-circuited past.
		{"nil pools with a valid schema", "river"},
		{"empty river schema", ""},
		{"river schema with a quote", `river"; DROP TABLE x --`},
		{"river schema with a dash", "river-schema"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repair, err := NewOrphanedUnitRepair(nil, nil, testCase.schema)
			if !errors.Is(err, ErrInvalidConfiguration) || repair != nil {
				t.Fatalf("repair=%v err=%v, want ErrInvalidConfiguration", repair, err)
			}
		})
	}
}

// A zero-value repair must refuse rather than panic: the pipeline holds this as
// an interface, and a nil implementation reaching Step is the shape CHAOS-4035
// shipped one level up.
func TestOrphanedUnitRepairStepRefusesInvalidCalls(t *testing.T) {
	var repair *OrphanedUnitRepair
	if _, err := repair.Step(context.Background(), time.Now(), 10); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil repair: err = %v, want ErrInvalidConfiguration", err)
	}
	unwired := &OrphanedUnitRepair{}
	if _, err := unwired.Step(context.Background(), time.Now(), 10); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("unwired repair: err = %v, want ErrInvalidConfiguration", err)
	}
}

// The survey and the FOR UPDATE re-read must state the domain rule ONCE. Two
// hand-kept copies of a predicate mask each other's mutations -- neither can be
// pinned, because the other still refuses the row -- which is exactly what
// 5456's guard matrix found and collapsed.
func TestOrphanedUnitDomainPredicateIsSharedNotRestated(t *testing.T) {
	for _, query := range []struct {
		name string
		sql  string
	}{
		{"survey", selectOrphanedUnitDeliverySQL},
		{"lock", lockOrphanedUnitSQL},
	} {
		if !strings.Contains(query.sql, orphanedUnitDomainPredicate) {
			t.Errorf("%s does not embed orphanedUnitDomainPredicate verbatim; a restated copy "+
				"drifts silently and cannot be mutation-tested", query.name)
		}
	}
	if !strings.Contains(orphanedUnitDomainPredicate, orphanedUnitIdlePredicate) {
		t.Error("the domain predicate no longer embeds the idle predicate")
	}
	if !strings.Contains(orphanedUnitDomainPredicate, nonterminalSyncRunStatusPredicate) {
		t.Error("the domain predicate no longer embeds nonterminalSyncRunStatusPredicate; " +
			"a second definition of 'this run is not finished' is a second thing to keep in sync")
	}
	// The idle gate must not be updated_at. See CHAOS-5453 remedy 3: the
	// redispatch loop bumps updated_at on every no-op pass, so an updated_at
	// gate never opens.
	if strings.Contains(orphanedUnitIdlePredicate, "updated_at") {
		t.Error("the idle gate reads updated_at; the false-success redispatch loop keeps that " +
			"column fresh on exactly this population, so the gate would never fire")
	}
}
