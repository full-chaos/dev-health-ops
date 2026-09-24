package joboutbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// TestStoredArgsTextIsWhatPythonsJSONColumnWrites pins the exact text. The
// expectation was produced by CPython, not written from a reading of it:
//
//	json.dumps(json.loads(canonical))
//
// for a document with non-ASCII text (a BMP character and an astral one, so
// both \uXXXX forms), a float that Go's encoder would spell 2, an integer past
// int64, nested objects whose keys are not in alphabetical order, and empty
// containers.
func TestStoredArgsTextIsWhatPythonsJSONColumnWrites(t *testing.T) {
	canonical := `{"contract_version":1,"correlation_id":"café ☕ 😀","idempotency_key":"k","domain":{"type":"t","id":"i"},"payload":{"z":[1,2.0,{"b":null,"a":true}],"empty":{},"list":[],"n":-0.5,"big":12345678901234567890}}`
	want := `{"contract_version": 1, "correlation_id": "caf\u00e9 \u2615 \ud83d\ude00", "idempotency_key": "k", "domain": {"type": "t", "id": "i"}, "payload": {"z": [1, 2.0, {"b": null, "a": true}], "empty": {}, "list": [], "n": -0.5, "big": 12345678901234567890}}`
	got, err := StoredArgsText([]byte(canonical))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("StoredArgsText =\n %s\nwant (CPython json.dumps)\n %s", got, want)
	}
}

func TestStoredArgsTextRefusesTextThatIsNotJSON(t *testing.T) {
	if got, err := StoredArgsText([]byte(`{"a":`)); err == nil {
		t.Fatalf("StoredArgsText accepted a truncated document and returned %q", got)
	}
}

// heartbeatEnvelope is the envelope the row and producer tests share.
func heartbeatEnvelope() jobcontract.Envelope {
	return jobcontract.Envelope{
		ContractVersion: 1,
		CorrelationID:   "relay-test-1",
		IdempotencyKey:  "heartbeat:2026-07-21T12:00:00Z",
		Domain: jobcontract.DomainLink{
			Type: "schedule_occurrence",
			ID:   "00000000-0000-4000-8000-000000000001",
		},
		Payload: jobcontract.HeartbeatPayload{ScheduledFor: "2026-07-21T12:00:00Z"},
	}
}

// TestStoredArgsTextRoundTripsToTheCanonicalBytes is the property the relay
// depends on: it decodes args and re-canonicalizes before it compares
// payload_hash, so the stored text must decode back to exactly the bytes the
// hash was taken over.
func TestStoredArgsTextRoundTripsToTheCanonicalBytes(t *testing.T) {
	canonical, err := jobcontract.MarshalCanonical(heartbeatEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	stored, err := StoredArgsText(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, "\n") || string(canonical) == stored {
		t.Fatalf("stored text is not the one-line json.dumps form: %q", stored)
	}
	decoded, err := jobcontract.Decode(jobcontract.KindHeartbeat, []byte(stored))
	if err != nil {
		t.Fatal(err)
	}
	again, err := jobcontract.MarshalCanonical(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(canonical) {
		t.Fatalf("Decode(StoredArgsText(x)) does not canonicalize back to x:\n got  %s\n want %s", again, canonical)
	}
}

// capturedInsert holds what the producer handed the database.
type capturedInsert struct {
	pgx.Tx // only Exec is ever called; any other call is a bug and panics
	args   []any
}

func (tx *capturedInsert) Exec(_ context.Context, _ string, arguments ...any) (pgconn.CommandTag, error) {
	tx.args = arguments
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

// TestProducerStoresPythonJSONTextAndHashesTheCanonicalForm runs the real
// producer against a capturing transaction. args is Python's text; payload_hash
// is still sha256 of the canonical bytes; and the relay's own row check accepts
// a row built from exactly those two captured values.
func TestProducerStoresPythonJSONTextAndHashesTheCanonicalForm(t *testing.T) {
	registry := staticRegistry{descriptors: map[string]jobruntime.Descriptor{
		jobcontract.KindHeartbeat: testDescriptor(),
	}}
	producer, err := NewTransactionProducer(registry)
	if err != nil {
		t.Fatal(err)
	}
	tx := &capturedInsert{}
	if err := producer.Publish(context.Background(), tx, jobcontract.KindHeartbeat, heartbeatEnvelope()); err != nil {
		t.Fatal(err)
	}
	if len(tx.args) < 6 {
		t.Fatalf("insert carried %d arguments, want the row's columns", len(tx.args))
	}
	storedArgs, ok := tx.args[4].(string)
	if !ok {
		t.Fatalf("args argument is %T, want string", tx.args[4])
	}
	storedHash, ok := tx.args[5].(string)
	if !ok {
		t.Fatalf("payload_hash argument is %T, want string", tx.args[5])
	}

	want := `{"contract_version": 1, "correlation_id": "relay-test-1", "idempotency_key": "heartbeat:2026-07-21T12:00:00Z", "domain": {"type": "schedule_occurrence", "id": "00000000-0000-4000-8000-000000000001"}, "payload": {"scheduled_for": "2026-07-21T12:00:00Z"}}`
	if storedArgs != want {
		t.Fatalf("stored args =\n %s\nwant\n %s", storedArgs, want)
	}
	canonical, err := jobcontract.MarshalCanonical(heartbeatEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	if wantHash := "sha256:" + hex.EncodeToString(digest[:]); storedHash != wantHash {
		t.Fatalf("payload_hash = %s, want sha256 of the canonical bytes %s", storedHash, wantHash)
	}

	row := testRow(t)
	row.Args, row.PayloadHash = []byte(storedArgs), storedHash
	if _, _, err := prepareRow(registry, row); err != nil {
		t.Fatalf("the relay's row check refused a row built from the producer's own args and hash: %v", err)
	}
}
