package repoidentityprojection

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func corpusDir(t *testing.T) string {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "contracts/auth/v1/examples/repository-identity-event")
}

// rowOf is the outbox row a fixture describes: the envelope members the outbox has as columns, the payload as bytes.
func rowOf(t *testing.T, raw []byte) OutboxRow {
	t.Helper()
	var doc struct {
		AggregateID    string          `json:"aggregate_id"`
		EventType      string          `json:"event_type"`
		IdempotencyKey string          `json:"idempotency_key"`
		Payload        json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return OutboxRow{ID: 1, AggregateID: doc.AggregateID, EventType: doc.EventType, IdempotencyKey: doc.IdempotencyKey, Payload: doc.Payload, CreatedAt: time.Now()}
}

// The reader's decode agrees with the contract on every fixture whose verdict rests on an outbox column or the
// payload: it accepts every accept fixture and refuses every reject and client-reject fixture of those members.
// Envelope-only members (schema_version, aggregate_type, created_at) are columns the outbox fixes, not payload.
func TestEventFromRowAgreesWithTheContractCorpus(t *testing.T) {
	dir := corpusDir(t)
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Accept []struct {
			File string `json:"file"`
		} `json:"accept"`
		Reject []struct {
			File     string `json:"file"`
			Location string `json:"expect_instance_location"`
		} `json:"reject"`
		RejectByClient []struct {
			File string `json:"file"`
		} `json:"reject_by_client"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	read := func(name string) []byte {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	root, _ := moduleroot.Root()
	for _, a := range manifest.Accept {
		body := read(a.File)
		parsed, err := contracts.ParseRepositoryIdentityEvent(root, body)
		if err != nil {
			t.Fatalf("%s: contract: %v", a.File, err)
		}
		got, err := EventFromRow(rowOf(t, body))
		if err != nil {
			t.Errorf("%s: the reader refused an accept fixture: %v", a.File, err)
			continue
		}
		if got.Path != parsed.Payload.Change.Path || got.Revision != int64(parsed.Payload.Revision) || !got.ObservedAt.Equal(parsed.Payload.Change.ObservedAt.Time) {
			t.Errorf("%s: decoded %+v, contract %+v", a.File, got, parsed.Payload)
		}
	}
	checked := 0
	for _, r := range manifest.Reject {
		if r.Location == "/" || r.Location == "/schema_version" || r.Location == "/aggregate_type" || r.Location == "/created_at" {
			continue
		}
		checked++
		if _, err := EventFromRow(rowOf(t, read(r.File))); err == nil {
			t.Errorf("%s: the reader accepted a document the schema refuses at %s", r.File, r.Location)
		}
	}
	for _, r := range manifest.RejectByClient {
		if strings.Contains(r.File, "created-at") {
			continue
		}
		checked++
		if _, err := EventFromRow(rowOf(t, read(r.File))); err == nil {
			t.Errorf("%s: the reader accepted a document the client refuses", r.File)
		}
	}
	if checked < 20 {
		t.Fatalf("checked %d refusals; the corpus walk is probably wrong", checked)
	}
}

// The reader, the 0057 function and the contract hold one set of shapes.
func TestTheReaderShapesAreTheContractShapes(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(contracts.SchemaPath(root, contracts.RepositoryIdentityEventSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Payload struct {
				Properties struct {
					OrganizationID struct {
						Pattern string `json:"pattern"`
					} `json:"organization_id"`
					Change struct {
						Properties map[string]struct {
							Pattern   string `json:"pattern"`
							MaxLength int    `json:"maxLength"`
						} `json:"properties"`
					} `json:"change"`
				} `json:"properties"`
			} `json:"payload"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	change := schema.Properties.Payload.Properties.Change.Properties
	for name, got := range map[string]string{
		"organization_id":  uuidShape.String(),
		"provider":         providerShape.String(),
		"provider_repo_id": repoIDShape.String(),
		"path":             pathShape.String(),
	} {
		want := change[name].Pattern
		if name == "organization_id" {
			want = schema.Properties.Payload.Properties.OrganizationID.Pattern
		}
		if want == "" || got != want {
			t.Errorf("%s: reader %q, contract %q", name, got, want)
		}
	}
	if change["path"].MaxLength != 255 {
		t.Errorf("path maxLength %d, want 255", change["path"].MaxLength)
	}
}

func TestEventFromRowRefusesAnotherEventTypeAndAMismatchedKey(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(corpusDir(t), "valid-observed.json"))
	if err != nil {
		t.Fatal(err)
	}
	row := rowOf(t, body)
	if _, err := EventFromRow(row); err != nil {
		t.Fatal(err)
	}
	other := row
	other.EventType = "repository_identity.removed"
	if _, err := EventFromRow(other); err == nil {
		t.Error("another event type was accepted")
	}
	other = row
	other.IdempotencyKey = strings.TrimSuffix(row.IdempotencyKey, ":1") + ":2"
	if _, err := EventFromRow(other); err == nil {
		t.Error("a key naming another revision was accepted")
	}
	other = row
	other.AggregateID = strings.Replace(row.AggregateID, "/github/", "/gitlab/", 1)
	if _, err := EventFromRow(other); err == nil {
		t.Error("an aggregate naming another provider was accepted")
	}
}

func TestRetryDelayDoublesToFiveMinutes(t *testing.T) {
	for attempts, want := range map[int]time.Duration{-1: 5 * time.Second, 0: 5 * time.Second, 1: 10 * time.Second, 5: 160 * time.Second, 6: 5 * time.Minute, 40: 5 * time.Minute} {
		if got := RetryDelay(attempts); got != want {
			t.Errorf("RetryDelay(%d) = %v, want %v", attempts, got, want)
		}
	}
}

func TestNewReaderRefusesAnUnusableConfig(t *testing.T) {
	if _, err := NewReader(Config{}); err == nil {
		t.Fatal("a reader without a pool was built")
	}
}
