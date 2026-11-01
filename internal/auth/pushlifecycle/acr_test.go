package pushlifecycle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func TestAcrFingerprintIsThePublicHashOfTheID(t *testing.T) {
	if got, want := AcrFingerprint("cred_exampleparentaaa"), "b04d9435b4621ea9"; got != want {
		t.Fatalf("AcrFingerprint = %q, want %q", got, want)
	}
}

// FromAcr holds over every valid example: each event type maps, a revision 1
// event carries the fingerprint, and a later one does not.
func TestFromAcrMapsEveryValidExample(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		contracts.AcrCredentialIssued: contracts.ExternalPushCreated, contracts.AcrCredentialDeviceRedeemed: contracts.ExternalPushCreated,
		contracts.AcrCredentialRotated: contracts.ExternalPushRotated, contracts.AcrCredentialRevoked: contracts.ExternalPushRevoked,
		contracts.AcrCredentialExpired: contracts.ExternalPushExpired,
	}
	names, err := filepath.Glob(filepath.Join(root, "contracts/auth/v1/examples/acr-credential-lifecycle-event/valid-*.json"))
	if err != nil || len(names) == 0 {
		t.Fatalf("no examples: %v", err)
	}
	seen := map[string]bool{}
	for _, name := range names {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		event, err := contracts.ParseAcrCredentialLifecycleEvent(root, raw)
		if err != nil {
			t.Fatalf("%s: %v", filepath.Base(name), err)
		}
		seen[event.EventType] = true
		mapped := FromAcr(event)
		if mapped.EventType != want[event.EventType] || mapped.EventType == "" {
			t.Errorf("%s: type %q", filepath.Base(name), mapped.EventType)
		}
		if mapped.AggregateID != event.AggregateID || mapped.Payload.Revision != event.Payload.Revision ||
			mapped.Payload.OrganizationID != event.Payload.OrganizationID || mapped.Payload.SourceID != AcrSourceID {
			t.Errorf("%s: mapped = %+v", filepath.Base(name), mapped)
		}
		if (mapped.Payload.Change.Fingerprint != "") != (event.Payload.Revision == 1) {
			t.Errorf("%s: fingerprint %q at revision %d", filepath.Base(name), mapped.Payload.Change.Fingerprint, event.Payload.Revision)
		}
		text, err := json.Marshal(mapped)
		if err != nil {
			t.Fatal(err)
		}
		if event.Payload.Change.OwnerSubject != "" && strings.Contains(string(text), event.Payload.Change.OwnerSubject) {
			t.Errorf("%s: the owner subject is in the mapped event", filepath.Base(name))
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("event types seen = %v", seen)
	}
}
