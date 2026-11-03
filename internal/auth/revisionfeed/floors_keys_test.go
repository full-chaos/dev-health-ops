package revisionfeed

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Codex r2 P2 (malformed key): a field of the hash that is not "<type>:<id>" is corrupt, never skipped: the restore fence
// counts a successful input as usable, so one bad field makes the whole read an error (fail closed).
func TestPublishedFloorsRefusesAMalformedAggregateKeyWithoutQuotingIt(t *testing.T) {
	for _, field := range []string{"not-an-aggregate-key", ":prn_SENTINEL", "principal:", ":", "", "principal"} {
		hash := map[string]string{"user:u1": "3", field: "9"}
		got, err := PublishedFloors(context.Background(), &fakeClient{hash: hash})
		if !errors.Is(err, ErrCorruptFloors) || got != nil {
			t.Fatalf("field %q: floors %v, err %v, want nil and ErrCorruptFloors", field, got, err)
		}
		if strings.Contains(err.Error(), "SENTINEL") || (len(field) > 3 && strings.Contains(err.Error(), field)) {
			t.Fatalf("the error quotes the field: %v", err)
		}
	}
}

func TestTheFloorInputIsNotUsableWithAMalformedAggregateKey(t *testing.T) {
	client := &fakeClient{info: infoFields("noeviction"), ttl: -1, hash: map[string]string{"user:u1": "3", "not-an-aggregate-key": "9"}}
	input := NewFloorInput(client)
	if err := input.Status(context.Background()); err != nil {
		t.Fatalf("the status rule is fine: %v", err)
	}
	floors, err := input.Floors(context.Background())
	if !errors.Is(err, ErrCorruptFloors) || floors != nil {
		t.Fatalf("floors %v, err %v, want nil and ErrCorruptFloors: the fence would count this input as usable", floors, err)
	}
}

func TestAFullyValidHashStillWorks(t *testing.T) {
	hash := map[string]string{"user:u1": "3", "organization:o:9": "4", "principal:prn_1": "7"}
	got, err := PublishedFloors(context.Background(), &fakeClient{hash: hash})
	if err != nil || len(got) != 3 || got["organization:o:9"] != 4 {
		t.Fatalf("floors %v, err %v", got, err)
	}
}
