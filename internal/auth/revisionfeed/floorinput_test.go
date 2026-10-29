package revisionfeed

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The restore fence's census (restorefence TestEveryOutboxConsumerHasAFenceFloorInput) requires every outbox consumer to
// hand its high-water marks to the fence as a FloorInput. The input is an ADVISORY lower bound: usable only while the
// status rule holds.
func TestTheFloorInputNamesItselfAndReadsThePublishedFloors(t *testing.T) {
	client := &fakeClient{hash: map[string]string{"principal:prn_1": "7"}}
	input := NewFloorInput(client)
	if input == nil || input.Name() != "valkey-revision-feed" {
		t.Fatalf("input %v", input)
	}
	floors, err := input.Floors(context.Background())
	if err != nil || floors["principal:prn_1"] != 7 {
		t.Fatalf("floors %v, %v", floors, err)
	}
}

func TestTheFloorInputIsUsableOnlyWhileTheStatusRuleHolds(t *testing.T) {
	usable := &fakeClient{config: map[string]string{"appendonly": "yes", "maxmemory-policy": "noeviction"}, ttl: -1}
	if err := NewFloorInput(usable).Status(context.Background()); err != nil {
		t.Fatalf("a usable store: %v", err)
	}
	for name, config := range map[string]map[string]string{
		"appendonly off": {"appendonly": "no", "maxmemory-policy": "noeviction"},
		"evicting":       {"appendonly": "yes", "maxmemory-policy": "allkeys-lru"},
	} {
		err := NewFloorInput(&fakeClient{config: config}).Status(context.Background())
		if !errors.Is(err, ErrNotUsableAsFloorSource) {
			t.Fatalf("%s: err = %v, want ErrNotUsableAsFloorSource", name, err)
		}
	}
	err := NewFloorInput(&fakeClient{configErr: errors.New("boom SENTINEL")}).Status(context.Background())
	if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "SENTINEL") {
		t.Fatalf("an unreachable store: err = %v", err)
	}
}

func TestTheNotUsableErrorNamesTheClosedReason(t *testing.T) {
	err := NewFloorInput(&fakeClient{config: map[string]string{"appendonly": "no", "maxmemory-policy": "noeviction"}}).Status(context.Background())
	if err == nil || !strings.Contains(err.Error(), "appendonly_off") {
		t.Fatalf("err = %v", err)
	}
}
