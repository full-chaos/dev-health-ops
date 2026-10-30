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
	usable := &fakeClient{info: infoFields("noeviction"), ttl: -1}
	if err := NewFloorInput(usable).Status(context.Background()); err != nil {
		t.Fatalf("a usable store: %v", err)
	}
	for name, info := range map[string]map[string]string{
		"appendonly off":   infoFields("noeviction", map[string]string{"aof_enabled": "0"}),
		"evicting":         infoFields("allkeys-lru"),
		"aof write failed": infoFields("noeviction", map[string]string{"aof_last_write_status": "err"}),
	} {
		err := NewFloorInput(&fakeClient{info: info}).Status(context.Background())
		if !errors.Is(err, ErrNotUsableAsFloorSource) {
			t.Fatalf("%s: err = %v, want ErrNotUsableAsFloorSource", name, err)
		}
	}
	err := NewFloorInput(&fakeClient{infoErr: errors.New("boom SENTINEL")}).Status(context.Background())
	if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "SENTINEL") {
		t.Fatalf("an unreachable store: err = %v", err)
	}
}

func TestTheNotUsableErrorNamesTheClosedReason(t *testing.T) {
	err := NewFloorInput(&fakeClient{info: infoFields("noeviction", map[string]string{"aof_enabled": "0"})}).Status(context.Background())
	if err == nil || !strings.Contains(err.Error(), "appendonly_off") {
		t.Fatalf("err = %v", err)
	}
}

// ACP ruling on PR B (C1): the input fails closed. It never passes and never gives a zero floor.
func TestTheFloorInputFailsClosed(t *testing.T) {
	ctx := context.Background()
	usable := infoFields("noeviction")
	t.Run("not usable: the chart default policy", func(t *testing.T) {
		err := NewFloorInput(&fakeClient{info: infoFields("allkeys-lru")}).Status(ctx)
		if !errors.Is(err, ErrNotUsableAsFloorSource) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("not usable: appendonly off", func(t *testing.T) {
		err := NewFloorInput(&fakeClient{info: infoFields("noeviction", map[string]string{"aof_enabled": "0"})}).Status(ctx)
		if !errors.Is(err, ErrNotUsableAsFloorSource) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unreachable: status and floors both fail, floors are nil", func(t *testing.T) {
		client := &fakeClient{infoErr: ErrUnavailable, hgetallErr: ErrUnavailable}
		input := NewFloorInput(client)
		if err := input.Status(ctx); err == nil || !errors.Is(err, ErrUnavailable) {
			t.Fatalf("status err = %v, want ErrUnavailable (never nil)", err)
		}
		floors, err := input.Floors(ctx)
		if err == nil || !errors.Is(err, ErrUnavailable) || floors != nil {
			t.Fatalf("floors %v, err %v, want nil and ErrUnavailable", floors, err)
		}
	})
	t.Run("usable status and an empty hash: an explicit no-data error, never an empty map or a zero floor", func(t *testing.T) {
		input := NewFloorInput(&fakeClient{info: usable, ttl: -1, hash: map[string]string{}})
		if err := input.Status(ctx); err != nil {
			t.Fatalf("status err = %v", err)
		}
		floors, err := input.Floors(ctx)
		if !errors.Is(err, ErrNoFloors) || floors != nil {
			t.Fatalf("floors %v, err %v, want nil and ErrNoFloors: the fence would count an empty input as a usable source and refuse the blind margin", floors, err)
		}
	})
	t.Run("usable status and floors: the floors", func(t *testing.T) {
		floors, err := NewFloorInput(&fakeClient{info: usable, ttl: -1, hash: map[string]string{"principal:p": "3"}}).Floors(ctx)
		if err != nil || floors["principal:p"] != 3 {
			t.Fatalf("floors %v, %v", floors, err)
		}
	})
}
