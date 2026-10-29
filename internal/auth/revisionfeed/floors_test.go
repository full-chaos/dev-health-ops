package revisionfeed

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// PublishedFloors and ReadStatus are a STABLE interface (acp PLAN GO 10983, ruling 2): the CHAOS-11271 restore fence reads
// them. These tests pin the contract; a change to a name, a return shape or the usable rule is a contract change.
func TestPublishedFloorsReturnsOneRevisionPerAggregate(t *testing.T) {
	client := &fakeClient{hash: map[string]string{"principal:prn_1": "7", "user:prn_1": "2", "organization:org_9": "31"}}
	got, err := PublishedFloors(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"principal:prn_1": 7, "user:prn_1": 2, "organization:org_9": 31}
	if len(got) != len(want) {
		t.Fatalf("floors %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("floor %s = %d, want %d", k, got[k], v)
		}
	}
}

func TestPublishedFloorsOfAnEmptyHashIsAnEmptyMapNotAnError(t *testing.T) {
	got, err := PublishedFloors(context.Background(), &fakeClient{hash: map[string]string{}})
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("got %v, %v, want an empty non-nil map", got, err)
	}
}

func TestPublishedFloorsRefusesACorruptValueWithoutQuotingIt(t *testing.T) {
	for _, v := range []string{"", "abc", "0", "-3", "1.5", "99999999999999999999"} {
		_, err := PublishedFloors(context.Background(), &fakeClient{hash: map[string]string{"principal:prn_SENTINEL": v}})
		if !errors.Is(err, ErrCorruptFloors) {
			t.Fatalf("value %q: err = %v, want ErrCorruptFloors", v, err)
		}
		if strings.Contains(err.Error(), "SENTINEL") || (v != "" && strings.Contains(err.Error(), v)) {
			t.Fatalf("the error quotes the field or the value: %v", err)
		}
	}
}

func TestPublishedFloorsMapsAnyClientFailureToErrUnavailable(t *testing.T) {
	_, err := PublishedFloors(context.Background(), &fakeClient{hgetallErr: errors.New("dial redis://u:SENTINEL@h:6379/1")})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "SENTINEL") {
		t.Fatalf("the error carries the cause: %v", err)
	}
}

func TestStatusRule(t *testing.T) {
	for _, tc := range []struct {
		name       string
		appendonly string
		policy     string
		ttl        int64
		usable     bool
		reason     string
	}{
		{"appendonly yes and noeviction", "yes", "noeviction", -1, true, "ok"},
		{"appendonly no", "no", "noeviction", -1, false, "appendonly_off"},
		{"appendonly no and an evicting policy", "no", "allkeys-lru", -1, false, "appendonly_off"},
		{"allkeys-lru evicts the hash", "yes", "allkeys-lru", -1, false, "policy_evicts"},
		{"allkeys-lfu evicts the hash", "yes", "allkeys-lfu", -1, false, "policy_evicts"},
		{"allkeys-random evicts the hash", "yes", "allkeys-random", -1, false, "policy_evicts"},
		{"volatile-lru and the hash has no TTL", "yes", "volatile-lru", -1, true, "ok"},
		{"volatile-ttl and the hash has no TTL", "yes", "volatile-ttl", -1, true, "ok"},
		{"volatile-lru and the hash is absent", "yes", "volatile-lru", -2, true, "ok"},
		{"volatile-lru and the hash has a TTL", "yes", "volatile-lru", 3600, false, "hash_has_ttl"},
		{"volatile-random and the hash has a TTL", "yes", "volatile-random", 10, false, "hash_has_ttl"},
		{"an unknown policy is not usable", "yes", "mystery", -1, false, "policy_evicts"},
		{"an empty policy is not usable", "yes", "", -1, false, "policy_evicts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{config: map[string]string{"appendonly": tc.appendonly, "maxmemory-policy": tc.policy}, ttl: tc.ttl}
			got, err := ReadStatus(context.Background(), client)
			if err != nil {
				t.Fatal(err)
			}
			if got.Usable != tc.usable || got.Reason != tc.reason {
				t.Fatalf("status %+v, want usable=%v reason=%q", got, tc.usable, tc.reason)
			}
			if got.AppendOnly != (tc.appendonly == "yes") || got.MaxMemoryPolicy != tc.policy {
				t.Fatalf("status %+v does not report what it read", got)
			}
		})
	}
}

func TestStatusReasonsAreAClosedSet(t *testing.T) {
	want := []string{"ok", "appendonly_off", "policy_evicts", "hash_has_ttl"}
	got := StatusReasons()
	if len(got) != len(want) {
		t.Fatalf("reasons %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reasons %v, want %v", got, want)
		}
	}
}

func TestStatusMapsAnyClientFailureToErrUnavailable(t *testing.T) {
	for name, client := range map[string]*fakeClient{
		"config get": {configErr: errors.New("boom SENTINEL")},
		"ttl":        {config: map[string]string{"appendonly": "yes", "maxmemory-policy": "volatile-lru"}, ttlErr: errors.New("boom SENTINEL")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadStatus(context.Background(), client)
			if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "SENTINEL") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}
