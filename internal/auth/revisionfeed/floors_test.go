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
		name   string
		policy string
		mods   map[string]string
		ttl    int64
		usable bool
		reason string
	}{
		{"aof on, healthy, noeviction", "noeviction", nil, -1, true, "ok"},
		{"aof off", "noeviction", map[string]string{"aof_enabled": "0"}, -1, false, "appendonly_off"},
		{"aof off and an evicting policy", "allkeys-lru", map[string]string{"aof_enabled": "0"}, -1, false, "appendonly_off"},
		{"aof_enabled missing", "noeviction", map[string]string{"aof_enabled": ""}, -1, false, "aof_status_unknown"},
		{"aof_enabled garbage", "noeviction", map[string]string{"aof_enabled": "maybe"}, -1, false, "aof_status_unknown"},
		{"last write failed", "noeviction", map[string]string{"aof_last_write_status": "err"}, -1, false, "aof_write_failed"},
		{"last write status missing", "noeviction", map[string]string{"aof_last_write_status": ""}, -1, false, "aof_status_unknown"},
		{"last write status unknown value", "noeviction", map[string]string{"aof_last_write_status": "?"}, -1, false, "aof_write_failed"},
		{"last rewrite failed", "noeviction", map[string]string{"aof_last_bgrewrite_status": "err"}, -1, false, "aof_rewrite_failed"},
		{"last rewrite status missing", "noeviction", map[string]string{"aof_last_bgrewrite_status": ""}, -1, false, "aof_status_unknown"},
		{"maxmemory_policy missing", "", nil, -1, false, "policy_evicts"},
		{"allkeys-lru evicts the hash", "allkeys-lru", nil, -1, false, "policy_evicts"},
		{"allkeys-lfu evicts the hash", "allkeys-lfu", nil, -1, false, "policy_evicts"},
		{"allkeys-random evicts the hash", "allkeys-random", nil, -1, false, "policy_evicts"},
		{"volatile-lru and the hash has no TTL", "volatile-lru", nil, -1, true, "ok"},
		{"volatile-ttl and the hash has no TTL", "volatile-ttl", nil, -1, true, "ok"},
		{"volatile-lru and the hash is absent", "volatile-lru", nil, -2, true, "ok"},
		{"volatile-lru and the hash has a TTL", "volatile-lru", nil, 3600, false, "hash_has_ttl"},
		{"volatile-random and the hash has a TTL", "volatile-random", nil, 10, false, "hash_has_ttl"},
		{"an unknown policy is not usable", "mystery", nil, -1, false, "policy_evicts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{info: infoFields(tc.policy, tc.mods), ttl: tc.ttl}
			got, err := ReadStatus(context.Background(), client)
			if err != nil {
				t.Fatal(err)
			}
			if got.Usable != tc.usable || got.Reason != tc.reason {
				t.Fatalf("status %+v, want usable=%v reason=%q", got, tc.usable, tc.reason)
			}
			if got.MaxMemoryPolicy != tc.policy {
				t.Fatalf("status %+v does not report the policy it read", got)
			}
		})
	}
}

// ACP ruling on PR B r1 (P1-2): the status reads INFO persistence and INFO memory only, never CONFIG GET; any failed,
// missing or unknown AOF field is not usable.
func TestStatusReadsInfoOnlyAndNeverConfigGet(t *testing.T) {
	client := &fakeClient{info: infoFields("noeviction"), config: map[string]string{"appendonly": "yes", "maxmemory-policy": "noeviction"}}
	if _, err := ReadStatus(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if n := client.configGets(); n != 0 {
		t.Fatalf("ReadStatus issued %d CONFIG GET calls, want 0", n)
	}
	if got := client.infoCalls(); len(got) != 1 {
		t.Fatalf("ReadStatus issued %d INFO calls, want exactly 1: %v", len(got), got)
	}
}

func TestStatusReasonsAreAClosedSet(t *testing.T) {
	want := []string{"ok", "appendonly_off", "aof_status_unknown", "aof_write_failed", "aof_rewrite_failed", "policy_evicts", "hash_has_ttl"}
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
		"info": {infoErr: errors.New("boom SENTINEL")},
		"ttl":  {info: infoFields("volatile-lru"), ttlErr: errors.New("boom SENTINEL")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadStatus(context.Background(), client)
			if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "SENTINEL") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}
