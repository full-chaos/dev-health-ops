package membership

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestLockOrderSortsByIDAndKeepsTheStrongestMode(t *testing.T) {
	a := uuid.MustParse("10000000-0000-0000-0000-000000000000")
	b := uuid.MustParse("20000000-0000-0000-0000-000000000000")
	c := uuid.MustParse("30000000-0000-0000-0000-000000000000")
	d := uuid.MustParse("ffffffff-0000-0000-0000-000000000000")
	cases := []struct {
		name       string
		targets    []uuid.UUID
		inviter    *uuid.UUID
		actor      *uuid.UUID
		referenced []uuid.UUID
		want       []LockRow
	}{
		{"nothing", nil, nil, nil, nil, []LockRow{}},
		{"target, inviter, actor apart, out of order", []uuid.UUID{c}, &a, &b, nil,
			[]LockRow{{a, LockKeyShare}, {b, LockShare}, {c, LockUpdate}}},
		{"two targets, a duplicate", []uuid.UUID{d, b, d}, nil, nil, nil,
			[]LockRow{{b, LockUpdate}, {d, LockUpdate}}},
		{"the actor is a target", []uuid.UUID{b}, nil, &b, nil, []LockRow{{b, LockUpdate}}},
		{"the inviter is the actor", nil, &a, &a, nil, []LockRow{{a, LockShare}}},
		{"the inviter is a target", []uuid.UUID{a}, &a, nil, nil, []LockRow{{a, LockUpdate}}},
		{"a referenced id is the actor", nil, nil, &c, []uuid.UUID{c, a}, []LockRow{{a, LockKeyShare}, {c, LockShare}}},
		{"referenced ids only", nil, nil, nil, []uuid.UUID{d, a, d}, []LockRow{{a, LockKeyShare}, {d, LockKeyShare}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := LockOrder(tc.targets, tc.inviter, tc.actor, tc.referenced...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("LockOrder = %v, want %v", got, tc.want)
			}
		})
	}
}

// The byte order of uuid values is Postgres's uuid order: the first byte
// decides before any later one.
func TestLockOrderComparesTheWholeIDInPostgresOrder(t *testing.T) {
	lo := uuid.MustParse("0fffffff-ffff-ffff-ffff-ffffffffffff")
	hi := uuid.MustParse("10000000-0000-0000-0000-000000000000")
	got := LockOrder([]uuid.UUID{hi}, &lo, nil)
	want := []LockRow{{lo, LockKeyShare}, {hi, LockUpdate}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LockOrder = %v, want %v", got, want)
	}
}
