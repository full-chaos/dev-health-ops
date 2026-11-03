package acrmint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type noDelegation struct{}

func (noDelegation) ActorInDelegation(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return false, nil
}

type delegationStub struct {
	involved bool
	err      error
	calls    int
	got      [2]uuid.UUID
}

func (d *delegationStub) ActorInDelegation(_ context.Context, principal, session uuid.UUID) (bool, error) {
	d.calls++
	d.got = [2]uuid.UUID{principal, session}
	return d.involved, d.err
}

type legacyRow struct {
	active bool
	err    error
}

func (r legacyRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*bool) = r.active
	return nil
}

type legacyPool struct{ row legacyRow }

func (p legacyPool) QueryRow(context.Context, string, ...any) pgx.Row { return p.row }

// TestTheActorCheckRefusesOnEachOfItsThreeReads: the check is one checker of three
// reads (acp, CHAOS-12251 Q4). The legacy row refuses, the delegated side refuses,
// each read's error is an error and never a pass, and a principal on neither side
// passes: the positive control.
func TestTheActorCheckRefusesOnEachOfItsThreeReads(t *testing.T) {
	boom := errors.New("read failed")
	principal, session := uuid.New(), uuid.New()
	for _, tc := range []struct {
		name       string
		legacy     legacyRow
		delegation *delegationStub
		want       bool
		wantErr    error
	}{
		{name: "on neither side", delegation: &delegationStub{}},
		{name: "a legacy impersonation row", legacy: legacyRow{active: true}, delegation: &delegationStub{}, want: true},
		{name: "the delegated side", delegation: &delegationStub{involved: true}, want: true},
		{name: "the legacy read fails", legacy: legacyRow{err: boom}, delegation: &delegationStub{}, wantErr: boom},
		{name: "the delegated read fails", delegation: &delegationStub{err: boom}, wantErr: boom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := PGImpersonation{Pool: legacyPool{tc.legacy}, Sessions: tc.delegation, Now: time.Now}
			got, err := check.ImpersonatingActor(context.Background(), principal, session)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			if tc.legacy.err == nil && !tc.legacy.active && tc.delegation.got != [2]uuid.UUID{principal, session} {
				t.Fatalf("the delegated side read %v", tc.delegation.got)
			}
		})
	}
}

// TestTheActorCheckRefusesWithNoDelegatedReader: a check built without the delegated
// side is an error, never a pass on the legacy read alone.
func TestTheActorCheckRefusesWithNoDelegatedReader(t *testing.T) {
	check := PGImpersonation{Pool: legacyPool{}}
	if got, err := check.ImpersonatingActor(context.Background(), uuid.New(), uuid.New()); err == nil || got {
		t.Fatalf("got %v, %v; want an error", got, err)
	}
}
