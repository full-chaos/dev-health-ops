package local

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

type recordingStore struct {
	provisioned identity.LocalProvisionRequest
	verifiedAt  time.Time
	actors      []identity.Actor
	principal   *identity.Principal
	ended       bool
	cause       identity.PasswordCause
	endErr      error
}

type recordingEnder struct {
	consumed, created int
}

func (r *recordingEnder) EndAllInTx(context.Context, audit.TxOps, uuid.UUID) (session.Ended, error) {
	return session.Ended{}, nil
}

func (r *recordingEnder) CreateInTx(context.Context, audit.TxOps, uuid.UUID, *uuid.UUID, []string) (session.Issued, error) {
	r.created++
	return session.Issued{}, nil
}

func (r *recordingEnder) PeekResetLink(context.Context, [32]byte) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil
}

func (r *recordingEnder) ConsumeResetLinkInTx(context.Context, audit.TxOps, uuid.UUID, [32]byte) error {
	r.consumed++
	return nil
}

func (r *recordingEnder) EndResetLinksInTx(context.Context, audit.TxOps, uuid.UUID) (int64, error) {
	return 0, nil
}

func (s *recordingStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	return s.principal, nil
}
func (s *recordingStore) ByID(context.Context, uuid.UUID) (*identity.Principal, error) {
	return s.principal, nil
}
func (s *recordingStore) ProvisionLocal(_ context.Context, r identity.LocalProvisionRequest) (uuid.UUID, error) {
	s.provisioned = r
	s.actors = append(s.actors, r.Actor)
	return uuid.New(), nil
}
func (s *recordingStore) SetEmailVerified(_ context.Context, _ uuid.UUID, at time.Time, a identity.Actor) error {
	s.verifiedAt = at
	s.actors = append(s.actors, a)
	return nil
}
func (s *recordingStore) SetPasswordHashFor(ctx context.Context, _ uuid.UUID, _ string, a identity.Actor, cause identity.PasswordCause, end func(context.Context, audit.TxOps) error) error {
	s.actors = append(s.actors, a)
	s.ended = end != nil
	s.cause = cause
	if s.endErr != nil {
		return s.endErr
	}
	if end != nil {
		return end(ctx, audit.TxOps{})
	}
	return nil
}

func TestIdentityAccountsPassesTheHashNotThePasswordAndOneRequestID(t *testing.T) {
	rid := uuid.NewString()
	fixed := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	store := &recordingStore{principal: &identity.Principal{ID: uuid.New(), IsActive: true, IsVerified: true}}
	ender := &recordingEnder{}
	acc := IdentityAccounts{Store: store, Sessions: ender, Links: ender, RequestID: func(context.Context) *string { return &rid }, Now: func() time.Time { return fixed }}
	ctx := context.Background()
	name := "Ada"
	if _, err := acc.ProvisionLocal(ctx, ProvisionLocalRequest{Email: "a@example.com", FullName: &name, PasswordHash: "$2b$12$hash"}); err != nil {
		t.Fatal(err)
	}
	if store.provisioned.PasswordHash == nil || *store.provisioned.PasswordHash != "$2b$12$hash" || store.provisioned.FullName != "Ada" {
		t.Errorf("provisioned %+v", store.provisioned)
	}
	if err := acc.SetEmailVerified(ctx, uuid.New()); err != nil || !store.verifiedAt.Equal(fixed) {
		t.Errorf("verified at %v, err %v", store.verifiedAt, err)
	}
	if _, err := acc.SetPasswordEndingSessions(ctx, PasswordEnd{ID: uuid.New(), Hash: "$2b$12$h"}); err != nil || !store.ended || store.cause != identity.PasswordChanged {
		t.Fatalf("set password: err %v, ending hook passed %v, cause %v", err, store.ended, store.cause)
	}
	link := [32]byte{1}
	res, err := acc.SetPasswordEndingSessions(ctx, PasswordEnd{ID: uuid.New(), Hash: "$2b$12$h", ResetLink: &link, NewSession: &NewSession{AMR: []string{"pwd"}}})
	if err != nil || store.cause != identity.PasswordReset || ender.consumed != 1 || ender.created != 1 || res.Issued == nil {
		t.Fatalf("reset write: err %v, cause %v, consumed %d, created %d, issued %v", err, store.cause, ender.consumed, ender.created, res.Issued)
	}
	store.endErr = identity.ErrLinkUnusable
	if _, err := acc.SetPasswordEndingSessions(ctx, PasswordEnd{ID: uuid.New(), Hash: "$2b$12$h", ResetLink: &link}); !errors.Is(err, ErrResetLinkUnusable) {
		t.Fatalf("an unusable link = %v, want ErrResetLinkUnusable", err)
	}
	store.endErr = nil
	for i, a := range store.actors {
		if a.RequestID == nil || *a.RequestID != rid {
			t.Errorf("actor %d request id = %v, want %s", i, a.RequestID, rid)
		}
	}
	u, err := acc.ByID(ctx, uuid.New())
	if err != nil || u == nil || !u.IsVerified {
		t.Errorf("ByID = %+v, %v", u, err)
	}
	store.principal = nil
	if u, _ := acc.ByEmail(ctx, "x"); u != nil {
		t.Error("an unknown user must map to nil")
	}
}
