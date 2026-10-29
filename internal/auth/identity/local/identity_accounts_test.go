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
	consumed, created, recorded, consumedVerification int
}

func (r *recordingEnder) RecordEnded(_ context.Context, ended session.Ended) {
	r.recorded += ended.Sessions
}

func (r *recordingEnder) EndAllInTx(context.Context, audit.TxOps, uuid.UUID) (session.Ended, error) {
	return session.Ended{Sessions: 2}, nil
}

func (r *recordingEnder) CreateInTx(context.Context, audit.TxOps, uuid.UUID, *uuid.UUID, []string) (session.Issued, error) {
	r.created++
	return session.Issued{}, nil
}

func (r *recordingEnder) PeekResetLink(context.Context, [32]byte) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil
}

func (r *recordingEnder) PeekVerificationLink(context.Context, [32]byte) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil
}

func (r *recordingEnder) ConsumeVerificationLinkInTx(context.Context, audit.TxOps, uuid.UUID, [32]byte) error {
	r.consumedVerification++
	return nil
}

func (r *recordingEnder) ConsumeBoundResetLinkInTx(context.Context, audit.TxOps, uuid.UUID, [32]byte) error {
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
func (s *recordingStore) SetEmailVerifiedWith(ctx context.Context, _ uuid.UUID, at time.Time, a identity.Actor, end func(context.Context, audit.TxOps) error) error {
	s.verifiedAt = at
	s.actors = append(s.actors, a)
	if end != nil {
		if err := end(ctx, audit.TxOps{}); err != nil {
			return err
		}
	}
	return s.endErr
}
func (s *recordingStore) SetPasswordHashFor(ctx context.Context, _ uuid.UUID, _ string, a identity.Actor, cause identity.PasswordCause, end func(context.Context, audit.TxOps) error) (int64, error) {
	s.actors = append(s.actors, a)
	s.ended = end != nil
	s.cause = cause
	if end != nil {
		if err := end(ctx, audit.TxOps{}); err != nil {
			return 0, err
		}
	}
	if s.endErr != nil {
		return 0, s.endErr
	}
	return 7, nil
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
	if store.provisioned.PasswordHash == nil || *store.provisioned.PasswordHash != "$2b$12$hash" || store.provisioned.FullName == nil || *store.provisioned.FullName != "Ada" {
		t.Errorf("provisioned %+v", store.provisioned)
	}
	if err := acc.SetEmailVerifiedConsuming(ctx, uuid.New(), [32]byte{2}); err != nil || !store.verifiedAt.Equal(fixed) || ender.consumedVerification != 1 {
		t.Errorf("verified at %v, consumed %d, err %v", store.verifiedAt, ender.consumedVerification, err)
	}
	if _, err := acc.SetPasswordEndingSessions(ctx, PasswordEnd{ID: uuid.New(), Hash: "$2b$12$h"}); err != nil || !store.ended || store.cause != identity.PasswordChanged {
		t.Fatalf("set password: err %v, ending hook passed %v, cause %v", err, store.ended, store.cause)
	}
	link := [32]byte{1}
	res, err := acc.SetPasswordEndingSessions(ctx, PasswordEnd{ID: uuid.New(), Hash: "$2b$12$h", ResetLink: &link, NewSession: &NewSession{AMR: []string{"pwd"}}})
	if err != nil || store.cause != identity.PasswordReset || ender.consumed != 1 || ender.created != 1 || res.Issued == nil {
		t.Fatalf("reset write: err %v, cause %v, consumed %d, created %d, issued %v", err, store.cause, ender.consumed, ender.created, res.Issued)
	}
	if ender.recorded != 4 {
		t.Fatalf("revocations recorded = %d after two committed writes of 2 sessions, want 4", ender.recorded)
	}
	store.endErr = identity.ErrLinkUnusable
	if _, err := acc.SetPasswordEndingSessions(ctx, PasswordEnd{ID: uuid.New(), Hash: "$2b$12$h", ResetLink: &link}); !errors.Is(err, ErrResetLinkUnusable) {
		t.Fatalf("an unusable link = %v, want ErrResetLinkUnusable", err)
	}
	if ender.recorded != 4 {
		t.Fatalf("revocations recorded = %d after a failed write, want unchanged 4", ender.recorded)
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

func TestIdentityAccountsCopiesThePrincipalFullNameToTheUser(t *testing.T) {
	name := "Zoë Müller 山田"
	store := &recordingStore{principal: &identity.Principal{ID: uuid.New(), IsActive: true, FullName: &name}}
	acc := IdentityAccounts{Store: store}
	ctx := context.Background()
	byEmail, err := acc.ByEmail(ctx, "a@example.com")
	if err != nil || byEmail == nil || byEmail.FullName == nil || *byEmail.FullName != name {
		t.Errorf("ByEmail user = %+v, %v; want the full name", byEmail, err)
	}
	byID, err := acc.ByID(ctx, uuid.New())
	if err != nil || byID == nil || byID.FullName == nil || *byID.FullName != name {
		t.Errorf("ByID user = %+v, %v; want the full name", byID, err)
	}
}

func TestIdentityAccountsMapsAnUnusableVerificationLink(t *testing.T) {
	store := &recordingStore{principal: &identity.Principal{ID: uuid.New()}, endErr: identity.ErrLinkUnusable}
	acc := IdentityAccounts{Store: store, Links: &recordingEnder{}}
	if err := acc.SetEmailVerifiedConsuming(context.Background(), uuid.New(), [32]byte{3}); !errors.Is(err, ErrVerificationLinkUnusable) {
		t.Fatalf("an unusable link = %v, want ErrVerificationLinkUnusable", err)
	}
}
