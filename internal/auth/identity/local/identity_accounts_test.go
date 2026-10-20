package local

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

type recordingStore struct {
	provisioned identity.LocalProvisionRequest
	verifiedAt  time.Time
	actors      []identity.Actor
	principal   *identity.Principal
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
func (s *recordingStore) SetPasswordHash(_ context.Context, _ uuid.UUID, _ string, a identity.Actor) error {
	s.actors = append(s.actors, a)
	return nil
}

func TestIdentityAccountsPassesTheHashNotThePasswordAndOneRequestID(t *testing.T) {
	rid := uuid.NewString()
	fixed := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	store := &recordingStore{principal: &identity.Principal{ID: uuid.New(), IsActive: true, IsVerified: true}}
	acc := IdentityAccounts{Store: store, RequestID: func(context.Context) *string { return &rid }, Now: func() time.Time { return fixed }}
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
	if err := acc.SetPasswordHash(ctx, uuid.New(), "$2b$12$h"); err != nil {
		t.Fatal(err)
	}
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
