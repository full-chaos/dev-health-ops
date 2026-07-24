package externalrecompute

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/google/uuid"
)

type controllerStoreFake struct {
	scheduled []streamhandlers.ExternalRecomputeScope
	claims    []Claim
	completed []string
}

func (store *controllerStoreFake) Coalesce(
	_ context.Context,
	scope streamhandlers.ExternalRecomputeScope,
	_ time.Time,
	_ time.Duration,
) error {
	store.scheduled = append(store.scheduled, scope)
	return nil
}

func (store *controllerStoreFake) ClaimDue(context.Context, time.Time, int, time.Duration) ([]Claim, error) {
	return append([]Claim(nil), store.claims...), nil
}

func (store *controllerStoreFake) Complete(_ context.Context, claim Claim) error {
	store.completed = append(store.completed, claim.ID)
	store.claims = nil
	return nil
}

type controllerDispatcherFake struct {
	pending      []streamhandlers.ExternalRecomputeScope
	dispatched   []string
	dispatchErrs []error
}

func (dispatcher *controllerDispatcherFake) PendingScopes(context.Context, int) ([]streamhandlers.ExternalRecomputeScope, error) {
	pending := dispatcher.pending
	dispatcher.pending = nil
	return pending, nil
}

func (dispatcher *controllerDispatcherFake) Dispatch(_ context.Context, claim Claim) error {
	dispatcher.dispatched = append(dispatcher.dispatched, claim.ID)
	if len(dispatcher.dispatchErrs) == 0 {
		return nil
	}
	err := dispatcher.dispatchErrs[0]
	dispatcher.dispatchErrs = dispatcher.dispatchErrs[1:]
	return err
}

func TestControllerRecoversPendingBatchesAndCanonicalizesScope(t *testing.T) {
	scope := streamhandlers.ExternalRecomputeScope{
		OrgID: "org-1", SourceSystem: "github", SourceInstance: "Acme/API",
		IngestionID: uuid.MustParse("11111111-2222-4333-8444-555555555555"),
		RepoIDs:     []string{"repo-b", "repo-a", "repo-a"},
		TeamIDs:     []string{"team-b", "team-a"}, RecordKinds: []string{"review.v1", "commit.v1"},
	}
	store := &controllerStoreFake{}
	dispatcher := &controllerDispatcherFake{pending: []streamhandlers.ExternalRecomputeScope{scope}}
	controller, err := New(store, dispatcher, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.scheduled) != 1 ||
		!slices.Equal(store.scheduled[0].RepoIDs, []string{"repo-a", "repo-b"}) ||
		!slices.Equal(store.scheduled[0].TeamIDs, []string{"team-a", "team-b"}) {
		t.Fatalf("recovered scope = %#v", store.scheduled)
	}
}

func TestControllerDispatchFailureIsNonFatalAndInflightClaimRetries(t *testing.T) {
	claim := Claim{
		ID: "claim-1",
		Scope: streamhandlers.ExternalRecomputeScope{
			OrgID: "org-1", SourceSystem: "github", SourceInstance: "Acme/API",
			IngestionID: uuid.MustParse("11111111-2222-4333-8444-555555555555"),
		},
	}
	store := &controllerStoreFake{claims: []Claim{claim}}
	dispatcher := &controllerDispatcherFake{dispatchErrs: []error{errors.New("bridge unavailable")}}
	controller, err := New(store, dispatcher, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.step(context.Background()); err == nil {
		t.Fatal("expected the control window to report its non-fatal dispatch error")
	}
	if len(store.completed) != 0 || len(store.claims) != 1 {
		t.Fatalf("failed dispatch lost inflight claim: completed=%v claims=%v", store.completed, store.claims)
	}
	if err := controller.step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dispatcher.dispatched, []string{"claim-1", "claim-1"}) ||
		!slices.Equal(store.completed, []string{"claim-1"}) {
		t.Fatalf("retry outcome dispatched=%v completed=%v", dispatcher.dispatched, store.completed)
	}
}

func TestControllerScheduleRejectsInvalidOrReversedScope(t *testing.T) {
	controller, err := New(&controllerStoreFake{}, &controllerDispatcherFake{}, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC)
	end := start.Add(-time.Hour)
	err = controller.Schedule(context.Background(), streamhandlers.ExternalRecomputeScope{
		OrgID: "org", SourceSystem: "github", SourceInstance: "repo",
		IngestionID: uuid.New(), WindowStart: &start, WindowEnd: &end,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("reversed window error = %v", err)
	}
}
