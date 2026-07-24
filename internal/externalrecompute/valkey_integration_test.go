//go:build integration

package externalrecompute

import (
	"context"
	"slices"
	"testing"
	"time"

	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
)

func TestValkeyCoalescingAndAtomicDrainCrashWindows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	store, err := NewValkeyStore(client)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	startA, endA := now.Add(-48*time.Hour), now.Add(-24*time.Hour)
	startB, endB := now.Add(-72*time.Hour), now
	firstID := uuid.MustParse("11111111-2222-4333-8444-555555555555")
	secondID := uuid.MustParse("22222222-3333-4444-8555-666666666666")
	base := streamhandlers.ExternalRecomputeScope{
		OrgID: "org-a", SourceSystem: "github", SourceInstance: "Acme/API",
		IngestionID: firstID, RepoIDs: []string{"repo-b"}, TeamIDs: []string{"team-a"},
		RecordKinds: []string{"commit.v1"}, WindowStart: &startA, WindowEnd: &endA,
	}
	if err := store.Coalesce(ctx, base, now, time.Second); err != nil {
		t.Fatal(err)
	}
	second := base
	second.IngestionID = secondID
	second.RepoIDs = []string{"repo-a", "repo-b"}
	second.RecordKinds = []string{"review.v1"}
	second.WindowStart, second.WindowEnd = &startB, &endB
	if err := store.Coalesce(ctx, second, now.Add(100*time.Millisecond), time.Second); err != nil {
		t.Fatal(err)
	}
	if claims, err := store.ClaimDue(ctx, now.Add(999*time.Millisecond), 10, 30*time.Second); err != nil || len(claims) != 0 {
		t.Fatalf("claimed before debounce: claims=%v err=%v", claims, err)
	}
	claims, err := store.ClaimDue(ctx, now.Add(time.Second), 10, 30*time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatalf("coalesced claims=%v err=%v", claims, err)
	}
	older := claims[0]
	if !slices.Equal(older.ingestionIDs, []string{firstID.String(), secondID.String()}) ||
		!slices.Equal(older.Scope.RepoIDs, []string{"repo-a", "repo-b"}) ||
		!slices.Equal(older.Scope.RecordKinds, []string{"commit.v1", "review.v1"}) ||
		!older.Scope.WindowStart.Equal(startB) || !older.Scope.WindowEnd.Equal(endB) {
		t.Fatalf("coalesced scope = %#v ids=%v", older.Scope, older.ingestionIDs)
	}

	// Crash-before-dispatch: ClaimDue moved the blob to a durable inflight key.
	// It reappears after the lease without depending on process memory.
	retried, err := store.ClaimDue(ctx, now.Add(31*time.Second), 10, 30*time.Second)
	if err != nil || len(retried) != 1 || retried[0].ID != older.ID {
		t.Fatalf("inflight recovery = %v err=%v", retried, err)
	}

	// A newer generation can land after the old atomic drain. Completing the
	// old claim must not delete the new pending scope or its due ticket.
	third := base
	third.IngestionID = uuid.MustParse("33333333-4444-4555-8666-777777777777")
	third.RepoIDs = []string{"repo-new"}
	if err := store.Coalesce(ctx, third, now.Add(31*time.Second), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, older); err != nil {
		t.Fatal(err)
	}
	newer, err := store.ClaimDue(ctx, now.Add(32*time.Second), 10, 30*time.Second)
	if err != nil || len(newer) != 1 || newer[0].ID == older.ID ||
		!slices.Equal(newer[0].Scope.RepoIDs, []string{"repo-new"}) {
		t.Fatalf("newer generation = %v err=%v", newer, err)
	}
}
