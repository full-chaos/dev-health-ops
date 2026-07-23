package syncreconciler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

const (
	candidateID1 = "00000000-0000-4000-8000-000000000001"
	candidateID2 = "00000000-0000-4000-8000-000000000002"
	candidateID3 = "00000000-0000-4000-8000-000000000003"
	candidateID4 = "00000000-0000-4000-8000-000000000004"
)

type registryMap map[string]syncdispatchcontract.Descriptor

func (registry registryMap) Lookup(kind string) (syncdispatchcontract.Descriptor, bool) {
	descriptor, ok := registry[kind]
	return descriptor, ok
}

func testRegistry(t *testing.T, riverKind string) registryMap {
	t.Helper()
	registry := make(registryMap, len(frozenKinds))
	for _, kind := range frozenKinds {
		route := syncdispatchcontract.RouteCelery
		if kind == riverKind {
			route = syncdispatchcontract.RouteRiver
		}
		delivery := syncdispatchcontract.DeliveryAtLeastOnce
		registry[kind] = syncdispatchcontract.Descriptor{
			Kind:          kind,
			Delivery:      delivery,
			Route:         route,
			RollbackRoute: syncdispatchcontract.RouteCelery,
		}
	}
	return registry
}

func TestObserverStepBuildsBoundedClaimOrderSnapshotAndRoutePartitions(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Second)
	observer, err := newObserver(
		testRegistry(t, syncdispatchcontract.KindPostSync),
		func(_ context.Context, gotNow time.Time, fetchLimit int) ([]candidateRow, error) {
			if !gotNow.Equal(now) || fetchLimit != 3 {
				t.Fatalf("read arguments = %s, %d", gotNow, fetchLimit)
			}
			return []candidateRow{
				{id: candidateID1, kind: syncdispatchcontract.KindDispatchSyncRun, claimExpiresAt: &expired},
				{id: candidateID2, kind: syncdispatchcontract.KindPostSync},
				{id: candidateID3, kind: syncdispatchcontract.KindFinalizeSyncRun},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := observer.Step(context.Background(), now, 2)
	if err != nil {
		t.Fatal(err)
	}
	if observation.PredicateVersion != PredicateVersion ||
		observation.DigestVersion != DigestVersion ||
		!observation.ObservedAt.Equal(now) || observation.ObservedAt.Location() != time.UTC ||
		observation.Limit != 2 ||
		observation.SampledCandidates != 2 || !observation.Truncated ||
		observation.CeleryDuePending != 1 || observation.RiverDuePending != 1 {
		t.Fatalf("observation = %#v", observation)
	}
	if observation.Kinds[0].DuePending != 1 || observation.Kinds[0].ExpiredClaims != 1 ||
		observation.Kinds[2].DuePending != 1 || observation.Kinds[2].Route != syncdispatchcontract.RouteRiver {
		t.Fatalf("kind observations = %#v", observation.Kinds)
	}
	const expectedDigest = "sha256:6bc27cbf7ac850d910ad225ac42fdafd287b1fb5254333621d6ee32294771545"
	if observation.CandidateDigest != expectedDigest {
		t.Fatalf("candidate digest = %s, want %s", observation.CandidateDigest, expectedDigest)
	}
}

func TestObserverUnknownKindScopeIsOnlySampledCandidates(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	t.Run("unknown beyond sample only proves truncation", func(t *testing.T) {
		observer, err := newObserver(testRegistry(t, ""), func(context.Context, time.Time, int) ([]candidateRow, error) {
			return []candidateRow{
				{id: candidateID1, kind: syncdispatchcontract.KindDispatchSyncRun},
				{id: candidateID2, kind: "future_contract_kind"},
			}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		observation, err := observer.Step(context.Background(), now, 1)
		if err != nil {
			t.Fatal(err)
		}
		if observation.UnknownKindCount != 0 || observation.SampledCandidates != 1 || !observation.Truncated {
			t.Fatalf("observation = %#v", observation)
		}
	})

	t.Run("unknown inside sample fails closed", func(t *testing.T) {
		observer, err := newObserver(testRegistry(t, ""), func(context.Context, time.Time, int) ([]candidateRow, error) {
			return []candidateRow{{id: candidateID1, kind: "unknown_kind"}}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		observation, err := observer.Step(context.Background(), now, 1)
		if !errors.Is(err, ErrUnknownKind) || observation.UnknownKindCount != 1 ||
			observation.SampledCandidates != 1 || observation.Truncated {
			t.Fatalf("Step() = %#v, %v", observation, err)
		}
	})
}

func TestCandidateDigestIncludesNormalizedSnapshotMetadata(t *testing.T) {
	observer, err := newObserver(testRegistry(t, ""), func(context.Context, time.Time, int) ([]candidateRow, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	localCutoff := time.Date(2026, time.July, 22, 5, 0, 0, 123, time.FixedZone("offset", -7*60*60))
	first, err := observer.Step(context.Background(), localCutoff, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := observer.Step(context.Background(), localCutoff.Add(time.Nanosecond), 1)
	if err != nil {
		t.Fatal(err)
	}
	third, err := observer.Step(context.Background(), localCutoff, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !first.ObservedAt.Equal(localCutoff.UTC()) || first.ObservedAt.Location() != time.UTC ||
		first.Limit != 1 || first.PredicateVersion != PredicateVersion || first.DigestVersion != DigestVersion {
		t.Fatalf("self-describing observation = %#v", first)
	}
	if first.CandidateDigest == second.CandidateDigest || first.CandidateDigest == third.CandidateDigest {
		t.Fatalf("digest did not bind cutoff and limit: first=%s second=%s third=%s",
			first.CandidateDigest, second.CandidateDigest, third.CandidateDigest)
	}
}

func TestObserverValidatesRegistryInputsAndCandidateBounds(t *testing.T) {
	validRegistry := testRegistry(t, "")
	if _, err := newObserver(validRegistry, nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil read error = %v", err)
	}
	if _, err := newObserver(registryMap{}, func(context.Context, time.Time, int) ([]candidateRow, error) { return nil, nil }); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("incomplete registry error = %v", err)
	}
	for name, mutate := range map[string]func(*syncdispatchcontract.Descriptor){
		"wrong delivery": func(descriptor *syncdispatchcontract.Descriptor) {
			descriptor.Delivery = "at_most_once_mark_before"
		},
		"wrong rollback": func(descriptor *syncdispatchcontract.Descriptor) {
			descriptor.RollbackRoute = syncdispatchcontract.RouteRiver
		},
		"invalid route": func(descriptor *syncdispatchcontract.Descriptor) {
			descriptor.Route = "shadow"
		},
	} {
		t.Run(name, func(t *testing.T) {
			registry := testRegistry(t, "")
			descriptor := registry[syncdispatchcontract.KindDispatchSyncRun]
			mutate(&descriptor)
			registry[syncdispatchcontract.KindDispatchSyncRun] = descriptor
			if _, err := newObserver(registry, func(context.Context, time.Time, int) ([]candidateRow, error) { return nil, nil }); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("newObserver error = %v", err)
			}
		})
	}

	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	for name, rows := range map[string][]candidateRow{
		"more than limit plus one": {
			{id: candidateID1, kind: frozenKinds[0]},
			{id: candidateID2, kind: frozenKinds[0]},
			{id: candidateID3, kind: frozenKinds[0]},
		},
		"duplicate id": {
			{id: candidateID1, kind: frozenKinds[0]},
			{id: candidateID1, kind: frozenKinds[1]},
		},
		"invalid id": {
			{id: "not-a-uuid", kind: frozenKinds[0]},
		},
		"live claim": {
			{id: candidateID1, kind: frozenKinds[0], claimExpiresAt: timePointer(now.Add(time.Second))},
		},
	} {
		t.Run(name, func(t *testing.T) {
			observer, err := newObserver(validRegistry, func(context.Context, time.Time, int) ([]candidateRow, error) {
				return rows, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := observer.Step(context.Background(), now, 1); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Step() error = %v", err)
			}
		})
	}
}

func TestObserverRejectsInvalidStepInputs(t *testing.T) {
	observer, err := newObserver(testRegistry(t, ""), func(context.Context, time.Time, int) ([]candidateRow, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []struct {
		now   time.Time
		limit int
	}{
		{limit: 1},
		{now: time.Now(), limit: 0},
		{now: time.Now(), limit: maximumStepLimit + 1},
	} {
		if _, err := observer.Step(context.Background(), call.now, call.limit); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("Step(%s, %d) error = %v", call.now, call.limit, err)
		}
	}
}

func TestObserveSnapshotRejectsNilTransactionBeforeAnyQuery(t *testing.T) {
	_, err := ObserveSnapshot(
		context.Background(),
		nil,
		testRegistry(t, ""),
		time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC),
		1,
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("ObserveSnapshot() error = %v", err)
	}
}

func TestObserverPreservesContextCancellationAndDeadlines(t *testing.T) {
	for name, contextErr := range map[string]error{
		"canceled": context.Canceled,
		"deadline": context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			observer, err := newObserver(testRegistry(t, ""), func(context.Context, time.Time, int) ([]candidateRow, error) {
				return nil, contextErr
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := observer.Step(context.Background(), time.Now(), 1); !errors.Is(err, contextErr) {
				t.Fatalf("Step() error = %v, want %v", err, contextErr)
			}
		})
	}
}

func TestObservationSQLIsReadOnlyAndMatchesPythonClaimWindow(t *testing.T) {
	upper := strings.ToUpper(observationSQL)
	for _, required := range []string{
		"SELECT OUTBOX.ID::TEXT, OUTBOX.KIND, OUTBOX.CLAIM_EXPIRES_AT",
		"JOIN PUBLIC.SYNC_DISPATCH_TRANSPORT_ROUTES AS ROUTE",
		"ROUTE.KIND = OUTBOX.KIND",
		"OUTBOX.STATUS = 'PENDING'",
		"OUTBOX.AVAILABLE_AT <= $1",
		"ROUTE.TRANSPORT = 'CELERY'",
		"ROUTE.PAUSED = FALSE",
		"OUTBOX.CLAIM_EXPIRES_AT IS NULL OR OUTBOX.CLAIM_EXPIRES_AT <= $1",
		"ORDER BY OUTBOX.AVAILABLE_AT, OUTBOX.ID",
		"LIMIT $2",
	} {
		if !strings.Contains(upper, required) {
			t.Fatalf("observation SQL missing %q:\n%s", required, observationSQL)
		}
	}
	for _, forbidden := range []string{
		"INSERT", "UPDATE", "DELETE", "MERGE", "LOCK", "FOR UPDATE",
		"SKIP LOCKED", "CALL", "NOTIFY", "COUNT(", "GROUP BY",
	} {
		if strings.Contains(upper, forbidden) {
			t.Fatalf("read-only bounded SQL contains %q:\n%s", forbidden, observationSQL)
		}
	}
}

func timePointer(value time.Time) *time.Time { return &value }
