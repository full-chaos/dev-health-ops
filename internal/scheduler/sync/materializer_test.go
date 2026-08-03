package sync

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRequestedDatasetKeysPreservesLiveConfigRoutingSemantics(t *testing.T) {
	child := "00000000-0000-4000-8000-000000000001"
	if got := requestedDatasetKeys("jira", []string{"work-items"}, nil); got != nil {
		t.Fatalf("parent dataset scope=%v, want all enabled (nil)", got)
	}
	wantChild := map[string]bool{
		"work-items": true, "work-item-labels": true, "work-item-projects": true,
		"work-item-history": true, "work-item-comments": true,
	}
	if got := requestedDatasetKeys("jira", []string{"work-items"}, &child); !reflect.DeepEqual(got, wantChild) {
		t.Fatalf("recognized child dataset scope=%v, want %v", got, wantChild)
	}
	if got := requestedDatasetKeys("jira", []string{"not-a-provider-target"}, &child); got != nil {
		t.Fatalf("unrecognized child dataset scope=%v, want all enabled (nil)", got)
	}
}

func TestNewNativeMaterializerPortsPythonEnvironmentBounds(t *testing.T) {
	t.Setenv("SYNC_WATERMARK_OVERLAP", "-10")
	t.Setenv("SYNC_RUN_MAX_UNITS", "0")
	materializer, err := NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	if materializer.watermarkOverlap != 0 || materializer.defaultUnitCap != 1 {
		t.Fatalf("bounded settings: overlap=%s cap=%d", materializer.watermarkOverlap, materializer.defaultUnitCap)
	}

	t.Setenv("SYNC_WATERMARK_OVERLAP", "invalid")
	t.Setenv("SYNC_RUN_MAX_UNITS", "invalid")
	materializer, err = NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	if materializer.watermarkOverlap != 0*time.Second || materializer.defaultUnitCap != 1000 {
		t.Fatalf("fallback settings: overlap=%s cap=%d", materializer.watermarkOverlap, materializer.defaultUnitCap)
	}
}

func TestDeterministicMaterializationIDsAreStableAndPartitioned(t *testing.T) {
	first, err := deterministicMaterializationIDs("occurrence-v1:abc")
	if err != nil {
		t.Fatal(err)
	}
	second, err := deterministicMaterializationIDs("occurrence-v1:abc")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("replay changed graph identities: first=%+v second=%+v", first, second)
	}
	seen := map[string]bool{}
	for _, value := range []string{
		first.JobRunID,
		first.SyncRunID,
		first.ReferenceDiscoveryID,
		first.DispatchOutboxID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			t.Fatalf("derived invalid UUID %q: %v", value, err)
		}
		if seen[value] {
			t.Fatalf("different graph rows share deterministic ID %q", value)
		}
		seen[value] = true
	}

	other, err := deterministicMaterializationIDs("occurrence-v1:def")
	if err != nil {
		t.Fatal(err)
	}
	if first.SyncRunID == other.SyncRunID {
		t.Fatal("different occurrences share a sync-run identity")
	}
}

func TestDeterministicUnitIDsUseRunAndOrdinal(t *testing.T) {
	ids, err := deterministicMaterializationIDs("occurrence-v1:abc")
	if err != nil {
		t.Fatal(err)
	}
	first, err := deterministicUnitID(ids.SyncRunID, 0)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := deterministicUnitID(ids.SyncRunID, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := deterministicUnitID(ids.SyncRunID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first != replay || first == second {
		t.Fatalf("unit identities are not stable and ordinal-partitioned: first=%s replay=%s second=%s", first, replay, second)
	}
}
