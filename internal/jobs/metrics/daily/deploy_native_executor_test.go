package daily

import (
	"context"
	"errors"
	"testing"
)

func TestNewDeployExecutorRejectsNilConn(t *testing.T) {
	if _, err := NewDeployExecutor(nil); !errors.Is(err, errDeployUnavailable) {
		t.Fatalf("err=%v, want errDeployUnavailable", err)
	}
}

func TestDeployComputeFamilyRejectsMissingOrganizationOrDay(t *testing.T) {
	executor, err := NewDeployExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(context.Background(), Run{}, Partition{ID: testPartitionID}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestDeployComputeFamilyRejectsUnparseablePartitionRepoIDs(t *testing.T) {
	executor, err := NewDeployExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: testOrgID, TargetDay: mustParseDay(t, "2026-08-24")}
	partition := Partition{ID: testPartitionID, RepoIDs: []RepositoryID{"not-a-uuid"}}
	if _, err := executor.ComputeFamily(context.Background(), run, partition); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

// TestDeployComputeFamilyNoRepoIDsIsNoop proves an empty partition scope
// short-circuits BEFORE any ClickHouse round-trip -- stubDriverConn's every
// method panics, so reaching one would fail this test loudly rather than
// silently returning zero rows for the wrong reason.
func TestDeployComputeFamilyNoRepoIDsIsNoop(t *testing.T) {
	executor, err := NewDeployExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: testOrgID, TargetDay: mustParseDay(t, "2026-08-24")}
	partition := Partition{ID: testPartitionID}
	written, err := executor.ComputeFamily(context.Background(), run, partition)
	if err != nil {
		t.Fatal(err)
	}
	if written != 0 {
		t.Fatalf("written=%d, want 0", written)
	}
}
