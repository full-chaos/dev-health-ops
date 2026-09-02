package daily

import (
	"context"
	"errors"
	"testing"
)

func TestNewCICDExecutorRejectsNilConn(t *testing.T) {
	if _, err := NewCICDExecutor(nil); !errors.Is(err, errCICDUnavailable) {
		t.Fatalf("err=%v, want errCICDUnavailable", err)
	}
}

func TestCICDComputeFamilyRejectsMissingOrganizationOrDay(t *testing.T) {
	executor, err := NewCICDExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(context.Background(), Run{}, Partition{ID: testPartitionID}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestCICDComputeFamilyRejectsUnparseablePartitionRepoIDs(t *testing.T) {
	executor, err := NewCICDExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: testOrgID, TargetDay: mustParseDay(t, "2026-08-24")}
	partition := Partition{ID: testPartitionID, RepoIDs: []RepositoryID{"not-a-uuid"}}
	if _, err := executor.ComputeFamily(context.Background(), run, partition); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestCICDComputeFamilyNoOpsOnEmptyPartitionRepoIDs(t *testing.T) {
	executor, err := NewCICDExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: testOrgID, TargetDay: mustParseDay(t, "2026-08-24")}
	partition := Partition{ID: testPartitionID}
	rowsWritten, err := executor.ComputeFamily(context.Background(), run, partition)
	if err != nil {
		t.Fatalf("err=%v, want nil (no repositories is a no-op, not an error)", err)
	}
	if rowsWritten != 0 {
		t.Fatalf("rowsWritten=%d, want 0", rowsWritten)
	}
}
