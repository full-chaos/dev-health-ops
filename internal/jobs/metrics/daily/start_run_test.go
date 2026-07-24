package daily

import (
	"testing"
	"time"
)

func TestNormalizeStartRunRequestIsDuplicateStableAndBounded(t *testing.T) {
	t.Parallel()
	const (
		org   = "00000000-0000-4000-8000-000000000001"
		repoA = "00000000-0000-4000-8000-000000000002"
		repoB = "00000000-0000-4000-8000-000000000003"
	)
	request, partitions, err := normalizeStartRunRequest(StartRunRequest{
		OrganizationID: org,
		TargetDay:      time.Date(2026, 7, 23, 18, 0, 0, 0, time.FixedZone("offset", -7*60*60)),
		Generation:     "post-sync:00000000-0000-4000-8000-000000000004",
		RepositoryIDs:  []string{repoB, repoA, repoB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := request.TargetDay.UTC().Format("2006-01-02"), "2026-07-24"; got != want {
		t.Fatalf("target day=%s want=%s", got, want)
	}
	if len(partitions) != 1 || len(partitions[0]) != 2 ||
		partitions[0][0] != repoA || partitions[0][1] != repoB {
		t.Fatalf("partitions=%#v", partitions)
	}
}

func TestNormalizeStartRunRequestCreatesOneOrgPartitionWithoutRepositories(t *testing.T) {
	t.Parallel()
	_, partitions, err := normalizeStartRunRequest(StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000001",
		TargetDay:      time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC),
		Generation:     "post-sync:00000000-0000-4000-8000-000000000004",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 1 || len(partitions[0]) != 0 {
		t.Fatalf("partitions=%#v", partitions)
	}
}

func TestNormalizeStartRunRequestRejectsNonAuthoritativeReferences(t *testing.T) {
	t.Parallel()
	for _, request := range []StartRunRequest{
		{OrganizationID: "bad", TargetDay: time.Now(), Generation: "generation"},
		{OrganizationID: "00000000-0000-4000-8000-000000000001", Generation: "generation"},
		{
			OrganizationID: "00000000-0000-4000-8000-000000000001",
			TargetDay:      time.Now(),
			Generation:     "generation",
			RepositoryIDs:  []string{"repo-slug"},
		},
	} {
		if _, _, err := normalizeStartRunRequest(request); err != ErrInvalidState {
			t.Fatalf("request=%#v err=%v", request, err)
		}
	}
}

func TestDailyPartitionIDIsStablePerOrdinal(t *testing.T) {
	t.Parallel()
	runID := "00000000-0000-4000-8000-000000000001"
	if dailyPartitionID(runID, 0) != dailyPartitionID(runID, 0) {
		t.Fatal("partition identity changed across duplicate generation")
	}
	if dailyPartitionID(runID, 0) == dailyPartitionID(runID, 1) {
		t.Fatal("partition ordinals collided")
	}
}
