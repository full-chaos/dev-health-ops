package cicd

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFenceDayKeepsNewerRowsAndRetiresDroppedLiveKeys(t *testing.T) {
	day := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	snapshot := time.Date(2026, 8, 21, 6, 0, 0, 0, time.UTC)
	older, same, later := snapshot.Add(-time.Hour), snapshot, snapshot.Add(time.Second)
	fresh, newerStored, sameStored := uuid.MustParse("00000000-0000-4000-8000-000000000001"),
		uuid.MustParse("00000000-0000-4000-8000-000000000002"), uuid.MustParse("00000000-0000-4000-8000-000000000003")
	dropped, droppedNewer, droppedRetired, droppedSame := uuid.MustParse("00000000-0000-4000-8000-000000000004"),
		uuid.MustParse("00000000-0000-4000-8000-000000000005"), uuid.MustParse("00000000-0000-4000-8000-000000000006"),
		uuid.MustParse("00000000-0000-4000-8000-000000000007")
	records := []CICDMetric{
		{RepoID: fresh, Day: day, PipelinesCount: 1, ComputedAt: snapshot},
		{RepoID: newerStored, Day: day, PipelinesCount: 2, ComputedAt: snapshot},
		{RepoID: sameStored, Day: day, PipelinesCount: 3, ComputedAt: snapshot},
	}
	stored := map[uuid.UUID]StoredKey{
		newerStored:    {ComputedAt: later, Live: true},
		sameStored:     {ComputedAt: same, Live: true},
		dropped:        {ComputedAt: older, Live: true},
		droppedNewer:   {ComputedAt: later, Live: true},
		droppedRetired: {ComputedAt: older, Live: false},
		droppedSame:    {ComputedAt: same, Live: true},
	}
	got := FenceDay(day, records, stored, snapshot)
	if len(got) != 2 {
		t.Fatalf("FenceDay = %+v, want the fresh record and one retraction row", got)
	}
	if got[0].RepoID != fresh || got[0].PipelinesCount != 1 || got[0].Retracted {
		t.Errorf("row 0 = %+v, want the record of the key with no stored row", got[0])
	}
	if got[1].RepoID != dropped || !got[1].Retracted || got[1].PipelinesCount != 0 || got[1].SuccessRate != 0 ||
		got[1].AvgDurationMinutes != nil || got[1].P90DurationMinutes != nil || got[1].AvgQueueMinutes != nil ||
		!got[1].ComputedAt.Equal(snapshot) || !got[1].Day.Equal(day) {
		t.Errorf("row 1 = %+v, want a retraction row of the dropped live key at the snapshot", got[1])
	}
}
