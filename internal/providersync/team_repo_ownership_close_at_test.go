package providersync

import (
	"testing"
	"time"
)

func TestTeamRepoOwnershipCloseAtNeverLandsAtOrBeforeItsOpen(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	cases := map[string]time.Time{
		"open before now":   now.Add(-time.Hour),
		"open in same ms":   now,
		"open ahead of now": now.Add(5 * time.Second),
	}
	for name, validFrom := range cases {
		closeAt := teamRepoOwnershipCloseAt(validFrom, now)
		if !closeAt.After(validFrom) {
			t.Errorf("%s: close %v is not after open %v", name, closeAt, validFrom)
		}
	}
	if got := teamRepoOwnershipCloseAt(now.Add(-time.Hour), now); !got.Equal(now) {
		t.Errorf("a normal close must stay at now, got %v", got)
	}
}
