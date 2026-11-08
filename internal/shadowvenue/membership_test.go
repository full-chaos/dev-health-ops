package shadowvenue

import (
	"strings"
	"testing"
	"time"
)

var memT0 = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

func snapText(rows ...string) []byte {
	return []byte(snapshotHeader + "\n" + strings.Join(rows, "\n") + "\n")
}

func TestParseMembershipSnapshotReadsRowsAndRefusesMalformedOnes(t *testing.T) {
	s, err := ParseMembershipSnapshot(snapText(claimUser + "\t" + claimOrgE + "\t0\t0\t" + memT0.Format(time.RFC3339)))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := s.Counts(claimUser, claimOrgE); !ok || got != [2]int{0, 0} || !s.TakenAt.Equal(memT0) {
		t.Fatalf("%v %v %v", got, ok, s.TakenAt)
	}
	for name, raw := range map[string]string{
		"no header":    claimUser + "\t" + claimOrgE + "\t0\t0\t" + memT0.Format(time.RFC3339) + "\n",
		"five fields":  snapshotHeader + "\n" + claimUser + "\t" + claimOrgE + "\t0\t0\n",
		"negative":     snapshotHeader + "\n" + claimUser + "\t" + claimOrgE + "\t-1\t0\t" + memT0.Format(time.RFC3339) + "\n",
		"not a number": snapshotHeader + "\n" + claimUser + "\t" + claimOrgE + "\tx\t0\t" + memT0.Format(time.RFC3339) + "\n",
		"bad uuid":     snapshotHeader + "\nu\t" + claimOrgE + "\t0\t0\t" + memT0.Format(time.RFC3339) + "\n",
		"bad time":     snapshotHeader + "\n" + claimUser + "\t" + claimOrgE + "\t0\t0\tnow\n",
		"twice":        snapshotHeader + "\n" + claimUser + "\t" + claimOrgE + "\t0\t0\t" + memT0.Format(time.RFC3339) + "\n" + claimUser + "\t" + claimOrgE + "\t0\t0\t" + memT0.Format(time.RFC3339) + "\n",
		"no rows":      snapshotHeader + "\n",
		"two stamps":   snapshotHeader + "\n" + claimUser + "\t" + claimOrgE + "\t0\t0\t" + memT0.Format(time.RFC3339) + "\n" + claimUser + "\t" + bdOrg1 + "\t0\t0\t" + memT0.Add(time.Minute).Format(time.RFC3339) + "\n",
	} {
		if _, err := ParseMembershipSnapshot([]byte(raw)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
