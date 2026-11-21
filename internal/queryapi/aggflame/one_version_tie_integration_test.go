//go:build integration

package aggflame

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Two versions of one work_item_state_durations_daily key that share computed_at serve ONE version in the cycle
// breakdown, whatever the insert order and whether the parts merged: the version with the larger (computed_at,
// cityHash64 of its measures). No team and one team serve that same version, and the live-row rule is read from it.

func flameTieCase(t *testing.T, name string, a, b flameTeamViewRow) {
	ctx, admin, client := startFlameTeamView(t)
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES work_item_state_durations_daily"); err != nil {
		t.Fatal(err)
	}
	read := func(org, team string) string {
		rows, err := fetchCycleBreakdown(ctx, client, org, flameTeamViewDay(flameD0), flameTeamViewDay(flameD2).AddDate(0, 0, 1), team, "", "")
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, r := range rows {
			parts = append(parts, fmt.Sprintf("%s=%g", r.Status, r.TotalHours))
		}
		sort.Strings(parts)
		return strings.Join(parts, ",")
	}
	var orgs []string
	for _, order := range [][2]flameTeamViewRow{{a, b}, {b, a}} {
		org := fmt.Sprintf("org-14430-flame-%s-%g-then-%g", name, order[0].hours, order[1].hours)
		orgs = append(orgs, org)
		seedFlameTeamView(ctx, t, admin, org, []flameTeamViewRow{order[0]})
		seedFlameTeamView(ctx, t, admin, org, []flameTeamViewRow{order[1]})
	}
	// The winner: the stored row with the largest (computed_at, cityHash64 of the three measures). The merge keeps one
	// stored version (the last inserted of a tie), so it is read again after it.
	winner := func(org string) string {
		var hours float64
		if err := admin.QueryRow(ctx, `SELECT duration_hours FROM work_item_state_durations_daily WHERE org_id = ?
			ORDER BY computed_at DESC, cityHash64(tuple(duration_hours, items_touched, avg_wip)) DESC LIMIT 1`, org).Scan(&hours); err != nil {
			t.Fatal(err)
		}
		if hours == 0 {
			return ""
		}
		return fmt.Sprintf("blocked=%g", hours)
	}
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			if err := admin.Exec(ctx, "SYSTEM START MERGES work_item_state_durations_daily"); err != nil {
				t.Fatal(err)
			}
			if err := admin.Exec(ctx, "OPTIMIZE TABLE work_item_state_durations_daily FINAL"); err != nil {
				t.Fatal(err)
			}
		}
		for _, org := range orgs {
			w := winner(org)
			t.Logf("%s %s: winner %q", phase, org, w)
			for _, team := range []string{"", "P"} {
				wantFlame(t, fmt.Sprintf("%s %s team=%q", phase, org, team), read(org, team), w)
			}
		}
	}
}

func TestCycleBreakdownServesOneVersionOnAComputedAtTie(t *testing.T) {
	t1, _, _ := flameTeamViewVersions()
	flameTieCase(t, "two-live",
		flameTeamViewRow{flameD2, "P", "blocked", 5, 1, t1, "org"},
		flameTeamViewRow{flameD2, "P", "blocked", 9, 2, t1, "org"})
}

// A live version and a retraction row (every measure 0) of one computed_at: the key is present iff the picked version
// is live.
func TestCycleBreakdownTieOfALiveAndARetractedVersion(t *testing.T) {
	t1, _, _ := flameTeamViewVersions()
	flameTieCase(t, "live-retracted",
		flameTeamViewRow{flameD2, "P", "blocked", 6, 1, t1, "org"},
		flameTeamViewRow{flameD2, "P", "blocked", 0, 0, t1, "org"})
}

// No tie: the newer version serves (the answer of main), no team and one team, before and after the merge.
func TestCycleBreakdownNoTieControl(t *testing.T) {
	ctx, admin, client := startFlameTeamView(t)
	t1, t2, _ := flameTeamViewVersions()
	const org = "org-14430-flame-no-tie"
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES work_item_state_durations_daily"); err != nil {
		t.Fatal(err)
	}
	seedFlameTeamView(ctx, t, admin, org, []flameTeamViewRow{{flameD2, "P", "blocked", 5, 1, t2, "org"}})
	seedFlameTeamView(ctx, t, admin, org, []flameTeamViewRow{{flameD2, "P", "blocked", 9, 2, t1, "org"}})
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			if err := admin.Exec(ctx, "SYSTEM START MERGES work_item_state_durations_daily"); err != nil {
				t.Fatal(err)
			}
			if err := admin.Exec(ctx, "OPTIMIZE TABLE work_item_state_durations_daily FINAL"); err != nil {
				t.Fatal(err)
			}
		}
		for _, team := range []string{"", "P"} {
			rows, err := fetchCycleBreakdown(ctx, client, org, flameTeamViewDay(flameD0), flameTeamViewDay(flameD2).AddDate(0, 0, 1), team, "", "")
			if err != nil {
				t.Fatal(err)
			}
			got := ""
			for _, r := range rows {
				got += fmt.Sprintf("%s=%g", r.Status, r.TotalHours)
			}
			wantFlame(t, fmt.Sprintf("%s team=%q", phase, team), got, "blocked=5")
		}
	}
}
