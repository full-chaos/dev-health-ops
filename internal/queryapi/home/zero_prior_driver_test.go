package home

import (
	"strings"
	"testing"
)

// A move from a measured 0 (both windows measured, prior 0) in the no-team and team-set forms (CHAOS-14441): the
// summary sentence and the signal caption name the driver, from one driver read each in the absolute form, with the
// value's own reducer (sum for throughput and blocked_work) and, for blocked_work, the blocked rows only
// (CHAOS-14425). Main b737db18f3b2 named no caption and a summary driver only for blocked_work, from every status
// (log l14441-red-1.log 85e08322e742).
func TestHomeZeroPriorNoTeamAndTeamSetNameTheDriver(t *testing.T) {
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		reducer := map[string]string{"cycle_time": "avg", "throughput": "sum", "wip_saturation": "avg", "blocked_work": "sum"}[metric]
		for _, form := range []struct {
			name string
			ids  []string
		}{{"no-team", nil}, {"team-set", []string{driverDoubleTeam, "jira:other"}}, {"team-set-duplicate", []string{driverDoubleTeam, "jira:other", driverDoubleTeam}}} {
			resp, summaryRead, captionRead := driverDoubleHome(t, metric, false, true, form.ids, "from-zero")
			d := driverDoubleDelta(resp, metric)
			if !d.FromZero() {
				t.Fatalf("%s %s: not a move from a measured 0: %+v", metric, form.name, d)
			}
			summary, captions := driverDoubleSummary(resp)
			t.Logf("ZERO-PRIOR %s %s summary=%q captions=%q summary-read=%s caption-read=%s", metric, form.name, summary, captions, summaryRead, captionRead)
			if !strings.HasSuffix(summary, " driven by Platform.") {
				t.Errorf("NAMED %s %s: summary %q names no driver", metric, form.name, summary)
			}
			if len(captions) != 1 || !strings.Contains(captions[0], "Platform") {
				t.Errorf("NAMED %s %s: captions %q name no driver", metric, form.name, captions)
			}
			for _, read := range []struct {
				name string
				r    driverDoubleRead
			}{{"summary", summaryRead}, {"caption", captionRead}} {
				if !read.r.zero || read.r.currentSQL != reducer || read.r.priorSQL != reducer || read.r.dayFilter != (metric == "blocked_work") ||
					read.r.absentIsZero != (reducer == "sum") {
					t.Errorf("MEASURE %s %s: the %s read is %s, want the absolute form, reducer %s, blocked-only %t, absent prior counted as 0 %t",
						metric, form.name, read.name, read.r, reducer, metric == "blocked_work", reducer == "sum")
				}
			}
		}
	}
}

// A team with no prior row beside a team with a measured 0 (fcdev ruling 11-21, option (a)): a summed value counts the
// absent prior as 0, as the value does, so that team is a driver, ranked by its rise (New 10 or 20 before Platform 4
// or 8); an average needs a measured 0, so only Platform is named.
func TestHomeZeroPriorAbsentPriorTeamFollowsTheReducer(t *testing.T) {
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		sums := metric == "throughput" || metric == "blocked_work"
		wantSummary, wantCaption := " driven by Platform.", " appears to have moved most in Platform."
		if sums {
			wantSummary, wantCaption = " driven by New, Platform.", " appears to have moved most in New and Platform."
		}
		for _, ids := range [][]string{nil, {driverDoubleTeam, driverDoubleNewTeam}} {
			resp, summaryRead, captionRead := driverDoubleHome(t, metric, false, true, ids, "from-zero-absent")
			if d := driverDoubleDelta(resp, metric); !d.FromZero() {
				t.Fatalf("%s: not a move from a measured 0: %+v", metric, d)
			}
			summary, captions := driverDoubleSummary(resp)
			t.Logf("ABSENT %s teams=%d summary=%q captions=%q summary-read=%s caption-read=%s", metric, len(ids), summary, captions, summaryRead, captionRead)
			if !strings.HasSuffix(summary, wantSummary) {
				t.Errorf("ABSENT %s teams=%d: summary %q, want suffix %q", metric, len(ids), summary, wantSummary)
			}
			if len(captions) != 1 || !strings.HasSuffix(captions[0], wantCaption) {
				t.Errorf("ABSENT %s teams=%d: captions %q, want suffix %q", metric, len(ids), captions, wantCaption)
			}
		}
	}
}

// No team and a team set: a steady value (0 -> 0) and a value with no prior window name no driver and send no driver
// read.
func TestHomeNoTeamDriverControls(t *testing.T) {
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		for _, scenario := range []string{"steady", "missing"} {
			for _, ids := range [][]string{nil, {driverDoubleTeam, "jira:other"}} {
				resp, summaryRead, captionRead := driverDoubleHome(t, metric, false, true, ids, scenario)
				summary, captions := driverDoubleSummary(resp)
				t.Logf("CONTROL %s %s teams=%d summary=%q captions=%q", metric, scenario, len(ids), summary, captions)
				if strings.Contains(summary, "driven by") || len(captions) > 0 || summaryRead.currentSQL != "" || captionRead.currentSQL != "" {
					t.Errorf("CONTROL %s %s teams=%d: summary %q captions %q reads %s / %s, want no driver and no driver read",
						metric, scenario, len(ids), summary, captions, summaryRead, captionRead)
				}
			}
		}
	}
}
