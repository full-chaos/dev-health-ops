// Package helmrev is the helm history gates of `dho deploy`: the live newest
// revision re-read right before a write (kit newest_snippet and the v3z1
// FAILED_REVS form), ROLLBACK_REV == the LIVEREV read live (g_rbx), and the
// rollback revision is in the history (g_HAS). It parses the real `helm
// history` table (tab-separated, padded columns).
package helmrev

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/deploy/run"
)

// Release is one helm release (kit REL_ARGS).
type Release struct {
	Name      string
	Namespace string
}

// DevHealth is the kit `dev-health -n dev-health`.
var DevHealth = Release{Name: "dev-health", Namespace: "dev-health"}

// HistoryCmd is the read-only `helm history <name> -n <ns> --max <n>`.
func (rel Release) HistoryCmd(max int) run.Cmd {
	return run.Cmd{Argv: []string{"helm", "history", rel.Name, "-n", rel.Namespace, "--max", strconv.Itoa(max)}}
}

// Row is one history row.
type Row struct {
	Rev    int
	Status string
}

var header = []string{"REVISION", "UPDATED", "STATUS", "CHART", "APP VERSION", "DESCRIPTION"}

// ParseHistory reads the helm history table: the exact header, then rows of
// 6 tab-separated columns with an ascending revision.
func ParseHistory(out []byte) ([]Row, error) {
	ls := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(ls) < 2 {
		return nil, fmt.Errorf("helm history: no row")
	}
	if !cellsAre(strings.Split(ls[0], "\t"), header) {
		return nil, fmt.Errorf("helm history: not the helm header")
	}
	var rows []Row
	for i, l := range ls[1:] {
		c := strings.Split(l, "\t")
		if len(c) != len(header) {
			return nil, fmt.Errorf("helm history: row %d has %d columns", i+1, len(c))
		}
		rev, err := strconv.Atoi(strings.TrimSpace(c[0]))
		if err != nil || rev <= 0 {
			return nil, fmt.Errorf("helm history: row %d has no revision", i+1)
		}
		if len(rows) > 0 && rev <= rows[len(rows)-1].Rev {
			return nil, fmt.Errorf("helm history: row %d is not in revision order", i+1)
		}
		st := strings.TrimSpace(c[2])
		if st == "" || strings.ContainsAny(st, " ") {
			return nil, fmt.Errorf("helm history: row %d has no status", i+1)
		}
		rows = append(rows, Row{Rev: rev, Status: st})
	}
	return rows, nil
}

func cellsAre(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if strings.TrimRight(got[i], " ") != want[i] {
			return false
		}
	}
	return true
}

// Newest is a passed newest-revision check.
type Newest struct {
	Rev    int
	Failed []int
}

func (n Newest) Record() string {
	if len(n.Failed) == 0 {
		return fmt.Sprintf("NEWEST %d deployed = the pre LIVEREV", n.Rev)
	}
	return fmt.Sprintf("NEWEST deployed %d = the pre LIVEREV; rows above it %s", n.Rev, failedForm(n.Failed))
}

func failedForm(failed []int) string {
	p := make([]string, len(failed))
	for i, f := range failed {
		p[i] = strconv.Itoa(f) + ":failed"
	}
	return strings.Join(p, ",")
}

// CheckNewest is the kit rule on a parsed history: with no failed rows, the
// newest row is liveRev, deployed; with failed rows, the newest deployed row
// is liveRev and the rows above it are exactly failed, in order, each failed.
func CheckNewest(rows []Row, liveRev int, failed []int) (Newest, error) {
	got := Newest{Rev: liveRev, Failed: failed}
	if len(rows) == 0 {
		return Newest{}, run.Stop("live history has no row")
	}
	if len(failed) == 0 {
		nw := rows[len(rows)-1]
		if nw.Rev != liveRev || nw.Status != "deployed" {
			return Newest{}, run.Stop("live newest revision [%d %s] is not the pre LIVEREV %d deployed", nw.Rev, nw.Status, liveRev)
		}
		return got, nil
	}
	dp := -1
	for i, r := range rows {
		if r.Status == "deployed" {
			dp = i
		}
	}
	var above []string
	if dp >= 0 {
		for _, r := range rows[dp+1:] {
			above = append(above, fmt.Sprintf("%d:%s", r.Rev, r.Status))
		}
	}
	if dp < 0 || rows[dp].Rev != liveRev || strings.Join(above, ",") != failedForm(failed) {
		dr := 0
		if dp >= 0 {
			dr = rows[dp].Rev
		}
		return Newest{}, run.Stop("live newest deployed revision [%d] and rows above it [%s] are not the pre LIVEREV %d and the failed rows %s", dr, strings.Join(above, ","), liveRev, failedForm(failed))
	}
	return got, nil
}

// NewestIs reads the live history (--max 1, or failed rows + 2) and checks it.
func NewestIs(ctx context.Context, r run.Runner, rel Release, liveRev int, failed []int) (Newest, error) {
	max := 1
	if len(failed) > 0 {
		max = len(failed) + 2
	}
	o, err := r.Run(ctx, rel.HistoryCmd(max))
	if err != nil || o.RC != 0 {
		return Newest{}, run.Stop("helm history failed")
	}
	rows, perr := ParseHistory(o.Stdout)
	if perr != nil {
		return Newest{}, run.Stop("%v", perr)
	}
	return CheckNewest(rows, liveRev, failed)
}

// Guarded runs next right after NewestIs passes, with no other command
// between them (the kit runs both in the same ssh). A failed check runs
// nothing.
func Guarded(ctx context.Context, r run.Runner, rel Release, liveRev int, failed []int, next run.Cmd) (Newest, run.Output, error) {
	nw, err := NewestIs(ctx, r, rel, liveRev, failed)
	if err != nil {
		return Newest{}, run.Output{}, err
	}
	o, err := r.Run(ctx, next)
	return nw, o, err
}

// Live is the kit I1 read: the newest deployed revision of the history.
type Live struct{ Rev int }

func (l Live) Record() string { return fmt.Sprintf("STATE LIVEREV=%d", l.Rev) }

// LiveRev reads the history (--max 256, the helm default) and returns the
// newest deployed revision and the rows.
func LiveRev(ctx context.Context, r run.Runner, rel Release) (Live, []Row, error) {
	o, err := r.Run(ctx, rel.HistoryCmd(256))
	if err != nil || o.RC != 0 {
		return Live{}, nil, run.Stop("helm history failed")
	}
	rows, perr := ParseHistory(o.Stdout)
	if perr != nil {
		return Live{}, nil, run.Stop("%v", perr)
	}
	live := 0
	for _, r := range rows {
		if r.Status == "deployed" {
			live = r.Rev
		}
	}
	if live == 0 {
		return Live{}, nil, run.Stop("helm history: no deployed revision")
	}
	return Live{Rev: live}, rows, nil
}

// RollbackGate is I1 + g_rbx + g_HAS on one history read; a miss stops
// before any later read.
func RollbackGate(ctx context.Context, r run.Runner, rel Release, rollbackRev int) (Live, error) {
	live, rows, err := LiveRev(ctx, r, rel)
	if err != nil {
		return Live{}, err
	}
	if err := RollbackIsLive(rollbackRev, live.Rev); err != nil {
		return Live{}, err
	}
	if _, err := InHistory(rows, rollbackRev); err != nil {
		return Live{}, err
	}
	return live, nil
}

// RollbackIsLive is g_rbx: the inputs ROLLBACK_REV equals the LIVEREV this pre read live.
func RollbackIsLive(rollbackRev, liveRev int) error {
	if rollbackRev != liveRev {
		return run.Stop("GATE ROLLBACK_REV: inputs ROLLBACK_REV %d != the LIVEREV %d read live", rollbackRev, liveRev)
	}
	return nil
}

// InHistory is g_HAS: the rollback revision is a row of the history.
func InHistory(rows []Row, rev int) (Row, error) {
	for _, r := range rows {
		if r.Rev == rev {
			return r, nil
		}
	}
	return Row{}, run.Stop("GATE HISTORY: rollback rev %d is not in the helm history", rev)
}
