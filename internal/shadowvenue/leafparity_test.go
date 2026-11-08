package shadowvenue

import (
	"strings"
	"testing"
)

func leaves(id, ts string, extra ...Leaf) []Leaf {
	return append([]Leaf{{"$.id", id}, {"$.ts", ts}}, extra...)
}

// leafRun is a strict run whose every sent slot has the same two leaves in both runs but for the volatile $.ts, which
// differs between the runs on row 1; $.ts of row 1 is frozen volatile.
func leafRun(frozen string) *enfRun {
	r := strictEnfRun()
	vol, err := ParseFrozenVolatile([]byte(frozen))
	if err != nil {
		panic(err)
	}
	p := &LeafParity{Volatile: vol, Yard: map[int][]Leaf{}, Enforced: map[int][]Leaf{}}
	for _, y := range r.in.Yard {
		p.Yard[y.Seq] = leaves("aaaaaaaaaaaa", "bbbbbbbbbbbb")
	}
	for _, s := range r.in.Slots {
		if s.Gap() {
			continue
		}
		ts := "bbbbbbbbbbbb"
		if s.Row == "1" || s.Row == "2" {
			ts = "cccccccccccc"
		}
		p.Enforced[s.Seq] = leaves("aaaaaaaaaaaa", ts)
	}
	r.in.Leaves = p
	return r
}

// cellSeq is the slot seq of row 1, k=1 of the caller in the enforced run, or in the yardstick run.
func cellSeq(r *enfRun, caller string, yard bool) int {
	if yard {
		for _, y := range r.in.Yard {
			if y.Row == "1" && y.Caller == caller && y.K == 1 {
				return y.Seq
			}
		}
	}
	for _, s := range r.in.Slots {
		if s.Row == "1" && s.Caller == caller && s.K == 1 {
			return s.Seq
		}
	}
	panic("no such cell")
}

func TestLeafParityDropsTheFrozenVolatileLeavesOfTheirRowOnly(t *testing.T) {
	cases := map[string]struct {
		frozen string
		plant  func(*enfRun)
		want   string
		void   string
	}{
		"volatile leaf of the right rows is dropped": {frozen: "1\t$.ts\n2\t$.ts\n", want: ""},
		"volatile listed for the wrong row":          {frozen: "9\t$.ts\n", want: VerdictMismatchBody},
		"volatile of row 1 does not cover row 2":     {frozen: "1\t$.ts\n", want: VerdictMismatchBody},
		"another path is not volatile":               {frozen: "1\t$.id\n2\t$.id\n", want: VerdictMismatchBody},
		"a leaf only the enforced answer has": {frozen: "1\t$.ts\n2\t$.ts\n", want: VerdictMismatchBody, plant: func(r *enfRun) {
			seq := cellSeq(r, "member", false)
			r.in.Leaves.Enforced[seq] = append(r.in.Leaves.Enforced[seq], Leaf{"$.extra", "dddddddddddd"})
		}},
		"a leaf only the yardstick has": {frozen: "1\t$.ts\n2\t$.ts\n", want: VerdictMismatchBody, plant: func(r *enfRun) {
			seq := cellSeq(r, "member", true)
			r.in.Leaves.Yard[seq] = append(r.in.Leaves.Yard[seq], Leaf{"$.extra", "dddddddddddd"})
		}},
		"a side line missing for an enforced slot": {frozen: "1\t$.ts\n2\t$.ts\n", void: "side file", plant: func(r *enfRun) { delete(r.in.Leaves.Enforced, r.in.Slots[6].Seq) }},
		"a side line missing for a yardstick slot": {frozen: "1\t$.ts\n2\t$.ts\n", void: "side file", plant: func(r *enfRun) { delete(r.in.Leaves.Yard, r.in.Yard[6].Seq) }},
		"the old digest does not decide when the leaves are given": {frozen: "1\t$.ts\n2\t$.ts\n", want: "", plant: func(r *enfRun) {
			for i := range r.in.Slots {
				if !r.in.Slots[i].Gap() {
					r.in.Slots[i].BodyNormSHA = bodyB
				}
			}
		}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			run := leafRun(c.frozen)
			if c.plant != nil {
				c.plant(run)
			}
			report := run.judge()
			text := strings.ToLower(strings.Join(report.Reasons, "\n"))
			switch {
			case c.void != "":
				if !report.Void || report.Exit() != 3 || !strings.Contains(text, c.void) {
					t.Fatalf("want void with %q: void %v exit %d %v", c.void, report.Void, report.Exit(), report.Reasons)
				}
			case c.want == "":
				if report.Void || !report.Pass || report.VerdictCounts()[VerdictMismatchBody] != 0 {
					t.Fatalf("want a pass: void %v pass %v %v %v", report.Void, report.Pass, report.VerdictCounts(), report.Reasons)
				}
			default:
				if report.Void || report.Pass || report.VerdictCounts()[c.want] == 0 {
					t.Fatalf("want a failed run with %s: void %v pass %v %v", c.want, report.Void, report.Pass, report.VerdictCounts())
				}
			}
		})
	}
}

func TestParseFrozenVolatileRefusesAnythingButSortedUniqueRowPathLines(t *testing.T) {
	good, err := ParseFrozenVolatile([]byte("1\t$.a\n1\t$.b\n2\t$.a\n"))
	if err != nil || len(good.ByRow["1"]) != 2 || good.PathsPerRow() != "row 1: 2, row 2: 1" {
		t.Fatalf("good file: %v %+v %q", err, good, good.PathsPerRow())
	}
	if v, err := ParseFrozenVolatile(nil); err != nil || len(v.ByRow) != 0 {
		t.Fatalf("an empty file is the empty set: %v", err)
	}
	for name, raw := range map[string]string{
		"a comment line":           "# note\n1\t$.a\n",
		"a blank line":             "1\t$.a\n\n1\t$.b\n",
		"a junk line":              "1\t$.a\njunk\n",
		"a third column":           "1\t$.a\textra\n",
		"a path that is no path":   "1\ta\n",
		"an empty row":             "\t$.a\n",
		"unsorted":                 "2\t$.a\n1\t$.a\n",
		"a duplicate line":         "1\t$.a\n1\t$.a\n",
		"no final newline":         "1\t$.a",
		"a row with a space":       "1 2\t$.a\n",
		"a counts line (bodydiff)": "bodydiff: 3 cells differ\n",
	} {
		if _, err := ParseFrozenVolatile([]byte(raw)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestDiffPathLinesAreRowTabPathSortedUnique(t *testing.T) {
	a := []Slot{{Seq: 1, Row: "2", Caller: "admin", K: 1}, {Seq: 2, Row: "2", Caller: "admin", K: 2}, {Seq: 3, Row: "10", Caller: "member", K: 1}, {Seq: 4, Row: "10", Caller: "gap", K: 1, Note: "gap: x"}}
	b := []Slot{{Seq: 7, Row: "2", Caller: "admin", K: 1}, {Seq: 8, Row: "2", Caller: "admin", K: 2}, {Seq: 9, Row: "10", Caller: "member", K: 1}, {Seq: 10, Row: "10", Caller: "gap", K: 1}}
	la := map[int][]Leaf{1: {{"$.ts", "aaaaaaaaaaaa"}, {"$.id", "x"}}, 2: {{"$.ts", "bbbbbbbbbbbb"}, {"$.id", "x"}}, 3: {{"$.id", "x"}}, 4: {{"$.z", "q"}}}
	lb := map[int][]Leaf{7: {{"$.ts", "cccccccccccc"}, {"$.id", "x"}}, 8: {{"$.ts", "dddddddddddd"}, {"$.id", "x"}}, 9: {{"$.id", "x"}, {"$.new", "n"}}, 10: {{"$.z", "r"}}}
	got := strings.Join(DiffPathLines(a, b, la, lb), "|")
	if got != "10\t$.new|2\t$.ts" {
		t.Fatalf("lines %q", got)
	}
}
