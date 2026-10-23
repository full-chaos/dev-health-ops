package pgmigrate

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The chain through FrozenHead is frozen with the Python goldens. A revision above it is Go-only and is judged here,
// by checks Go owns (CHAOS-10751). tailProblems is the pure judgement; the tests run it on the real chain (an empty
// tail today) and on planted fixtures under testdata/postfreeze, which are never part of the real sql dir.

var downStatement = regexp.MustCompile(`(?im)^\s*(CREATE|DROP|ALTER|UPDATE|DELETE)\b`)

func nextRevision(revision string) string {
	n, err := strconv.Atoi(revision)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%0*d", len(revision), n+1)
}

func tailProblems(baseline Baseline, chain []ChainFile, down []DownFile) []string {
	var problems []string
	tail := ChainAbove(chain, FrozenHead)
	want := nextRevision(FrozenHead)
	seen := map[string]bool{}
	for _, file := range tail {
		switch {
		case seen[file.Revision]:
			problems = append(problems, "revision "+file.Revision+" has more than one file ("+file.Name+")")
			continue
		case file.Revision != want:
			problems = append(problems, fmt.Sprintf("revision %s follows %s: the Go-only revisions are consecutive, want %s", file.Revision, previousOf(want), want))
		}
		seen[file.Revision] = true
		want = nextRevision(file.Revision)
		if found := statementsThatCannotShareATransaction(file.SQL); len(found) > 0 {
			problems = append(problems, file.Name+" holds statements that cannot run in the walk's one transaction: "+strings.Join(found, "; "))
		}
	}
	downByName := map[string]DownFile{}
	for _, file := range ChainAbove(downAsChain(down), FrozenHead) {
		downByName[file.Name] = DownFile{Revision: file.Revision, Name: file.Name, SQL: file.SQL}
	}
	for _, file := range tail {
		pair, ok := downByName[file.Name]
		switch {
		case !ok:
			problems = append(problems, "sql/"+file.Name+" has no sql/down/"+file.Name)
		case !downStatement.MatchString(pair.SQL):
			problems = append(problems, "sql/down/"+file.Name+" holds no statement")
		default:
			if found := statementsThatCannotShareATransaction(pair.SQL); len(found) > 0 {
				problems = append(problems, "sql/down/"+file.Name+" holds statements that cannot run in one transaction: "+strings.Join(found, "; "))
			}
		}
		delete(downByName, file.Name)
	}
	for name := range downByName {
		problems = append(problems, "sql/down/"+name+" has no sql/"+name)
	}
	if len(tail) > 0 {
		last := tail[len(tail)-1].Revision
		if got := Heads(baseline, chain); !reflect.DeepEqual(got, []string{cutoverRevision, last}) {
			problems = append(problems, fmt.Sprintf("the chain's heads are %v, want one application head %s beside the cutover head", got, last))
		}
	}
	return problems
}

func previousOf(revision string) string {
	n, err := strconv.Atoi(revision)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%0*d", len(revision), n-1)
}

func downAsChain(down []DownFile) []ChainFile {
	out := make([]ChainFile, len(down))
	for i, file := range down {
		out[i] = ChainFile{Revision: file.Revision, Name: file.Name, SQL: file.SQL}
	}
	return out
}

func loadTail(t *testing.T) (Baseline, []ChainFile, []DownFile) {
	t.Helper()
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	down, err := LoadDownChain()
	if err != nil {
		t.Fatal(err)
	}
	return baseline, chain, down
}

func TestTheRealChainTailPassesTheGoOwnedChecks(t *testing.T) {
	baseline, chain, down := loadTail(t)
	if problems := tailProblems(baseline, chain, down); len(problems) > 0 {
		t.Errorf("the Go-only revisions above %s break the rules:\n%s", FrozenHead, strings.Join(problems, "\n"))
	}
	if frozen := ChainThrough(chain, FrozenHead); len(frozen) == 0 || frozen[len(frozen)-1].Revision != FrozenHead {
		t.Errorf("the chain does not reach FrozenHead %s", FrozenHead)
	}
}

func TestTheTailChecksRefuseAGapAndADuplicate(t *testing.T) {
	for _, test := range []struct {
		fixture string
		want    string
	}{
		{"gap", "revision 0155 follows 0153: the Go-only revisions are consecutive, want 0154"},
		{"duplicate", "revision 0153 has more than one file"},
	} {
		t.Run(test.fixture, func(t *testing.T) {
			UsePlantedChain(t, os.DirFS("testdata/postfreeze/"+test.fixture))
			baseline, chain, down := loadTail(t)
			problems := tailProblems(baseline, chain, down)
			if !strings.Contains(strings.Join(problems, "\n"), test.want) {
				t.Errorf("problems %q do not name %q", problems, test.want)
			}
		})
	}
	t.Run("a down file with no statement", func(t *testing.T) {
		baseline, chain, down := plantedTail(t, "plant")
		down[len(down)-1].SQL = "-- nothing\n"
		if problems := tailProblems(baseline, chain, down); !strings.Contains(strings.Join(problems, "\n"), "holds no statement") {
			t.Errorf("problems %q", problems)
		}
	})
	t.Run("a revision without its down file", func(t *testing.T) {
		baseline, chain, down := plantedTail(t, "plant")
		down = down[:len(down)-1]
		if problems := tailProblems(baseline, chain, down); !strings.Contains(strings.Join(problems, "\n"), "has no sql/down/") {
			t.Errorf("problems %q", problems)
		}
	})
	t.Run("a statement the walk's one transaction cannot hold", func(t *testing.T) {
		baseline, chain, down := plantedTail(t, "plant")
		chain[len(chain)-1].SQL = "CREATE INDEX CONCURRENTLY ix ON t (a);"
		if problems := tailProblems(baseline, chain, down); !strings.Contains(strings.Join(problems, "\n"), "cannot run in the walk's one transaction") {
			t.Errorf("problems %q", problems)
		}
	})
}

func plantedTail(t *testing.T, fixture string) (Baseline, []ChainFile, []DownFile) {
	t.Helper()
	UsePlantedChain(t, os.DirFS("testdata/postfreeze/"+fixture))
	return loadTail(t)
}

// docOf is the description a Go-only revision prints in `history`: its slug as a sentence (WithChain).
func docOf(file ChainFile) string {
	slug := strings.TrimSuffix(strings.SplitN(file.Name, "_", 2)[1], ".sql")
	words := strings.ReplaceAll(slug, "_", " ")
	return strings.ToUpper(words[:1]) + words[1:] + "."
}

// Each planted tail is accepted whole: the chain loads, its heads, downgrade range, history lines and known
// revisions follow from the chain, and a database at the new head decides as at-head. Every expectation is derived
// from the chain, not typed as a golden.
func TestAPlantedGoOnlyTailIsAcceptedEverywhere(t *testing.T) {
	for _, fixture := range []string{"plant", "two"} {
		t.Run(fixture, func(t *testing.T) {
			baseline, chain, down := plantedTail(t, fixture)
			if problems := tailProblems(baseline, chain, down); len(problems) > 0 {
				t.Fatalf("the planted tail is refused: %v", problems)
			}
			tail := ChainAbove(chain, FrozenHead)
			if len(tail) == 0 {
				t.Fatal("the fixture planted no revision")
			}
			last := tail[len(tail)-1].Revision
			if !reflect.DeepEqual(Heads(baseline, chain), []string{cutoverRevision, last}) {
				t.Errorf("heads %v, want [%s %s]", Heads(baseline, chain), cutoverRevision, last)
			}
			if !reflect.DeepEqual(ChainThrough(chain, FrozenHead), ChainThrough(embeddedChain(t), FrozenHead)) {
				t.Error("planting a tail changed the frozen prefix")
			}
			r := NewDownRange(baseline, chain, down)
			if r.Covered[len(r.Covered)-1] != last {
				t.Errorf("the downgrade range ends at %s, want %s", r.Covered[len(r.Covered)-1], last)
			}

			entries, err := LoadHistory()
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := WriteHistory(&out, WithChain(entries, baseline, chain)); err != nil {
				t.Fatal(err)
			}
			text := out.String()
			previous := FrozenHead
			for _, file := range tail {
				line := fmt.Sprintf("%s -> %s", previous, file.Revision)
				if file.Revision == last {
					line += " (head)"
				}
				line += ", " + docOf(file)
				if !strings.Contains(text, line+"\n") {
					t.Errorf("history lacks %q:\n%s", line, text)
				}
				previous = file.Revision
			}
			known := KnownRevisions(entries, baseline, chain)
			for _, file := range tail {
				if !known[file.Revision] {
					t.Errorf("revision %s is not known to the build", file.Revision)
				}
			}
			plan := Decide(Observation{HasVersionTable: true, Versions: []string{cutoverRevision, last}, PublicTables: []string{"users"}}, Baseline{Cutover: true, Heads: []string{cutoverRevision, applicationHead(baseline)}, Schema: baseline.Schema}, chain, known)
			if plan.State != StateAtHead || plan.ApplicationHead != last {
				t.Errorf("a database at %s decides %+v, want at-head %s", last, plan, last)
			}
			assertTailDowngradeCells(t, baseline, chain, down)
		})
	}
}

func embeddedChain(t *testing.T) []ChainFile {
	t.Helper()
	previous := chainFS.Load()
	chainFS.Store(nil)
	defer chainFS.Store(previous)
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

// assertTailDowngradeCells generates the downgrade cells of the tail from the chain order and checks each one.
func assertTailDowngradeCells(t *testing.T, baseline Baseline, chain []ChainFile, down []DownFile) {
	t.Helper()
	entries, err := LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	known := KnownRevisions(entries, baseline, chain)
	r := NewDownRange(baseline, chain, down)
	order := append([]string{r.Floor}, r.Covered...)
	index := map[string]int{}
	for i, revision := range order {
		index[revision] = i
	}
	steps := func(from, to string) []DownStep {
		var out []DownStep
		for position := index[from]; position > index[to]; position-- {
			out = append(out, DownStep{Revision: order[position], Previous: order[position-1]})
		}
		return out
	}
	tail := ChainAbove(chain, FrozenHead)
	top := tail[len(tail)-1].Revision
	check := func(name string, recorded []string, arg string, want []DownStep) {
		target, refusal := ParseDowngradeTarget(arg, known)
		if refusal == nil {
			refusal = r.CheckTarget(target)
		}
		if refusal != nil {
			t.Errorf("%s: refused before the database: %+v", name, refusal)
			return
		}
		got, refusal := PlanDowngrade(recorded, target, r, known)
		if refusal != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: steps %v refusal %+v, want %v", name, got, refusal, want)
		}
	}
	for _, file := range tail {
		previous := order[index[file.Revision]-1]
		check("explicit "+file.Revision+" to its predecessor", []string{file.Revision}, previous, steps(file.Revision, previous))
	}
	check("-1 from the head", []string{top}, "-1", steps(top, order[index[top]-1]))
	check("the head to FrozenHead", []string{cutoverRevision, top}, FrozenHead, steps(top, FrozenHead))
	check("-N from the head to the floor", []string{top}, "-"+strconv.Itoa(index[top]), steps(top, r.Floor))
	target, _ := ParseDowngradeTarget("-"+strconv.Itoa(index[top]+1), known)
	if _, refusal := PlanDowngrade([]string{top}, target, r, known); refusal == nil || refusal.Code != "below_baseline_floor" {
		t.Errorf("one step below the floor: refusal %+v, want below_baseline_floor", refusal)
	}
}

// With an empty tail the generated cells have nothing to check; the planted tests above run them on a real tail.
func TestTheRealChainTailDowngradeCells(t *testing.T) {
	baseline, chain, down := loadTail(t)
	if len(ChainAbove(chain, FrozenHead)) == 0 {
		t.Skip("the Go-only tail is empty")
	}
	assertTailDowngradeCells(t, baseline, chain, down)
}
