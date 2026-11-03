package pgmigrate

import (
	"reflect"
	"testing"
)

func revisionsOf(chain []ChainFile) []string {
	out := []string{}
	for _, file := range chain {
		out = append(out, file.Revision)
	}
	return out
}

func widthMixedChain() []ChainFile {
	return []ChainFile{{Revision: "9998"}, {Revision: "9999"}, {Revision: "10000"}, {Revision: "10001"}}
}

func TestChainThroughComparesRevisionsAsIntegers(t *testing.T) {
	got := revisionsOf(ChainThrough(widthMixedChain(), "9999"))
	if want := []string{"9998", "9999"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ChainThrough(.., 9999) = %v, want %v", got, want)
	}
	got = revisionsOf(ChainThrough(widthMixedChain(), "10000"))
	if want := []string{"9998", "9999", "10000"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ChainThrough(.., 10000) = %v, want %v", got, want)
	}
}

func TestChainAboveComparesRevisionsAsIntegers(t *testing.T) {
	got := revisionsOf(ChainAbove(widthMixedChain(), "9999"))
	if want := []string{"10000", "10001"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ChainAbove(.., 9999) = %v, want %v", got, want)
	}
	got = revisionsOf(ChainAbove(widthMixedChain(), "10000"))
	if want := []string{"10001"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ChainAbove(.., 10000) = %v, want %v", got, want)
	}
}

func TestLoadDownChainSortsRevisionsAsIntegers(t *testing.T) {
	UseChainFS(t, plantedFiles(map[string]string{
		"sql/down/10000_step.sql": "DROP TABLE a;\n",
		"sql/down/9999_step.sql":  "DROP TABLE b;\n",
		"sql/down/10001_step.sql": "DROP TABLE c;\n",
		"sql/down/9998_step.sql":  "DROP TABLE d;\n",
	}))
	down, err := LoadDownChain()
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, file := range down {
		got = append(got, file.Revision)
	}
	if want := []string{"9998", "9999", "10000", "10001"}; !reflect.DeepEqual(got, want) {
		t.Errorf("LoadDownChain order = %v, want %v", got, want)
	}
}

// Production records two alembic_version rows: the cutover head 0066 and the application head. Decide reads the rows
// as a set, so neither the row order nor the row that comes first changes the plan; a database one archived revision
// behind is below the baseline.
func TestDecideTwoHeadsAtTheRealChainHead(t *testing.T) {
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	history, err := LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	known := KnownRevisions(history, baseline, chain)
	head := Heads(baseline, chain)
	application := head[0]
	if application == cutoverRevision {
		application = head[1]
	}
	for name, versions := range map[string][]string{
		"cutover row first":     {cutoverRevision, application},
		"application row first": {application, cutoverRevision},
	} {
		t.Run(name, func(t *testing.T) {
			plan := Decide(Observation{HasVersionTable: true, Versions: versions, PublicTables: baseline.Tables()}, baseline, chain, known)
			if plan.State != StateAtHead || plan.ApplicationHead != application || len(plan.Pending) != 0 {
				t.Fatalf("state %d, application head %q, pending %v; want StateAtHead at %s", plan.State, plan.ApplicationHead, revisionsOf(plan.Pending), application)
			}
		})
	}
	behind := Decide(Observation{HasVersionTable: true, Versions: []string{cutoverRevision, "0161"}, PublicTables: baseline.Tables()}, baseline, chain, known)
	if behind.State != StateBelowHead || !reflect.DeepEqual(behind.Missing, []string{applicationHead(baseline)}) {
		t.Fatalf("one archived revision behind: state %d, missing %v; want below the head missing %s", behind.State, behind.Missing, applicationHead(baseline))
	}
}
