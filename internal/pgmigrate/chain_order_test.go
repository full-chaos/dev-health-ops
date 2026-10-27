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
