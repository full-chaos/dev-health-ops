package main

import (
	"io"
	"strings"
	"testing"
)

func merge(t *testing.T, profiles ...string) (string, error) {
	t.Helper()
	readers := make([]io.Reader, len(profiles))
	for i, p := range profiles {
		readers[i] = strings.NewReader(p)
	}
	return Merge(readers, "example.com/m/")
}

func TestMergeSumsCountsAndMaxesLines(t *testing.T) {
	a := "mode: count\nexample.com/m/p/a.go:1.1,3.2 2 1\nexample.com/m/p/a.go:5.1,5.9 1 0\n"
	b := "mode: count\nexample.com/m/p/a.go:1.1,3.2 2 4\nexample.com/m/p/a.go:5.1,5.9 1 0\nexample.com/m/q/b.go:2.1,2.5 1 3\n"
	got, err := merge(t, a, b)
	if err != nil {
		t.Fatal(err)
	}
	want := "TN:\nSF:p/a.go\nDA:1,5\nDA:2,5\nDA:3,5\nDA:5,0\nLF:4\nLH:3\nend_of_record\n" +
		"TN:\nSF:q/b.go\nDA:2,3\nLF:1\nLH:1\nend_of_record\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestMergeSetModeIsOR(t *testing.T) {
	a := "mode: set\nexample.com/m/a.go:1.1,1.5 1 0\n"
	b := "mode: set\nexample.com/m/a.go:1.1,1.5 1 1\n"
	got, err := merge(t, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "DA:1,1\n") || !strings.Contains(got, "LH:1\n") {
		t.Fatalf("set mode OR lost the hit:\n%s", got)
	}
}

func TestMergeOverlappingBlocksTakeMaxPerLine(t *testing.T) {
	a := "mode: set\nexample.com/m/a.go:1.1,4.2 3 0\nexample.com/m/a.go:2.1,2.9 1 1\n"
	got, err := merge(t, a)
	if err != nil {
		t.Fatal(err)
	}
	want := "TN:\nSF:a.go\nDA:1,0\nDA:2,1\nDA:3,0\nDA:4,0\nLF:4\nLH:1\nend_of_record\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestMergeRefusesBadInput(t *testing.T) {
	for name, in := range map[string][]string{
		"mode mismatch":    {"mode: set\nexample.com/m/a.go:1.1,1.5 1 1\n", "mode: count\nexample.com/m/a.go:1.1,1.5 1 1\n"},
		"empty":            {""},
		"no mode line":     {"example.com/m/a.go:1.1,1.5 1 1\n"},
		"garbage block":    {"mode: set\nnot a block\n"},
		"end before start": {"mode: set\nexample.com/m/a.go:3.1,1.5 1 1\n"},
		"negative count":   {"mode: set\nexample.com/m/a.go:1.1,1.5 1 -1\n"},
	} {
		if _, err := merge(t, in...); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
