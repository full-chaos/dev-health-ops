// Package callseq records the calls a request makes and compares sequences,
// for tests that pin "a refused request takes the steps of an unknown one".
package callseq

import (
	"reflect"
	"slices"
	"sync"
)

// Reporter is the part of testing.TB the comparisons use.
type Reporter interface {
	Helper()
	Errorf(format string, args ...any)
}

// Recorder is an ordered, concurrency-safe call log.
type Recorder struct {
	mu    sync.Mutex
	calls []string
}

// Add records one call.
func (r *Recorder) Add(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
}

// Names is a copy of the calls so far.
func (r *Recorder) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// Same fails unless every sequence equals every other.
func Same(t Reporter, label string, seqs map[string][]string) {
	t.Helper()
	var first string
	for _, key := range slices.Sorted(mapKeys(seqs)) {
		if first == "" {
			first = key
			continue
		}
		if !slices.Equal(nilToEmpty(seqs[first]), nilToEmpty(seqs[key])) {
			t.Errorf("%s: call sequences differ: %s = %v, %s = %v", label, first, seqs[first], key, seqs[key])
		}
	}
}

// Delta fails unless other is base plus exactly wantExtra: base is a
// subsequence of other, and the calls left over are wantExtra in any order.
func Delta(t Reporter, label string, base, other, wantExtra []string) {
	t.Helper()
	rest := slices.Clone(other)
	for _, call := range base {
		i := slices.Index(rest, call)
		if i < 0 {
			t.Errorf("%s: %v lacks the base call %s of %v", label, other, call, base)
			return
		}
		rest = slices.Delete(rest, i, i+1)
	}
	got, want := slices.Clone(rest), slices.Clone(wantExtra)
	slices.Sort(got)
	slices.Sort(want)
	if !reflect.DeepEqual(nilToEmpty(got), nilToEmpty(want)) {
		t.Errorf("%s: extra calls %v, want exactly %v", label, rest, wantExtra)
	}
}

func nilToEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func mapKeys(m map[string][]string) func(yield func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}
