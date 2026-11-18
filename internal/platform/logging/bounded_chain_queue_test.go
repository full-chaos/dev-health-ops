package logging

import (
	"errors"
	"runtime"
	"strconv"
	"testing"
)

func wideJoin(width int) error {
	errs := make([]error, width)
	errs[0] = stubTimeout{true}
	for i := 1; i < width; i++ {
		errs[i] = errors.New("ordinary")
	}
	return errors.Join(errs...)
}

func TestBoundedChainQueueIsBoundedByNodeBound(t *testing.T) {
	join := wideJoin(1 << 20)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	chain := boundedChain(join)
	runtime.ReadMemStats(&after)
	if len(chain) != maxChainNodes {
		t.Fatalf("chain length %d, want %d", len(chain), maxChainNodes)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Errorf("a 1M-entry join allocated %d bytes, want at most 1 MiB", allocated)
	}
}

func TestBoundedChainKeepsBreadthFirstOrderAcrossJoins(t *testing.T) {
	leafA, leafB, leafC, leafD := errors.New("a"), errors.New("b"), errors.New("c"), errors.New("d")
	nested := errors.Join(leafC, nil, leafD)
	root := errors.Join(errors.Join(leafA, leafB), nil, nested, leafA)
	chain := boundedChain(root)
	var got []error
	for _, link := range chain {
		if link == leafA || link == leafB || link == leafC || link == leafD {
			got = append(got, link)
		}
	}
	want := []error{leafA, leafB, leafC, leafD}
	if len(got) != len(want) {
		t.Fatalf("leaves %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("leaf %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func BenchmarkBoundedChainWideJoin(b *testing.B) {
	for _, width := range []int{1 << 10, 1 << 20} {
		join := wideJoin(width)
		b.Run("width="+strconv.Itoa(width), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = boundedChain(join)
			}
		})
	}
}
