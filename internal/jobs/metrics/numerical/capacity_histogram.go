package numerical

import "sort"

// Histogram is a Monte Carlo sample distribution stored as its distinct values
// (ascending) and how many simulation runs produced each one.
//
// It exists so a capacity forecast can carry its WHOLE distribution, not only
// the three percentiles taken from it (CHAOS-7624). The kernels already hold
// every run (`completionDays`, `itemsCompleted`); the percentiles threw them
// away. A histogram is exact -- nothing is binned or rounded: Expand() returns
// the original multiset of samples, so every percentile can be recomputed from
// it -- and it is small: completion days are capped at 365 distinct values, and
// the item totals of 10 000 runs collapse to far fewer than 10 000 distinct
// values for any real throughput history.
//
// Values and Counts are parallel and the same length. An empty histogram (no
// bins) means "no simulation ran for this mode", never "every run was zero".
type Histogram struct {
	Values []int
	Counts []int
}

// NewHistogram counts the samples. The input is not modified.
func NewHistogram(samples []int) Histogram {
	if len(samples) == 0 {
		return Histogram{}
	}
	sorted := append([]int(nil), samples...)
	sort.Ints(sorted)

	var histogram Histogram
	for _, sample := range sorted {
		last := len(histogram.Values) - 1
		if last >= 0 && histogram.Values[last] == sample {
			histogram.Counts[last]++
			continue
		}
		histogram.Values = append(histogram.Values, sample)
		histogram.Counts = append(histogram.Counts, 1)
	}
	return histogram
}

// Total is the number of simulation runs the histogram holds.
func (histogram Histogram) Total() int {
	total := 0
	for _, count := range histogram.Counts {
		total += count
	}
	return total
}

// Expand returns the samples in ascending order, each value repeated by its
// count. It is the inverse of NewHistogram up to ordering, and what a caller
// uses to recompute a percentile from a stored distribution.
func (histogram Histogram) Expand() []int {
	expanded := make([]int, 0, histogram.Total())
	for index, value := range histogram.Values {
		for repeat := 0; repeat < histogram.Counts[index]; repeat++ {
			expanded = append(expanded, value)
		}
	}
	return expanded
}
