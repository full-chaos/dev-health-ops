package decisioneval

import (
	"hash/fnv"
	"math"
	"math/rand/v2"
	"sort"
)

// BootstrapSeed is the fixed seed of every bootstrap (design.md 9.4).
const BootstrapSeed = 8712

// DefaultResamples is the bootstrap size of design.md 9.4.
const DefaultResamples = 10000

// Num is a number that can be missing. A missing value is never printed as a
// zero: NA names why.
type Num struct {
	V    *float64  `json:"v,omitempty"`
	N    int       `json:"n"`
	CI95 []float64 `json:"ci95,omitempty"`
	NA   string    `json:"na,omitempty"`
}

func na(reason string) Num { return Num{NA: reason} }

func val(v float64, n int) Num { return Num{V: &v, N: n} }

// Rate is a proportion with a Wilson interval.
type Rate struct {
	K    int       `json:"k"`
	N    int       `json:"n"`
	V    *float64  `json:"v,omitempty"`
	CI95 []float64 `json:"ci95_wilson,omitempty"`
	NA   string    `json:"na,omitempty"`
}

func rate(k, n int, reasonIfEmpty string) Rate {
	if n == 0 {
		return Rate{K: k, N: n, NA: reasonIfEmpty}
	}
	v := float64(k) / float64(n)
	lo, hi := wilson(k, n)
	return Rate{K: k, N: n, V: &v, CI95: []float64{lo, hi}}
}

// wilson is the 95% Wilson score interval.
func wilson(k, n int) (float64, float64) {
	if n == 0 {
		return 0, 0
	}
	const z = 1.959963984540054
	p := float64(k) / float64(n)
	nf := float64(n)
	den := 1 + z*z/nf
	centre := (p + z*z/(2*nf)) / den
	half := z * math.Sqrt(p*(1-p)/nf+z*z/(4*nf*nf)) / den
	return math.Max(0, centre-half), math.Min(1, centre+half)
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := 0.0
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

// percentileNearestRank is the nearest-rank percentile (p in 0..100).
func percentileNearestRank(v []float64, p float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	rank := int(math.Ceil(p / 100 * float64(len(s))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(s) {
		rank = len(s)
	}
	return s[rank-1]
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// l1 is sum |p-q| over the keys of both maps.
func l1(p, q map[string]float64) float64 {
	keys := map[string]bool{}
	for k := range p {
		keys[k] = true
	}
	for k := range q {
		keys[k] = true
	}
	s := 0.0
	for k := range keys {
		s += math.Abs(p[k] - q[k])
	}
	return s
}

// jsd is the Jensen-Shannon divergence in bits (range 0..1), 0*log 0 = 0.
func jsd(p, q map[string]float64) float64 {
	keys := map[string]bool{}
	for k := range p {
		keys[k] = true
	}
	for k := range q {
		keys[k] = true
	}
	klHalf := func(a map[string]float64) float64 {
		s := 0.0
		for k := range keys {
			if a[k] > 0 {
				m := (p[k] + q[k]) / 2
				s += a[k] * math.Log2(a[k]/m)
			}
		}
		return s
	}
	return 0.5*klHalf(p) + 0.5*klHalf(q)
}

func labelSeed(label string) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(label))
	return BootstrapSeed, h.Sum64()
}

// bootMeanCI is the 95% percentile bootstrap interval of the mean of v: B
// resamples of the fixtures, a fixed seed derived from the label so that each
// interval is reproducible and independent of call order.
func bootMeanCI(v []float64, b int, label string) (lo, hi float64) {
	if len(v) == 0 {
		return math.NaN(), math.NaN()
	}
	s1, s2 := labelSeed(label)
	rng := rand.New(rand.NewPCG(s1, s2))
	means := make([]float64, b)
	n := len(v)
	for i := 0; i < b; i++ {
		sum := 0.0
		for j := 0; j < n; j++ {
			sum += v[rng.IntN(n)]
		}
		means[i] = sum / float64(n)
	}
	sort.Float64s(means)
	return means[int(0.025*float64(b))], means[int(math.Ceil(0.975*float64(b)))-1]
}

// bootRatioCI is the interval of sum(num)/sum(den), resampling fixtures.
func bootRatioCI(num, den []float64, b int, label string) (lo, hi float64) {
	if len(num) == 0 {
		return math.NaN(), math.NaN()
	}
	s1, s2 := labelSeed(label)
	rng := rand.New(rand.NewPCG(s1, s2))
	vals := make([]float64, 0, b)
	n := len(num)
	for i := 0; i < b; i++ {
		sn, sd := 0.0, 0.0
		for j := 0; j < n; j++ {
			k := rng.IntN(n)
			sn += num[k]
			sd += den[k]
		}
		if sd > 0 {
			vals = append(vals, sn/sd)
		}
	}
	if len(vals) == 0 {
		return math.NaN(), math.NaN()
	}
	sort.Float64s(vals)
	return vals[int(0.025*float64(len(vals)))], vals[int(math.Ceil(0.975*float64(len(vals))))-1]
}

// meanNum builds a Num: the mean of v with a bootstrap interval when ci is set.
func meanNum(v []float64, ci bool, b int, label, emptyReason string) Num {
	if len(v) == 0 {
		return na(emptyReason)
	}
	n := val(mean(v), len(v))
	if ci {
		lo, hi := bootMeanCI(v, b, label)
		n.CI95 = []float64{lo, hi}
	}
	return n
}
