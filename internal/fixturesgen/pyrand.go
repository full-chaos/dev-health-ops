// Package fixturesgen ports the deterministic fixture generators of dev-hops
// (src/dev_health_ops/fixtures/generators) to Go. The contract is byte
// compatibility: the same seed and inputs give the same rows as the Python
// generator, which is checked by live-Python oracles (see the *_oracle_test.go
// files), not by hand-written expectations.
package fixturesgen

import (
	"fmt"
	"math/big"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical/cpyrandom"
)

// Rand is CPython's random.Random for the calls the fixture generators make:
// getrandbits, randrange(n), randint(a, b), choice and random(). It rides on
// cpyrandom's MT19937 and getrandbits (verified against the live interpreter
// there) and adds only what is built from getrandbits: _randbelow's rejection
// loop for randrange/randint, and the 53-bit float of random().
//
// Not safe for concurrent use, like random.Random. The first failure (a request
// wider than cpyrandom's audited 64-bit getrandbits) is kept and reported by
// Err; the value returned after it is meaningless, so a caller checks Err once
// at the end of a generation rather than after every draw.
type Rand struct {
	source *cpyrandom.Source
	err    error
}

// NewRand seeds a generator as random.Random(seed) does for an integer seed.
func NewRand(seed *big.Int) *Rand {
	source := &cpyrandom.Source{}
	source.Seed(seed)
	return &Rand{source: source}
}

// Err is the first error a draw hit, or nil.
func (r *Rand) Err() error { return r.err }

func (r *Rand) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

// GetRandBits is random.getrandbits(k) for k up to 64.
func (r *Rand) GetRandBits(k int) uint64 {
	value, err := r.source.GetRandBits(k)
	if err != nil {
		r.fail(err)
	}
	return value
}

// RandBelow is Random._randbelow_with_getrandbits(n): k = n.bit_length() bits,
// redrawn while the draw is >= n. n == 0 draws nothing and returns 0 (CPython
// raises ValueError there; every caller passes a positive bound, and Err would
// not tell the two apart, so the misuse is refused loudly instead).
func (r *Rand) RandBelow(n uint64) uint64 {
	if n == 0 {
		r.fail(fmt.Errorf("randbelow: empty range"))
		return 0
	}
	k := 0
	for v := n; v > 0; v >>= 1 {
		k++
	}
	for {
		value := r.GetRandBits(k)
		if r.err != nil || value < n {
			return value
		}
	}
}

// RandRange is random.randrange(n) for n > 0.
func (r *Rand) RandRange(n int) int {
	if n <= 0 {
		r.fail(fmt.Errorf("randrange: empty range for n=%d", n))
		return 0
	}
	return int(r.RandBelow(uint64(n)))
}

// RandInt is random.randint(a, b) = randrange(a, b+1) = a + _randbelow(b-a+1).
func (r *Rand) RandInt(a, b int) int {
	if b < a {
		r.fail(fmt.Errorf("randint: empty range [%d, %d]", a, b))
		return a
	}
	return a + int(r.RandBelow(uint64(b-a+1)))
}

// Choice is the index random.choice would take from a sequence of length n:
// seq[self._randbelow(len(seq))].
func (r *Rand) Choice(n int) int { return r.RandRange(n) }

// Random is random.random(): (a*2^26 + b) / 2^53 with a = the top 27 bits of
// one MT19937 word and b = the top 26 bits of the next, each exactly what
// getrandbits(27) and getrandbits(26) return.
func (r *Rand) Random() float64 {
	a := float64(r.GetRandBits(27))
	b := float64(r.GetRandBits(26))
	return (a*67108864.0 + b) * (1.0 / 9007199254740992.0)
}
