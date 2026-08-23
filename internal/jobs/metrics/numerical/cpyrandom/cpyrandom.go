// Package cpyrandom reproduces CPython's `random` module stream exactly.
//
// # Why this exists
//
// capacity's product rows ARE Monte Carlo output. p50_days, p85_days, p50_items
// and p85_items all come from monte_carlo_forecast_days/_items
// (compute_capacity.py:137,191), which seed with `random.seed(seed)` and then
// draw with `random.choice(throughputs)` ten thousand times.
//
// Go's math/rand seeded with the same integer produces a COMPLETELY DIFFERENT
// sequence. There is no fixture that makes an exact row-for-row comparison pass
// between a math/rand port and Python, because the two are not computing the
// same function of the seed. Porting CPython's generator is what makes the
// capacity family comparable at all -- the alternative was to loosen the
// comparator to a statistical tolerance, which would have surrendered the
// property the whole parity programme rests on.
//
// # What is reproduced, and what deliberately is not
//
// Reproduced: MT19937 with CPython's init_by_array seeding over the ABSOLUTE
// value of the seed in 32-bit little-endian chunks, getrandbits' word
// assembly, _randbelow_with_getrandbits' rejection loop, and choice.
//
// NOT reproduced: CPython's module-global state. `random.seed()` and
// `random.choice()` mutate one process-wide generator, which is why an
// interleaved GraphQL capacity request and a worker job perturb each other's
// streams today. This package is an INSTANCE. The divergence is in mechanism
// only -- the stream produced for a given seed is identical, which is the part
// the parity comparison observes -- and it removes a contamination class rather
// than porting it.
//
// Scope is narrow on purpose: only the calls capacity actually makes. An audit
// of compute_capacity.py, job_capacity.py and capacity_queries.py found exactly
// `random.seed` and `random.choice` and nothing else -- no random(), no
// shuffle, no sample, no uniform, no gauss. The float path is therefore absent
// here, and its absence is deliberate rather than an omission: reproducing
// CPython's 53-bit float construction is a harder problem that capacity does
// not pose.
package cpyrandom

import (
	"fmt"
	"math/big"
)

const (
	stateSize  = 624
	shiftSize  = 397
	matrixA    = 0x9908b0df
	upperMask  = 0x80000000
	lowerMask  = 0x7fffffff
	initSeed   = 19650218
	maxBitsInt = 64
)

// Source is one independent CPython-compatible generator.
//
// Not safe for concurrent use, matching CPython's Random. Each capacity
// partition builds its own from that run's generation seed, so there is no
// shared instance to contend for.
type Source struct {
	state [stateSize]uint32
	index int
}

// New builds a Source seeded exactly as CPython's random.seed(seed) would.
func New(seed int64) *Source {
	source := &Source{}
	source.Seed(big.NewInt(seed))
	return source
}

// Seed reproduces CPython's random_seed for an integer argument.
//
// CPython takes the ABSOLUTE value of the seed and splits it into 32-bit
// little-endian words, then runs init_by_array. Dropping the absolute value is
// an easy and silent mistake: it agrees with CPython on every non-negative seed
// and disagrees on every negative one, so a test suite seeded only with
// positive numbers would never notice. The golden vectors include negative
// seeds for exactly that reason.
func (source *Source) Seed(seed *big.Int) {
	absolute := new(big.Int).Abs(seed)

	var key []uint32
	if absolute.Sign() == 0 {
		// CPython treats a zero seed as a single zero word rather than an
		// empty key; an empty key would make init_by_array's j index divide by
		// zero-length and produce a different stream.
		key = []uint32{0}
	} else {
		// Decomposed by arithmetic rather than through big.Int's own words,
		// because big.Word is 32 or 64 bits depending on the platform and
		// CPython's chunking is defined as 32-bit regardless.
		remaining := new(big.Int).Set(absolute)
		mask := big.NewInt(0xffffffff)
		chunk := new(big.Int)
		for remaining.Sign() != 0 {
			chunk.And(remaining, mask)
			key = append(key, uint32(chunk.Uint64()))
			remaining.Rsh(remaining, 32)
		}
	}
	source.initByArray(key)
}

func (source *Source) initGenrand(s uint32) {
	source.state[0] = s
	for i := 1; i < stateSize; i++ {
		previous := source.state[i-1]
		source.state[i] = 1812433253*(previous^(previous>>30)) + uint32(i)
	}
	source.index = stateSize
}

func (source *Source) initByArray(key []uint32) {
	source.initGenrand(initSeed)
	i, j := 1, 0
	k := stateSize
	if len(key) > k {
		k = len(key)
	}
	for ; k > 0; k-- {
		previous := source.state[i-1]
		source.state[i] = (source.state[i] ^ ((previous ^ (previous >> 30)) * 1664525)) +
			key[j] + uint32(j)
		i++
		j++
		if i >= stateSize {
			source.state[0] = source.state[stateSize-1]
			i = 1
		}
		if j >= len(key) {
			j = 0
		}
	}
	for k = stateSize - 1; k > 0; k-- {
		previous := source.state[i-1]
		source.state[i] = (source.state[i] ^ ((previous ^ (previous >> 30)) * 1566083941)) -
			uint32(i)
		i++
		if i >= stateSize {
			source.state[0] = source.state[stateSize-1]
			i = 1
		}
	}
	// Assures a non-zero initial array.
	source.state[0] = upperMask
}

// genrandUint32 returns the next tempered word, regenerating the state block
// every 624 draws.
//
// The regeneration -- the "twist" -- is where a port stays correct for 623
// draws and then diverges, which is why the golden set includes a ten-thousand
// draw stream rather than only short ones.
func (source *Source) genrandUint32() uint32 {
	if source.index >= stateSize {
		source.regenerate()
	}
	y := source.state[source.index]
	source.index++
	y ^= y >> 11
	y ^= (y << 7) & 0x9d2c5680
	y ^= (y << 15) & 0xefc60000
	y ^= y >> 18
	return y
}

func (source *Source) regenerate() {
	magic := [2]uint32{0, matrixA}
	var kk int
	for kk = 0; kk < stateSize-shiftSize; kk++ {
		y := (source.state[kk] & upperMask) | (source.state[kk+1] & lowerMask)
		source.state[kk] = source.state[kk+shiftSize] ^ (y >> 1) ^ magic[y&1]
	}
	for ; kk < stateSize-1; kk++ {
		y := (source.state[kk] & upperMask) | (source.state[kk+1] & lowerMask)
		source.state[kk] = source.state[kk+(shiftSize-stateSize)] ^ (y >> 1) ^ magic[y&1]
	}
	y := (source.state[stateSize-1] & upperMask) | (source.state[0] & lowerMask)
	source.state[stateSize-1] = source.state[shiftSize-1] ^ (y >> 1) ^ magic[y&1]
	source.index = 0
}

// GetRandBits reproduces CPython's getrandbits for k up to 64.
//
// The word ORDER matters and is easy to invert: CPython fills from LEAST
// significant word upward, so the FIRST draw becomes the low 32 bits. A port
// that assembles the other way agrees whenever k <= 32 -- where there is only
// one word -- and disagrees above it, which is why the golden set brackets the
// boundary at 31, 32 and 33.
//
// Capping at 64 is sufficient rather than restrictive: the only production
// caller is randBelow over a slice length, so k is at most 63.
func (source *Source) GetRandBits(k int) (uint64, error) {
	if k < 0 {
		return 0, fmt.Errorf("getrandbits: negative width %d", k)
	}
	if k > maxBitsInt {
		return 0, fmt.Errorf(
			"getrandbits: width %d exceeds this port's %d-bit ceiling; capacity "+
				"never asks for more, so a wider request means a caller this "+
				"package was not audited against", k, maxBitsInt)
	}
	if k == 0 {
		return 0, nil
	}
	if k <= 32 {
		return uint64(source.genrandUint32() >> (32 - k)), nil
	}
	words := (k-1)/32 + 1
	var result uint64
	remaining := k
	for word := 0; word < words; word++ {
		r := source.genrandUint32()
		if remaining < 32 {
			r >>= uint(32 - remaining)
		}
		result |= uint64(r) << (32 * word)
		remaining -= 32
	}
	return result, nil
}

// randBelow reproduces _randbelow_with_getrandbits.
//
// The rejection loop is the whole subtlety. k is n.bit_length() -- verified
// against the live interpreter, NOT (n-1).bit_length() as some versions of the
// idiom use -- and any draw at or above n is discarded and redrawn. When n is a
// power of two nothing is ever rejected, so a port with a broken or missing
// loop matches perfectly on lengths 2, 4, 8 and 1024 and fails on 3, 5, 6, 7,
// 10 and 100. The golden set and the capacity fixture both carry rejecting
// lengths for that reason.
func (source *Source) randBelow(n uint64) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	k := bitLength(n)
	for {
		r, err := source.GetRandBits(k)
		if err != nil {
			return 0, err
		}
		if r < n {
			return r, nil
		}
	}
}

// Choice returns the INDEX random.choice would land on for a sequence of the
// given length, matching seq[self._randbelow(len(seq))].
func (source *Source) Choice(length int) (int, error) {
	if length <= 0 {
		return 0, fmt.Errorf("choice: cannot choose from an empty sequence")
	}
	index, err := source.randBelow(uint64(length))
	if err != nil {
		return 0, err
	}
	return int(index), nil
}

func bitLength(value uint64) int {
	bits := 0
	for value > 0 {
		bits++
		value >>= 1
	}
	return bits
}
