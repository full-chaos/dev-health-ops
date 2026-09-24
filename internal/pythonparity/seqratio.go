package pythonparity

// SequenceRatio is CPython's `difflib.SequenceMatcher(a=a, b=b).ratio()`
// (default isjunk=None, autojunk=True) over the code points of two strings:
// 2*M/T where M is the total size of the matching blocks the Ratcliff/
// Obershelp search finds and T = len(a)+len(b) in code points; two empty
// strings give 1.0.
//
// autojunk is ported too: when b has 200 or more elements, any element that
// occurs in more than len(b)/100+1 positions is "popular" and is dropped
// from the index (not from the extension steps), which changes which longest
// match is found for long inputs.
func SequenceRatio(a, b string) float64 {
	ar, br := []rune(a), []rune(b)
	total := len(ar) + len(br)
	if total == 0 {
		return 1.0
	}
	matches := sequenceMatches(ar, br)
	return 2.0 * float64(matches) / float64(total)
}

func sequenceMatches(a, b []rune) int {
	b2j := map[rune][]int{}
	for j, r := range b {
		b2j[r] = append(b2j[r], j)
	}
	if n := len(b); n >= 200 {
		ntest := n/100 + 1
		for r, idxs := range b2j {
			if len(idxs) > ntest {
				delete(b2j, r)
			}
		}
	}

	longest := func(alo, ahi, blo, bhi int) (int, int, int) {
		besti, bestj, bestsize := alo, blo, 0
		j2len := map[int]int{}
		for i := alo; i < ahi; i++ {
			newj2len := map[int]int{}
			for _, j := range b2j[a[i]] {
				if j < blo {
					continue
				}
				if j >= bhi {
					break
				}
				k := j2len[j-1] + 1
				newj2len[j] = k
				if k > bestsize {
					besti, bestj, bestsize = i-k+1, j-k+1, k
				}
			}
			j2len = newj2len
		}
		// Extend the match over elements the index dropped (popular
		// elements); with isjunk=None there are no junk elements, so the
		// "junk" extension loops CPython also runs are no-ops.
		for besti > alo && bestj > blo && a[besti-1] == b[bestj-1] {
			besti, bestj, bestsize = besti-1, bestj-1, bestsize+1
		}
		for besti+bestsize < ahi && bestj+bestsize < bhi && a[besti+bestsize] == b[bestj+bestsize] {
			bestsize++
		}
		return besti, bestj, bestsize
	}

	type span struct{ alo, ahi, blo, bhi int }
	queue := []span{{0, len(a), 0, len(b)}}
	matched := 0
	for len(queue) > 0 {
		s := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		i, j, k := longest(s.alo, s.ahi, s.blo, s.bhi)
		if k == 0 {
			continue
		}
		matched += k
		if s.alo < i && s.blo < j {
			queue = append(queue, span{s.alo, i, s.blo, j})
		}
		if i+k < s.ahi && j+k < s.bhi {
			queue = append(queue, span{i + k, s.ahi, j + k, s.bhi})
		}
	}
	return matched
}
