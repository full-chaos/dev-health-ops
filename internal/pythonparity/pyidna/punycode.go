package pyidna

import "sort"

// punycodeEncode is CPython's str.encode("punycode") (encodings.punycode,
// RFC 3492 as that codec implements it).
func punycodeEncode(text []rune) []byte {
	var base []byte
	seen := map[rune]bool{}
	var extended []rune
	for _, r := range text {
		if r < 0x80 {
			base = append(base, byte(r))
		} else if !seen[r] {
			seen[r] = true
			extended = append(extended, r)
		}
	}
	sort.Slice(extended, func(i, j int) bool { return extended[i] < extended[j] })

	// 3.2 insertion unsort coding.
	var deltas []int
	oldChar, oldIndex := rune(0x80), -1
	for _, c := range extended {
		index, pos := -1, -1
		curLen := 0
		for _, r := range text {
			if r < c {
				curLen++
			}
		}
		delta := (curLen + 1) * int(c-oldChar)
		for {
			index, pos = selectiveFind(text, c, index, pos)
			if index == -1 {
				break
			}
			delta += index - oldIndex
			deltas = append(deltas, delta-1)
			oldIndex = index
			delta = 0
		}
		oldChar = c
	}

	// 3.4 bias adaptation.
	var digitsOut []byte
	bias := 72
	for points, delta := range deltas {
		digitsOut = append(digitsOut, generalizedInteger(delta, bias)...)
		bias = adapt(delta, points == 0, len(base)+points+1)
	}
	if len(base) > 0 {
		return append(append(base, '-'), digitsOut...)
	}
	return digitsOut
}

func selectiveFind(text []rune, char rune, index, pos int) (int, int) {
	for {
		pos++
		if pos == len(text) {
			return -1, -1
		}
		c := text[pos]
		if c == char {
			return index + 1, pos
		}
		if c < char {
			index++
		}
	}
}

const punycodeDigits = "abcdefghijklmnopqrstuvwxyz0123456789"

func threshold(j, bias int) int {
	res := 36*(j+1) - bias
	if res < 1 {
		return 1
	}
	if res > 26 {
		return 26
	}
	return res
}

func generalizedInteger(n, bias int) []byte {
	var out []byte
	for j := 0; ; j++ {
		t := threshold(j, bias)
		if n < t {
			return append(out, punycodeDigits[n])
		}
		out = append(out, punycodeDigits[t+((n-t)%(36-t))])
		n = (n - t) / (36 - t)
	}
}

func adapt(delta int, first bool, numChars int) int {
	if first {
		delta /= 700
	} else {
		delta /= 2
	}
	delta += delta / numChars
	divisions := 0
	for delta > 455 {
		delta /= 35
		divisions += 36
	}
	return divisions + (36*delta)/(delta+38)
}

// punycodeCeiling bounds the decoder's arithmetic. Python's integers are
// unbounded; here any value past the ceiling already pushes the decoded
// code point beyond U+10FFFF (the base is at most a few hundred code
// points long), which is a decode error either way, so saturating keeps
// the verdict while avoiding overflow.
const punycodeCeiling = 1 << 50

// punycodeDecode is bytes.decode("punycode") with errors="strict". ok is
// false for every input the codec rejects; the callers only need that it
// failed (idna turns every failure into "Invalid A-label").
func punycodeDecode(text []byte) ([]rune, bool) {
	pos := -1
	for i := len(text) - 1; i >= 0; i-- {
		if text[i] == '-' {
			pos = i
			break
		}
	}
	var base []rune
	var extended []byte
	if pos == -1 {
		extended = text
	} else {
		for _, b := range text[:pos] {
			if b >= 0x80 {
				return nil, false
			}
			base = append(base, rune(b))
		}
		extended = text[pos+1:]
	}
	char := 0x80
	position := -1
	bias := 72
	extPos := 0
	for extPos < len(extended) {
		newPos, delta, ok := decodeGeneralizedNumber(extended, extPos, bias)
		if !ok {
			return nil, false
		}
		position = saturatingAdd(position, delta+1)
		char = saturatingAdd(char, position/(len(base)+1))
		if char > 0x10ffff {
			return nil, false
		}
		position %= len(base) + 1
		base = append(base[:position], append([]rune{rune(char)}, base[position:]...)...)
		bias = adapt(delta, extPos == 0, len(base))
		extPos = newPos
	}
	return base, true
}

func decodeGeneralizedNumber(extended []byte, extPos, bias int) (int, int, bool) {
	result, w := 0, 1
	for j := 0; ; j++ {
		if extPos >= len(extended) {
			return 0, 0, false
		}
		char := extended[extPos]
		extPos++
		var digit int
		switch {
		// CPython upper-cases the extended part first; matching both cases
		// here is the same thing for ASCII, and a non-ASCII byte is refused
		// either way.
		case char >= 'A' && char <= 'Z':
			digit = int(char - 'A')
		case char >= 'a' && char <= 'z':
			digit = int(char - 'a')
		case char >= '0' && char <= '9':
			digit = int(char) - 22
		default:
			return 0, 0, false
		}
		t := threshold(j, bias)
		result = saturatingAdd(result, saturatingMul(digit, w))
		if digit < t {
			return extPos, result, true
		}
		w = saturatingMul(w, 36-t)
	}
}

func saturatingAdd(a, b int) int {
	if a+b > punycodeCeiling {
		return punycodeCeiling
	}
	return a + b
}

func saturatingMul(a, b int) int {
	if a != 0 && b > punycodeCeiling/a {
		return punycodeCeiling
	}
	return a * b
}
