package pythonparity

import "strings"

// DecodeUTF8Replace is bytes.decode("utf-8", "replace") for the bytes of
// text: every well-formed UTF-8 sequence is kept, and every maximal subpart
// of an ill-formed one becomes a single U+FFFD (Unicode's "U+FFFD
// substitution of maximal subparts", which CPython follows). A lone
// continuation byte, a byte that never starts a sequence (C0, C1, F5..FF),
// and a lead byte followed by a byte outside its second-byte range are one
// subpart each; a truncated sequence (lead plus its valid continuation bytes)
// is one subpart. Go's utf8.DecodeRuneInString replaces per byte instead,
// and strings.ToValidUTF8 per run, so neither is Python's rule.
//
// urllib.parse.unquote (errors="replace") decodes percent-escapes this way,
// which is how uvicorn builds scope["path"].
func DecodeUTF8Replace(text string) string {
	var out strings.Builder
	out.Grow(len(text))
	for index := 0; index < len(text); {
		c := text[index]
		if c < 0x80 {
			out.WriteByte(c)
			index++
			continue
		}
		width, low, high := sequenceShape(c)
		if width == 0 {
			out.WriteString("�")
			index++
			continue
		}
		valid := 1
		for valid < width && index+valid < len(text) {
			next := text[index+valid]
			lo, hi := byte(0x80), byte(0xBF)
			if valid == 1 {
				lo, hi = low, high
			}
			if next < lo || next > hi {
				break
			}
			valid++
		}
		if valid == width {
			out.WriteString(text[index : index+width])
		} else {
			out.WriteString("�")
		}
		index += valid
	}
	return out.String()
}

// sequenceShape is Unicode Table 3-7 (well-formed UTF-8 byte sequences) for
// a lead byte: the sequence width and the allowed range of its second byte.
// Width 0 means c never starts a well-formed sequence.
func sequenceShape(c byte) (width int, low, high byte) {
	switch {
	case c >= 0xC2 && c <= 0xDF:
		return 2, 0x80, 0xBF
	case c == 0xE0:
		return 3, 0xA0, 0xBF
	case c == 0xED:
		return 3, 0x80, 0x9F
	case c >= 0xE1 && c <= 0xEF:
		return 3, 0x80, 0xBF
	case c == 0xF0:
		return 4, 0x90, 0xBF
	case c >= 0xF1 && c <= 0xF3:
		return 4, 0x80, 0xBF
	case c == 0xF4:
		return 4, 0x80, 0x8F
	default:
		return 0, 0, 0
	}
}
