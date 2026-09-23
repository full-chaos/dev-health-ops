package pyjson

import (
	"errors"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// A Python str may hold lone surrogates (json.loads keeps "\ud800" as
// one); Go strings from this package carry them as WTF-8: a surrogate code
// point encoded with the generalized UTF-8 three-byte form. Every other
// code point is plain UTF-8, so a string without surrogates is unchanged.

// Runes decodes a WTF-8 string into code points, surrogates kept.
func Runes(text string) []rune {
	out := make([]rune, 0, len(text))
	for index := 0; index < len(text); {
		if surrogate, ok := surrogateAt(text, index); ok {
			out = append(out, surrogate)
			index += 3
			continue
		}
		r, size := utf8.DecodeRuneInString(text[index:])
		out = append(out, r)
		index += size
	}
	return out
}

func surrogateAt(text string, index int) (rune, bool) {
	if index+2 >= len(text) || text[index] != 0xed || text[index+1] < 0xa0 || text[index+1] > 0xbf ||
		text[index+2] < 0x80 || text[index+2] > 0xbf {
		return 0, false
	}
	return rune(0xd000 | rune(text[index+1]&0x3f)<<6 | rune(text[index+2]&0x3f)), true
}

// FromRunes encodes code points as WTF-8.
func FromRunes(runes []rune) string {
	var builder strings.Builder
	for _, r := range runes {
		writeRune(&builder, r)
	}
	return builder.String()
}

func writeRune(builder *strings.Builder, r rune) {
	if utf16.IsSurrogate(r) {
		builder.WriteByte(0xed)
		builder.WriteByte(byte(0x80 | (r>>6)&0x3f))
		builder.WriteByte(byte(0x80 | r&0x3f))
		return
	}
	builder.WriteRune(r)
}

// Len is len() of the Python str.
func Len(text string) int { return len(Runes(text)) }

// HasSurrogate reports whether text holds a lone surrogate (it cannot be
// encoded as UTF-8: Python raises UnicodeEncodeError where it is written).
func HasSurrogate(text string) bool {
	for _, r := range Runes(text) {
		if utf16.IsSurrogate(r) {
			return true
		}
	}
	return false
}

// ErrUnicode is Python's UnicodeDecodeError/UnicodeEncodeError.
var ErrUnicode = errors.New("pyjson: text is not valid in the requested encoding")

// DecodeBody is json.loads(bytes)'s decode step: json.detect_encoding, then
// bytes.decode(encoding, "surrogatepass"). The result is WTF-8.
func DecodeBody(raw []byte) (string, error) {
	var bomUTF32BE, bomUTF32LE = []byte{0, 0, 0xfe, 0xff}, []byte{0xff, 0xfe, 0, 0}
	var bomUTF16BE, bomUTF16LE = []byte{0xfe, 0xff}, []byte{0xff, 0xfe}
	switch {
	case hasPrefix(raw, bomUTF32BE):
		return decodeUTF32(raw[4:], true)
	case hasPrefix(raw, bomUTF32LE):
		return decodeUTF32(raw[4:], false)
	case hasPrefix(raw, bomUTF16BE):
		return decodeUTF16(raw[2:], true)
	case hasPrefix(raw, bomUTF16LE):
		return decodeUTF16(raw[2:], false)
	case hasPrefix(raw, []byte{0xef, 0xbb, 0xbf}):
		return decodeUTF8(raw[3:])
	}
	switch {
	case len(raw) >= 4 && raw[0] == 0:
		if raw[1] != 0 {
			return decodeUTF16(raw, true)
		}
		return decodeUTF32(raw, true)
	case len(raw) >= 4 && raw[1] == 0:
		if raw[2] != 0 || raw[3] != 0 {
			return decodeUTF16(raw, false)
		}
		return decodeUTF32(raw, false)
	case len(raw) == 2 && raw[0] == 0:
		return decodeUTF16(raw, true)
	case len(raw) == 2 && raw[1] == 0:
		return decodeUTF16(raw, false)
	}
	return decodeUTF8(raw)
}

func hasPrefix(raw, prefix []byte) bool {
	return len(raw) >= len(prefix) && string(raw[:len(prefix)]) == string(prefix)
}

// decodeUTF8 accepts UTF-8 plus encoded surrogates ("surrogatepass").
func decodeUTF8(raw []byte) (string, error) {
	text := string(raw)
	for index := 0; index < len(text); {
		if _, ok := surrogateAt(text, index); ok {
			index += 3
			continue
		}
		r, size := utf8.DecodeRuneInString(text[index:])
		if r == utf8.RuneError && size <= 1 {
			return "", ErrUnicode
		}
		index += size
	}
	return text, nil
}

// decodeUTF16 keeps unpaired surrogates ("surrogatepass").
func decodeUTF16(raw []byte, bigEndian bool) (string, error) {
	if len(raw)%2 != 0 {
		return "", ErrUnicode
	}
	units := make([]rune, 0, len(raw)/2)
	for index := 0; index < len(raw); index += 2 {
		if bigEndian {
			units = append(units, rune(raw[index])<<8|rune(raw[index+1]))
		} else {
			units = append(units, rune(raw[index+1])<<8|rune(raw[index]))
		}
	}
	var out []rune
	for index := 0; index < len(units); index++ {
		unit := units[index]
		if unit >= 0xd800 && unit < 0xdc00 && index+1 < len(units) && units[index+1] >= 0xdc00 && units[index+1] <= 0xdfff {
			out = append(out, utf16.DecodeRune(unit, units[index+1]))
			index++
			continue
		}
		out = append(out, unit)
	}
	return FromRunes(out), nil
}

func decodeUTF32(raw []byte, bigEndian bool) (string, error) {
	if len(raw)%4 != 0 {
		return "", ErrUnicode
	}
	var out []rune
	for index := 0; index < len(raw); index += 4 {
		var value uint32
		if bigEndian {
			value = uint32(raw[index])<<24 | uint32(raw[index+1])<<16 | uint32(raw[index+2])<<8 | uint32(raw[index+3])
		} else {
			value = uint32(raw[index+3])<<24 | uint32(raw[index+2])<<16 | uint32(raw[index+1])<<8 | uint32(raw[index])
		}
		if value > 0x10ffff {
			return "", ErrUnicode
		}
		out = append(out, rune(value))
	}
	return FromRunes(out), nil
}
