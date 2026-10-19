//go:build race

package contracts

import "unicode"

const (
	astralSet     = "stride+probes"
	astralMinimum = 16000
)

func astralRunes() []rune {
	var runes []rune
	for r := rune(0x10000); r <= unicode.MaxRune; r++ {
		first := r&0xffff == 0 || r&0xffff == 1 || r&0xffff == 0xffff
		if first || r%61 == 0 {
			runes = append(runes, r)
		}
	}
	return runes
}
