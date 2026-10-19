//go:build !race

package contracts

import "unicode"

const (
	astralSet     = "exhaustive"
	astralMinimum = int(unicode.MaxRune) - 0xffff
)

func astralRunes() []rune {
	runes := make([]rune, 0, astralMinimum)
	for r := rune(0x10000); r <= unicode.MaxRune; r++ {
		runes = append(runes, r)
	}
	return runes
}
