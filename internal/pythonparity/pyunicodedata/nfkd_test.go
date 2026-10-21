package pyunicodedata_test

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// Each want is unicodedata.normalize("NFKD", text) of Python 3.14.7 (Unicode 16.0.0), recorded once.
func TestNFKDMatchesPython(t *testing.T) {
	for text, want := range map[string]string{
		"Café Résumé — x": "Café Résumé — x",
		"Ǆemo ﬁne ①":      "DŽemo fine 1",
		" nbsp":           " nbsp",
		"한글":              "한글",
		"ẛ̣":              "ṩ",
		"x̣́":             "x̣́",
		"Å":               "Å",
		"ｶﾞ":              "ガ",
		"㍿":               "株式会社",
		"\U0001d400":      "A",
		"̈́":               "̈́",
	} {
		if got := string(pyunicodedata.NFKD([]rune(text))); got != want {
			t.Errorf("NFKD(%+q) = %+q, Python %+q", text, got, want)
		}
	}
}
