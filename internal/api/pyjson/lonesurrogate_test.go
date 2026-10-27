package pyjson

import "testing"

func TestLoneSurrogateReplacement(t *testing.T) {
	lone, _ := DecodeString(`"a\ud800b"`)
	loneText, _ := lone.(string)
	pair, _ := DecodeString(`"x😀y"`)
	pairText, _ := pair.(string)
	for name, c := range map[string]struct {
		in   string
		has  bool
		want string
	}{
		"lone high":      {loneText, true, "a�b"},
		"lone low":       {FromRunes([]rune{'a', 0xdc00}), true, "a�"},
		"two in a row":   {FromRunes([]rune{0xd800, 0xd800}), true, "��"},
		"valid pair":     {pairText, false, pairText},
		"plain":          {"Bob Müller \U0001F600", false, "Bob Müller \U0001F600"},
		"empty":          {"", false, ""},
		"truncated tail": {"ab\xed\xa0", false, "ab\xed\xa0"},
	} {
		if got := HasLoneSurrogate(c.in); got != c.has {
			t.Errorf("%s: HasLoneSurrogate = %v, want %v", name, got, c.has)
		}
		if got := ReplaceLoneSurrogates(c.in); got != c.want {
			t.Errorf("%s: ReplaceLoneSurrogates = %q, want %q", name, got, c.want)
		}
	}
	if pairText == "" || HasLoneSurrogate(pairText) {
		t.Errorf("a decoded valid pair must not hold a surrogate: %q", pairText)
	}
}
