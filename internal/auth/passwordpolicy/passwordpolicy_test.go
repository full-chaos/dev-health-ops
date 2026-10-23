package passwordpolicy

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		password string
		want     []string
	}{
		{"correct-horse-9", nil},
		// The denylist match is case-insensitive.
		{"PassWord1000", []string{"Password is too common"}},
		{"short1", []string{"Password must be at least 12 characters long"}},
		{"nodigitsatallhere", []string{"Password must include at least one number"}},
		{"123456789012", []string{"Password must include at least one letter"}},
		// str.isdigit() accepts a superscript digit; unicode.IsDigit does not.
		{"Abcdefghijk²", nil},
		{"", []string{
			"Password must be at least 12 characters long",
			"Password must include at least one letter",
			"Password must include at least one number",
		}},
	} {
		if got := Validate(tc.password); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Validate(%q) = %q, want %q", tc.password, got, tc.want)
		}
	}
	long := make([]rune, 129)
	for i := range long {
		long[i] = 'a'
	}
	long[0] = '1'
	if got := Validate(string(long)); !reflect.DeepEqual(got, []string{"Password must be no more than 128 characters long"}) {
		t.Errorf("Validate(129 runes) = %q", got)
	}
}

func TestEmbeddedDenylistParses(t *testing.T) {
	if len(loadCommonPasswords()) == 0 {
		t.Fatal("the embedded denylist parsed to no entries")
	}
}

func TestLengthBoundsAre12To128(t *testing.T) {
	build := func(n int) string { return "A1" + strings.Repeat("b", n-2) }
	for n, want := range map[int]bool{11: false, 12: true, 128: true, 129: false} {
		if got := len(Validate(build(n))) == 0; got != want {
			t.Errorf("Validate of %d characters accepted = %v, want %v", n, got, want)
		}
	}
}

func TestValidateForHashAddsTheByteLimit(t *testing.T) {
	ok72 := "A1" + strings.Repeat("b", 70)
	over := "A1" + strings.Repeat("\u00e9", 36)
	if got := ValidateForHash(ok72); len(got) != 0 {
		t.Errorf("72 bytes = %q, want none", got)
	}
	if got := ValidateForHash(over); len(got) != 1 || got[0] != ViolationTooLong {
		t.Errorf("74 bytes = %q, want the byte violation", got)
	}
	if got := Validate(over); len(got) != 0 {
		t.Errorf("Validate counts characters, got %q", got)
	}
}
