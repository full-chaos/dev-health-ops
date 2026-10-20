package passwordpolicy

import (
	"reflect"
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
