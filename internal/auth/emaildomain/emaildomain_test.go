package emaildomain

import "testing"

func TestAllowed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		list   []string
		domain string
		want   bool
	}{
		{"same case", []string{"example.com"}, "example.com", true},
		{"asserted upper case", []string{"example.com"}, "EXAMPLE.COM", true},
		{"configured upper case", []string{"Example.COM"}, "example.com", true},
		{"mixed on both sides", []string{"ExAmPle.Com"}, "eXaMpLe.cOm", true},
		{"second entry", []string{"other.org", "Example.com"}, "example.COM", true},
		{"configured entry padded", []string{" example.com\t"}, "example.com", true},
		{"asserted padded left", []string{"example.com"}, " example.com", false},
		{"asserted padded right", []string{"example.com"}, "example.com ", false},
		{"asserted padded with a tab", []string{"example.com"}, "example.com\t", false},
		{"asserted empty", []string{"example.com"}, "", false},
		{"asserted empty and an empty entry", []string{""}, "", false},
		{"asserted empty and a blank entry", []string{"  "}, "", false},
		{"padded asserted and a blank entry", []string{"  "}, " ", false},
		{"sub-domain of an allowed domain", []string{"example.com"}, "sub.example.com", false},
		{"suffix lookalike", []string{"example.com"}, "evil-example.com", false},
		{"suffix lookalike without a dash", []string{"example.com"}, "evilexample.com", false},
		{"allowed domain as a prefix", []string{"example.com"}, "example.com.evil.com", false},
		{"other domain", []string{"example.com"}, "example.org", false},
		{"trailing dot", []string{"example.com"}, "example.com.", false},
		{"empty list", nil, "example.com", false},
		{"non-ASCII compares exactly", []string{"exámple.com"}, "exámple.com", true},
		{"non-ASCII upper case is a different domain", []string{"exámple.com"}, "exÁmple.com", false},
		{"non-ASCII lookalike of an ASCII entry", []string{"example.com"}, "exámple.com", false},
		{"Kelvin sign is not K", []string{"k.example"}, "K.example", false},
		{"Kelvin sign in the configured entry", []string{"K.example"}, "k.example", false},
		{"dotless i is not i", []string{"mail.example"}, "maıl.example", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Allowed(tc.list, tc.domain); got != tc.want {
				t.Fatalf("Allowed(%q, %q) = %v, want %v", tc.list, tc.domain, got, tc.want)
			}
		})
	}
}

func TestAllowedDoesNotChangeItsInputs(t *testing.T) {
	list := []string{" Example.COM "}
	domain := "EXAMPLE.com"
	_ = Allowed(list, domain)
	if list[0] != " Example.COM " || domain != "EXAMPLE.com" {
		t.Fatalf("inputs changed: %q %q", list, domain)
	}
}
