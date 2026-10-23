package clientip

import "testing"

func TestWidePrefixesCountsEntriesWiderThanASlash28(t *testing.T) {
	for raw, want := range map[string]int{
		"":                                   0,
		"10.42.0.6":                          0,
		"10.42.0.0/28":                       0,
		"10.42.0.0/29,192.0.2.1":             0,
		"10.42.0.0/24":                       1,
		"10.42.0.0/24,10.0.0.0/8,192.0.2.1":  2,
		"2001:db8::1":                        0,
		"2001:db8::/124":                     0,
		"2001:db8::/64":                      1,
		"::ffff:10.42.0.0/120":               1,
		"::ffff:10.42.0.0/124,not-a-network": 0,
	} {
		t.Setenv("TRUSTED_PROXIES", raw)
		if got := WidePrefixes(); got != want {
			t.Errorf("%q: %d wide prefixes, want %d", raw, got, want)
		}
	}
}
