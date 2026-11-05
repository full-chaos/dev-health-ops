package netaddr

import "testing"

// TestListenAddressesOverlap: two addresses collide when the socket could not
// bind both (CHAOS-6520 r1: ":39621" and "127.0.0.1:39621" differ as strings and
// the second bind fails "address already in use").
func TestListenAddressesOverlap(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{":8000", ":8000", true},
		{":8000", "127.0.0.1:8000", true},
		{"127.0.0.1:8000", ":8000", true},
		{"0.0.0.0:8000", "10.1.2.3:8000", true},
		{"[::]:8000", "127.0.0.1:8000", true},
		{"[::]:8000", "[::1]:8000", true},
		{"localhost:8000", "127.0.0.1:8000", true},
		{"localhost:8000", "[::1]:8000", true},
		{"[::ffff:127.0.0.1]:8000", "127.0.0.1:8000", true},
		{"[::ffff:10.1.2.3]:8000", "10.1.2.3:8000", true},
		{"Example.COM:8000", "example.com:8000", true},
		{"127.0.0.1:08000", "127.0.0.1:8000", true},
		{":8000", ":8001", false},
		{"127.0.0.1:8000", "127.0.0.2:8000", false},
		{"10.1.2.3:8000", "10.1.2.4:8000", false},
		{"a.example:8000", "b.example:8000", false},
		{":0", ":0", false},
		{"127.0.0.1:0", ":0", false},
		{"nohostport", ":8000", false},
	}
	for _, test := range cases {
		if got := ListenAddressesOverlap(test.a, test.b); got != test.want {
			t.Errorf("ListenAddressesOverlap(%q, %q) = %v, want %v", test.a, test.b, got, test.want)
		}
		if got := ListenAddressesOverlap(test.b, test.a); got != test.want {
			t.Errorf("ListenAddressesOverlap(%q, %q) = %v, want %v (symmetry)", test.b, test.a, got, test.want)
		}
	}
}
