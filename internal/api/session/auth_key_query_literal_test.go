package session

import (
	"net/http/httptest"
	"testing"
)

// The verify route's limiter key reads the e-mail query parameter the way
// Starlette does: a ";" or a bad escape stays in the value, and bytes that
// decode to invalid UTF-8 read as U+FFFD, so %FF and %FE share one bucket.
func TestQueryAuthKeyReadsTheLiteralEmail(t *testing.T) {
	key := func(query string) string {
		r := httptest.NewRequest("GET", "/api/v1/auth/verify?"+query, nil)
		r.RemoteAddr = "198.51.100.7:4242"
		return queryAuthKey(r)
	}
	for _, tc := range []struct{ query, want string }{
		{"email=A@x.test", "198.51.100.7:a@x.test"},
		{"email=a@x.test;b", "198.51.100.7:a@x.test;b"},
		{"email=a@x.test&email=B@y.test;", "198.51.100.7:b@y.test;"},
		{"email=a%zz@x.test", "198.51.100.7:a%zz@x.test"},
		{"email=%FF", "198.51.100.7:�"},
		{"email=%FE", "198.51.100.7:�"},
		{"token=t;x", "198.51.100.7:unknown"},
	} {
		if got := key(tc.query); got != tc.want {
			t.Errorf("%q: key %q, want %q", tc.query, got, tc.want)
		}
	}
}
