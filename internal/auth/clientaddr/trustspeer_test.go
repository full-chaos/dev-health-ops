package clientaddr

import (
	"net/http/httptest"
	"testing"
)

func TestTrustsPeerReadsOnlyTheSocketPeer(t *testing.T) {
	trust, err := ParseTrust("10.1.2.0/28,2001:db8::7")
	if err != nil {
		t.Fatal(err)
	}
	for remote, want := range map[string]bool{
		"10.1.2.3:443":        true,
		"[::ffff:10.1.2.3]:1": true,
		"[2001:db8::7]:1":     true,
		"10.1.2.16:443":       false,
		"192.0.2.1:1234":      false,
		"not-an-address":      false,
		"":                    false,
	} {
		request := httptest.NewRequest("GET", "/", nil)
		request.RemoteAddr = remote
		request.Header.Set("X-Forwarded-For", "10.1.2.3")
		if got := trust.TrustsPeer(request); got != want {
			t.Errorf("TrustsPeer(%q) = %v, want %v", remote, got, want)
		}
	}
}
