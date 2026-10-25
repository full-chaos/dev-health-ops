package session

import "testing"

func TestResendActsOnlyOnAnUnverifiedAccountAndForgotOnAnyAccount(t *testing.T) {
	unverified, verified := &userRow{IsVerified: false}, &userRow{IsVerified: true}
	for _, tc := range []struct {
		name           string
		user           *userRow
		resend, forgot bool
	}{
		{"unknown address", nil, false, false},
		{"verified account", verified, false, true},
		{"unverified account", unverified, true, true},
	} {
		if got := resendActsOn(tc.user); got != tc.resend {
			t.Errorf("%s: resend acts = %v, want %v", tc.name, got, tc.resend)
		}
		if got := forgotActsOn(tc.user); got != tc.forgot {
			t.Errorf("%s: forgot acts = %v, want %v", tc.name, got, tc.forgot)
		}
	}
}
