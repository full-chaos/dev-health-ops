package session

import (
	"errors"
	"testing"
)

func TestDecideSocialLink(t *testing.T) {
	cases := []struct {
		name                    string
		stored, verified, holds bool
		want                    socialLinkOutcome
		wantLookup              bool
	}{
		{"stored identity signs in", true, true, true, socialSignIn, false},
		{"stored identity signs in unverified", true, false, false, socialSignIn, false},
		{"unverified refused", false, false, false, socialRefuseUnverified, false},
		{"unverified refused though email held", false, false, true, socialRefuseUnverified, false},
		{"email match refused", false, true, true, socialRefuseEmailMatch, true},
		{"new account", false, true, false, socialCreate, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			looked := false
			got, err := decideSocialLink(c.stored, c.verified, func() (bool, error) { looked = true; return c.holds, nil })
			if err != nil || got != c.want || looked != c.wantLookup {
				t.Fatalf("got (%v, %v, lookup=%v), want (%v, lookup=%v)", got, err, looked, c.want, c.wantLookup)
			}
		})
	}
}

func TestDecideSocialLinkLookupError(t *testing.T) {
	boom := errors.New("boom")
	if _, err := decideSocialLink(false, true, func() (bool, error) { return false, boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}
