package secrets

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestBasicAuthSpellingsAreTheFourEncodingsOfTheExactPair(t *testing.T) {
	const email, token = "sync@example.test", "zz9tok-synthetic-aaaaaaaa "
	raw := []byte(email + ":" + token)
	want := []string{
		base64.StdEncoding.EncodeToString(raw), base64.RawStdEncoding.EncodeToString(raw),
		base64.URLEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(raw),
	}
	got := BasicAuthSpellings(email, token)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v, want %v", got, want)
	}
	if BasicAuthSpellings("", token) != nil || BasicAuthSpellings(email, "") != nil {
		t.Error("a pair with an empty half has no spelling")
	}
}

func TestBasicAuthSpellingsMakeABoundaryRedactTheHeaderAndItsEscapedForms(t *testing.T) {
	const email, token = "sync@example.test", "zz9tok-synthetic-aaaaaaaa"
	boundary := NewBoundaryWith("", BasicAuthSpellings(email, token)...)
	for _, spelling := range BasicAuthSpellings(email, token) {
		if got := boundary.RedactText("Authorization: Basic " + spelling + " refused"); strings.Contains(got, spelling) {
			t.Errorf("%q survives in %q", spelling, got)
		}
	}
}
