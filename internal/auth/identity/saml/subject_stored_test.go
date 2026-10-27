package saml

import (
	"errors"
	"testing"
)

func strp(s string) *string { return &s }

func TestSubjectFromStoredUsesTheRuntimeRule(t *testing.T) {
	const persistent = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"
	want, err := Subject(NameID{Format: persistent, SPNameQualifier: "sp", Value: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := SubjectFromStored("abc", strp(persistent), strp("sp"))
	if err != nil || got != want {
		t.Fatalf("SubjectFromStored = %q, %v; want %q", got, err, want)
	}
	absent, err := SubjectFromStored("abc", strp(""), strp(""))
	wantAbsent, _ := Subject(NameID{Value: "abc"})
	if err != nil || absent != wantAbsent {
		t.Fatalf("empty parts = %q, %v; want %q", absent, err, wantAbsent)
	}
}

func TestSubjectFromStoredRefusesWhatTheRuntimeRefuses(t *testing.T) {
	if _, err := SubjectFromStored("abc", nil, nil); !errors.Is(err, ErrNameIDNotCaptured) {
		t.Fatalf("NULL format: %v, want ErrNameIDNotCaptured", err)
	}
	if _, err := SubjectFromStored("abc", strp(NameIDFormatTransient), strp("")); !errors.Is(err, ErrSubject) {
		t.Fatalf("transient: %v, want ErrSubject", err)
	}
	if _, err := SubjectFromStored("", strp(""), strp("")); !errors.Is(err, ErrSubject) {
		t.Fatalf("empty value: %v, want ErrSubject", err)
	}
}
