package authrows

import (
	"os"
	"testing"
)

func TestForwardedSetIsParsedFromEnvAndRejectsAnUnknownAggregate(t *testing.T) {
	cases := []struct {
		text    string
		want    string
		wantErr bool
	}{
		{"", "", false},
		{"  ", "", false},
		{"sessions", "sessions", false},
		{"social,sessions", "sessions,social", false},
		{"sessions, local_login ,credentials", "sessions,local_login,credentials", false},
		{"sessions,session", "", true},
		{"sessions,sessions", "", true},
		{"sessions,", "", true},
		{"Sessions", "", true},
	}
	for _, c := range cases {
		got, err := ParseForwarded(c.text)
		if (err != nil) != c.wantErr {
			t.Errorf("ParseForwarded(%q) err = %v, want error %v", c.text, err, c.wantErr)
			continue
		}
		if err == nil && got.String() != c.want {
			t.Errorf("ParseForwarded(%q) = %q, want %q", c.text, got, c.want)
		}
	}
}

func TestFromEnvRefusesAnUnsetVariableAndReadsAnEmptyOne(t *testing.T) {
	t.Setenv(EnvForwarded, "x")
	if err := os.Unsetenv(EnvForwarded); err != nil {
		t.Fatal(err)
	}
	if _, err := FromEnv(); err == nil {
		t.Fatal("an unset variable was read as none")
	}
	t.Setenv(EnvForwarded, "x")
	if _, err := FromEnv(); err == nil {
		t.Fatal("an unknown aggregate was accepted")
	}
	t.Setenv(EnvForwarded, "")
	f, err := FromEnv()
	if err != nil || f.Any() {
		t.Fatalf("an empty variable: %v, any %v; want none", err, f.Any())
	}
	t.Setenv(EnvForwarded, "sessions,social")
	f, err = FromEnv()
	if err != nil || !f.Has(Sessions) || !f.Has(Social) || f.Has(LocalLogin) {
		t.Fatalf("sessions,social: %v, %s", err, f)
	}
}
