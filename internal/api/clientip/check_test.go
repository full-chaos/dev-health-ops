package clientip

import (
	"os"
	"strings"
	"testing"
)

// Check is what go-api runs at start: the counts of a list it reads, or the error of one it refuses.
func TestCheckCountsTheEntriesAndTheExclusions(t *testing.T) {
	for list, want := range map[string][2]int{
		"10.42.0.0/24,!10.42.0.0,!10.42.0.1": {1, 2},
		"10.42.0.0/24":                       {1, 0},
		" 10.42.0.6 , 192.0.2.1 ":            {2, 0},
		"":                                   {0, 0},
	} {
		t.Setenv(Env, list)
		entries, exclusions, err := Check()
		if err != nil || entries != want[0] || exclusions != want[1] {
			t.Errorf("%q: entries=%d exclusions=%d err=%v, want %v", list, entries, exclusions, err, want)
		}
	}
	t.Setenv(Env, "10.42.0.6")
	if err := os.Unsetenv(Env); err != nil {
		t.Fatal(err)
	}
	if entries, exclusions, err := Check(); err != nil || entries != 0 || exclusions != 0 {
		t.Errorf("unset: entries=%d exclusions=%d err=%v, want 0 0 nil", entries, exclusions, err)
	}
}

func TestCheckRefusesABadListByTheVariableName(t *testing.T) {
	for _, list := range []string{"10.42.0.0/24,bogus", "0.0.0.0/0", "::/0", "::ffff:10.42.0.7", "10.42.0.0/24,!10.43.0.1", "10.42.0.0/24,"} {
		t.Setenv(Env, list)
		_, _, err := Check()
		if err == nil || !strings.HasPrefix(err.Error(), "TRUSTED_PROXIES entry ") || strings.Contains(err.Error(), "AUTH_SERVICE") {
			t.Errorf("%q: err = %v, want a refusal named by TRUSTED_PROXIES", list, err)
		}
	}
}
