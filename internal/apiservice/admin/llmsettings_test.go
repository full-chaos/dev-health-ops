package admin

import "testing"

func TestOperatorMaximumMicroUSD(t *testing.T) {
	defer func(saved func(string) (string, bool)) { lookupEnv = saved }(lookupEnv)
	for _, tc := range []struct {
		name  string
		set   bool
		value string
		want  string
	}{
		{"unset", false, "", "100000000"},
		{"plain", true, "5000", "5000"},
		{"padded", true, " 7 ", "7"},
		{"underscored", true, "1_000", "1000"},
		{"empty", true, "", "0"},
		{"text", true, "abc", "0"},
		{"negative", true, "-5", "0"},
		{"beyond int64", true, "99999999999999999999", "99999999999999999999"},
	} {
		lookupEnv = func(string) (string, bool) { return tc.value, tc.set }
		if got := operatorMaximumMicroUSD().String(); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestMaskAPIKey(t *testing.T) {
	for in, want := range map[string]string{"": "", "1234": "********", "12345678": "********", "123456789": "1234…6789", "ключ-секрет-ключ": "ключ…ключ"} {
		got := maskAPIKey(in)
		if in == "" {
			if got != nil {
				t.Errorf("empty: %v", *got)
			}
			continue
		}
		if got == nil || *got != want {
			t.Errorf("%q: got %v, want %q", in, got, want)
		}
	}
}

// TestBudgetLockKeyMatchesPython pins the advisory-lock key against values
// computed by llm/budget.py's _acquire_advisory_lock formula
// (int.from_bytes(sha256("byo-llm-budget:<org>")[:8], "big") & (1<<63)-1), so
// a Go writer and a Python reservation contend on the same lock.
func TestBudgetLockKeyMatchesPython(t *testing.T) {
	for org, want := range map[string]int64{
		"00000000-0000-0000-0000-000000000000": 734291639604921375,
		"70d529e0-0000-4000-8000-000000000001": 4899195206064546492,
		"3f2b8c1e-5a7d-4e9a-9b1c-2d4e6f8a0b1c": 9008887641742855183,
	} {
		if got := budgetLockKey(org); got != want {
			t.Errorf("budgetLockKey(%s) = %d, python %d", org, got, want)
		}
	}
}
