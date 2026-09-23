package admin

import (
	"os"
	"testing"
	"time"
)

func setTTLEnv(t *testing.T, value string) {
	t.Helper()
	t.Setenv("IMPERSONATION_TTL_MINUTES", value)
}

func clearTTLEnv(t *testing.T) {
	t.Helper()
	_ = os.Unsetenv("IMPERSONATION_TTL_MINUTES")
}

// TestImpersonationTTLDefault pins the unset-env default (60 minutes),
// matching _impersonation_ttl_minutes()'s os.getenv(..., "60").
func TestImpersonationTTLDefault(t *testing.T) {
	clearTTLEnv(t)
	ttl, unhandled, pyErr := impersonationTTL()
	if unhandled || pyErr != nil {
		t.Fatalf("default: unhandled=%v pyErr=%v", unhandled, pyErr)
	}
	if ttl != 60*time.Minute {
		t.Fatalf("default ttl = %v, want 60m", ttl)
	}
}

// TestImpersonationTTLUnicodeDigits pins the P1 a live venue-oracle round
// found: Python's int() accepts Unicode decimal digits (verified live:
// int("١") == 1), so a narrower ASCII-only parser answers 500 where Python
// answers 200. "١٠" is the Arabic-Indic digits for 10.
func TestImpersonationTTLUnicodeDigits(t *testing.T) {
	setTTLEnv(t, "١٠")
	ttl, unhandled, pyErr := impersonationTTL()
	if unhandled || pyErr != nil {
		t.Fatalf("unicode digits: unhandled=%v pyErr=%v", unhandled, pyErr)
	}
	if ttl != 10*time.Minute {
		t.Fatalf("unicode digits ttl = %v, want 10m", ttl)
	}
}

// TestImpersonationTTLUnderscoreSeparator pins PEP 515 (moved here from the
// deleted parsePyInt's own test, now covered through pythonparity.ParseInt).
func TestImpersonationTTLUnderscoreSeparator(t *testing.T) {
	setTTLEnv(t, "1_0")
	ttl, unhandled, pyErr := impersonationTTL()
	if unhandled || pyErr != nil {
		t.Fatalf("underscore: unhandled=%v pyErr=%v", unhandled, pyErr)
	}
	if ttl != 10*time.Minute {
		t.Fatalf("underscore ttl = %v, want 10m", ttl)
	}
}

// TestImpersonationTTLInvalid pins the explicit
// HTTPException(500, "Invalid IMPERSONATION_TTL_MINUTES configuration")
// case: a string int() itself raises ValueError on.
func TestImpersonationTTLInvalid(t *testing.T) {
	for _, raw := range []string{"not-a-number", "_10", "10_", "1__0"} {
		setTTLEnv(t, raw)
		_, unhandled, pyErr := impersonationTTL()
		if unhandled {
			t.Fatalf("invalid %q: got unhandled, want the explicit 500", raw)
		}
		if pyErr == nil || pyErr.Msg != "Invalid IMPERSONATION_TTL_MINUTES configuration" {
			t.Fatalf("invalid %q: pyErr = %+v, want the configuration error", raw, pyErr)
		}
	}
}

// TestImpersonationTTLNotPositive pins the explicit
// HTTPException(500, "IMPERSONATION_TTL_MINUTES must be > 0") case.
func TestImpersonationTTLNotPositive(t *testing.T) {
	for _, raw := range []string{"0", "-1", "-100"} {
		setTTLEnv(t, raw)
		_, unhandled, pyErr := impersonationTTL()
		if unhandled {
			t.Fatalf("not-positive %q: got unhandled, want the explicit 500", raw)
		}
		if pyErr == nil || pyErr.Msg != "IMPERSONATION_TTL_MINUTES must be > 0" {
			t.Fatalf("not-positive %q: pyErr = %+v, want the must-be-positive error", raw, pyErr)
		}
	}
}

// TestImpersonationTTLClampsBeyondGoDuration pins the second P1: a TTL
// Python's timedelta(minutes=...) honours (well within its own ~2.7M-year
// max) but Go's time.Duration cannot literally represent (int64
// nanoseconds, ~292 years) used to silently overflow and wrap NEGATIVE --
// an already-expired session for a config value Python treats as active.
// 153722868 minutes (~292.47 years) is the smallest value the live oracle
// found wrapping; it must now clamp to the largest safe Go Duration
// instead, never go negative or shrink as the input grows.
func TestImpersonationTTLClampsBeyondGoDuration(t *testing.T) {
	setTTLEnv(t, "153722868")
	ttl, unhandled, pyErr := impersonationTTL()
	if unhandled || pyErr != nil {
		t.Fatalf("clamp case: unhandled=%v pyErr=%v", unhandled, pyErr)
	}
	if ttl <= 0 {
		t.Fatalf("clamp case: ttl = %v, want a large positive duration, not negative/zero", ttl)
	}
	if ttl != maxSafeTTL {
		t.Fatalf("clamp case: ttl = %v, want maxSafeTTL = %v", ttl, maxSafeTTL)
	}

	// A larger, still Python-valid TTL must clamp to the SAME ceiling, not
	// wrap again or shrink.
	setTTLEnv(t, "999999999999")
	ttl2, unhandled2, pyErr2 := impersonationTTL()
	if unhandled2 || pyErr2 != nil {
		t.Fatalf("clamp case (larger): unhandled=%v pyErr=%v", unhandled2, pyErr2)
	}
	if ttl2 != maxSafeTTL {
		t.Fatalf("clamp case (larger): ttl = %v, want maxSafeTTL = %v", ttl2, maxSafeTTL)
	}
}

// TestImpersonationTTLBeyondPythonOwnLimit pins the boundary where
// Python's OWN timedelta(minutes=...) call raises OverflowError
// (unhandled, so the generic 500) -- verified live against
// /usr/bin/python3: timedelta(minutes=1_439_999_999_999) succeeds,
// timedelta(minutes=1_440_000_000_000) raises OverflowError.
func TestImpersonationTTLBeyondPythonOwnLimit(t *testing.T) {
	setTTLEnv(t, "1439999999999")
	_, unhandled, pyErr := impersonationTTL()
	if unhandled || pyErr != nil {
		t.Fatalf("at Python's own limit: unhandled=%v pyErr=%v, want success", unhandled, pyErr)
	}

	setTTLEnv(t, "1440000000000")
	_, unhandled2, pyErr2 := impersonationTTL()
	if pyErr2 != nil {
		t.Fatalf("beyond Python's own limit: pyErr = %+v, want unhandled not an explicit error", pyErr2)
	}
	if !unhandled2 {
		t.Fatalf("beyond Python's own limit: unhandled = false, want true (matches Python's OverflowError)")
	}
}
