package providerfoundation

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// LaunchDarkly documents X-Ratelimit-Reset as "The time at which the current
// rate limit window resets in epoch milliseconds"
// (https://launchdarkly.com/docs/api/overview.md, "Rate limiting"). Every other
// provider's reset header is read as epoch seconds.
func resetNow() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

func resetMillis(offset time.Duration) string {
	return strconv.FormatInt(resetNow().Add(offset).UnixMilli(), 10)
}

func resetSeconds(offset time.Duration) string {
	return strconv.FormatInt(resetNow().Add(offset).Unix(), 10)
}

// mainResetDelay is a copy of the seconds formula as it stands on main, for the
// in-range inputs; out-of-range inputs are asserted as exactly 0, never through
// this copy, because their conversion is arch-dependent.
func mainResetDelay(raw string, now time.Time) time.Duration {
	seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	delay := time.Duration((seconds - float64(now.UnixNano())/float64(time.Second)) * float64(time.Second))
	if delay < 0 {
		return 0
	}
	return delay
}

func TestLaunchDarklyResetIsReadInEpochMilliseconds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"5 seconds ahead", resetMillis(5 * time.Second), 5 * time.Second},
		{"1 hour ahead", resetMillis(time.Hour), time.Hour},
		{"exactly 24 hours ahead", resetMillis(24 * time.Hour), 24 * time.Hour},
		{"24 hours plus 1 ms (green on amd64 at the pin, red on arm64)", resetMillis(24*time.Hour + time.Millisecond), 0},
		{"a past reset (green on amd64 at the pin, red on arm64)", resetMillis(-time.Minute), 0},
		{"absurd milliseconds (green on amd64 at the pin, red on arm64)", "1000000000000000000", 0},
		{"a seconds-sized value is read as milliseconds, a past instant", resetSeconds(5 * time.Second), 0},
		{"a fraction (green on amd64 at the pin, red on arm64)", resetMillis(5*time.Second) + ".5", 0},
		{"an exponent (green on amd64 at the pin, red on arm64)", "1.7e12", 0},
		{"a negative value", "-5", 0},
		{"text", "abc", 0},
		{"empty", "", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := resetDelay("launchdarkly", http.Header{"X-Ratelimit-Reset": {test.raw}}, resetNow())
			if got != test.want {
				t.Fatalf("X-Ratelimit-Reset %q gave RetryAfter %s, want %s", test.raw, got, test.want)
			}
		})
	}
}

func TestLaunchDarklyResetHeaderIsReadInEpochMilliseconds(t *testing.T) {
	t.Parallel()
	raw := strconv.FormatInt(time.Now().Add(5*time.Second).UnixMilli(), 10)
	classification := ClassifyHTTPWithMessage("launchdarkly", http.StatusTooManyRequests,
		http.Header{"X-Ratelimit-Reset": {raw}}, `{"message":"rate limited"}`)
	if classification == nil || classification.Class != ErrorRateLimited {
		t.Fatalf("classification = %+v, want rate_limited", classification)
	}
	if classification.RetryAfter < 3*time.Second || classification.RetryAfter > 6*time.Second {
		t.Fatalf("X-Ratelimit-Reset %s (epoch ms, about now+5s) gave RetryAfter %s, want about 5s", raw, classification.RetryAfter)
	}
}

func TestLaunchDarklyRatelimitResetWithoutThePrefixStaysSeconds(t *testing.T) {
	t.Parallel()
	got := resetDelay("launchdarkly", http.Header{"Ratelimit-Reset": {resetSeconds(5 * time.Second)}}, resetNow())
	if got != 5*time.Second {
		t.Fatalf("Ratelimit-Reset (seconds) gave RetryAfter %s, want 5s", got)
	}
}

// The seconds path of every provider but LaunchDarkly: byte-equal to main for
// every input, an overflow of the Duration conversion excepted (read as 0).
func TestSecondsResetKeepsMainsAnswerForEveryOtherProvider(t *testing.T) {
	t.Parallel()
	nearOverflow := strconv.FormatInt(resetNow().Unix()+9223372036, 10)
	pastOverflow := strconv.FormatInt(resetNow().Unix()+9223372037, 10)
	inRange := []struct{ name, raw string }{
		{"5 seconds", resetSeconds(5 * time.Second)},
		{"25 hours", resetSeconds(25 * time.Hour)},
		{"one year", resetSeconds(365 * 24 * time.Hour)},
		{"a fraction", resetSeconds(5*time.Second) + ".5"},
		{"9223372036 seconds, just under 2^63 ns", nearOverflow},
	}
	zero := []struct{ name, raw string }{
		{"9223372037 seconds, just over 2^63 ns (green on amd64 at the pin, red on arm64)", pastOverflow},
		{"milliseconds read as seconds (green on amd64 at the pin, red on arm64)", "1767225605000"},
		{"Inf (green on amd64 at the pin, red on arm64)", "+Inf"},
		{"-Inf", "-Inf"},
		{"NaN", "NaN"},
		{"negative", "-5"},
		{"a past reset", resetSeconds(-time.Minute)},
		{"1e400, a range error", "1e400"},
		{"text", "abc"},
		{"empty", ""},
	}
	for _, provider := range []string{"github", "gitlab", "pagerduty", "jira", "linear"} {
		for _, header := range []string{"X-Ratelimit-Reset", "Ratelimit-Reset"} {
			for _, test := range inRange {
				t.Run(provider+"/"+header+"/"+test.name, func(t *testing.T) {
					want := mainResetDelay(test.raw, resetNow())
					if want <= 0 {
						t.Fatalf("the main copy gave %s, want a positive delay", want)
					}
					if got := resetDelay(provider, http.Header{header: {test.raw}}, resetNow()); got != want {
						t.Fatalf("%q gave %s, want %s", test.raw, got, want)
					}
				})
			}
			for _, test := range zero {
				t.Run(provider+"/"+header+"/"+test.name, func(t *testing.T) {
					if got := resetDelay(provider, http.Header{header: {test.raw}}, resetNow()); got != 0 {
						t.Fatalf("%q gave %s, want 0", test.raw, got)
					}
				})
			}
		}
	}
	if got := ParseRateLimitReset(nearOverflow, resetNow()); got != mainResetDelay(nearOverflow, resetNow()) {
		t.Fatalf("ParseRateLimitReset just under 2^63 ns gave %s", got)
	}
}

func TestClassifiedLimitsKeepReadingTheResetInSeconds(t *testing.T) {
	t.Parallel()
	raw := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	within := func(t *testing.T, name string, got time.Duration) {
		t.Helper()
		if got < 59*time.Minute || got > time.Hour+2*time.Second {
			t.Fatalf("%s gave RetryAfter %s, want about 1h", name, got)
		}
	}
	for _, provider := range []string{"github", "gitlab", "pagerduty", "jira", "linear"} {
		got := ClassifyHTTPWithMessage(provider, http.StatusTooManyRequests, http.Header{"X-Ratelimit-Reset": {raw}}, "")
		within(t, provider+" 429", got.RetryAfter)
	}
	forbidden := ClassifyHTTPWithMessage("github", http.StatusForbidden,
		http.Header{"X-Ratelimit-Remaining": {"0"}, "X-Ratelimit-Reset": {raw}}, "")
	within(t, "github 403", forbidden.RetryAfter)
	gitlab := ClassifyHTTPWithMessage("gitlab", http.StatusForbidden,
		http.Header{"Ratelimit-Remaining": {"0"}, "Ratelimit-Reset": {raw}}, "")
	within(t, "gitlab 403", gitlab.RetryAfter)
}

func TestTheAppTokenExchangeKeepsReadingTheResetInSeconds(t *testing.T) {
	t.Parallel()
	raw := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	response := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"X-Ratelimit-Reset": {raw}},
		Body:       io.NopCloser(strings.NewReader(`{}`)),
	}
	got := classifyResponseWithBody("github", "/app/installations/1/access_tokens", response)
	if got == nil || got.Class != ErrorRateLimited || got.RetryAfter < 59*time.Minute || got.RetryAfter > time.Hour+2*time.Second {
		t.Fatalf("token exchange 429 classification = %+v, want rate_limited with about 1h", got)
	}
}

func TestTheAppTokenExchangeHonoursAResetMoreThanADayAhead(t *testing.T) {
	t.Parallel()
	raw := strconv.FormatInt(time.Now().Add(48*time.Hour).Unix(), 10)
	response := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"X-Ratelimit-Reset": {raw}},
		Body:       io.NopCloser(strings.NewReader(`{}`)),
	}
	got := classifyResponseWithBody("github", "/app/installations/1/access_tokens", response)
	if got == nil || got.Class != ErrorRateLimited || got.RetryAfter < 48*time.Hour-2*time.Second || got.RetryAfter > 48*time.Hour+2*time.Second {
		t.Fatalf("token exchange 429 with a reset 48h ahead = %+v, want rate_limited with about 48h, as main", got)
	}
}
