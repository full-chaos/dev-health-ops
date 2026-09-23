package webhookintake

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// pagerdutyLimitPerMinute is rate_limit.py's "60/minute" on pagerduty_webhook
// (@limiter.limit), keyed by get_forwarded_ip (IP-keyed -- see rate_limit.py's
// key_func=get_forwarded_ip; GitHub/GitLab/Jira/health carry no limit either,
// matching router.py, which has no @limiter.limit decorator on any of them).
const pagerdutyLimitPerMinute = 60

// keyedBucket is a per-key fixed-window counter: ceil requests per key per
// window, reset at the window boundary. Sized for a single process; every
// caller of a route shares the api Service's one instance.
type keyedBucket struct {
	mu       sync.Mutex
	ceil     int
	window   time.Duration
	now      func() time.Time
	counts   map[string]int
	resetsAt map[string]time.Time
}

func newKeyedBucket(ceil int, window time.Duration, now func() time.Time) *keyedBucket {
	if now == nil {
		now = time.Now
	}
	return &keyedBucket{ceil: ceil, window: window, now: now, counts: map[string]int{}, resetsAt: map[string]time.Time{}}
}

// Allow reports whether key may proceed, incrementing its counter either way
// so a caller cannot probe the ceiling for free.
func (b *keyedBucket) Allow(key string) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if resetAt, ok := b.resetsAt[key]; !ok || !now.Before(resetAt) {
		b.counts[key] = 0
		b.resetsAt[key] = now.Add(b.window)
	}
	b.counts[key]++
	return b.counts[key] <= b.ceil
}

type rateLimiters struct {
	pagerduty *keyedBucket
}

func newRateLimiters(now func() time.Time) *rateLimiters {
	return &rateLimiters{pagerduty: newKeyedBucket(pagerdutyLimitPerMinute, time.Minute, now)}
}

// forwardedIP ports get_forwarded_ip's shape: the first X-Forwarded-For
// entry when present, else the connection's own remote address.
func forwardedIP(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		first, _, _ := strings.Cut(forwarded, ",")
		return strings.TrimSpace(first)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
