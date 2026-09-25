package graph

import (
	"math"
	"sync"
	"time"

	"atlassian/atlassian"
)

type tokenBucket struct {
	capacity   float64
	refillRate float64
	tokens     float64
	lastRefill time.Time
	now        func() time.Time
	sleep      func(time.Duration)
	mu         sync.Mutex
}

func newTokenBucket(now func() time.Time, sleep func(time.Duration)) *tokenBucket {
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = time.Sleep
	}
	return &tokenBucket{
		capacity:   10000,
		refillRate: 10000.0 / 60.0,
		tokens:     10000,
		lastRefill: now(),
		now:        now,
		sleep:      sleep,
	}
}

func (b *tokenBucket) consume(cost float64, maxWait time.Duration) (time.Duration, error) {
	if cost <= 0 {
		return 0, nil
	}
	if maxWait < 0 {
		maxWait = 0
	}

	waited := time.Duration(0)
	deadline := b.now().Add(maxWait)
	var lastWait time.Duration

	for {
		now := b.now()
		b.mu.Lock()
		elapsed := now.Sub(b.lastRefill).Seconds()
		if elapsed > 0 {
			b.tokens = math.Min(b.capacity, b.tokens+(elapsed*b.refillRate))
			b.lastRefill = now
		}
		if b.tokens >= cost {
			b.tokens -= cost
			b.mu.Unlock()
			return waited, nil
		}
		needed := cost - b.tokens
		lastWait = time.Duration(float64(time.Second) * (needed / b.refillRate))
		b.mu.Unlock()

		remaining := deadline.Sub(b.now())
		if remaining <= 0 || lastWait <= 0 {
			return waited, &atlassian.LocalRateLimitError{
				EstimatedCost:  cost,
				WaitSeconds:    lastWait.Seconds(),
				MaxWaitSeconds: maxWait.Seconds(),
			}
		}
		sleepFor := lastWait
		if sleepFor > remaining {
			sleepFor = remaining
		}
		if sleepFor <= 0 {
			return waited, &atlassian.LocalRateLimitError{
				EstimatedCost:  cost,
				WaitSeconds:    lastWait.Seconds(),
				MaxWaitSeconds: maxWait.Seconds(),
			}
		}
		b.sleep(sleepFor)
		waited += sleepFor
	}
}
