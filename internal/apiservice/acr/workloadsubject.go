package acr

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrSubjectNotConfigured is a SubjectSource's answer when the registry holds no single live acr-api workload (no
// row, several rows, a disabled one, or one that does not admit the go-api audience): every workload token is
// refused as not_configured.
var ErrSubjectNotConfigured = errors.New("acr: no single live acr-api workload in the registry")

// SubjectSource names the one workload principal (prn_, the session wire form a workload token's sub carries) the
// internal acr routes accept. A nil source refuses every workload token as not_configured; an error other than
// ErrSubjectNotConfigured is a refusal as unavailable, never a pass.
type SubjectSource interface {
	Subject(ctx context.Context) (string, error)
}

// StaticSubject is a SubjectSource of one fixed value.
type StaticSubject string

// Subject returns the value; empty is not configured.
func (s StaticSubject) Subject(context.Context) (string, error) {
	if s == "" {
		return "", ErrSubjectNotConfigured
	}
	return string(s), nil
}

// subjectReadTimeout bounds one registry read.
const subjectReadTimeout = 3 * time.Second

// CachedSubject caches a registry read for TTL, so a registry change (a rotation to a new principal, a disabled
// workload) takes effect within TTL with no restart. An answer, including not configured, is cached for TTL. A read
// error serves the last good answer only while it is younger than twice TTL; past that the error is the answer
// (unavailable), so a registry that stays unreadable stops serving a stale principal.
type CachedSubject struct {
	Read func(context.Context) (string, error)
	TTL  time.Duration
	Now  func() time.Time

	mu    sync.Mutex
	have  bool
	value string
	err   error
	at    time.Time
}

// Subject implements SubjectSource.
func (c *CachedSubject) Subject(ctx context.Context) (string, error) {
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := now()
	if c.have && t.Sub(c.at) < c.TTL {
		return c.value, c.err
	}
	readCtx, cancel := context.WithTimeout(ctx, subjectReadTimeout)
	defer cancel()
	value, err := c.Read(readCtx)
	if err != nil && !errors.Is(err, ErrSubjectNotConfigured) {
		if c.have && c.err == nil && t.Sub(c.at) < 2*c.TTL {
			return c.value, nil
		}
		return "", err
	}
	c.have, c.value, c.err, c.at = true, value, err, t
	return value, err
}
