package apiservice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// CHAOS-8654: the cached posture check's execution ran on context.Background() and logged after the
// api had shut down. The pool component stops it, and waits for it, before it closes the pool.
func TestPgxpoolComponentShutdownStopsThePostureCheckFirst(t *testing.T) {
	started := make(chan struct{})
	var ended atomic.Bool
	posture := postgres.NewCachedRunCheck("api_postgres", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		ended.Store(true)
		return ctx.Err()
	}, postgres.PostureCheckOptions{RunTimeout: time.Hour})
	posture.Warm()
	<-started

	component := &pgxpoolComponent{posture: posture}
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !ended.Load() {
		t.Fatal("Shutdown returned while the posture check still ran")
	}
}

// The runtime releases a pool component it never started; Release stops the posture check first.
func TestPgxpoolComponentReleaseStopsThePostureCheck(t *testing.T) {
	started := make(chan struct{})
	var ended atomic.Bool
	posture := postgres.NewCachedRunCheck("api_postgres", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		ended.Store(true)
		return ctx.Err()
	}, postgres.PostureCheckOptions{RunTimeout: time.Hour})
	posture.Warm()
	<-started

	(&pgxpoolComponent{posture: posture}).Release()
	if !ended.Load() {
		t.Fatal("Release returned while the posture check still ran")
	}
}
