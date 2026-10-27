//go:build integration

package authruntime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

func componentNamed(t *testing.T, components []lifecycle.Component, name string) lifecycle.Component {
	t.Helper()
	for _, component := range components {
		if component.Name() == name {
			return component
		}
	}
	t.Fatalf("no component %q", name)
	return nil
}

// A request that meets a closed pool (the forced-close probe: the pool is closed under the
// live routes, as it is once the API budget has expired and a handler still runs) answers 503
// with Retry-After 1, on every route that reaches the store, never 500.
func TestShutdownInFlightRefreshNever500IntegrationClosedPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org := e.seed(t, ctx)
	hash, err := passwordhash.Hash("shutdown-password")
	if err != nil {
		t.Fatal(err)
	}
	email := "shutdown-" + user.String()[:8] + "@example.com"
	if _, err := e.admin.Exec(ctx, `UPDATE users SET password_hash = $2, email = $3 WHERE id = $1`, user, hash, email); err != nil {
		t.Fatal(err)
	}
	issued := e.start(t, ctx, user, &org)
	client := map[string]string{"X-Forwarded-For": "198.51.100.9", "Content-Type": "application/json"}
	login := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"shutdown-password"}`, client)
	if login.status != http.StatusOK {
		t.Fatalf("login before the close = %d %s", login.status, login.body)
	}
	refreshToken := object(t, login)["refresh_token"].(string)

	if err := componentNamed(t, e.components, "auth-postgres").Shutdown(ctx); err != nil {
		t.Fatalf("close the pool: %v", err)
	}

	for name, got := range map[string]answer{
		"login":          e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"shutdown-password"}`, client),
		"compat refresh": e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+refreshToken+`"}`, client),
		"v1 refresh":     e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(issued.Credential.Reveal()), ""),
	} {
		if got.status != http.StatusServiceUnavailable {
			t.Errorf("%s on a closed pool = %d, want 503: %s", name, got.status, got.body)
			continue
		}
		if ra := got.header.Get("Retry-After"); ra != "1" {
			t.Errorf("%s 503 Retry-After = %q, want 1", name, ra)
		}
	}
}

// SIGTERM during traffic: the runtime is cancelled while logins and refreshes are in flight.
// Every answer is a success, a refusal or a 503; none is a 500.
func TestShutdownInFlightRefreshNever500IntegrationDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnvWith(t, ctx, map[string]string{
		authconfig.EnvOperatorAddress: "127.0.0.2:0",
		authconfig.EnvShutdownTimeout: "10s",
	})
	user, org := e.seed(t, ctx)
	hash, err := passwordhash.Hash("shutdown-password")
	if err != nil {
		t.Fatal(err)
	}
	email := "shutdown-" + user.String()[:8] + "@example.com"
	if _, err := e.admin.Exec(ctx, `UPDATE users SET password_hash = $2, email = $3 WHERE id = $1`, user, hash, email); err != nil {
		t.Fatal(err)
	}
	_ = org

	runtime, err := lifecycle.New(lifecycle.Options{
		Logger:          discard(),
		ShutdownTimeout: 10 * time.Second,
		Components:      e.components,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	var base string
	for deadline := time.Now().Add(20 * time.Second); base == "" && time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if announced, ok := componentNamed(t, e.components, "auth-api-http").(announcedListener); ok {
			base = announced.listener.Address()
		}
	}
	if base == "" {
		t.Fatal("the API listener never bound")
	}
	base = "http://" + base

	var statuses sync.Map
	var sent, answered, allowedErrors, failedOutside atomic.Int64
	var unexpected sync.Map
	var stopping atomic.Bool
	var wg sync.WaitGroup
	body := `{"email":"` + email + `","password":"shutdown-password"}`
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			client := &http.Client{Timeout: 20 * time.Second}
			for !stopping.Load() {
				request, _ := http.NewRequest(http.MethodPost, base+"/api/v1/auth/login", strings.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", 10+worker))
				sent.Add(1)
				response, err := client.Do(request)
				if err != nil {
					// A transport error is allowed only once shutdown has begun (the listener
					// is closed); before it, a dropped request is a failure.
					if stopping.Load() {
						allowedErrors.Add(1)
					} else {
						failedOutside.Add(1)
						unexpected.Store(err.Error(), true)
					}
					return
				}
				answered.Add(1)
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				count, _ := statuses.LoadOrStore(response.StatusCode, new(atomic.Int64))
				count.(*atomic.Int64).Add(1)
			}
		}(worker)
	}
	time.Sleep(1500 * time.Millisecond)
	stopping.Store(true)
	stop()
	wg.Wait()
	if err := <-done; err != nil {
		t.Logf("runtime stopped with %v", err)
	}
	statuses.Range(func(key, value any) bool {
		t.Logf("status %v: %d", key, value.(*atomic.Int64).Load())
		return true
	})
	if count, ok := statuses.Load(http.StatusInternalServerError); ok {
		t.Fatalf("%d requests answered 500 during shutdown", count.(*atomic.Int64).Load())
	}
	unexpected.Range(func(key, _ any) bool {
		t.Errorf("a request failed outside the shutdown window: %v", key)
		return true
	})
	if sent.Load() == 0 || answered.Load() == 0 {
		t.Fatalf("sent %d, answered %d: the test saw no answer", sent.Load(), answered.Load())
	}
	if got := answered.Load() + allowedErrors.Load() + failedOutside.Load(); got != sent.Load() {
		t.Fatalf("sent %d but answered %d + errors after shutdown began %d + errors before %d: requests are unaccounted for",
			sent.Load(), answered.Load(), allowedErrors.Load(), failedOutside.Load())
	}
	t.Logf("sent %d, answered %d, errors after shutdown began %d", sent.Load(), answered.Load(), allowedErrors.Load())
}
