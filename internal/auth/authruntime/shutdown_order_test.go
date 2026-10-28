package authruntime

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

func configuredForShutdown(t *testing.T) (authconfig.Config, []lifecycle.Component) {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(map[string]string{
		authconfig.EnvDatabaseURI:     "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:  "/nonexistent",
		authconfig.EnvSigningKeyID:    "shutdown-order-test",
		authconfig.EnvAPIAddress:      "127.0.0.1:0",
		authconfig.EnvOperatorAddress: "127.0.0.2:0",
		authconfig.EnvShutdownTimeout: "20s",
	})})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	components, err := configure(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	return cfg, components
}

func indexOf(t *testing.T, components []lifecycle.Component, name string) int {
	t.Helper()
	for i, component := range components {
		if component.Name() == name {
			return i
		}
	}
	t.Fatalf("component %q is not registered", name)
	return -1
}

// lifecycle stops components in reverse of this list: the readiness gate goes
// first, the API listener drains before anything it serves from closes, and the
// pool closes last.
func TestShutdownOrderGateFirstAPIBeforeStoreStoreLast(t *testing.T) {
	_, components := configuredForShutdown(t)
	gate := indexOf(t, components, "readiness-gate")
	api := indexOf(t, components, "auth-api-http")
	operator := indexOf(t, components, "operator-http")
	store := indexOf(t, components, "auth-postgres")
	if gate != len(components)-1 {
		t.Errorf("readiness-gate at %d, want last (%d): it must stop first", gate, len(components)-1)
	}
	if store != 0 {
		t.Errorf("auth-postgres at %d, want 0: the pool must close last", store)
	}
	if api <= store {
		t.Errorf("auth-api-http at %d is not after auth-postgres (%d): the API would stop after the pool closed", api, store)
	}
	if api <= operator {
		t.Errorf("auth-api-http at %d is not after operator-http (%d): the API must drain before the operator surface stops", api, operator)
	}
	for i, component := range components {
		if i != store && i != gate && i <= store {
			t.Errorf("%s at %d precedes the store", component.Name(), i)
		}
	}
}

// The API listener reserves three quarters of the shutdown timeout, so a slow
// in-flight login is not cut at an equal share of it.
func TestAPIListenerReservesThreeQuartersOfTheShutdownTimeout(t *testing.T) {
	cfg, components := configuredForShutdown(t)
	api := components[indexOf(t, components, "auth-api-http")]
	source, ok := api.(lifecycle.ShutdownBudgetSource)
	if !ok {
		t.Fatal("the API listener does not implement lifecycle.ShutdownBudgetSource")
	}
	if got, want := source.ShutdownBudget(), cfg.ShutdownTimeout*3/4; got != want || got != 15*time.Second {
		t.Fatalf("API shutdown budget = %v, want %v (15s of 20s)", got, want)
	}
	if source, ok := components[indexOf(t, components, "operator-http")].(lifecycle.ShutdownBudgetSource); ok && source.ShutdownBudget() != 0 {
		t.Errorf("operator-http reserves %v: only the API listener holds a reserved budget", source.ShutdownBudget())
	}
}

// timedComponent records, the moment the component's Shutdown returns, whether the attempt context the runtime handed it was still
// live: it stopped inside the slice of the shutdown budget it was given.
type timedComponent struct {
	lifecycle.Component
	insideSlice *bool
}

func (c timedComponent) Shutdown(ctx context.Context) error {
	err := c.Component.Shutdown(ctx)
	*c.insideSlice = ctx.Err() == nil
	return err
}

func (c timedComponent) ShutdownBudget() time.Duration {
	if source, ok := c.Component.(lifecycle.ShutdownBudgetSource); ok {
		return source.ShutdownBudget()
	}
	return 0
}

// Worst case the API listener uses its whole 15s; the other components share the
// remaining 5s by remaining/(index+1), which is an equal 5s/(n-2) each for the
// n-2 components after the gate and the API. A quiet stop must fit the smallest
// slice, and the pool (index 0) is last, so it gets whatever is left.
func TestShutdownSlicesFitAfterTheAPIBudget(t *testing.T) {
	cfg, components := configuredForShutdown(t)
	insideSlice := make([]bool, len(components))
	wrapped := make([]lifecycle.Component, len(components))
	for i, component := range components {
		wrapped[i] = timedComponent{Component: component, insideSlice: &insideSlice[i]}
	}
	runtime, err := lifecycle.New(lifecycle.Options{
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		ShutdownTimeout: cfg.ShutdownTimeout,
		Components:      wrapped,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(cfg.ShutdownTimeout):
		t.Fatal("shutdown overran the whole timeout")
	}
	apiBudget := cfg.ShutdownTimeout * 3 / 4
	smallest := (cfg.ShutdownTimeout - apiBudget) / time.Duration(len(components)-2)
	t.Logf("components=%d api budget=%v smallest slice=%v", len(components), apiBudget, smallest)
	for i, component := range components {
		t.Logf("%s stopped inside its attempt budget: %v", component.Name(), insideSlice[i])
		if component.Name() == "auth-api-http" || component.Name() == "readiness-gate" {
			continue
		}
		// This checks the component's real slice through its attempt context, not a theoretical share of the shutdown budget: the
		// context of every other component is still live when its stop returns. A stop that is slow because the host is loaded fails
		// this too: the budget is the runtime's own, no constant is added here.
		if !insideSlice[i] {
			t.Errorf("%s did not stop inside its attempt budget (the smallest slice is %v)", component.Name(), smallest)
		}
	}
}
