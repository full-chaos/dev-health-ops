package authruntime

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// The key-state reader is registered right after the store, so shutdown (the
// reverse of this order) ends its read before the pool closes.
func TestConfigureRegistersTheKeyStatesComponentAfterTheStore(t *testing.T) {
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(map[string]string{
		authconfig.EnvDatabaseURI:     "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:  "/nonexistent",
		authconfig.EnvSigningKeyID:    "key-states-test",
		authconfig.EnvAPIAddress:      "127.0.0.1:0",
		authconfig.EnvOperatorAddress: "127.0.0.1:1",
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
	for i, component := range components {
		if _, ok := component.(keyStatesComponent); ok {
			if i != 1 {
				t.Fatalf("key-state component at index %d, want 1 (right after the store)", i)
			}
			return
		}
	}
	t.Fatal("configure did not register the key-state component")
}
