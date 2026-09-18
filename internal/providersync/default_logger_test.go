package providersync

import (
	"log/slog"
	"testing"
)

// swapDefaultLogger installs logger as the process-wide slog default until the
// test ends. The default is shared by every goroutine in the test binary, so a
// swap that overlaps a parallel test races with that test's logging. t.Setenv
// panics in a test that is parallel or has a parallel ancestor, which turns
// that misuse into a deterministic failure at the call site.
func swapDefaultLogger(t *testing.T, logger *slog.Logger) {
	t.Helper()
	t.Setenv("DEV_HEALTH_TEST_SWAPS_DEFAULT_LOGGER", "1")
	previous := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(previous) })
}
