package authruntime

import (
	"context"
	"net"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// A busy runner hands a released port to another process before the service binds it. The test plays that process: it
// takes the first address of the draw the moment it is freed, and the service must still start.
func TestAStartSurvivesAnAddressTakenAfterItWasFreed(t *testing.T) {
	afterFreed = func(t *testing.T, addrs []string) {
		thief, err := net.Listen("tcp", addrs[0])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = thief.Close() })
	}
	t.Cleanup(func() { afterFreed = nil })

	ports := reservePorts(t, 2)
	env := brokenEnvironment(t, ports[0], ports[1])
	env[authconfig.EnvLogLevel] = "info"
	logs := &safeBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exit := make(chan int, 1)
	go func() { exit <- Execute(ctx, nil, lookupFrom(env), IO{Stdout: logs, Stderr: logs}) }()
	awaitBoundAddressOrExit(t, logs, exit, "auth-api-http", ports[0])
}
