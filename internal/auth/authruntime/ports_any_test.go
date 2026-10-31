package authruntime

import (
	"net"
	"strings"
	"testing"
	"time"
)

// The service binds port 0 and reports the address it got, so no port is freed and bound later. A neighbour that
// churns ephemeral ports the whole time (a busy runner) cannot make a start fail, and each reported address is the one
// its listener really accepts on.
func TestAStartOnPortZeroReportsTheAddressItBoundWhileNeighboursChurnPorts(t *testing.T) {
	stopChurn := make(chan struct{})
	churned := make(chan struct{})
	go func() {
		defer close(churned)
		for {
			select {
			case <-stopChurn:
				return
			default:
			}
			var held []net.Listener
			for range 64 {
				if l, err := net.Listen("tcp", "127.0.0.1:0"); err == nil {
					held = append(held, l)
				}
			}
			for _, l := range held {
				_ = l.Close()
			}
		}
	}()
	t.Cleanup(func() { close(stopChurn); <-churned })

	for range 10 {
		_, stop, api, operator := startOnAnyPort(t, brokenEnvironment(t, anyAPIAddress, anyOperatorAddress))
		if api == operator || strings.HasSuffix(api, ":0") || strings.HasSuffix(operator, ":0") {
			t.Fatalf("api %q and operator %q: want two distinct, real ports", api, operator)
		}
		for _, address := range []string{api, operator} {
			connection, err := net.DialTimeout("tcp", address, time.Second)
			if err != nil {
				t.Fatalf("the service reported %s but nothing accepts there: %v", address, err)
			}
			_ = connection.Close()
		}
		if code := stop(); code != 0 {
			t.Fatalf("Execute exited %d, want 0", code)
		}
	}
}
