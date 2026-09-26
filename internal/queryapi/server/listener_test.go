package server

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func listenerPlane(t *testing.T) *Plane {
	t.Helper()
	plane, err := Build(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	return plane
}

// A Listener binds in Start (a taken address fails there), serves until Shutdown, and reports
// its bound address; before Start it has none.
func TestListenerBindsInStartAndStopsInShutdown(t *testing.T) {
	plane := listenerPlane(t)
	defer plane.Close()
	public, internal := Listeners("127.0.0.1:0", "", plane, nil)
	if internal != nil {
		t.Fatalf("an internal listener exists with no internal address")
	}
	if public.Address() != "" {
		t.Fatalf("Address() = %q before Start", public.Address())
	}
	if err := public.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	address := public.Address()
	if address == "" {
		t.Fatal("Address() is empty after Start")
	}
	response, err := http.Get("http://" + address + "/api/v1/not-a-route")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", response.StatusCode)
	}
	if err := public.Start(context.Background()); err == nil {
		t.Fatal("a second Start succeeded")
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := public.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	if connection, err := net.DialTimeout("tcp", address, time.Second); err == nil {
		_ = connection.Close()
		t.Fatalf("%s still accepts connections after Shutdown", address)
	}
}

func TestListenerFailsStartWhenTheAddressIsTaken(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	plane := listenerPlane(t)
	defer plane.Close()
	public, _ := Listeners(occupied.Addr().String(), "", plane, nil)
	if err := public.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded on a taken address")
	}
	if public.Address() != "" {
		t.Fatalf("Address() = %q after a failed Start", public.Address())
	}
	// A listener that never started has nothing to shut down.
	if err := public.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown of a listener that never started: %v", err)
	}
}

// Both listeners share the plane's handler but differ in identity handling (CHAOS-6780): the
// public one strips X-DH-Internal-* headers before any handler, the internal one alone honours them.
func TestInternalListenerExistsOnlyWhenItsAddressIsSet(t *testing.T) {
	plane := listenerPlane(t)
	defer plane.Close()
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil)
	if public == nil || internal == nil {
		t.Fatalf("public=%v internal=%v, want both", public, internal)
	}
	if public.Name() == internal.Name() {
		t.Fatalf("both listeners are named %q", public.Name())
	}
}

// Closer releases the routes' dependencies exactly when the runtime shuts it down, and starts nothing.
func TestCloserReleasesThePlaneOnShutdown(t *testing.T) {
	closed := 0
	closer := Closer{Plane: &Plane{Close: func() { closed++ }}}
	if err := closer.Start(context.Background()); err != nil || closed != 0 {
		t.Fatalf("Start: %v, closed %d: starting must release nothing", err, closed)
	}
	if err := closer.Shutdown(context.Background()); err != nil || closed != 1 {
		t.Fatalf("Shutdown: %v, closed %d, want the plane closed once", err, closed)
	}
	if err := (Closer{}).Shutdown(context.Background()); err != nil {
		t.Fatalf("a Closer with no plane: %v", err)
	}
}
