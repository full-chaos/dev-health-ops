package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
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
	public, internal := Listeners("127.0.0.1:0", "", plane, nil, nil)
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
	public, _ := Listeners(occupied.Addr().String(), "", plane, nil, nil)
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

// The two listeners are built from different Plane fields (Handler / InternalHandler,
// CHAOS-7078) and differ in identity handling (CHAOS-6780): the public one strips
// X-DH-Internal-* headers before any handler, the internal one alone honours them.
func TestInternalListenerExistsOnlyWhenItsAddressIsSet(t *testing.T) {
	plane := listenerPlane(t)
	defer plane.Close()
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil, nil)
	if public == nil || internal == nil {
		t.Fatalf("public=%v internal=%v, want both", public, internal)
	}
	if public.Name() == internal.Name() {
		t.Fatalf("both listeners are named %q", public.Name())
	}
}

// CHAOS-7078: a route mounted only on Plane.InternalHandler must be reachable through the
// internal listener and NOT EXIST on the public one -- proved structurally, by which
// listener served the request, never by the X-DH-Internal-* header presence (a future
// route on this mux, e.g. CHAOS-7096's /query/proof-write, gets this for free). The
// existing, shared route set (proved here with the Wave-0 catch-all's custom 404 body,
// distinguishable from Go's plain-text default) must still be reachable through BOTH
// listeners, unchanged, via InternalHandler's fallthrough to Handler.
func TestInternalOnlyRouteExistsOnlyOnTheInternalListener(t *testing.T) {
	plane := listenerPlane(t)
	defer plane.Close()
	internalMux, ok := plane.InternalHandler.(*http.ServeMux)
	if !ok {
		t.Fatalf("Plane.InternalHandler is a %T, want *http.ServeMux", plane.InternalHandler)
	}
	internalMux.HandleFunc("/internal-only-probe", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil)
	ctx := context.Background()
	if err := public.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer public.Shutdown(ctx)
	if err := internal.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer internal.Shutdown(ctx)

	get := func(addr, path string) (int, string) {
		resp, err := http.Get("http://" + addr + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(body)
	}

	if code, _ := get(internal.Address(), "/internal-only-probe"); code != http.StatusTeapot {
		t.Fatalf("internal listener + internal-only route: %d, want 418", code)
	}
	// Plain 404, not the custom catch-all body checked below: this path is genuinely
	// unregistered on the public mux, not merely answering the same status by chance.
	if code, body := get(public.Address(), "/internal-only-probe"); code != http.StatusNotFound || strings.Contains(body, "Not Found") {
		t.Fatalf("public listener + internal-only route: %d %q, want a plain 404 (must not exist there)", code, body)
	}
	// The shared route set (the Wave-0 catch-all) is unchanged on both: still reachable,
	// still the SAME custom-404 handler, via InternalHandler's fallthrough on the internal
	// side -- the exact JSON body distinguishes "this handler ran" from "no route matched".
	const wantBody = `{"detail":"Not Found"}` + "\n"
	if code, body := get(public.Address(), "/api/v1/not-a-route"); code != http.StatusNotFound || body != wantBody {
		t.Fatalf("public listener + shared route: %d %q, want 404 %q", code, body, wantBody)
	}
	if code, body := get(internal.Address(), "/api/v1/not-a-route"); code != http.StatusNotFound || body != wantBody {
		t.Fatalf("internal listener + shared route: %d %q, want 404 %q", code, body, wantBody)
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
