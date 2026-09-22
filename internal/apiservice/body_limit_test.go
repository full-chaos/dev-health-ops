package apiservice

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// zeros is an endless body that never materialises its bytes.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

// TestRouteBodyBoundIsTheIngressBound mounts a route that reads its body and
// drives it at the 50 MiB bound (the prod ops ingress proxy-body-size) in
// both request shapes: a declared Content-Length and an undeclared (chunked)
// body. At the bound the route runs and sees every byte; one byte over, the
// route never runs and the client gets the Python api's 413 body.
func TestRouteBodyBoundIsTheIngressBound(t *testing.T) {
	if maxBodyBytes != 50<<20 {
		t.Fatalf("maxBodyBytes = %d, want 50 MiB", maxBodyBytes)
	}
	var seen int64
	ran := false
	route := httpapi.Route{Method: http.MethodPost, Pattern: "/echo", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran = true
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("route read: %v", err)
		}
		seen = n
		w.WriteHeader(http.StatusNoContent)
	})}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(cfg, quietLogger(), []httpapi.Route{route})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name     string
		size     int64
		declared bool
		want     int
	}{
		{"declared at the bound", maxBodyBytes, true, http.StatusNoContent},
		{"declared one over", maxBodyBytes + 1, true, http.StatusRequestEntityTooLarge},
		{"chunked at the bound", maxBodyBytes, false, http.StatusNoContent},
		{"chunked one over", maxBodyBytes + 1, false, http.StatusRequestEntityTooLarge},
	} {
		t.Run(c.name, func(t *testing.T) {
			ran, seen = false, 0
			request := httptest.NewRequest(http.MethodPost, "/echo", io.LimitReader(zeros{}, c.size))
			request.ContentLength = -1
			if c.declared {
				request.ContentLength = c.size
			}
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != c.want {
				t.Fatalf("status %d, want %d", response.Code, c.want)
			}
			if c.want == http.StatusNoContent {
				if !ran || seen != c.size {
					t.Fatalf("route ran=%v saw %d bytes, want %d", ran, seen, c.size)
				}
				return
			}
			if ran {
				t.Fatal("route ran for an over-bound body")
			}
			if body := response.Body.Bytes(); !bytes.Equal(body, []byte(`{"detail":"Request Entity Too Large"}`)) {
				t.Fatalf("body %s", body)
			}
		})
	}
}
