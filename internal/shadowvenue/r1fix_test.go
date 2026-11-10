package shadowvenue

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A body that ends before its declared end is a drive error: the slot has no body digest, no slot line and no side-file
// line, whatever way the answer broke.
func TestDriveFailsOnAnyBodyReadError(t *testing.T) {
	hijack := func(raw string, hold time.Duration) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			conn, buf, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = buf.WriteString(raw)
			_ = buf.Flush()
			time.Sleep(hold)
		}
	}
	cases := map[string]struct {
		handler http.HandlerFunc
		timeout time.Duration
	}{
		"short Content-Length (6 of 11 bytes)": {func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "11")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("hello "))
		}, 0},
		"chunked without its terminator": {hijack("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nhello \r\n", 0), 0},
		"a read that times out":          {hijack("HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nhello ", 2*time.Second), 300 * time.Millisecond},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(c.handler)
			defer srv.Close()
			plan, err := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
			if err != nil {
				t.Fatal(err)
			}
			venue, _ := testPrincipals(t)
			client := srv.Client()
			client.Timeout = c.timeout
			var out, keys bytes.Buffer
			d := &Driver{Base: srv.URL, Client: client, Gap: MinGap, Sleep: func(context.Context, time.Duration) error { return nil },
				BodyKey: bytes.Repeat([]byte{1}, 32), KeysOut: &keys}
			err = d.Drive(context.Background(), plan, healthyTokens(t, venue, time.Now().Add(time.Hour)), &out)
			if err == nil || !strings.Contains(err.Error(), "body") {
				t.Fatalf("a broken body did not fail the drive: %v", err)
			}
			if lines := strings.Count(out.String(), "\n"); lines > 1 {
				t.Fatalf("a slot line was written for a broken body:\n%s", out.String())
			}
			if keys.String() != keysHeader+"\n" {
				t.Fatalf("a side-file line was written for a broken body: %q", keys.String())
			}
		})
	}
}
