package server

import (
	"net"
	"os"
	"testing"
)

// /readyz picks its fixed body from the failed check NAMES only, so an I/O timeout answers exactly as a plain
// failure does (CHAOS-11617 changes the registry cause, never this decision).
func TestCompatReadyzIgnoresTheCauseOfAnIOTimeout(t *testing.T) {
	for name, failure := range map[string]error{
		"os_deadline":  os.ErrDeadlineExceeded,
		"net_op_error": &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded},
	} {
		code, body, _ := compatServe(t, map[string]error{"query_clickhouse": nil, "query_postgres": failure}, true, "/readyz")
		if code != 503 || body != "not ready: postgres" {
			t.Errorf("%s: /readyz = %d %q, want 503 %q", name, code, body, "not ready: postgres")
		}
	}
}
