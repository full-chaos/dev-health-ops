package server

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// A request id with CR or LF never puts a line break in a log line of the 9 CodeQL go/log-injection sites (D6795);
// the response is the one a clean id gets, and the id is not echoed (as before). The header map is written directly so a value the HTTP parser would
// reject reaches the handler. Site: flame_aggregated's report-window log (end_date at the first day, default range).
func TestRequestIDWithLineBreakLogsOneLineAndLeavesTheResponse(t *testing.T) {
	serve := func(id string) (string, *httptest.ResponseRecorder) {
		var logs bytes.Buffer
		prevOut, prevFlags := log.Writer(), log.Flags()
		log.SetOutput(&logs)
		log.SetFlags(0)
		defer func() { log.SetOutput(prevOut); log.SetFlags(prevFlags) }()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=throughput&end_date=0001-01-01", nil)
		req.Header["X-Request-Id"] = []string{id}
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1", Scope: pepgate.Legacy("org-1")}))
		rec := httptest.NewRecorder()
		serveRoute(t, newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{}), rec, req)
		return logs.String(), rec
	}
	_, clean := serve("abcdef")
	for name, id := range map[string]string{"lf": "abc\ndef", "cr": "abc\rdef", "crlf forge": "abc\r\nlevel=ERROR msg=forged"} {
		t.Run(name, func(t *testing.T) {
			out, rec := serve(id)
			line := strings.TrimSuffix(out, "\n")
			if !strings.Contains(line, "report window out of Python's date range") || strings.ContainsAny(line, "\r\n") {
				t.Fatalf("log output %q is not exactly one line of the site", out)
			}
			if rec.Code != clean.Code || rec.Body.String() != clean.Body.String() {
				t.Fatalf("response %d %q, want the clean id's %d %q", rec.Code, rec.Body.String(), clean.Code, clean.Body.String())
			}
			if got := rec.Header().Values("X-Request-Id"); len(got) != 0 {
				t.Fatalf("X-Request-Id echoed %q, want none (echoRequestID refuses the id, as before)", got)
			}
			if rec.Code != http.StatusInternalServerError || rec.Body.String() != `{"detail":"Internal Server Error"}`+"\n" {
				t.Fatalf("response %d %q, want 500 Internal Server Error", rec.Code, rec.Body.String())
			}
		})
	}
	if got := logSafeRequestID("a\r\nb\rc\nd"); got != "abcd" {
		t.Fatalf("logSafeRequestID = %q, want %q", got, "abcd")
	}
	if got := logSafeRequestID("req-123_ok.~"); got != "req-123_ok.~" {
		t.Fatalf("logSafeRequestID changed a clean id: %q", got)
	}
}
