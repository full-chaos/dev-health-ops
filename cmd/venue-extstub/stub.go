package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Hosts are the outside hosts go-api calls on the external GAP rows, by service. The venue pins each name to the stub
// and gives go-api a trust store that holds only the venue CA, so no real host can complete TLS.
var Hosts = map[string]string{
	"api.stripe.com":         "stripe",
	"api.pagerduty.com":      "pagerduty",
	"identity.pagerduty.com": "pagerduty",
	"github.com":             "github",
	"api.github.com":         "github",
}

// unknownService is the service a request to another host is logged as; the venue judge voids a run on it.
const unknownService = "unknown"

// hit is one log line: when, which service, the method, host and path. No header, body or query is logged, so no
// secret and no address can reach the log.
type hit struct {
	T       string `json:"t"`
	Service string `json:"service"`
	Method  string `json:"method"`
	Host    string `json:"host"`
	Path    string `json:"path"`
}

// hitLog writes hits as JSON lines to a new owner-only file.
type hitLog struct {
	mu  sync.Mutex
	w   io.Writer
	now func() time.Time
}

// openLog creates path (it must not exist) with mode 0600.
func openLog(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
}

func (l *hitLog) record(service, method, host, path string) error {
	line, err := json.Marshal(hit{T: l.now().UTC().Format(time.RFC3339Nano), Service: service, Method: method, Host: host, Path: path})
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.w.Write(append(line, '\n')); err != nil {
		return err
	}
	if f, ok := l.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

// answer is a canned reply.
type answer struct {
	status int
	body   string
}

// answerFor is the stub's frozen reply to a request: the same for every caller and both runs of a pair, so the
// yardstick and the enforced run see one outside world. Stripe answers the plan pull's two lists empty (nothing to
// write) and refuses every other call as an invalid request (no retry, no write after it); PagerDuty and GitHub refuse
// the credential. A host the stub does not serve is 421.
func answerFor(service, method, path string) answer {
	switch service {
	case "stripe":
		if method == http.MethodGet && (path == "/v1/products" || path == "/v1/prices") {
			return answer{http.StatusOK, `{"object":"list","data":[],"has_more":false,"url":"` + path + `"}`}
		}
		return answer{http.StatusBadRequest, `{"error":{"type":"invalid_request_error","message":"venue stub: refused"}}`}
	case "pagerduty":
		return answer{http.StatusUnauthorized, `{"error":{"message":"venue stub: credential refused","code":2006}}`}
	case "github":
		return answer{http.StatusUnauthorized, `{"message":"venue stub: credential refused"}`}
	}
	return answer{http.StatusMisdirectedRequest, `{"message":"venue stub: host not served"}`}
}

// serviceOf is the service of a request host (port and case dropped).
func serviceOf(hostport string) (string, string) {
	host := strings.ToLower(hostport)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if service, ok := Hosts[host]; ok {
		return service, host
	}
	return unknownService, host
}

// handler logs each request, then answers it. A request whose hit cannot be logged is answered 500: an outside call
// the judge cannot see must not look like a served one.
func handler(log *hitLog, mode string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
		service, host := serviceOf(r.Host)
		if err := log.record(service, r.Method, host, r.URL.Path); err != nil {
			http.Error(w, "venue stub: log failed", http.StatusInternalServerError)
			return
		}
		a := answerFor(service, r.Method, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(a.status)
		_, _ = fmt.Fprint(w, a.body)
	})
}

const (
	modeRun  = "run"
	modeSeed = "seed"
)

// configDigest: skeleton.
func configDigest(mode string) string { return "" }
