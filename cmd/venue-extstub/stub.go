package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
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

// UnservedHosts are the other code-constant hosts go-api can dial (ADDENDUM-13267-3 section 2 B). The venue pins them
// to the stub too and the leaf names them, so a dial of one reaches the stub, is answered 421 and is logged with service
// unknown, which voids the run (acp Q-C1): no outside call is silent.
var UnservedHosts = []string{
	"accounts.google.com", "api.anthropic.com", "api.atlassian.com", "api.eu.pagerduty.com", "api.linear.app", "api.openai.com",
	"api.resend.com", "api.typesafe.ai", "app.launchdarkly.com", "connect.stripe.com", "files.stripe.com",
	"generativelanguage.googleapis.com", "gitlab.com", "meter-events.stripe.com", "oauth2.googleapis.com", "www.googleapis.com",
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

// Modes: run is the frozen answer table the judged runs see; seed answers only the org E PagerDuty credential's
// validation probe (acp ADDENDUM-13267-2 Q-B1), before the runs, to its own log.
const (
	modeRun  = "run"
	modeSeed = "seed"
)

// seedProbe is the one request seed mode answers: the PagerDuty credential validation read
// (internal/providerfoundation/pagerduty_credential_validation.go:166).
const (
	seedHost  = "api.pagerduty.com"
	seedPath  = "/services"
	seedQuery = "limit=1"
	// seedBody names the venue's stub PagerDuty account (subdomain venue-stub).
	seedBody = `{"services":[{"id":"PVENUE1","account":{"id":"PVENUEACCT","subdomain":"venue-stub","name":"venue-stub"}}],"limit":1,"offset":0,"more":false}`
)

// answerIn is the reply in mode: seed mode answers the seed probe and refuses everything else (403); run mode is the
// frozen table (answerFor).
func answerIn(mode, service, method, host, path, query string) answer {
	if mode == modeSeed {
		if method == http.MethodGet && host == seedHost && path == seedPath && query == seedQuery {
			return answer{http.StatusOK, seedBody}
		}
		return answer{http.StatusForbidden, bodySeedRefused}
	}
	return answerFor(service, method, host, path)
}

// answerFor is the stub's frozen reply to a request: the same for every caller and both runs of a pair, so the
// yardstick and the enforced run see one outside world. Stripe answers the plan pull's two lists empty, the subscription
// update and retrieve, and the invoice void, with fixed objects, and refuses every other call as an invalid request (no
// retry, no write after it); PagerDuty answers the client-credentials token and the services list, GitHub the OAuth
// code exchange and the user's installations, with fixed bodies, and both refuse every other call's credential. A host
// the stub does not serve is 421.
func answerFor(service, method, host, path string) answer {
	switch service {
	case "stripe":
		switch {
		case method == http.MethodGet && path == "/v1/products":
			return answer{http.StatusOK, bodyProducts}
		case method == http.MethodGet && path == "/v1/prices":
			return answer{http.StatusOK, bodyPrices}
		case (method == http.MethodPost || method == http.MethodGet) && isOneSegment(path, "/v1/subscriptions/", ""):
			return answer{http.StatusOK, bodySubscription}
		case method == http.MethodPost && isOneSegment(path, "/v1/invoices/", "/void"):
			return answer{http.StatusOK, bodyInvoiceVoid}
		}
		return answer{http.StatusBadRequest, bodyStripeRefused}
	case "pagerduty":
		switch {
		case method == http.MethodPost && host == "identity.pagerduty.com" && path == "/oauth/token":
			return answer{http.StatusOK, bodyPagerDutyToken}
		case method == http.MethodGet && host == "api.pagerduty.com" && path == "/services":
			return answer{http.StatusOK, bodyPagerDutyServices}
		}
		return answer{http.StatusUnauthorized, bodyPagerDutyRefused}
	case "github":
		switch {
		case method == http.MethodPost && host == "github.com" && path == "/login/oauth/access_token":
			return answer{http.StatusOK, bodyGitHubToken}
		case method == http.MethodGet && host == "api.github.com" && path == "/user/installations":
			return answer{http.StatusOK, bodyGitHubInstallations}
		}
		return answer{http.StatusUnauthorized, bodyGitHubRefused}
	}
	return answer{http.StatusMisdirectedRequest, bodyNotServed}
}

// isOneSegment reports whether path is prefix, one non-empty segment without a slash, then suffix.
func isOneSegment(path, prefix, suffix string) bool {
	rest, ok := strings.CutPrefix(path, prefix)
	if !ok {
		return false
	}
	id, ok := strings.CutSuffix(rest, suffix)
	return ok && id != "" && !strings.Contains(id, "/")
}

// The frozen answer bodies: constants only, so no request byte can reach a response.
const (
	bodyProducts            = `{"object":"list","data":[],"has_more":false,"url":"/v1/products"}`
	bodyPrices              = `{"object":"list","data":[],"has_more":false,"url":"/v1/prices"}`
	bodySubscription        = `{"id":"sub_venue_stub","object":"subscription","status":"active","cancel_at_period_end":false,"customer":"cus_venue_stub","items":{"object":"list","data":[{"id":"si_venue_stub","object":"subscription_item","price":{"id":"price_venue_stub","object":"price"}}],"has_more":false,"url":"/v1/subscription_items"}}`
	bodyInvoiceVoid         = `{"id":"in_venue_stub","object":"invoice","status":"void"}`
	bodyPagerDutyToken      = `{"access_token":"venue-stub-token","token_type":"bearer","expires_in":3600,"scope":"escalation_policies.read incidents.read oncalls.read schedules.read services.read teams.read users.read"}`
	bodyPagerDutyServices   = `{"services":[{"id":"PVENUE1","name":"venue-stub-service","status":"active"}],"limit":100,"offset":0,"more":false}`
	bodyGitHubToken         = `{"access_token":"venue-stub-token","token_type":"bearer","scope":""}`
	bodyGitHubInstallations = `{"total_count":1,"installations":[{"id":1,"account":{"login":"venue-stub","type":"Organization"}}]}`
	bodyStripeRefused       = `{"error":{"type":"invalid_request_error","message":"venue stub: refused"}}`
	bodyPagerDutyRefused    = `{"error":{"message":"venue stub: credential refused","code":2006}}`
	bodyGitHubRefused       = `{"message":"venue stub: credential refused"}`
	bodyNotServed           = `{"message":"venue stub: host not served"}`
	bodySeedRefused         = `{"message":"venue stub: seed mode answers only the seed probe"}`
)

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

// handler logs each request, then answers it in mode. A request whose hit cannot be logged is answered 500: an outside
// call the judge cannot see must not look like a served one.
func handler(log *hitLog, mode string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
		service, host := serviceOf(r.Host)
		if err := log.record(service, r.Method, host, r.URL.Path); err != nil {
			http.Error(w, "venue stub: log failed", http.StatusInternalServerError)
			return
		}
		a := answerIn(mode, service, r.Method, host, r.URL.Path, r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.body))
	})
}

// configProbes are the requests whose answers make up the config digest: every served host and one unknown host, with
// the paths and methods the answer table tells apart.
var configProbes = []struct{ method, path, query string }{
	{"GET", "/v1/products", ""}, {"GET", "/v1/prices", ""}, {"POST", "/v1/subscriptions/x", ""},
	{"GET", "/v1/subscriptions/x", ""}, {"DELETE", "/v1/subscriptions/x", ""}, {"POST", "/v1/invoices/x/void", ""},
	{"POST", "/login/oauth/access_token", ""}, {"GET", "/user/installations", "per_page=100&page=1"},
	{"GET", seedPath, seedQuery}, {"GET", seedPath, ""}, {"POST", "/oauth/token", ""}, {"GET", "/", ""},
}

// configDigest is the sha256 of mode and the answer of every probe on every served host and one unknown host: the
// status file names it, and the venue judge holds it equal across the egress reads.
func configDigest(mode string) string {
	hosts := make([]string, 0, len(Hosts)+len(UnservedHosts)+1)
	for host := range Hosts {
		hosts = append(hosts, host)
	}
	hosts = append(hosts, UnservedHosts...)
	slices.Sort(hosts)
	hosts = append(hosts, "unknown.invalid")
	sum := sha256.New()
	fmt.Fprintf(sum, "mode=%s\n", mode)
	for _, host := range hosts {
		service, _ := serviceOf(host)
		for _, p := range configProbes {
			a := answerIn(mode, service, p.method, host, p.path, p.query)
			fmt.Fprintf(sum, "%s %s %s?%s %d %s\n", host, p.method, p.path, p.query, a.status, a.body)
		}
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil))
}

// writeStatus creates path (it must not exist, mode 0644) with the stub's mode, config digest and start time.
func writeStatus(path, mode string, started time.Time) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, werr := fmt.Fprintf(f, "mode=%s\nconfig=%s\nstarted=%s\n", mode, configDigest(mode), started.UTC().Format(time.RFC3339Nano))
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	return werr
}
