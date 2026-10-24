package chartgates_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/apimetrics"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

const (
	authGroup = "auth_service"
	// authStoreGroup is the rate-limit group of the same service; the critical store-error tier lives there.
	authStoreGroup = "auth_service_rate_limit"
	authStoreWarn  = "AuthServiceRateLimitStoreErrors"
	authStoreHigh  = "AuthServiceRateLimitStoreErrorsHigh"
)

var authAlertNames = []string{
	"AuthServiceTargetDown",
	"AuthRateLimitRejectionsHigh",
	"AuthServiceHighServerErrorRate",
	"AuthServiceMetricsAbsent",
}

func authGroupRules(g *guard, group string) []doc {
	for _, grp := range ruleGroups(g) {
		if str(grp["name"]) == group {
			var out []doc
			for _, r := range list(grp["rules"]) {
				out = append(out, mp(r))
			}
			return out
		}
	}
	g.failf("group %s not found in %s", group, rulesRel)
	return nil
}

func authAlerts(g *guard) []doc { return authGroupRules(g, authGroup) }

// authRuleSet is every rule this leaf pins: the auth_service group plus the critical store-error tier kept in the rate-limit group.
func authRuleSet(g *guard) []doc {
	out := authAlerts(g)
	for _, a := range authGroupRules(g, authStoreGroup) {
		if str(a["alert"]) == authStoreHigh {
			out = append(out, a)
		}
	}
	return out
}

func authAlertNamed(g *guard, name string) doc {
	for _, a := range authRuleSet(g) {
		if str(a["alert"]) == name {
			return a
		}
	}
	g.failf("alert %s not found in groups %s or %s", name, authGroup, authStoreGroup)
	return doc{}
}

// scrapeAuthServiceSeries registers the rate-limit counters and the request
// source on a registry as the auth service does, counts one request and one
// declared limiter, and returns the operator /metrics text.
func scrapeAuthServiceSeries(g *guard) string {
	reg := health.NewRegistry(time.Second)
	if err := apimetrics.Register(reg); err != nil {
		g.failf("cannot register the request source: %v", err)
		return ""
	}
	if err := reg.RegisterMetrics("api_rate_limit_store", httpapi.RateLimitStoreErrors); err != nil {
		g.failf("cannot register the store-error counter: %v", err)
	}
	if err := reg.RegisterMetrics("api_rate_limit_rejections", httpapi.RateLimitRejections); err != nil {
		g.failf("cannot register the rejections counter: %v", err)
	}
	httpapi.NewKeyedLimiter(httpapi.NewMemoryCounters(nil), httpapi.Limit{ID: "alert_pin_probe", Count: 1, Window: time.Hour}).Declare()
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address: "127.0.0.1:0", Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)), Listener: "auth-api-http",
		RequestTimeout: time.Second, MaxBodyBytes: 1024, RateLimit: 1000, RateLimitBurst: 1000,
		Routes: []httpapi.Route{{Method: http.MethodGet, Pattern: "/v1/pin", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})}},
	})
	if err != nil {
		g.failf("cannot build the request source's server: %v", err)
		return ""
	}
	server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/pin", nil))
	var out bytes.Buffer
	if err := reg.WriteMetrics(&out); err != nil {
		g.failf("the registry scrape failed: %v", err)
	}
	return out.String()
}

func authAlertSetIsExact(g *guard) {
	var got []string
	for _, a := range authAlerts(g) {
		got = append(got, str(a["alert"]))
	}
	g.expect(equal(sortedCopy(got), sortedCopy(authAlertNames)), "the %s group holds %v, want exactly %v", authGroup, got, authAlertNames)
	inStore := 0
	for _, a := range authGroupRules(g, authStoreGroup) {
		if str(a["alert"]) == authStoreHigh {
			inStore++
		}
	}
	g.expect(inStore == 1, "%s must be in the %s group exactly once, found %d", authStoreHigh, authStoreGroup, inStore)
	warn, high := storeSelector(str(authGroupRule(g, authStoreGroup, authStoreWarn)["expr"])), storeSelector(str(authGroupRule(g, authStoreGroup, authStoreHigh)["expr"]))
	g.expect(warn != "" && warn == high, "the %s selector %q differs from the warning tier's %q: both tiers must read the same series", authStoreHigh, high, warn)
}

var storeErrorSelector = regexp.MustCompile(`dev_health_api_rate_limit_store_errors_total(\{[^}]*\})`)

func storeSelector(expr string) string {
	if m := storeErrorSelector.FindStringSubmatch(expr); m != nil {
		return m[1]
	}
	return ""
}

func authGroupRule(g *guard, group, name string) doc {
	for _, a := range authGroupRules(g, group) {
		if str(a["alert"]) == name {
			return a
		}
	}
	g.failf("alert %s not found in group %s", name, group)
	return doc{}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func authAlertMetricsAreEmitted(g *guard) {
	scrape := scrapeAuthServiceSeries(g)
	checked := 0
	for _, a := range authRuleSet(g) {
		for metric := range metricNames(str(a["expr"])) {
			if !strings.HasPrefix(metric, "dev_health_api_") {
				continue
			}
			checked++
			g.expect(hasSample(scrape, metric), "%s reads %q, which the auth service scrape does not carry as a sample: an alert on a metric nobody publishes never fires", str(a["alert"]), metric)
		}
	}
	g.expect(checked == 3, "checked %d metric reads across the auth rules, want 3 (a parser that returns nothing must not pass)", checked)
}

func authAlertsSelectTheAuthJob(g *guard) {
	for _, a := range authAlerts(g) {
		g.expect(strings.Contains(str(a["expr"]), `job="auth-service"`), "%s does not select job=\"auth-service\": it would also read another service's series", str(a["alert"]))
	}
}

var groupBy = regexp.MustCompile(`\bby\s*\(([^)]*)\)`)

func authAlertLabelsAreBounded(g *guard) {
	for _, a := range authRuleSet(g) {
		for _, m := range groupBy.FindAllStringSubmatch(str(a["expr"]), -1) {
			for _, label := range strings.Split(m[1], ",") {
				label = strings.TrimSpace(label)
				g.expect(label == "limit" || label == "job", "%s groups by %q: only the closed limit id (and job) may label an auth rule", str(a["alert"]), label)
			}
		}
		expr := strings.TrimSpace(str(a["expr"]))
		g.expect(strings.Contains(expr, "sum") || strings.Contains(expr, "max(") || strings.HasPrefix(expr, "absent("),
			"%s is a bare selector: its result would carry the scrape instance (the pod address); aggregate it away", str(a["alert"]))
		labels := mp(a["labels"])
		g.expect(str(labels["team"]) == "platform", "%s team label is %q, want the existing groups' value platform", str(a["alert"]), str(labels["team"]))
		g.expect(str(labels["severity"]) != "", "%s has no severity", str(a["alert"]))
	}
}

func authAlertShapes(g *guard) {
	high := str(authAlertNamed(g, authStoreHigh)["expr"])
	g.expect(regexp.MustCompile(`increase\([^)]*store_errors_total[^)]*\[10m\]\)\s*\)\s*>\s*20\s*$`).MatchString(strings.TrimSpace(high)), "the high store-error tier must compare > 20 over ten minutes: %q", high)
	rate := authAlertNamed(g, "AuthServiceHighServerErrorRate")
	expr := str(rate["expr"])
	g.expect(strings.Contains(expr, `status_class="5xx"`) && strings.Contains(expr, "/") && strings.Contains(expr, "> 0.05"), "the 5xx rule must be a 5xx-over-all ratio above 0.05: %q", expr)
	g.expect(regexp.MustCompile(`\band\b\s+sum\(rate\(dev_health_api_http_requests_total\{job="auth-service"\}\[5m\]\)\)\s*>\s*[0-9.]+`).MatchString(expr), "the 5xx rule lacks its traffic floor (an idle service must not page on one 5xx): %q", expr)
	absent := str(authAlertNamed(g, "AuthServiceMetricsAbsent")["expr"])
	g.expect(strings.Contains(absent, `absent(up{job="auth-service"})`), "the absent rule must be absent(up{job=\"auth-service\"}): %q", absent)
	down := str(authAlertNamed(g, "AuthServiceTargetDown")["expr"])
	g.expect(strings.Contains(down, `sum by (job) (up{job="auth-service"}) == 0`), "the target-down rule must aggregate the instance away: sum by (job) (up{job=\"auth-service\"}) == 0: %q", down)
	rejections := str(authAlertNamed(g, "AuthRateLimitRejectionsHigh")["expr"])
	g.expect(regexp.MustCompile(`rate\(dev_health_api_rate_limit_rejections_total\{job="auth-service"\}\[5m\]\)\)\s*>\s*1\s*$`).MatchString(strings.TrimSpace(rejections)),
		"the rejection rule must be rate(...[5m]) > 1 per limit: %q", rejections)
	for name, want := range map[string]string{
		"AuthServiceTargetDown":          "5m",
		"AuthServiceMetricsAbsent":       "10m",
		authStoreHigh:                    "5m",
		"AuthRateLimitRejectionsHigh":    "10m",
		"AuthServiceHighServerErrorRate": "10m",
	} {
		g.expect(str(authAlertNamed(g, name)["for"]) == want, "%s for: is %q, want %q", name, str(authAlertNamed(g, name)["for"]), want)
	}
}

func TestAuthServiceAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"auth-alert-set", authAlertSetIsExact},
		check{"auth-alert-metrics-emitted", authAlertMetricsAreEmitted},
		check{"auth-alert-job-selector", authAlertsSelectTheAuthJob},
		check{"auth-alert-labels-bounded", authAlertLabelsAreBounded},
		check{"auth-alert-shapes", authAlertShapes},
	)
}

func TestAuthServiceAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel}
	runPlanted(t, []planted{
		{"alert-renamed", rules, mut(rulesRel, "- alert: AuthServiceMetricsAbsent", "- alert: AuthServiceMetricsAbsentX"), authAlertSetIsExact, "holds"},
		{"metric-typo", rules, mut(rulesRel, "dev_health_api_rate_limit_rejections_total{", "dev_health_api_rate_limit_rejection_total{"), authAlertMetricsAreEmitted, "never fires"},
		{"job-selector-dropped", rules, mut(rulesRel, `sum by (job) (up{job="auth-service"}) == 0`, "sum by (job) (up) == 0"), authAlertsSelectTheAuthJob, "another service"},
		{"path-label-added", rules, mut(rulesRel, "sum by (limit) (increase(dev_health_api_rate_limit_store_errors_total", "sum by (limit, path) (increase(dev_health_api_rate_limit_store_errors_total"), authAlertLabelsAreBounded, "closed limit id"},
		{"team-invented", rules, mut(rulesRel, "          team: platform\n        annotations:\n          summary: \"Auth service target is down\"", "          team: identity\n        annotations:\n          summary: \"Auth service target is down\""), authAlertLabelsAreBounded, "want the existing groups"},
		{"store-error-high-selector-differs-from-the-warning-tier", rules, mut(rulesRel, `store_errors_total{limit=~"auth_svc_.*"}[10m]`, `store_errors_total{job="auth-service"}[10m]`), authAlertSetIsExact, "differs from the warning tier"},
		{"store-error-high-threshold-changed", rules, mut(rulesRel, "[10m])) > 20\n        for: 5m", "[10m])) > 200\n        for: 5m"), authAlertShapes, "high store-error tier"},
		{"store-error-high-renamed", rules, mut(rulesRel, "      - alert: AuthServiceRateLimitStoreErrorsHigh\n", "      - alert: AuthServiceRateLimitStoreErrorsHighX\n"), authAlertSetIsExact, "exactly once"},
		{"traffic-floor-removed", rules, mut(rulesRel, " and sum(rate(dev_health_api_http_requests_total{job=\"auth-service\"}[5m])) > 0.1", ""), authAlertShapes, "traffic floor"},
		{"absent-rule-removed", rules, mut(rulesRel, "absent(up{job=\"auth-service\"})", "up{job=\"auth-service\"}"), authAlertShapes, "absent"},
		{"instance-label-kept", rules, mut(rulesRel, `sum by (job) (up{job="auth-service"}) == 0`, `up{job="auth-service"} == 0`), authAlertLabelsAreBounded, "bare selector"},
		{"rejection-threshold-changed", rules, mut(rulesRel, "[5m])) > 1\n        for: 10m", "[5m])) > 10\n        for: 10m"), authAlertShapes, "rejection rule"},
		{"rejection-for-shortened", rules, mut(rulesRel, "[5m])) > 1\n        for: 10m", "[5m])) > 1\n        for: 1m"), authAlertShapes, "for:"},
		{"store-error-for-shortened", rules, mut(rulesRel, "[10m])) > 20\n        for: 5m", "[10m])) > 20\n        for: 1m"), authAlertShapes, "for:"},
	})
}
