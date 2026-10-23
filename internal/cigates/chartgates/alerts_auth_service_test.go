package chartgates_test

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authruntime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// The alerts on auth-service's rate-limit store (alerts/rules.yml, group
// auth_service_rate_limit). A rule on a series nobody publishes never fires and
// looks covered, so every series the two rules read is checked against what
// the real code writes.

const (
	storeErrorsAlert = "AuthServiceRateLimitStoreErrors"
	storeNotReadyAlt = "AuthServiceRateLimitStoreNotReady"
	storeErrorsName  = "dev_health_api_rate_limit_store_errors_total"
	checkFailedName  = "dev_health_runtime_check_failed"
	runtimeInfoName  = "dev_health_runtime_info"
)

// goAPIIDs are go-api's rate-limit ids (internal/api/session/limits.go and the
// admin routes): none may fall inside the alert's selector.
var goAPIIDs = []string{
	"auth_login_ip", "auth_refresh", "auth_validate", "auth_register", "auth_verify", "auth_resend_verification",
	"auth_forgot_password", "auth_change_password", "admin_password",
}

var quotedLiteral = regexp.MustCompile(`"[^"]*"`)

// exprMetrics are the metric names of expr, with label values left out (a
// selector like auth_svc_.* is a value, not a metric).
func exprMetrics(expr string) map[string]bool {
	return metricNames(quotedLiteral.ReplaceAllString(expr, `""`))
}

// authStoreScrape is what auth-service writes for the two rules: the store
// error counter at zero for every real limiter id, and the runtime block with
// the required "valkey" check, both through the real writers.
func authStoreScrape(g *guard) string {
	for _, id := range authapi.LimitIDs() {
		httpapi.NewKeyedLimiter(httpapi.NewMemoryCounters(nil), httpapi.Limit{ID: id, Count: 1, Window: time.Minute}).Declare()
	}
	registry := health.NewRegistry(time.Second)
	if err := registry.RegisterRequired(authruntime.CheckValkey, func(context.Context) error { return nil }); err != nil {
		g.failf("cannot register the valkey check: %v", err)
		return ""
	}
	registry.SetReady(true)
	var out bytes.Buffer
	registry.WriteRuntimeMetrics(context.Background(), authconfig.Service, "test", &out)
	if err := httpapi.RateLimitStoreErrors.WritePrometheus(&out); err != nil {
		g.failf("the store-error counter does not write: %v", err)
	}
	return out.String()
}

func authStoreAlertsRoutedAndBounded(g *guard) {
	for _, name := range []string{storeErrorsAlert, storeNotReadyAlt} {
		a := alertNamed(g, name)
		labels := mp(a["labels"])
		g.expect(str(labels["team"]) == "platform", "%s team label is %q, want platform", name, str(labels["team"]))
		g.expect(str(labels["severity"]) != "", "%s has no severity", name)
		g.expect(strings.Contains(str(mp(a["annotations"])["description"]), "readiness-and-slo.md"), "%s description does not point at the runbook section", name)
		g.expect(!regexp.MustCompile(`by \([^)]*(email|token|principal|path|address|ip)\b`).MatchString(str(a["expr"])), "%s groups by an unbounded label: %q", name, str(a["expr"]))
	}
	g.expect(regexp.MustCompile(`by \(limit\)`).MatchString(str(alertNamed(g, storeErrorsAlert)["expr"])), "the store-error alert must group by limit only")
	g.expect(regexp.MustCompile(`by \(instance\)`).MatchString(str(alertNamed(g, storeNotReadyAlt)["expr"])), "the not-ready alert must group by instance only")
}

func authStoreAlertsForOutlastsAScrape(g *guard) {
	for _, name := range []string{storeErrorsAlert, storeNotReadyAlt} {
		f := forDur.FindStringSubmatch(str(alertNamed(g, name)["for"]))
		if f == nil {
			g.failf("%s: unexpected `for` format %q", name, str(alertNamed(g, name)["for"]))
			continue
		}
		g.expect(minutes(f[1], f[2]) >= 5, "%s `for` is %s, below 5m: a store restart would page", name, str(alertNamed(g, name)["for"]))
	}
}

func authStoreErrorsAlertSelectsOnlyThisService(g *guard) {
	expr := str(alertNamed(g, storeErrorsAlert)["expr"])
	m := regexp.MustCompile(`limit=~"([^"]*)"`).FindStringSubmatch(expr)
	if m == nil {
		g.failf("the store-error alert has no limit=~ selector: %q", expr)
		return
	}
	selector, err := regexp.Compile("^(?:" + m[1] + ")$")
	if err != nil {
		g.failf("the selector %q is not a regexp: %v", m[1], err)
		return
	}
	ids := authapi.LimitIDs()
	g.expect(len(ids) > 0, "no limiter ids to match the selector against")
	for _, id := range ids {
		g.expect(selector.MatchString(id), "the selector %q misses limiter %s: its errors would never page", m[1], id)
	}
	for _, id := range goAPIIDs {
		g.expect(!selector.MatchString(id), "the selector %q also matches go-api's limiter %s: go-api's store errors would page as auth-service's", m[1], id)
	}
	g.expect(regexp.MustCompile(`rate\(`+storeErrorsName+`\{[^}]*\}\[5m\]\)\)? *\)? *> 0`).MatchString(expr), "the store-error alert is not rate(...[5m]) > 0: %q", expr)
}

// authStoreNotReadyShape is the whole expression, nothing before, between or
// after the pieces: the check series of the "valkey" check, kept only where the
// same target reports the auth-service's own runtime info series, grouped by
// instance, compared == 1. A multiply, an add, a different comparison or a
// dropped service selector is a different (or a never-firing) rule.
var authStoreNotReadyShape = regexp.MustCompile(`^max by \(instance\) \(` + checkFailedName + `\{check="([^"]*)"\} and on \(instance\) ` + runtimeInfoName + `\{service="([^"]*)"\}\) == 1$`)

func authStoreNotReadyAlertReadsTheValkeyCheck(g *guard) {
	expr := strings.Join(strings.Fields(str(alertNamed(g, storeNotReadyAlt)["expr"])), " ")
	m := authStoreNotReadyShape.FindStringSubmatch(expr)
	if m == nil {
		g.failf("the not-ready alert is not the pinned shape max by (instance) (check_failed{check} and on (instance) runtime_info{service}) == 1: %q", expr)
		return
	}
	g.expect(m[1] == authruntime.CheckValkey, "the not-ready alert selects check=%q, want %q", m[1], authruntime.CheckValkey)
	g.expect(m[2] == authconfig.Service, "the not-ready alert keeps service=%q, want %q: another service registers a check of the same name", m[2], authconfig.Service)
}

// The comparison means what the real writer writes: a failing required check
// is 1 and a passing one is 0 on the check series, and the info series carries
// the service name the rule keeps.
func authStoreNotReadyValuesMatchTheRealWriter(g *guard) {
	write := func(fail bool, service string) string {
		registry := health.NewRegistry(time.Second)
		if err := registry.RegisterRequired(authruntime.CheckValkey, func(context.Context) error {
			if fail {
				return context.DeadlineExceeded
			}
			return nil
		}); err != nil {
			g.failf("cannot register the check: %v", err)
		}
		registry.SetReady(true)
		var out bytes.Buffer
		registry.WriteRuntimeMetrics(context.Background(), service, "test", &out)
		return out.String()
	}
	failing, passing := write(true, authconfig.Service), write(false, authconfig.Service)
	g.expect(strings.Contains(failing, checkFailedName+`{check="valkey"} 1`), "a failing valkey check is not written as 1")
	g.expect(strings.Contains(passing, checkFailedName+`{check="valkey"} 0`), "a passing valkey check is not written as 0")
	g.expect(strings.Contains(failing, runtimeInfoName+`{service="`+authconfig.Service+`"`), "the info series does not carry service=%q", authconfig.Service)
	// Another service with a check of the same name writes the same check
	// series, and only the info series tells them apart.
	other := write(true, "dev-health-stream-runner")
	g.expect(strings.Contains(other, checkFailedName+`{check="valkey"} 1`), "the probe of another service did not write a failing valkey check")
	g.expect(!strings.Contains(other, `service="`+authconfig.Service+`"`), "another service's info series carries the auth-service name")
}

func authStoreAlertsReadSeriesTheServiceWrites(g *guard) {
	scrape := authStoreScrape(g)
	checked := 0
	for _, name := range []string{storeErrorsAlert, storeNotReadyAlt} {
		for metric := range exprMetrics(str(alertNamed(g, name)["expr"])) {
			checked++
			g.expect(hasSample(scrape, metric), "%s reads %q, which the service's real scrape does not carry as a sample: an alert on a metric nobody publishes never fires", name, metric)
		}
	}
	g.expect(checked == 3, "checked %d metric reads across the two rules, want 3 (a parser that returns nothing must not pass)", checked)
	g.expect(regexp.MustCompile(`(?m)^`+storeErrorsName+`\{limit="auth_svc_login_ip"\} 0$`).MatchString(scrape), "the counter is not published at zero for a real limiter id")
	g.expect(regexp.MustCompile(`(?m)^`+checkFailedName+`\{check="`+authruntime.CheckValkey+`"\} 0$`).MatchString(scrape), "the valkey check has no series")
}

func authStoreAlertNamesUnique(g *guard) {
	for _, name := range []string{storeErrorsAlert, storeNotReadyAlt} {
		alertNamed(g, name)
	}
}

func TestAuthStoreAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"auth-store-alerts-unique", authStoreAlertNamesUnique},
		check{"auth-store-alerts-routed-and-bounded", authStoreAlertsRoutedAndBounded},
		check{"auth-store-alerts-for-outlasts-a-scrape", authStoreAlertsForOutlastsAScrape},
		check{"auth-store-errors-selects-only-this-service", authStoreErrorsAlertSelectsOnlyThisService},
		check{"auth-store-not-ready-reads-the-valkey-check", authStoreNotReadyAlertReadsTheValkeyCheck},
		check{"auth-store-not-ready-values-match-the-real-writer", authStoreNotReadyValuesMatchTheRealWriter},
		check{"auth-store-alerts-read-series-the-service-writes", authStoreAlertsReadSeriesTheServiceWrites},
	)
}

func TestAuthStoreAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel}
	const errorsExpr = `sum by (limit) (rate(dev_health_api_rate_limit_store_errors_total{limit=~"auth_svc_.*"}[5m])) > 0`
	const readyExpr = `max by (instance) (dev_health_runtime_check_failed{check="valkey"} and on (instance) dev_health_runtime_info{service="auth-service"}) == 1`
	runPlanted(t, []planted{
		{"store-errors-alert-reads-a-metric-nobody-writes", rules, mut(rulesRel, "rate("+storeErrorsName+"{", "rate(dev_health_auth_rate_limit_store_errors_total{"), authStoreAlertsReadSeriesTheServiceWrites, "never fires"},
		{"store-errors-selector-widened-to-go-api", rules, mut(rulesRel, `limit=~"auth_svc_.*"`, `limit=~"auth_.*"`), authStoreErrorsAlertSelectsOnlyThisService, "go-api's limiter"},
		{"store-errors-selector-misses-a-limiter", rules, mut(rulesRel, `limit=~"auth_svc_.*"`, `limit=~"auth_svc_login.*"`), authStoreErrorsAlertSelectsOnlyThisService, "misses limiter"},
		{"store-errors-threshold-raised", rules, mut(rulesRel, errorsExpr, strings.Replace(errorsExpr, "> 0", "> 5", 1)), authStoreErrorsAlertSelectsOnlyThisService, "rate(...[5m]) > 0"},
		{"not-ready-alert-reads-another-check", rules, mut(rulesRel, `check="valkey"`, `check="postgres"`), authStoreNotReadyAlertReadsTheValkeyCheck, "want \"valkey\""},
		{"not-ready-alert-loses-the-comparison", rules, mut(rulesRel, readyExpr, strings.Replace(readyExpr, "== 1", "== 0", 1)), authStoreNotReadyAlertReadsTheValkeyCheck, "pinned shape"},
		{"not-ready-alert-never-fires-multiplied", rules, mut(rulesRel, readyExpr, strings.Replace(readyExpr, ") == 1", ") * 2 == 1", 1)), authStoreNotReadyAlertReadsTheValkeyCheck, "pinned shape"},
		{"not-ready-alert-always-fires-added", rules, mut(rulesRel, readyExpr, strings.Replace(readyExpr, ") == 1", ") + 1 == 1", 1)), authStoreNotReadyAlertReadsTheValkeyCheck, "pinned shape"},
		{"not-ready-alert-fires-on-any-value", rules, mut(rulesRel, readyExpr, strings.Replace(readyExpr, "== 1", ">= 0", 1)), authStoreNotReadyAlertReadsTheValkeyCheck, "pinned shape"},
		{"not-ready-alert-loses-the-service-join", rules, mut(rulesRel, readyExpr, `max by (instance) (dev_health_runtime_check_failed{check="valkey"}) == 1`), authStoreNotReadyAlertReadsTheValkeyCheck, "pinned shape"},
		{"not-ready-alert-keeps-another-service", rules, mut(rulesRel, `dev_health_runtime_info{service="auth-service"}`, `dev_health_runtime_info{service="dev-health-stream-runner"}`), authStoreNotReadyAlertReadsTheValkeyCheck, "another service registers"},
		{"store-errors-for-shortened", rules, mut(rulesRel, "        for: 5m\n        labels:\n          severity: warning\n          team: platform\n        annotations:\n          summary: \"auth-service rate-limit store errors", "        for: 1m\n        labels:\n          severity: warning\n          team: platform\n        annotations:\n          summary: \"auth-service rate-limit store errors"), authStoreAlertsForOutlastsAScrape, "below 5m"},
		{"store-errors-grouped-by-an-unbounded-label", rules, mut(rulesRel, errorsExpr, strings.Replace(errorsExpr, "by (limit)", "by (limit, token)", 1)), authStoreAlertsRoutedAndBounded, "unbounded"},
		{"store-errors-routed-to-nobody", rules, mut(rulesRel, "          severity: warning\n          team: platform\n        annotations:\n          summary: \"auth-service rate-limit store errors", "          severity: warning\n          team: nobody\n        annotations:\n          summary: \"auth-service rate-limit store errors"), authStoreAlertsRoutedAndBounded, "team label"},
		{"store-errors-alert-removed", rules, mut(rulesRel, "- alert: "+storeErrorsAlert, "- alert: AuthServiceRateLimitStoreErrorsRenamed"), authStoreAlertNamesUnique, "expected exactly one"},
	})
}
