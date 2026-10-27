package chartgates_test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The alerts on the in-chart Valkey (alerts/rules.yml, group valkey). The
// series come from the collector's redis receiver, which names them with dots
// (redis.keys.evicted, redis.memory.used) and writes no underscore form, so the
// rules select them by __name__.

const (
	valkeyGroup         = "valkey"
	valkeyEvictedAlert  = "ValkeyKeysEvicted"
	valkeyMemoryAlert   = "ValkeyMemoryNearMaxmemory"
	valkeyEvictedMetric = "redis.keys.evicted"
	valkeyMemoryMetric  = "redis.memory.used"
	// valkeyMaxmemory is 192mb in Valkey units (1mb = 1048576 bytes): the
	// maxmemory the S-V1 roll step sets for prod.
	valkeyMaxmemory        = 192 * 1048576
	valkeyMemoryThreshold  = 161061274
	valkeyThresholdComment = "0.8 x maxmemory 192mb set by roll step S-V1 (CHAOS-11630); change both together"
)

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func valkeyAlertsAreTheGroup(g *guard) {
	groups := 0
	for _, grp := range ruleGroups(g) {
		if str(grp["name"]) == valkeyGroup {
			groups++
		}
	}
	g.expect(groups == 1, "%d groups are named %s, want exactly one", groups, valkeyGroup)
	var names []string
	for _, r := range groupRules(g, valkeyGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == valkeyEvictedAlert+","+valkeyMemoryAlert, "group %s holds %v, want exactly %s and %s", valkeyGroup, names, valkeyEvictedAlert, valkeyMemoryAlert)
}

func valkeyEvictedAlertShape(g *guard) {
	a := alertNamed(g, valkeyEvictedAlert)
	want := `sum(increase({__name__="` + valkeyEvictedMetric + `"}[10m])) > 0`
	g.expect(oneLine(str(a["expr"])) == want, "%s expr is %q, want %q", valkeyEvictedAlert, oneLine(str(a["expr"])), want)
	_, hasFor := a["for"]
	g.expect(!hasFor, "%s has a `for`: one eviction in the window must page", valkeyEvictedAlert)
}

func valkeyMemoryAlertShape(g *guard) {
	a := alertNamed(g, valkeyMemoryAlert)
	want := `max({__name__="` + valkeyMemoryMetric + `"}) > ` + strconv.Itoa(valkeyMemoryThreshold)
	g.expect(oneLine(str(a["expr"])) == want, "%s expr is %q, want %q", valkeyMemoryAlert, oneLine(str(a["expr"])), want)
	g.expect(str(a["for"]) == "10m", "%s `for` is %q, want 10m", valkeyMemoryAlert, str(a["for"]))
}

// The threshold is 80% of the maxmemory, rounded up to a whole byte.
func valkeyMemoryThresholdIs80PercentOfMaxmemory(g *guard) {
	g.expect(valkeyMemoryThreshold*10 >= valkeyMaxmemory*8 && (valkeyMemoryThreshold-1)*10 < valkeyMaxmemory*8,
		"threshold %d is not 0.8 x %d rounded up", valkeyMemoryThreshold, valkeyMaxmemory)
}

// The comment that ties the threshold to the roll step sits on the memory rule.
var valkeyMemoryRuleWithComment = regexp.MustCompile(`(?m)^\s*# ` + regexp.QuoteMeta(valkeyThresholdComment) + `\n\s*- alert: ` + valkeyMemoryAlert + `\n`)

func valkeyMemoryThresholdCommentPinned(g *guard) {
	g.expect(valkeyMemoryRuleWithComment.MatchString(g.read(rulesRel)), "the line before `- alert: %s` is not the comment %q", valkeyMemoryAlert, valkeyThresholdComment)
}

func valkeyAlertsRouted(g *guard) {
	for _, name := range []string{valkeyEvictedAlert, valkeyMemoryAlert} {
		a := alertNamed(g, name)
		labels := mp(a["labels"])
		g.expect(str(labels["severity"]) == "warning", "%s severity is %q, want warning", name, str(labels["severity"]))
		g.expect(str(labels["team"]) == "platform", "%s team is %q, want platform", name, str(labels["team"]))
		g.expect(str(mp(a["annotations"])["summary"]) != "", "%s has no summary", name)
		g.expect(strings.Contains(str(mp(a["annotations"])["description"]), "auth-service-scrape-and-alerts.md"), "%s description does not point at the import runbook", name)
	}
}

const valkeyRunbook = "docs/operate/runbooks/auth-service-scrape-and-alerts.md"

func TestValkeyAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"valkey-alerts-are-the-group", valkeyAlertsAreTheGroup},
		check{"valkey-evicted-alert-shape", valkeyEvictedAlertShape},
		check{"valkey-memory-alert-shape", valkeyMemoryAlertShape},
		check{"valkey-memory-threshold-80-percent", valkeyMemoryThresholdIs80PercentOfMaxmemory},
		check{"valkey-memory-threshold-comment", valkeyMemoryThresholdCommentPinned},
		check{"valkey-alerts-routed", valkeyAlertsRouted},
	)
}

func TestValkeyAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel}
	runPlanted(t, []planted{
		{"evicted-alert-reads-the-underscore-name", rules, mut(rulesRel, `{__name__="redis.keys.evicted"}`, `redis_keys_evicted`), valkeyEvictedAlertShape, "expr is"},
		{"evicted-alert-gets-a-for", rules, mut(rulesRel, "[10m])) > 0\n", "[10m])) > 0\n        for: 5m\n"), valkeyEvictedAlertShape, "has a `for`"},
		{"memory-threshold-moved", rules, mut(rulesRel, "> 161061274", "> 201326592"), valkeyMemoryAlertShape, "expr is"},
		{"memory-for-shortened", rules, mut(rulesRel, "> 161061274\n        for: 10m", "> 161061274\n        for: 5m"), valkeyMemoryAlertShape, "`for` is"},
		{"threshold-comment-dropped", rules, mut(rulesRel, "# "+valkeyThresholdComment+"\n", ""), valkeyMemoryThresholdCommentPinned, "is not the comment"},
		{"memory-alert-paged-critical", rules, mut(rulesRel, "> 161061274\n        for: 10m\n        labels:\n          severity: warning", "> 161061274\n        for: 10m\n        labels:\n          severity: critical"), valkeyAlertsRouted, "severity is"},
		{"third-rule-in-the-group", rules, appendText(rulesRel, "      - alert: ValkeyExtra\n        expr: up == 0\n"), valkeyAlertsAreTheGroup, "want exactly"},
		{"second-valkey-group", rules, appendText(rulesRel, "  - name: valkey\n    rules:\n      - alert: ValkeyUnnoticedExtra\n        expr: up == 0\n"), valkeyAlertsAreTheGroup, "groups are named valkey"},
	})
}
