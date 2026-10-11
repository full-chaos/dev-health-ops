package investmentnormalize

import (
	"regexp"
	"strings"
)

// An estimate rule: the first rule (in order) whose pattern matches the item's text names the sub-theme.
// The order is the priority: a security word beats a fix word, an incident word beats a feature word.
// NO default bucket: an item no rule matches gets no row (CHAOS-9206, D5941).
type estimateRule struct {
	name        string
	subcategory string
	pattern     *regexp.Regexp
}

func rule(name, subcategory, pattern string) estimateRule {
	return estimateRule{name: name, subcategory: subcategory, pattern: regexp.MustCompile(`(?i)` + pattern)}
}

// conventionalPrefix rules read the title's conventional-commit prefix ("fix(api): ..."); they run first
// because the prefix is the author's own statement of kind.
var conventionalRules = []estimateRule{
	rule("cc:security", "risk.security", `^\s*(security|sec)(\([^)]*\))?!?:`),
	rule("cc:fix", "quality.bugfix", `^\s*(fix|bugfix|hotfix|revert)(\([^)]*\))?!?:`),
	rule("cc:perf", "quality.reliability", `^\s*perf(\([^)]*\))?!?:`),
	rule("cc:test", "quality.testing", `^\s*test(s)?(\([^)]*\))?!?:`),
	rule("cc:refactor", "maintenance.refactor", `^\s*refactor(\([^)]*\))?!?:`),
	rule("cc:deps", "maintenance.upgrade", `^\s*(deps|build|ci)(\([^)]*\))?!?:`),
	rule("cc:chore", "maintenance.debt", `^\s*(chore|style)(\([^)]*\))?!?:`),
	rule("cc:docs", "feature_delivery.enablement", `^\s*docs?(\([^)]*\))?!?:`),
	rule("cc:feat", "feature_delivery.roadmap", `^\s*feat(\([^)]*\))?!?:`),
}

// keywordRules read the title, the description and the labels, in priority order.
var keywordRules = []estimateRule{
	rule("kw:vulnerability", "risk.vulnerability", `\b(cve-\d+|vulnerabilit\w*|exploit\w*|xss|csrf|sql injection)\b`),
	rule("kw:security", "risk.security", `\b(security|secure|secret leak|credential leak|auth bypass|permission escalation|hardening)\b`),
	rule("kw:compliance", "risk.compliance", `\b(compliance|gdpr|soc ?2|hipaa|audit)\b`),
	rule("kw:incident", "operational.incident_response", `\b(incident|outage|postmortem|post-mortem|sev ?[0-4]|rollback|downtime)\b`),
	rule("kw:oncall", "operational.on_call", `\b(on-?call|pager(duty)?|runbook)\b`),
	rule("kw:support", "operational.support", `\b(customer support|support (ticket|request|question)|helpdesk|help desk|escalation)\b`),
	rule("kw:flaky", "quality.reliability", `\b(flak(y|e|iness)|intermittent|reliabilit\w*|stabili[sz]\w*)\b`),
	rule("kw:fix", "quality.bugfix", `\b(fix(es|ed|ing)?|bugs?|regression\w*|crash\w*|broken|defect\w*|hotfix)\b`),
	rule("kw:test", "quality.testing", `\b(tests?|testing|coverage|e2e|unit tests?)\b`),
	rule("kw:refactor", "maintenance.refactor", `\b(refactor\w*|restructur\w*|rename|simplif\w*|clean ?up|cleanup)\b`),
	rule("kw:upgrade", "maintenance.upgrade", `\b(upgrade\w*|bump\w*|dependenc\w*|deps|migrat\w*|deprecat\w*|chore)\b`),
	rule("kw:debt", "maintenance.debt", `\b(tech(nical)? debt|todo|legacy|remove (unused|dead))\b`),
	rule("kw:docs", "feature_delivery.enablement", `\b(docs?|documentation|readme|tutorial)\b`),
	rule("kw:feature", "feature_delivery.roadmap", `\b(feat(ure)?s?|add(s|ed|ing)?|new|implement\w*|introduc\w*|support(s|ed)? (for|of)|enable\w*|build)\b`),
}

// maxEstimateText bounds the text read per item (a description can be very long).
const maxEstimateText = 4000

// Estimate returns the sub-theme and the NAME of the rule that matched the item's text, or ok=false when no
// rule matches (the item then stays out: no data, never a default bucket).
func Estimate(title string, description *string, labels []string) (subcategory, ruleName string, ok bool) {
	for _, r := range conventionalRules {
		if r.pattern.MatchString(title) {
			return r.subcategory, r.name, true
		}
	}
	text := title
	if description != nil {
		d := *description
		if len(d) > maxEstimateText {
			d = d[:maxEstimateText]
		}
		text += "\n" + d
	}
	if len(labels) > 0 {
		text += "\n" + strings.Join(labels, " ")
	}
	for _, r := range keywordRules {
		if r.pattern.MatchString(text) {
			return r.subcategory, r.name, true
		}
	}
	return "", "", false
}

// RuleNames lists every estimate rule name, in priority order (the dry-run report and the tests).
func RuleNames() []string {
	var out []string
	for _, r := range conventionalRules {
		out = append(out, r.name)
	}
	for _, r := range keywordRules {
		out = append(out, r.name)
	}
	return out
}
