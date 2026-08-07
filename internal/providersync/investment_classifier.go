package providersync

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Go port of the legacy rule-based investment classifier
// (analytics/investment.py). It is a DEPRECATED path in Python -- the canonical
// Investment View is WorkUnit-based -- and this port exists only to serve the
// legacy daily investment_* destinations, exactly as the Python one does.
//
// The rules are read from the REAL config file rather than transcribed into Go.
// A transcribed copy is a second, unversioned source of truth: the same defect
// class that left the derived-destination integration tests asserting against a
// pre-053 enum while production emitted values the copy could not represent. The
// path is supplied by the caller, mirroring Python's `InvestmentClassifier(
// config_path)`, so both engines can be pointed at one file and compared.
//
// Per D16 this mirrors Python bug-for-bug. Four of the 44 rules in the checked-in
// config are UNREACHABLE from the work-item call site, and that is reproduced
// rather than corrected; see investmentClassifierUnreachableRules in the tests,
// which fails if a future config edit revives one.

const (
	legacyDefaultInvestmentArea = "product"
	legacyDefaultProjectStream  = "general"
	// legacyDefaultRulePriority mirrors `x.get("priority", 100)`: a rule with no
	// priority sorts as 100, which is AFTER the 40-and-below rules and BEFORE
	// the 999 catch-all.
	legacyDefaultRulePriority = 100
	legacyFallbackRuleID      = "legacy_default"
	legacyUnnamedRuleID       = "legacy_rule"
)

// InvestmentClassification mirrors the Python dataclass of the same name.
type InvestmentClassification struct {
	InvestmentArea string  `json:"investment_area"`
	ProjectStream  *string `json:"project_stream"`
	Confidence     float64 `json:"confidence"`
	RuleID         string  `json:"rule_id"`
}

// investmentRule is one entry of the config's `rules:` list. Every field is
// optional in the file, so each is decoded through a pointer or a slice whose
// nil-ness carries "the key was absent" -- which the matcher genuinely depends
// on: an ABSENT `component` key is skipped, while a PRESENT one with an empty
// list rejects everything.
type investmentRule struct {
	ID       string                `yaml:"id"`
	Priority *int                  `yaml:"priority"`
	Match    *investmentRuleMatch  `yaml:"match"`
	Output   *investmentRuleOutput `yaml:"output"`
}

type investmentRuleMatch struct {
	Always     *bool     `yaml:"always"`
	Label      *[]string `yaml:"label"`
	PathPrefix *[]string `yaml:"path_prefix"`
	Component  *[]string `yaml:"component"`
}

type investmentRuleOutput struct {
	InvestmentArea *string `yaml:"investment_area"`
	ProjectStream  *string `yaml:"project_stream"`
}

// InvestmentArtifact is the classifier's input. Python passes a plain dict and
// the call site (job_work_items.py:1377) supplies exactly four keys: labels,
// component, title and provider.
//
// Title and Provider are carried here even though NOTHING reads them, because
// the Python matcher does not read them either -- it inspects only labels,
// paths and component. Dropping them would make the Go signature quietly
// narrower than the contract it ports, and the docstring's claim that `title`
// and `epic` participate would then have no visible counter-evidence.
type InvestmentArtifact struct {
	Labels []string
	// Paths is what the matcher's path_prefix arm reads. The work-item call
	// site never populates it, which is precisely why every path_prefix rule is
	// dead on that path; it stays here because the field is what makes that
	// deadness a property of the CALLER rather than of this engine.
	Paths []string
	// Component is always "" from the work-item call site: WorkItem has no
	// `component` attribute, so `getattr(item, "component", "")` cannot return
	// anything else.
	Component string
	// Read by neither engine. See the type comment.
	Title    string
	Provider string
}

// InvestmentClassifier holds the priority-ordered rules.
type InvestmentClassifier struct {
	rules []investmentRule
}

// NewInvestmentClassifier mirrors `InvestmentClassifier.__init__` +
// `_load_rules`, including its MISSING-FILE behaviour: Python logs a warning
// and returns an empty rule list rather than raising, so a classifier built
// against an absent path still answers, always with the legacy default. That
// is reproduced instead of failing closed, because failing closed here would
// change which rows the legacy destinations emit.
func NewInvestmentClassifier(configPath string) (*InvestmentClassifier, error) {
	contents, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Python: logger.warning(...); return []
			return &InvestmentClassifier{rules: nil}, nil
		}
		return nil, err
	}
	var document struct {
		Rules []investmentRule `yaml:"rules"`
	}
	if err := yaml.Unmarshal(contents, &document); err != nil {
		return nil, fmt.Errorf("investment config %s: %w", configPath, err)
	}
	rules := append([]investmentRule(nil), document.Rules...)
	// Python sorts with sorted(), which is STABLE, and the checked-in config
	// relies on that: 37 of its 44 rules share priority 10 and a further 2
	// share priority 30. Among equal priorities the FILE ORDER decides which
	// rule classify() reaches first, and classify() returns on first match --
	// so an unstable sort would silently change the answer for any artifact
	// matching more than one priority-10 rule. sort.Slice is NOT stable.
	sort.SliceStable(rules, func(left, right int) bool {
		return investmentRulePriority(rules[left]) < investmentRulePriority(rules[right])
	})
	return &InvestmentClassifier{rules: rules}, nil
}

func investmentRulePriority(rule investmentRule) int {
	if rule.Priority == nil {
		return legacyDefaultRulePriority
	}
	return *rule.Priority
}

// Classify mirrors InvestmentClassifier.classify: first matching rule wins, and
// an unmatched artifact falls back to the legacy product/general bucket with
// confidence 0.0 rather than to anything unknown-like.
func (classifier *InvestmentClassifier) Classify(
	artifact InvestmentArtifact,
) InvestmentClassification {
	for _, rule := range classifier.rules {
		if !investmentRuleMatches(rule.Match, artifact) {
			continue
		}
		area := legacyDefaultInvestmentArea
		stream := legacyDefaultProjectStream
		if rule.Output != nil {
			if rule.Output.InvestmentArea != nil {
				area = *rule.Output.InvestmentArea
			}
			if rule.Output.ProjectStream != nil {
				stream = *rule.Output.ProjectStream
			}
		}
		id := rule.ID
		if id == "" {
			// Python: rule.get("id", "legacy_rule"). An id key that is present
			// but empty is NOT the same as an absent one in Python; this port
			// collapses them because YAML cannot distinguish a missing string
			// from an empty one without another pointer, and no rule in the
			// real config omits its id. Pinned by a synthetic-config case.
			id = legacyUnnamedRuleID
		}
		streamValue := stream
		return InvestmentClassification{
			InvestmentArea: area,
			ProjectStream:  &streamValue,
			Confidence:     1.0,
			RuleID:         id,
		}
	}
	stream := legacyDefaultProjectStream
	return InvestmentClassification{
		InvestmentArea: legacyDefaultInvestmentArea,
		ProjectStream:  &stream,
		Confidence:     0.0,
		RuleID:         legacyFallbackRuleID,
	}
}

// investmentRuleMatches mirrors `_matches`.
//
// The structure matters as much as the conditions: each arm is a REJECTION
// test, and a criterion that is absent is not tested at all. So an EMPTY
// `match: {}` reaches the final `return true` and matches every artifact --
// including one with no labels at all. The real config expresses its catch-all
// as `always: true` instead, so only a synthetic config reaches the empty-map
// form; it is pinned there.
func investmentRuleMatches(match *investmentRuleMatch, artifact InvestmentArtifact) bool {
	if match == nil {
		// `rule.get("match", {})` yields {} for an absent key, and {} matches
		// everything by the rule above.
		return true
	}
	// Python tests `match_criteria.get("always")` for TRUTHINESS, so
	// `always: false` does not short-circuit -- it falls through to the
	// remaining criteria exactly as an absent key would.
	if match.Always != nil && *match.Always {
		return true
	}
	if match.Label != nil {
		labels := make(map[string]struct{}, len(artifact.Labels))
		for _, label := range artifact.Labels {
			labels[strings.ToLower(label)] = struct{}{}
		}
		intersects := false
		for _, target := range *match.Label {
			if _, ok := labels[strings.ToLower(target)]; ok {
				intersects = true
				break
			}
		}
		if !intersects {
			return false
		}
	}
	if match.PathPrefix != nil {
		// Reads artifact PATHS, not path_prefix. The work-item call site
		// supplies none, so this arm rejects every artifact reaching it from
		// that path -- the reason three real rules are dead.
		found := false
		for _, path := range artifact.Paths {
			for _, prefix := range *match.PathPrefix {
				if strings.HasPrefix(path, prefix) {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return false
		}
	}
	if match.Component != nil {
		// Exact membership, NOT case-folded -- unlike the label arm. From the
		// work-item call site the component is always "", so a rule whose list
		// does not literally contain "" can never fire.
		matched := false
		for _, candidate := range *match.Component {
			if candidate == artifact.Component {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}
