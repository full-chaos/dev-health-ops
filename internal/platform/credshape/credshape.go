// Package credshape is the one detector for credential-shaped string values:
// JWTs, PEM blocks, URL userinfo, provider token prefixes, and the prefixes and
// authorization schemes the Auth Control Plane credential-classes contract
// names. The job contract decoder refuses a value this package matches, and the
// queue credential-scan gate reads the same rules.
package credshape

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// Shape is one provider token shape: Prefix, then at least Min characters of
// Class. Sample is one character of Class, used to build a probe.
type Shape struct {
	Prefix string
	Class  string
	Sample string
	Min    int
}

// shapes is the one table of provider token prefixes. The value rules and the
// probes are generated from it, so a prefix added here is both matched and probed.
var shapes = []Shape{
	{"ghp_", "[A-Za-z0-9]", "a", 20}, {"gho_", "[A-Za-z0-9]", "a", 20}, {"ghu_", "[A-Za-z0-9]", "a", 20},
	{"ghs_", "[A-Za-z0-9]", "a", 20}, {"ghr_", "[A-Za-z0-9]", "a", 20},
	{"github_pat_", "[A-Za-z0-9_]", "a", 20},
	{"glpat-", "[A-Za-z0-9_-]", "a", 16},
	{"xoxa-", "[A-Za-z0-9-]", "a", 10}, {"xoxb-", "[A-Za-z0-9-]", "a", 10}, {"xoxp-", "[A-Za-z0-9-]", "a", 10},
	{"xoxr-", "[A-Za-z0-9-]", "a", 10}, {"xoxs-", "[A-Za-z0-9-]", "a", 10},
	{"AKIA", "[0-9A-Z]", "A", 16}, {"ASIA", "[0-9A-Z]", "A", 16},
	{"sk_live_", "[A-Za-z0-9]", "a", 16}, {"rk_live_", "[A-Za-z0-9]", "a", 16},
}

// Shapes returns the provider token shape table.
func Shapes() []Shape { return append([]Shape(nil), shapes...) }

// Shapes returns the provider token shape table the scanner was built with.
func (scanner *Scanner) Shapes() []Shape { return Shapes() }

// rule is one compiled value rule. trigger is a literal every match must
// contain; it lets Match skip the regexp for an ordinary string. A fold rule's
// trigger is lower-case and matched without regard to ASCII case.
type rule struct {
	name    string
	pattern *regexp.Regexp
	trigger string
	fold    bool
}

// Scanner matches credential-shaped string values.
type Scanner struct {
	rules []rule
}

var (
	defaultOnce     sync.Once
	defaultInstance *Scanner
	defaultErr      error
)

// Default returns the scanner built from the embedded credential-classes contract.
func Default() (*Scanner, error) {
	defaultOnce.Do(func() { defaultInstance, defaultErr = New(authv1.CredentialClasses) })
	return defaultInstance, defaultErr
}

func staticRules() []rule {
	return []rule{
		{"jwt", regexp.MustCompile(`eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*`), "eyJ", false},
		{"private-key-block", regexp.MustCompile(`-----BEGIN [A-Z ]*(PRIVATE KEY|CERTIFICATE)-----`), "-----BEGIN ", false},
		{"url-userinfo", regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:[^/\s@]+@`), "://", false},
	}
}

// New derives the value rules from a credential-classes contract: every
// credential_prefix marker becomes a prefix rule, every authorization_scheme
// marker a scheme rule. The static shapes (JWT, PEM, URL userinfo, provider
// tokens) are added to them. A contract with no prefix or scheme marker is an
// error: a scan with no auth-derived rule did not measure.
func New(credentialClasses []byte) (*Scanner, error) {
	var contract struct {
		Classes []struct {
			Markers []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"markers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(credentialClasses, &contract); err != nil {
		return nil, fmt.Errorf("decode credential classes: %w", err)
	}
	prefixes, schemes := map[string]struct{}{}, map[string]struct{}{}
	for _, class := range contract.Classes {
		for _, marker := range class.Markers {
			switch marker.Kind {
			case "credential_prefix":
				prefixes[marker.Name] = struct{}{}
			case "authorization_scheme":
				schemes[marker.Name] = struct{}{}
			}
		}
	}
	if len(prefixes) == 0 || len(schemes) == 0 {
		return nil, errors.New("credential classes carry no credential_prefix or authorization_scheme marker")
	}
	scanner := &Scanner{rules: staticRules()}
	for _, shape := range shapes {
		scanner.rules = append(scanner.rules, rule{
			name:    "provider:" + shape.Prefix,
			pattern: regexp.MustCompile(`\b` + regexp.QuoteMeta(shape.Prefix) + shape.Class + `{` + strconv.Itoa(shape.Min) + `,}`),
			trigger: shape.Prefix,
		})
	}
	for _, prefix := range sortedKeys(prefixes) {
		scanner.rules = append(scanner.rules, foldRule("prefix:"+prefix, prefix,
			`(?i)(^|[^A-Za-z0-9])`+regexp.QuoteMeta(prefix)+`[A-Za-z0-9_-]{8,}`))
	}
	for _, scheme := range sortedKeys(schemes) {
		scanner.rules = append(scanner.rules, foldRule("scheme:"+scheme, scheme,
			`(?i)\b`+regexp.QuoteMeta(scheme)+`\s+[A-Za-z0-9._~+/=-]{8,}`))
	}
	return scanner, nil
}

// foldRule builds a case-insensitive rule. A marker with a byte outside ASCII
// gets no trigger, so the regexp always runs for it.
func foldRule(name, literal, expression string) rule {
	built := rule{name: name, pattern: regexp.MustCompile(expression)}
	if literal != "" && isASCII(literal) {
		built.trigger, built.fold = strings.ToLower(literal), true
	}
	return built
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// Match returns the name of the first rule that matches, or "". It allocates
// nothing and reaches a regexp only for a string that holds a rule's trigger.
func (scanner *Scanner) Match(value string) string {
	rule, _ := scanner.matchStats(value)
	return rule
}

// matchStats is Match plus the number of regexps it ran.
func (scanner *Scanner) matchStats(value string) (string, int) {
	runs := 0
	ascii := 0 // 0 unknown, 1 ASCII only, 2 not
	for index := range scanner.rules {
		candidate := &scanner.rules[index]
		if candidate.trigger != "" {
			if len(value) < len(candidate.trigger) {
				continue
			}
			if candidate.fold {
				if ascii == 0 {
					ascii = 2
					if isASCII(value) {
						ascii = 1
					}
				}
				// Unicode case folding maps U+212A and U+017F onto ASCII k and s,
				// so only an ASCII-only value can be skipped by an ASCII search.
				if ascii == 1 && !containsFold(value, candidate.trigger) {
					continue
				}
			} else if !strings.Contains(value, candidate.trigger) {
				continue
			}
		}
		runs++
		if candidate.pattern.MatchString(value) {
			return candidate.name, runs
		}
	}
	return "", runs
}

// matchRegexpOnly is the reference Match: every regexp, no prefilter.
func (scanner *Scanner) matchRegexpOnly(value string) string {
	for _, candidate := range scanner.rules {
		if candidate.pattern.MatchString(value) {
			return candidate.name
		}
	}
	return ""
}

func isASCII(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] >= 0x80 {
			return false
		}
	}
	return true
}

// containsFold reports whether value holds lowerLiteral, ignoring ASCII case.
// lowerLiteral is already lower-case.
func containsFold(value, lowerLiteral string) bool {
	last := len(value) - len(lowerLiteral)
	for start := 0; start <= last; start++ {
		matched := true
		for offset := 0; offset < len(lowerLiteral); offset++ {
			char := value[start+offset]
			if 'A' <= char && char <= 'Z' {
				char += 'a' - 'A'
			}
			if char != lowerLiteral[offset] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// Probes builds one probe string for every credential shape the scanner knows:
// a JWT, each credential_prefix marker followed by hex and by base64url text,
// each authorization_scheme marker followed by a token, a provider token, a PEM
// header and a URL with userinfo. The probes are built at run time so no
// secret-shaped literal sits in the tree. It fails when a probe matches no rule
// or a rule has no probe.
func (scanner *Scanner) Probes() ([]string, error) {
	segment := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
	hexText := strings.Repeat("0123456789abcdef", 2)
	urlText := segment("probe-probe-probe-probe-probe-probe")
	probes := []string{
		segment(`{"alg":"none"}`) + "." + segment(`{"sub":"probe"}`) + "." + segment("probe-signature"),
		"-----BEGIN " + "PRIVATE KEY-----",
		"postgres://user:" + "probe@host/db",
	}
	for _, shape := range shapes {
		probes = append(probes, shape.Prefix+strings.Repeat(shape.Sample, shape.Min))
	}
	for _, candidate := range scanner.rules {
		if prefix, ok := strings.CutPrefix(candidate.name, "prefix:"); ok {
			probes = append(probes, prefix+hexText, prefix+urlText)
		}
		if scheme, ok := strings.CutPrefix(candidate.name, "scheme:"); ok {
			probes = append(probes, scheme+" "+urlText)
		}
	}
	for _, probe := range probes {
		if scanner.Match(probe) == "" {
			return nil, fmt.Errorf("probe %q matches no value rule", probe)
		}
	}
	for _, candidate := range scanner.rules {
		covered := false
		for _, probe := range probes {
			covered = covered || candidate.pattern.MatchString(probe)
		}
		if !covered {
			return nil, fmt.Errorf("value rule %s has no probe", candidate.name)
		}
	}
	return probes, nil
}

// AnyCredentialShaped reports whether any value has a credential shape. It
// fails closed: when the default scanner cannot load, every value counts.
func AnyCredentialShaped(values ...string) bool {
	scanner, err := Default()
	return anyCredentialShaped(scanner, err, values)
}

func anyCredentialShaped(scanner *Scanner, err error, values []string) bool {
	if err != nil || scanner == nil {
		return true
	}
	for _, value := range values {
		if scanner.Match(value) != "" {
			return true
		}
	}
	return false
}
