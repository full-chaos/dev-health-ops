package producttelemetry

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// payloadRulesJSON is the one table of the intake's value rules. The patterns
// use only syntax that Go and JavaScript read alike, and "$" is the end of
// the text in both, so a trailing newline does not pass.
//
//go:embed payload_rules.json
var payloadRulesJSON []byte

type payloadRuleSet struct {
	Version       int    `json:"version"`
	KeyPattern    string `json:"keyPattern"`
	StringPattern string `json:"stringPattern"`
	// RefusedSubstrings are text a string value may not contain, whatever the pattern says.
	RefusedSubstrings []string `json:"refusedSubstrings"`
	UUIDPattern       string   `json:"uuidPattern"`
	RunPattern        string   `json:"runPattern"`
	OrgIDHashPattern  string   `json:"orgIdHashPattern"`
}

var (
	ruleOrgIDHashPattern string
	ruleKey              *regexp.Regexp
	ruleString           *regexp.Regexp
	ruleUUID             *regexp.Regexp
	ruleRun              *regexp.Regexp
	ruleOrgIDHash        *regexp.Regexp
	ruleRefusedText      []string
)

func init() {
	var set payloadRuleSet
	if err := json.Unmarshal(payloadRulesJSON, &set); err != nil {
		panic(fmt.Sprintf("producttelemetry: payload_rules.json: %v", err))
	}
	compile := func(pattern string) *regexp.Regexp {
		return regexp.MustCompile(pattern)
	}
	ruleKey = compile(set.KeyPattern)
	ruleString = compile(set.StringPattern)
	ruleUUID = compile(set.UUIDPattern)
	ruleRun = compile(set.RunPattern)
	ruleOrgIDHash = compile(set.OrgIDHashPattern)
	ruleOrgIDHashPattern = set.OrgIDHashPattern
	ruleRefusedText = set.RefusedSubstrings
}

func containsRefusedText(value string) bool {
	for _, text := range ruleRefusedText {
		if strings.Contains(value, text) {
			return true
		}
	}
	return false
}

// The reasons a payload is refused. A reason is a fixed label and never holds
// a value from the request.
const (
	refusedKey    = "key"
	refusedString = "string"
	refusedNumber = "number"
)

// invalidKeyToken stands for a key that broke the key rule in the log line: a
// key that is not valid is never written out.
const invalidKeyToken = "invalid_key"

type payloadRefusal struct {
	reason string
	// key is the key of the refused value when it passed the key rule, else
	// invalidKeyToken.
	key string
}

// refusePayload applies the value rules to a payload whose values are already
// known to be scalars. It returns nil when the payload may be stored.
func refusePayload(payload *pyjson.Object) *payloadRefusal {
	for _, key := range payload.Keys() {
		if !ruleKey.MatchString(key) {
			return &payloadRefusal{reason: refusedKey, key: invalidKeyToken}
		}
		value, _ := payload.Get(key)
		switch typed := value.(type) {
		case string:
			if !ruleString.MatchString(typed) || containsRefusedText(typed) || ruleUUID.MatchString(typed) || ruleRun.MatchString(typed) {
				return &payloadRefusal{reason: refusedString, key: key}
			}
		case pyjson.Float:
			if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
				return &payloadRefusal{reason: refusedNumber, key: key}
			}
		}
	}
	return nil
}
