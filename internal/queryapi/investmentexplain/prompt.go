package investmentexplain

import (
	_ "embed"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// promptText is investment_mix_explainer.py's PROMPT_PATH, embedded into
// the query-api binary rather than read from disk at runtime -- Python's
// load_prompt() reads it off the filesystem beside the source tree, which
// a deployed Go binary does not ship with. Copied byte-for-byte from
// src/dev_health_ops/llm/prompts/investment_mix_explain_prompt.txt; kept in
// sync by promptTextMatchesPythonSource_test.go, which fails loudly if the
// two ever diverge rather than letting this copy go stale silently.
//
//go:embed prompts/investment_mix_explain_prompt.txt
var promptText string

// LoadPrompt ports load_prompt (investment_mix_explainer.py:46-50).
//
// Python's version swallows a missing-file OSError to "" -- meaningful
// there because PROMPT_PATH is resolved at IMPORT time against whatever
// filesystem layout the process happens to run in, and a packaging
// mismatch must not crash the whole process. Go's embed is resolved at
// COMPILE time: the file not existing is a build failure, not a runtime
// one, so there is no missing-file case left for this function to guard --
// promptText is always present when this package compiles at all.
func LoadPrompt() string {
	return promptText
}

// responseFormatMarker and investmentMixResponseFormat port
// llm/providers/openai.py's RESPONSE_FORMAT_MARKER and
// INVESTMENT_MIX_RESPONSE_FORMAT -- the leading line build_prompt prepends
// so a provider can detect which structured-output schema this prompt
// wants.
const (
	responseFormatMarker        = "DEV_HEALTH_RESPONSE_FORMAT="
	investmentMixResponseFormat = "investment_mix_explanation"
)

// BuildPrompt ports build_prompt (investment_mix_explainer.py:53-60)
// exactly:
//
//	f"{RESPONSE_FORMAT_MARKER}{INVESTMENT_MIX_RESPONSE_FORMAT}\n"
//	+ base_prompt.rstrip()
//	+ "\n\n---\nPRECOMPUTED DATA (do not recalculate):\n"
//	+ json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
//	+ "\n---\n\nOutput must be valid JSON."
//
// payload must be a value pythonparity.MarshalPythonJSONIndentSorted
// accepts (nil, bool, string, int, int64, float64, map[string]any, []any,
// nested) -- see that function's doc comment for exactly why a hand-rolled
// encoder is required here rather than encoding/json. base_prompt.rstrip()
// is pythonparity.RStrip, not strings.TrimRight/TrimSpace -- see that
// function's own doc comment for the four code points where Python's
// str.isspace() and Go's unicode.IsSpace disagree.
func BuildPrompt(basePrompt string, payload map[string]any) (string, error) {
	payloadJSON, err := pythonparity.MarshalPythonJSONIndentSorted(any(payload))
	if err != nil {
		return "", err
	}
	return responseFormatMarker + investmentMixResponseFormat + "\n" +
		pythonparity.RStrip(basePrompt) +
		"\n\n---\nPRECOMPUTED DATA (do not recalculate):\n" +
		string(payloadJSON) +
		"\n---\n\nOutput must be valid JSON.", nil
}
