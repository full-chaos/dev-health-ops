package investment

// shadowsanitize.go holds the last bound before the shadow sinks. The columns
// it guards are free String columns (migrations 109 and 110), and a response
// of the provider can influence what the adapter and the client hand over: a
// returned model id, a request id, a code that quotes an answer id. The adapter
// and the client bound these values at their source; the phase bounds them
// again here, so the tables are safe whatever a later change of either does.
//
// Every function here answers with a value of a closed shape. A value outside
// the shape is replaced, never passed.

import (
	"math"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	shadowMaxIDBytes       = 64
	shadowMaxCodeBytes     = 160
	shadowMaxCodes         = 64
	shadowMaxSourceIDBytes = 256
	shadowMaxMapEntries    = 64
	shadowMaxLevels        = 8
	// shadowUnsafe is what a string outside its shape becomes.
	shadowUnsafe = "unsafe_value"
)

// shadowStates is the closed set of decision states, in a stable order.
func shadowStates() []string {
	return []string{
		decision.StateOK, decision.StateZeroSupport, decision.StateQuestionRefused,
		decision.StateAnswerMissing, decision.StateAnswerInvalid, decision.StateEvidenceNone,
		decision.StateEvidenceUnanswered, decision.StateRequestFailed, decision.StateAdapterDefect,
	}
}

// shadowState keeps a state of the closed set. Anything else is a defect of
// the adapter, and is stored as one.
func shadowState(state string) string {
	for _, known := range shadowStates() {
		if state == known {
			return known
		}
	}
	return decision.StateAdapterDefect
}

// shadowErrorClasses is the closed set of attempt failure classes of the
// client. "" is the class of the attempt that succeeded.
var shadowErrorClasses = map[string]struct{}{
	"":                                    {},
	string(categorize.SystemOneClassAuth): {},
	string(categorize.SystemOneClassModelNotFound): {},
	string(categorize.SystemOneClassRateLimit):     {},
	string(categorize.SystemOneClassServer):        {},
	string(categorize.SystemOneClassInvalid):       {},
	string(categorize.SystemOneClassTimeout):       {},
	string(categorize.SystemOneClassTransport):     {},
	string(categorize.SystemOneClassUnexpected):    {},
	string(categorize.SystemOneClassTooLarge):      {},
	string(categorize.SystemOneClassDecode):        {},
	string(categorize.SystemOneClassCanceled):      {},
	string(categorize.SystemOneClassBuildRequest):  {},
	string(categorize.SystemOneClassBodyReadError): {},
}

func shadowErrorClass(class string) string {
	if _, ok := shadowErrorClasses[class]; ok {
		return class
	}
	return "other"
}

// shadowShaped reports whether value has 1..maxBytes bytes, all of them ASCII
// letters, digits or one of extra, and holds no registered secret.
func shadowShaped(value string, maxBytes int, extra string) bool {
	if value == "" || len(value) > maxBytes {
		return false
	}
	for index := 0; index < len(value); index++ {
		b := value[index]
		switch {
		case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		case strings.IndexByte(extra, b) >= 0:
		default:
			return false
		}
	}
	return secrets.RedactRegistered(value) == value
}

// shadowRequestID keeps an id-shaped request id and drops any other value
// whole: a cut value would still be made of the peer's bytes.
func shadowRequestID(id string) string {
	if shadowShaped(id, shadowMaxIDBytes, "._-") {
		return id
	}
	return ""
}

// shadowModelID keeps a model-id-shaped value. "?" and "~" are the marks of
// the adapter's own bound (an unsafe rune, a cut).
func shadowModelID(model string) string {
	if model == "" {
		return ""
	}
	if shadowShaped(model, shadowMaxIDBytes, "._:-?~") {
		return model
	}
	return shadowUnsafe
}

// shadowSpanID keeps a span id or a handle ("E2_1", "E2"): both are built by
// the adapter from the bundle, never taken from a response as text.
func shadowSpanID(id string) string {
	if id == "" {
		return ""
	}
	if shadowShaped(id, shadowMaxIDBytes, "_") {
		return id
	}
	return shadowUnsafe
}

// shadowSourceType keeps one of the three source types of a bundle.
func shadowSourceType(sourceType string) string {
	switch sourceType {
	case "issue", "pr", "commit":
		return sourceType
	}
	return ""
}

// shadowSourceID bounds the id of the cited source. It is an id of the org's
// own work item, pull request or commit (from the bundle's handle map), so it
// is kept as it is when it is printable and not too long.
func shadowSourceID(id string) string {
	if len(id) > shadowMaxSourceIDBytes {
		return shadowUnsafe
	}
	for _, r := range id {
		if r < 0x20 || r == 0x7f {
			return shadowUnsafe
		}
	}
	if secrets.RedactRegistered(id) != id {
		return shadowUnsafe
	}
	return id
}

// shadowCodes bounds a list of codes or warnings: at most shadowMaxCodes
// entries, each of at most shadowMaxCodeBytes bytes of the code character set.
// The adapter already holds each code to this shape (its bound is for each
// value: up to 63 codes of 64 response-influenced bytes can be in one list,
// which is a standing ruling). A code outside the shape is replaced whole. It
// never returns nil.
func shadowCodes(codes []string) []string {
	out := make([]string, 0, min(len(codes), shadowMaxCodes))
	for index, code := range codes {
		if index == shadowMaxCodes {
			break
		}
		if shadowShaped(code, shadowMaxCodeBytes, "_.-:=+,'[] ?~") {
			out = append(out, code)
			continue
		}
		out = append(out, shadowUnsafe)
	}
	return out
}

// shadowLevels converts the levels of the support keys. A key outside the
// code shape or a level outside 0..255 is dropped.
func shadowLevels(levels map[string]int) map[string]uint8 {
	out := make(map[string]uint8, min(len(levels), shadowMaxMapEntries))
	for key, level := range levels {
		if len(out) == shadowMaxMapEntries {
			break
		}
		if level < 0 || level > math.MaxUint8 || !shadowShaped(key, shadowMaxIDBytes, "_.") {
			continue
		}
		out[key] = uint8(level)
	}
	return out
}

// shadowLevelProbabilities converts the level distributions. A value that is
// not a finite probability makes the whole entry unusable, and it is dropped.
func shadowLevelProbabilities(probabilities map[string][]float64) map[string][]float32 {
	out := make(map[string][]float32, min(len(probabilities), shadowMaxMapEntries))
	for key, values := range probabilities {
		if len(out) == shadowMaxMapEntries {
			break
		}
		if len(values) == 0 || len(values) > shadowMaxLevels || !shadowShaped(key, shadowMaxIDBytes, "_.") {
			continue
		}
		converted := make([]float32, 0, len(values))
		for _, value := range values {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
				converted = nil
				break
			}
			converted = append(converted, float32(value))
		}
		if converted != nil {
			out[key] = converted
		}
	}
	return out
}

// shadowSufficiency keeps a level 0..127; -1 means "not answered".
func shadowSufficiency(level int) int8 {
	if level < 0 || level > math.MaxInt8 {
		return -1
	}
	return int8(level)
}

func shadowAttemptNumber(index int) uint8 {
	if index < 0 || index >= math.MaxUint8 {
		return math.MaxUint8
	}
	return uint8(index + 1)
}

func shadowHTTPStatus(status int) uint16 {
	if status < 0 || status > 999 {
		return 0
	}
	return uint16(status)
}

func shadowMillis(duration time.Duration) uint32 {
	millis := duration.Milliseconds()
	switch {
	case millis < 0:
		return 0
	case millis > math.MaxUint32:
		return math.MaxUint32
	}
	// A request that took less than one millisecond still took time: a row with
	// latency 0 would read as "no request".
	if millis == 0 && duration > 0 {
		return 1
	}
	return uint32(millis)
}

func shadowTokens(tokens int64) uint32 {
	switch {
	case tokens < 0:
		return 0
	case tokens > math.MaxUint32:
		return math.MaxUint32
	}
	return uint32(tokens)
}
