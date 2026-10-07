package investment

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestShadowStateIsAClosedSet(t *testing.T) {
	for _, state := range shadowStates() {
		if shadowState(state) != state {
			t.Errorf("state %q is not kept", state)
		}
	}
	for _, hostile := range []string{"", "OK", "ok ", "ok\nnewline", strings.Repeat("x", 5000), "repaired"} {
		if got := shadowState(hostile); got != decision.StateAdapterDefect {
			t.Errorf("state %q became %q, want adapter_defect", hostile, got)
		}
	}
	if len(shadowStates()) != 9 {
		t.Fatalf("there are nine decision states, the set has %d", len(shadowStates()))
	}
}

func TestShadowErrorClassIsAClosedSet(t *testing.T) {
	for _, class := range []categorize.SystemOneClass{
		categorize.SystemOneClassAuth, categorize.SystemOneClassModelNotFound, categorize.SystemOneClassRateLimit,
		categorize.SystemOneClassServer, categorize.SystemOneClassInvalid, categorize.SystemOneClassTimeout,
		categorize.SystemOneClassTransport, categorize.SystemOneClassUnexpected, categorize.SystemOneClassTooLarge,
		categorize.SystemOneClassDecode, categorize.SystemOneClassCanceled, categorize.SystemOneClassBuildRequest,
		categorize.SystemOneClassBodyReadError,
	} {
		if shadowErrorClass(string(class)) != string(class) {
			t.Errorf("class %q is not kept", class)
		}
	}
	if shadowErrorClass("") != "" {
		t.Error(`the class "" of a successful attempt is not kept`)
	}
	for _, hostile := range []string{"auth ", "AUTH", "x\ny", strings.Repeat("a", 4000)} {
		if got := shadowErrorClass(hostile); got != "other" {
			t.Errorf("class %q became %q, want other", hostile, got)
		}
	}
}

// Every bound, at its boundary and with the hostile shapes a peer can send.
func TestShadowStringBounds(t *testing.T) {
	at := strings.Repeat("a", 64)
	over := strings.Repeat("a", 65)
	for _, tc := range []struct {
		name string
		got  string
		want string
	}{
		{"request id kept", shadowRequestID("req_A-1.b"), "req_A-1.b"},
		{"request id at 64 bytes kept", shadowRequestID(at), at},
		{"request id of 65 bytes dropped whole", shadowRequestID(over), ""},
		{"request id with a space dropped whole", shadowRequestID("req 1"), ""},
		{"request id with a line break dropped whole", shadowRequestID("req\n1"), ""},
		{"request id with a colon dropped whole", shadowRequestID("req:1"), ""},
		{"empty request id", shadowRequestID(""), ""},
		{"model kept", shadowModelID("jev-1.13.0-20261001"), "jev-1.13.0-20261001"},
		{"model with the adapter's marks kept", shadowModelID("jev?1~"), "jev?1~"},
		{"empty model stays empty", shadowModelID(""), ""},
		{"model of 65 bytes replaced", shadowModelID(over), shadowUnsafe},
		{"model with a line break replaced", shadowModelID("jev\n1"), shadowUnsafe},
		{"model with a slash replaced", shadowModelID("jev/1"), shadowUnsafe},
		{"span id kept", shadowSpanID("E12_3"), "E12_3"},
		{"empty span id", shadowSpanID(""), ""},
		{"span id with a dash replaced", shadowSpanID("E1-1"), shadowUnsafe},
		{"span id of 65 bytes replaced", shadowSpanID(over), shadowUnsafe},
		{"source type issue", shadowSourceType("issue"), "issue"},
		{"source type pr", shadowSourceType("pr"), "pr"},
		{"source type commit", shadowSourceType("commit"), "commit"},
		{"another source type", shadowSourceType("issue\n"), ""},
		{"source id kept", shadowSourceID("acme/exporter#77"), "acme/exporter#77"},
		{"source id at 256 bytes kept", shadowSourceID(strings.Repeat("s", 256)), strings.Repeat("s", 256)},
		{"source id of 257 bytes replaced", shadowSourceID(strings.Repeat("s", 257)), shadowUnsafe},
		{"source id with a control byte replaced", shadowSourceID("a\x00b"), shadowUnsafe},
		{"source id with a line break replaced", shadowSourceID("a\nb"), shadowUnsafe},
		{"source id with DEL replaced", shadowSourceID("a\x7fb"), shadowUnsafe},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

func TestShadowCodesAreBoundedInCountLengthAndCharacterSet(t *testing.T) {
	kept := []string{"invalid_llm_output", "answer_invalid:quality.bugfix:type=refusal", "codes_dropped:12", "a b+c,d'e[f]g=h?~"}
	if got := shadowCodes(kept); !reflect.DeepEqual(got, kept) {
		t.Fatalf("codes of the adapter's own shape were changed: %q", got)
	}
	at, over := strings.Repeat("c", 160), strings.Repeat("c", 161)
	got := shadowCodes([]string{at, over, "line\nbreak", "quote\"", "ünïcode", "", "back\\slash"})
	want := []string{at, shadowUnsafe, shadowUnsafe, shadowUnsafe, shadowUnsafe, shadowUnsafe, shadowUnsafe}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("codes = %q, want %q", got, want)
	}
	many := make([]string, 200)
	for index := range many {
		many[index] = fmt.Sprintf("code_%d", index)
	}
	if got := shadowCodes(many); len(got) != 64 || got[63] != "code_63" {
		t.Fatalf("200 codes gave %d, want 64", len(got))
	}
	if got := shadowCodes(nil); got == nil || len(got) != 0 {
		t.Fatalf("nil codes gave %#v, want an empty list", got)
	}
}

// A value that holds a registered secret is refused whole, whatever its shape.
func TestAValueThatHoldsARegisteredSecretIsRefused(t *testing.T) {
	const secret = "plain-registered-test-value-0001"
	secrets.Register("SHADOW_TEST_API_KEY", secret)
	if got := shadowRequestID("req-" + secret); got != "" {
		t.Errorf("request id = %q", got)
	}
	if got := shadowModelID("jev-" + secret); got != shadowUnsafe {
		t.Errorf("model = %q", got)
	}
	if got := shadowCodes([]string{"answer_invalid:" + secret}); got[0] != shadowUnsafe {
		t.Errorf("code = %q", got[0])
	}
	if got := shadowSourceID("id-" + secret); got != shadowUnsafe {
		t.Errorf("source id = %q", got)
	}
}

func TestShadowNumbersAreBoundedBeforeAnUnsignedColumn(t *testing.T) {
	if got := shadowLevels(map[string]int{"quality.bugfix": 3, "negative": -1, "too_high": 256, "bad key\n": 1, "top": 255}); !reflect.DeepEqual(got, map[string]uint8{"quality.bugfix": 3, "top": 255}) {
		t.Errorf("levels = %v", got)
	}
	probabilities := shadowLevelProbabilities(map[string][]float64{
		"quality.bugfix": {0.25, 0.75}, "nan": {math.NaN(), 1}, "inf": {math.Inf(1)}, "negative": {-0.1, 1.1},
		"above_one": {1.5}, "empty": {}, "too_long": make([]float64, 9), "bad key\n": {1}, "eight": make([]float64, 8),
	})
	if !reflect.DeepEqual(probabilities, map[string][]float32{"quality.bugfix": {0.25, 0.75}, "eight": make([]float32, 8)}) {
		t.Errorf("level probabilities = %v", probabilities)
	}
	for _, tc := range []struct{ in, want int }{{-1, -1}, {0, 0}, {2, 2}, {127, 127}, {128, -1}, {-7, -1}} {
		if got := shadowSufficiency(tc.in); int(got) != tc.want {
			t.Errorf("sufficiency %d = %d, want %d", tc.in, got, tc.want)
		}
	}
	for _, tc := range []struct {
		in   int64
		want uint32
	}{{-1, 0}, {0, 0}, {2383, 2383}, {math.MaxUint32, math.MaxUint32}, {math.MaxUint32 + 1, math.MaxUint32}} {
		if got := shadowTokens(tc.in); got != tc.want {
			t.Errorf("tokens %d = %d, want %d", tc.in, got, tc.want)
		}
	}
	for _, tc := range []struct {
		in   time.Duration
		want uint32
	}{{-time.Second, 0}, {0, 0}, {time.Microsecond, 1}, {116 * time.Millisecond, 116}, {time.Duration(math.MaxUint32+5) * time.Millisecond, math.MaxUint32}} {
		if got := shadowMillis(tc.in); got != tc.want {
			t.Errorf("millis %v = %d, want %d", tc.in, got, tc.want)
		}
	}
	for _, tc := range []struct {
		in   int
		want uint16
	}{{-1, 0}, {0, 0}, {200, 200}, {529, 529}, {999, 999}, {1000, 0}, {70000, 0}} {
		if got := shadowHTTPStatus(tc.in); got != tc.want {
			t.Errorf("status %d = %d, want %d", tc.in, got, tc.want)
		}
	}
	for _, tc := range []struct {
		in   int
		want uint8
	}{{-1, 255}, {0, 1}, {1, 2}, {253, 254}, {254, 255}, {255, 255}, {5000, 255}} {
		if got := shadowAttemptNumber(tc.in); got != tc.want {
			t.Errorf("attempt index %d = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// The integer rate table: the same usage gives the same cost on every
// architecture, and a response with no usable usage costs nothing.
func TestTheBillIsAnIntegerProductOfTheCheckedUsage(t *testing.T) {
	if got := shadowBilledNanoUSD(categorize.SystemOneUsage{InputTokens: 1_000_000, OutputTokens: 999, Reported: true}); got != 42_000_000 {
		t.Fatalf("one million input tokens = %d nano-USD, want 42,000,000 (USD 0.042); output tokens are free", got)
	}
	if got := shadowBilledNanoUSD(categorize.SystemOneUsage{InputTokens: 2383, Reported: false}); got != 0 {
		t.Fatalf("usage that was not reported = %d, want 0", got)
	}
	if got := shadowBilledNanoUSD(categorize.SystemOneUsage{InputTokens: -5, Reported: true}); got != 0 {
		t.Fatalf("negative usage = %d, want 0", got)
	}
	if got := shadowReservationNanoUSD(make([]byte, 10_001)); got != 5001*42 {
		t.Fatalf("reservation of a 10,001-byte body = %d, want %d", got, 5001*42)
	}
	// The float in the row is one division of an integer: exact for these values.
	if got := float64(int64(2383*42)) / 1e9; got != 0.000100086 {
		t.Fatalf("row cost = %v", got)
	}
}

const shadowHostileMarker = "HOSTILEMARKER"

// A hostile peer: a 5,000-byte marker with a line break in the returned model,
// in the request-id header, as an answer type and as 300 unknown answer ids.
// Nothing longer than a bounded value reaches a row, an attempt row or a log
// line, and no line break does.
func TestNoPeerControlledStringReachesASinkOrALogUnbounded(t *testing.T) {
	hostile := strings.Repeat(shadowHostileMarker, 400) + "\nline two \x00\x1b[31m\"<script>"
	for _, tc := range []struct {
		name  string
		reply func() jevReply
	}{
		{"returned model", func() jevReply { r := okReply(); r.model = hostile; return r }},
		{"request id header", func() jevReply {
			r := okReply()
			r.headers = map[string]string{"X-Typesafe-Request-Id": strings.Repeat(shadowHostileMarker, 400)}
			return r
		}},
		{"answer type", func() jevReply {
			r := okReply()
			r.extraAnswers = map[string]string{"support__quality__bugfix": hostile}
			return r
		}},
		{"unknown answer ids", func() jevReply {
			r := okReply()
			r.extraAnswers = map[string]string{}
			for index := 0; index < 300; index++ {
				r.extraAnswers[fmt.Sprintf("%s_%d_%s", shadowHostileMarker, index, strings.Repeat("x", 300))] = "score"
			}
			return r
		}},
		{"error body of a 500", func() jevReply {
			return jevReply{status: http.StatusInternalServerError, headers: map[string]string{"Retry-After": "0.01"}, body: []byte(hostile)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeJev(t, func(int, []byte) jevReply { return tc.reply() })
			logs := &syncBuffer{}
			phase := newTestShadowPhase(t, fake, shadowTestSettings(), debugLogger(logs))
			store := &memoryShadowStore{}
			phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1"))
			if len(store.records) != 1 || len(store.attempts) == 0 {
				t.Fatalf("rows = %d, attempts = %d", len(store.records), len(store.attempts))
			}
			record := store.records[0]
			strs := map[string][]string{
				"model_returned": {record.ModelReturned}, "state": {record.State}, "status": {record.CategorizationStatus},
				"warnings": record.Warnings, "error_codes": record.ErrorCodes,
				"evidence": {record.EvidenceSpanID, record.EvidenceHandle, record.EvidenceSourceType, record.EvidenceSourceID},
			}
			for _, attempt := range store.attempts {
				strs["attempt"] = append(strs["attempt"], attempt.RequestID, attempt.ModelReturned, attempt.ErrorClass, attempt.State, attempt.Kind, attempt.Role)
			}
			total := 0
			for column, values := range strs {
				if len(values) > 64*len(store.attempts)+64 {
					t.Errorf("%s holds %d values", column, len(values))
				}
				for _, value := range values {
					total += len(value)
					if len(value) > 160 {
						t.Errorf("%s holds a value of %d bytes", column, len(value))
					}
					if strings.ContainsAny(value, "\n\r\x00\x1b\"<>") {
						t.Errorf("%s holds an unsafe byte: %q", column, value)
					}
					if run := longestMarkerRun(value); run > 64 {
						t.Errorf("%s holds a marker run of %d bytes: %q", column, run, value)
					}
				}
			}
			// The standing ruling: the bound is for each value, so one list can
			// hold up to 64 codes. The total is bounded by that product.
			if total > 2*64*160+1024 {
				t.Errorf("the row and its attempts hold %d bytes of strings", total)
			}
			for _, line := range strings.Split(logs.String(), "\n") {
				if run := longestMarkerRun(line); run > 64 {
					t.Errorf("a log line holds a marker run of %d bytes: %.200q", run, line)
				}
				if strings.Contains(line, "line two") || strings.Contains(line, "<script>") {
					t.Errorf("a log line holds response text: %.200q", line)
				}
			}
			if record.ShadowConfig != decision.IdentityFor("").Stamp() {
				t.Errorf("the config stamp moved: %q", record.ShadowConfig)
			}
		})
	}
}

func longestMarkerRun(value string) int {
	longest, run := 0, 0
	for len(value) > 0 {
		if strings.HasPrefix(value, shadowHostileMarker) {
			run += len(shadowHostileMarker)
			value = value[len(shadowHostileMarker):]
			if run > longest {
				longest = run
			}
			continue
		}
		run = 0
		value = value[1:]
	}
	return longest
}

// No log line of a phase holds source text, request text, response text or the
// key, at DEBUG level, for an ok run, a failed run and a retried run.
func TestNoLogLineOfAPhaseHoldsSourceTextOrTheKey(t *testing.T) {
	for name, reply := range map[string]func(n int) jevReply{
		"ok":           func(int) jevReply { return okReply() },
		"zero support": func(int) jevReply { return jevReply{supported: map[string]int{}, inputTokens: 10} },
		"not json":     func(int) jevReply { return jevReply{body: []byte("the response text " + shadowSourceSentinel)} },
		"rejected key": func(int) jevReply { return jevReply{status: 401, body: []byte(shadowSourceSentinel)} },
		"retried": func(n int) jevReply {
			if n%2 == 1 {
				return jevReply{status: 503, headers: map[string]string{"Retry-After": "0.01"}, body: []byte(shadowSourceSentinel)}
			}
			return okReply()
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeJev(t, func(n int, _ []byte) jevReply { return reply(n) })
			logs := &syncBuffer{}
			phase := newTestShadowPhase(t, fake, shadowTestSettings(), debugLogger(logs))
			store := &memoryShadowStore{}
			phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2"))
			out := logs.String()
			if fake.count() == 0 || !strings.Contains(out, "investment shadow phase complete") {
				t.Fatalf("the phase did not run (requests %d):\n%s", fake.count(), out)
			}
			for _, banned := range []string{shadowSourceSentinel, "streamed writer", "Export job times out", shadowTestKeyValue, "Bearer", "source_block", "evidence_spans", "probabilities"} {
				if strings.Contains(out, banned) {
					t.Errorf("a log line holds %q:\n%s", banned, out)
				}
			}
			// The rows hold no source text either: evidence is ids only.
			for _, record := range store.records {
				for _, value := range append(append([]string{record.EvidenceSpanID, record.EvidenceHandle, record.EvidenceSourceID}, record.Warnings...), record.ErrorCodes...) {
					if strings.Contains(value, shadowSourceSentinel) || strings.Contains(value, "streamed writer") {
						t.Errorf("a shadow row holds source text: %q", value)
					}
				}
			}
		})
	}
}

// ShadowRecord and AttemptRecord have no field that could hold a prompt, a
// response or a quote: the phase cannot store one by mistake.
func TestTheSinkRecordsHaveNoTextField(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(chwrite.ShadowRecord{}), reflect.TypeOf(chwrite.AttemptRecord{})} {
		for index := 0; index < typ.NumField(); index++ {
			name := strings.ToLower(typ.Field(index).Name)
			for _, banned := range []string{"prompt", "response", "quote", "text", "body", "uncertainty"} {
				if strings.Contains(name, banned) {
					t.Errorf("%s.%s can hold text", typ.Name(), typ.Field(index).Name)
				}
			}
		}
	}
}
