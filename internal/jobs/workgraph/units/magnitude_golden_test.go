package units

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMaxComponentNodesMagnitudeMatchesPython covers the MAGNITUDE axis, added
// after lane-4752-go found a corpus whose largest positive id was 42 -- which
// let a port that refused values above 2^31 pass for as long as that corpus
// ran.
//
// It caught a real divergence here. Python's int() is unbounded, so
// INVESTMENT_MAX_COMPONENT_NODES set to forty 1s parses to 10^40: a cap so
// large that NO component is ever split. Go's strconv.Atoi returns ErrRange,
// and the original port treated that as "malformed" and fell back to the
// default of 150 -- which splits aggressively. The two planes then mint
// completely different work_unit_ids for any org holding a component above 150
// nodes, which is exactly the CHAOS-2775 split this port reproduces.
//
// The fix saturates. That is EXACT, not approximate: a cap of 10^40 and a cap
// of MaxInt produce identical splitting decisions, because no component can
// contain more than MaxInt nodes. So the assertion below accepts saturation
// only when Python's value genuinely exceeds MaxInt, and demands equality
// everywhere else -- a port that saturated eagerly would still fail.
func TestMaxComponentNodesMagnitudeMatchesPython(t *testing.T) {
	path := filepath.Join(
		repositoryRootPath(t), "tests", "fixtures",
		"max_component_nodes_python_golden.json",
	)
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read magnitude golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_max_component_nodes_golden.py)", err)
	}
	var golden struct {
		IntMaxStrDigits int `json:"int_max_str_digits"`
		Cases           []struct {
			RawHex   string `json:"raw_hex"`
			Resolved string `json:"resolved"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse magnitude golden: %v", err)
	}
	if len(golden.Cases) < 30 {
		t.Fatalf("only %d magnitude cases; the corpus should cover int32/int64 "+
			"boundaries in both directions, 40-digit values and 40 zeros",
			len(golden.Cases))
	}

	// The limit is a RUNTIME setting (sys.set_int_max_str_digits), so the Go
	// constant is a claim about the DEPLOYED interpreter, not about Python in
	// general. If they part company, values between the two limits are parsed
	// by one plane and refused by the other.
	if golden.IntMaxStrDigits != DefaultIntMaxStrDigits {
		t.Fatalf("deployed interpreter reports sys.get_int_max_str_digits() = %d, "+
			"but DefaultIntMaxStrDigits is %d -- every value between the two is "+
			"accepted by one plane and refused by the other",
			golden.IntMaxStrDigits, DefaultIntMaxStrDigits)
	}

	maxInt := big.NewInt(math.MaxInt64)
	var sawSaturation, sawFortyZeros, sawOverDigitLimit bool

	for _, testCase := range golden.Cases {
		decoded, err := hex.DecodeString(testCase.RawHex)
		if err != nil {
			t.Fatalf("decode raw hex: %v", err)
		}
		value := string(decoded)

		want, ok := new(big.Int).SetString(testCase.Resolved, 10)
		if !ok {
			t.Fatalf("python value %q is not an integer", testCase.Resolved)
		}

		t.Setenv(MaxComponentNodesEnvVar, value)
		got := big.NewInt(int64(ResolveMaxComponentNodes(nil)))

		if digitCount(value) > DefaultIntMaxStrDigits {
			sawOverDigitLimit = true
		}
		if got.Cmp(want) == 0 {
			if len(value) == 40 && value == "0000000000000000000000000000000000000000" {
				sawFortyZeros = true
			}
			continue
		}

		// The only tolerated difference: Python exceeded MaxInt and Go pinned
		// to MaxInt. Anything else is a divergence.
		if want.Cmp(maxInt) > 0 && got.Cmp(maxInt) == 0 {
			sawSaturation = true
			continue
		}
		t.Errorf("INVESTMENT_MAX_COMPONENT_NODES=%q: go = %s, python = %s",
			value, got, want)
	}

	// Guard the corpus, not just the code: without an out-of-range value the
	// saturation branch is never exercised and could regress to a silent
	// default unnoticed.
	if !sawSaturation {
		t.Error("no case exceeded MaxInt; the magnitude axis is not being " +
			"exercised and the ErrRange branch is untested")
	}
	// 40 zeros is a silent ZERO, not a huge number -- lane-4752-go's case.
	// It must resolve to the default via the `>= 1` check, not via a parse
	// failure, and the two are indistinguishable from the outside.
	if !sawFortyZeros {
		t.Error("the 40-zeros case is missing; it distinguishes a value that " +
			"PARSES to zero from one that fails to parse")
	}
	// Above sys.get_int_max_str_digits() Python RAISES, so the port must REFUSE
	// rather than saturate -- the mirror image of the ErrRange defect. Without
	// a case past the limit that branch is untested.
	if !sawOverDigitLimit {
		t.Error("no case exceeds int_max_str_digits; the refuse-rather-than-" +
			"saturate branch is untested")
	}
}

// digitCount counts what CPython counts toward int_max_str_digits: DIGITS
// only. Measured -- a sign does not count, surrounding whitespace does not
// count, and PEP 515 underscores do not count (4300 digits separated by
// underscores is 8599 characters and parses fine). Leading zeros DO count.
func digitCount(value string) int {
	count := 0
	for index := 0; index < len(value); index++ {
		if value[index] >= '0' && value[index] <= '9' {
			count++
		}
	}
	return count
}

// TestDigitLimitIsRefusedNotSaturated pins the three-way split at the top of
// the range, which the saturation fix alone got wrong.
//
//	<= MaxInt digits          parse exactly
//	> MaxInt, <= 4300 digits  Python parses; Go saturates (behaviourally exact)
//	> 4300 digits             Python RAISES; Go must REFUSE
//
// Saturating in the third band would hand back MaxInt for a value Python never
// accepted -- as divergent as refusing one it did.
func TestDigitLimitIsRefusedNotSaturated(t *testing.T) {
	atLimit := strings.Repeat("1", DefaultIntMaxStrDigits)
	overLimit := strings.Repeat("1", DefaultIntMaxStrDigits+1)

	if parsed, ok := parsePythonInt(atLimit); !ok || parsed != math.MaxInt {
		t.Errorf("at the digit limit: got (%d, %v), want (MaxInt, true) -- python "+
			"parses this to a huge value, so it saturates", parsed, ok)
	}
	if parsed, ok := parsePythonInt(overLimit); ok {
		t.Errorf("past the digit limit: got (%d, true), want refused -- python "+
			"RAISES here, so saturating invents a value it never produced", parsed)
	}

	// Underscores and a sign do not count toward the limit.
	underscored := strings.Join(strings.Split(atLimit, ""), "_")
	if _, ok := parsePythonInt(underscored); !ok {
		t.Errorf("%d digits separated by underscores (%d chars) must parse; "+
			"underscores are not counted", DefaultIntMaxStrDigits, len(underscored))
	}
	if _, ok := parsePythonInt("-" + atLimit); !ok {
		t.Error("a sign must not count toward the digit limit")
	}

	// Leading zeros DO count, even though the value is tiny.
	tinyValueManyDigits := strings.Repeat("0", DefaultIntMaxStrDigits+1) + "5"
	if parsed, ok := parsePythonInt(tinyValueManyDigits); ok {
		t.Errorf("got (%d, true) for a value of 5 behind %d leading zeros; python "+
			"counts DIGITS, not magnitude, and raises",
			parsed, DefaultIntMaxStrDigits+1)
	}
}

// TestRangeErrorIsNotTreatedAsMalformed pins the distinction directly, because
// the corpus test above would still pass if both branches returned the default
// for a different reason.
func TestRangeErrorIsNotTreatedAsMalformed(t *testing.T) {
	huge := "1" + string(make([]byte, 0))
	for index := 0; index < 39; index++ {
		huge += "1"
	}

	if parsed, ok := parsePythonInt(huge); !ok || parsed != math.MaxInt {
		t.Errorf("parsePythonInt(40 ones) = (%d, %v), want (MaxInt, true) -- an "+
			"out-of-range value is one Python PARSES, so refusing it as malformed "+
			"falls back to a default Python never uses", parsed, ok)
	}
	if parsed, ok := parsePythonInt("-" + huge); !ok || parsed != math.MinInt {
		t.Errorf("parsePythonInt(-40 ones) = (%d, %v), want (MinInt, true) -- the "+
			"sign must survive so the caller's `< 1` check still rejects it",
			parsed, ok)
	}
	// Genuinely malformed values must still be refused, not saturated.
	for _, malformed := range []string{"abc", "", "_1", "1_", "1.5", "0x10", "1e3"} {
		if _, ok := parsePythonInt(malformed); ok {
			t.Errorf("parsePythonInt(%q) was accepted; Python's int() raises", malformed)
		}
	}
}
