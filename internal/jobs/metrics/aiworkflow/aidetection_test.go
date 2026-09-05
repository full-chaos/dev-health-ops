package aiworkflow

import (
	"strings"
	"testing"
)

func TestDetectFromPRLabelsMatchesKnownLabels(t *testing.T) {
	signals := DetectFromPRLabels([]string{"needs-triage", "AI-Assisted", "copilot"})
	if len(signals) != 2 {
		t.Fatalf("got %d signals, want 2 (AI-Assisted, copilot); needs-triage must not match", len(signals))
	}
	if signals[0].Kind != KindAIAssisted || signals[0].Confidence != 0.95 {
		t.Errorf("signal[0] = %+v, want kind=ai_assisted confidence=0.95", signals[0])
	}
	if signals[0].Evidence["label"] != "AI-Assisted" {
		t.Errorf("evidence label = %v, want original-case label preserved", signals[0].Evidence["label"])
	}
}

func TestDetectFromPRLabelsAgentCreatedKind(t *testing.T) {
	signals := DetectFromPRLabels([]string{"agent-created"})
	if len(signals) != 1 || signals[0].Kind != KindAgentCreated {
		t.Fatalf("got %+v, want one agent_created signal", signals)
	}
}

func TestDetectFromAuthorExcludesCIBots(t *testing.T) {
	for _, login := range []string{"github-actions[bot]", "dependabot[bot]", "renovate[bot]"} {
		if signal := DetectFromAuthor(AuthorInfo{Login: login}); signal != nil {
			t.Errorf("CI bot %q must not produce a signal, got %+v", login, signal)
		}
	}
}

func TestDetectFromAuthorKnownAIBot(t *testing.T) {
	signal := DetectFromAuthor(AuthorInfo{Login: "copilot[bot]"})
	if signal == nil {
		t.Fatal("expected a signal for a known AI bot login")
	}
	if signal.Confidence != 0.90 || signal.Kind != KindAgentCreated {
		t.Errorf("got %+v, want confidence=0.90 kind=agent_created", signal)
	}
	if signal.Evidence["known_ai_bot"] != true {
		t.Errorf("evidence known_ai_bot = %v, want true", signal.Evidence["known_ai_bot"])
	}
}

func TestDetectFromAuthorUnknownBotWeakerSignal(t *testing.T) {
	userType := "Bot"
	signal := DetectFromAuthor(AuthorInfo{Login: "some-new-ai[bot]", UserType: userType})
	if signal == nil {
		t.Fatal("expected a weak signal for an unrecognized [bot]-suffixed login with user_type=Bot")
	}
	if signal.Confidence != 0.55 {
		t.Errorf("confidence = %v, want 0.55", signal.Confidence)
	}
	if signal.Evidence["known_ai_bot"] != false {
		t.Errorf("evidence known_ai_bot = %v, want false", signal.Evidence["known_ai_bot"])
	}
}

// TestDetectFromAuthorEmptyUserTypeIsAbsent pins CHAOS-4280 astra finding 2's
// production reality: the live PR loader never populates author_user_type,
// so it always arrives here as "" -- which must behave exactly like the
// unknown-bot branch being genuinely absent (Python: `"" and ...` is falsy),
// not like a present-but-empty value that somehow still matches.
func TestDetectFromAuthorEmptyUserTypeIsAbsent(t *testing.T) {
	signal := DetectFromAuthor(AuthorInfo{Login: "some-new-ai[bot]", UserType: ""})
	if signal != nil {
		t.Errorf("empty user_type must not trigger the unknown-bot branch, got %+v", signal)
	}
}

func TestDetectFromAuthorOrdinaryUserNoSignal(t *testing.T) {
	if signal := DetectFromAuthor(AuthorInfo{Login: "alice"}); signal != nil {
		t.Errorf("an ordinary non-bot login must not produce a signal, got %+v", signal)
	}
}

func TestDetectFromBranchNameMatchesDelimitedTokens(t *testing.T) {
	for _, tc := range []struct {
		branch string
		want   bool
		kind   string
	}{
		{"copilot/fix-bug", true, KindAIAssisted},
		{"feature/copilot-tweak", true, KindAIAssisted},
		{"devin/refactor", true, KindAgentCreated},
		{"feature/mycopilotthing-inline", false, ""}, // "copilot" not delimiter-framed (no "-"/"/" on either side)
		{"feature/cache", false, ""},
	} {
		signal := DetectFromBranchName(tc.branch)
		if tc.want && signal == nil {
			t.Errorf("branch %q: expected a match, got none", tc.branch)
			continue
		}
		if !tc.want && signal != nil {
			t.Errorf("branch %q: expected no match, got %+v", tc.branch, signal)
			continue
		}
		if tc.want && signal.Kind != tc.kind {
			t.Errorf("branch %q: kind = %q, want %q", tc.branch, signal.Kind, tc.kind)
		}
	}
}

func TestDetectFromBranchNameConfidenceIsWeak(t *testing.T) {
	signal := DetectFromBranchName("copilot/fix")
	if signal == nil || signal.Confidence != 0.35 {
		t.Fatalf("got %+v, want confidence=0.35", signal)
	}
}

func TestDetectFromPRBodyFirstMatchWins(t *testing.T) {
	// "ai assisted" (0.25, second pattern in source order) appears BEFORE
	// "copilot" (also 0.25, fourth pattern) in this body -- but DetectFromPRBody
	// returns the FIRST *pattern*, not the first *position in the body*, to
	// match Python's for-loop-over-patterns-with-return semantics exactly.
	signal := DetectFromPRBody("This PR mentions copilot but was actually ai-assisted work.")
	if signal == nil {
		t.Fatal("expected a match")
	}
	if signal.Evidence["matched_pattern"] != `\bai[\s\-]assisted\b` {
		t.Errorf("matched_pattern = %v, want the ai-assisted pattern (source order wins, not body position)",
			signal.Evidence["matched_pattern"])
	}
}

func TestDetectFromPRBodyEmptyIsNoSignal(t *testing.T) {
	if signal := DetectFromPRBody(""); signal != nil {
		t.Errorf("empty body must not produce a signal, got %+v", signal)
	}
}

func TestDetectFromPRBodyToolMentionWeakSignal(t *testing.T) {
	signal := DetectFromPRBody("Fixed with claude's help.")
	if signal == nil || signal.Confidence != 0.25 || signal.Actor == nil || *signal.Actor != "claude" {
		t.Fatalf("got %+v, want confidence=0.25 actor=claude", signal)
	}
}

// --------------------------------------------------------------------------
// Unicode boundary / whitespace goldens (CHAOS-4280 astra review, finding 4).
// Both directions of divergence were MEASURED against Python's re module,
// not assumed -- these pin the fix, not a guess.
// --------------------------------------------------------------------------

// TestDetectFromPRBodyMatchesNBSPWhitespace pins the FIRST measured
// divergence: Python's `\bai[\s\-]assisted\b` matches "ai\xa0assisted"
// because NBSP (U+00A0) is Python \s in Unicode mode; Go's RE2 \s is
// ASCII-only and would NOT match without the fix.
func TestDetectFromPRBodyMatchesNBSPWhitespace(t *testing.T) {
	body := "This was ai assisted work." // U+00A0 NBSP, not a plain space
	signal := DetectFromPRBody(body)
	if signal == nil {
		t.Fatal("expected a match on NBSP-separated 'ai\\u00A0assisted' -- Python's \\s is Unicode-aware and matches NBSP")
	}
	if signal.Evidence["matched_pattern"] != `\bai[\s\-]assisted\b` {
		t.Errorf("matched wrong pattern: %+v", signal)
	}
}

// TestDetectFromPRBodyDoesNotOverMatchNonASCIIWordChars pins the SECOND
// measured divergence: Python's `\bcopilot\b` does NOT match "écopiloté"
// because é is a Python \w character, so no boundary forms on either side.
// Go's ASCII-only \w would treat é as non-word, wrongly forming a boundary
// and over-matching, without the fix.
func TestDetectFromPRBodyDoesNotOverMatchNonASCIIWordChars(t *testing.T) {
	body := "This word is écopiloté, not a tool mention."
	if signal := DetectFromPRBody(body); signal != nil {
		t.Errorf("'écopiloté' must NOT match \\bcopilot\\b (é is a Python \\w character on both sides), got %+v", signal)
	}
}

// TestDetectFromPRBodyStillMatchesPlainCopilot is the non-vacuity check for
// the boundary-check helper: an ordinary ASCII-bounded "copilot" must still
// match, proving the Unicode-awareness fix didn't break the common case.
func TestDetectFromPRBodyStillMatchesPlainCopilot(t *testing.T) {
	if signal := DetectFromPRBody("uses copilot for suggestions"); signal == nil {
		t.Fatal("expected a match on plain ASCII-bounded 'copilot'")
	}
}

// --------------------------------------------------------------------------
// strongestSignal / max-confidence tie-break goldens (CHAOS-4280 ai_workflow
// design approval condition 2: equal-confidence fixtures for every ordered
// pair, proving Go's tie-break keeps the FIRST maximal element like
// CPython's max(), not the last.
// --------------------------------------------------------------------------

func TestStrongestSignalFirstWinsOnTie(t *testing.T) {
	makeSignal := func(source string, confidence float64) Signal {
		return Signal{Source: source, Confidence: confidence}
	}
	for _, tc := range []struct {
		name    string
		signals []Signal
		want    string // expected Source of the winner
	}{
		{
			"two labels, both 0.95, first wins",
			[]Signal{makeSignal("label-a", 0.95), makeSignal("label-b", 0.95)},
			"label-a",
		},
		{
			"reversed order, first-in-THIS-slice still wins",
			[]Signal{makeSignal("label-b", 0.95), makeSignal("label-a", 0.95)},
			"label-b",
		},
		{
			"strictly higher confidence overrides position",
			[]Signal{makeSignal("weak", 0.25), makeSignal("strong", 0.95)},
			"strong",
		},
		{
			"a later, LOWER-confidence signal never overrides",
			[]Signal{makeSignal("strong", 0.95), makeSignal("weak", 0.25)},
			"strong",
		},
		{
			"three-way tie keeps the first",
			[]Signal{makeSignal("first", 0.35), makeSignal("second", 0.35), makeSignal("third", 0.35)},
			"first",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := strongestSignal(tc.signals)
			if got.Source != tc.want {
				t.Errorf("strongestSignal() = %q, want %q", got.Source, tc.want)
			}
		})
	}
}

func TestUnicodeWordBoundaryFindLeftmostMatch(t *testing.T) {
	find := unicodeWordBoundaryFind("copilot")
	matched, ok := find("not copilot here, but copilot again")
	if !ok || matched != "copilot" {
		t.Fatalf("got (%q, %v), want the first occurrence to match", matched, ok)
	}
	if !strings.Contains("not copilot here, but copilot again", matched) {
		t.Fatalf("sanity: matched text %q not found in source string", matched)
	}
}
