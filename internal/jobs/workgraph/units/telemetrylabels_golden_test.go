package units

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

type telemetryLabelsGolden struct {
	PromptKinds             []string `json:"prompt_kinds"`
	Stages                  []string `json:"stages"`
	PromptVersions          []string `json:"prompt_versions"`
	Providers               []string `json:"providers"`
	CategorizationStatuses  []string `json:"categorization_statuses"`
	ParseStatuses           []string `json:"parse_statuses"`
	ValidationErrorFamilies []string `json:"validation_error_families"`

	ProviderCases []struct {
		InputCodepoints []int  `json:"input_codepoints"`
		Bucket          string `json:"bucket"`
	} `json:"provider_cases"`

	ModelCases []struct {
		InputCodepoints []int  `json:"input_codepoints"`
		IsNone          bool   `json:"is_none"`
		Bucket          string `json:"bucket"`
	} `json:"model_cases"`

	ValidationErrorCases []struct {
		InputCodepoints []int  `json:"input_codepoints"`
		Family          string `json:"family"`
	} `json:"validation_error_cases"`

	BoundedCases []struct {
		Value         string `json:"value"`
		InAllowed     bool   `json:"in_allowed"`
		CustomDefault string `json:"custom_default"`
		Result        string `json:"result"`
	} `json:"bounded_cases"`

	WhitespaceProbes []struct {
		CodePoint     int  `json:"code_point"`
		PythonIsSpace bool `json:"python_isspace"`
	} `json:"whitespace_probes"`
}

func loadTelemetryLabelsGolden(t *testing.T) telemetryLabelsGolden {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..",
		"tests", "fixtures", "telemetry_labels_python_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read telemetry labels golden: %v", err)
	}
	var golden telemetryLabelsGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse telemetry labels golden: %v", err)
	}
	if len(golden.ProviderCases) == 0 || len(golden.ModelCases) == 0 {
		t.Fatal("telemetry labels golden is empty; assertions would pass vacuously")
	}
	return golden
}

func runesToString(codepoints []int) string {
	var builder strings.Builder
	for _, codepoint := range codepoints {
		builder.WriteRune(rune(codepoint))
	}
	return builder.String()
}

// TestTelemetryAllowListsMatchPython pins the allow-lists themselves. These are
// transcribed from frozensets, so a typo produces a label that silently becomes
// "other" for every emission.
func TestTelemetryAllowListsMatchPython(t *testing.T) {
	golden := loadTelemetryLabelsGolden(t)
	for _, check := range []struct {
		name string
		got  []string
		want []string
	}{
		{"prompt_kinds", SortedPromptKinds[:], golden.PromptKinds},
		{"stages", SortedStages[:], golden.Stages},
		{"prompt_versions", SortedPromptVersions[:], golden.PromptVersions},
		{"providers", SortedProviders[:], golden.Providers},
		{"categorization_statuses", SortedCategorizationStatuses[:], golden.CategorizationStatuses},
		{"parse_statuses", SortedParseStatuses[:], golden.ParseStatuses},
		{"validation_error_families", SortedValidationErrorFamilies[:], golden.ValidationErrorFamilies},
	} {
		if !equalStrings(check.got, check.want) {
			t.Errorf("%s differ\n  go:     %v\n  python: %v", check.name, check.got, check.want)
		}
	}
}

// TestProviderBucketMatchesPython covers the strip set and the lower() rules.
func TestProviderBucketMatchesPython(t *testing.T) {
	golden := loadTelemetryLabelsGolden(t)
	for _, testCase := range golden.ProviderCases {
		input := runesToString(testCase.InputCodepoints)
		t.Run(strings.ToValidUTF8(input, "?"), func(t *testing.T) {
			if got := ProviderBucket(input); got != testCase.Bucket {
				t.Errorf("ProviderBucket(%q) = %q, python = %q", input, got, testCase.Bucket)
			}
		})
	}
}

// TestModelBucketMatchesPython covers the ordered prefix chain, including the
// "local" vs "local-" near-miss and the empty -> "unknown" case.
func TestModelBucketMatchesPython(t *testing.T) {
	golden := loadTelemetryLabelsGolden(t)
	for _, testCase := range golden.ModelCases {
		// Python's `model: str | None`. None and "" both normalise to empty and
		// both yield "unknown", so Go's "" covers both -- asserted rather than
		// assumed, since the corpus carries the None case explicitly.
		input := ""
		if !testCase.IsNone {
			input = runesToString(testCase.InputCodepoints)
		}
		t.Run(strings.ToValidUTF8(input, "?"), func(t *testing.T) {
			if got := ModelBucket(input); got != testCase.Bucket {
				t.Errorf("ModelBucket(%q) = %q, python = %q", input, got, testCase.Bucket)
			}
		})
	}
}

func TestValidationErrorFamilyMatchesPython(t *testing.T) {
	golden := loadTelemetryLabelsGolden(t)
	for _, testCase := range golden.ValidationErrorCases {
		input := runesToString(testCase.InputCodepoints)
		t.Run(strings.ToValidUTF8(input, "?"), func(t *testing.T) {
			if got := ValidationErrorFamily(input); got != testCase.Family {
				t.Errorf("ValidationErrorFamily(%q) = %q, python = %q", input, got, testCase.Family)
			}
		})
	}
}

func TestBoundedMatchesPython(t *testing.T) {
	golden := loadTelemetryLabelsGolden(t)
	for _, testCase := range golden.BoundedCases {
		defaultValue := "other"
		if testCase.CustomDefault != "" {
			defaultValue = testCase.CustomDefault
		}
		if got := Bounded(testCase.Value, Providers, defaultValue); got != testCase.Result {
			t.Errorf("Bounded(%q, PROVIDERS, %q) = %q, python = %q",
				testCase.Value, defaultValue, got, testCase.Result)
		}
	}
}

// TestStripUsesTheIsSpaceClassNotTheNumericOne is the assertion that names the
// distinction rather than relying on it implicitly.
//
// str.strip() uses str.isspace() -- 29 code points, INCLUDING U+001C-U+001F.
// float()/int() use a narrower 25-point set that excludes them and equals Go's
// unicode.IsSpace. floatcoerce.go needs the narrow one; this file needs the wide
// one. Both are correct, in different places, and the failure mode of picking
// the familiar one is silent.
func TestStripUsesTheIsSpaceClassNotTheNumericOne(t *testing.T) {
	golden := loadTelemetryLabelsGolden(t)
	if len(golden.WhitespaceProbes) == 0 {
		t.Fatal("no whitespace probes in the corpus")
	}

	var divergent int
	for _, probe := range golden.WhitespaceProbes {
		character := rune(probe.CodePoint)

		// pythonparity.Strip must agree with str.isspace() on every probe.
		stripped := pythonparity.Strip(string(character) + "openai" + string(character))
		if got := stripped == "openai"; got != probe.PythonIsSpace {
			t.Errorf("U+%04X: pythonparity.Strip removed=%v, python isspace=%v",
				probe.CodePoint, got, probe.PythonIsSpace)
		}

		// And where the two classes DIFFER, record it, so the test fails loudly
		// if a future Go or Unicode release collapses them and this file's
		// reason for existing quietly evaporates.
		if unicode.IsSpace(character) != probe.PythonIsSpace {
			divergent++
		}
	}

	if divergent == 0 {
		t.Error(
			"no probe distinguishes unicode.IsSpace from str.isspace(); the corpus " +
				"has lost the U+001C-U+001F cases and this test no longer proves " +
				"that the two whitespace classes are different",
		)
	}
	t.Logf("%d of %d probes distinguish str.isspace() from unicode.IsSpace",
		divergent, len(golden.WhitespaceProbes))
}

// TestSigmaLookaheadDivergesAtThirtyOne pins the exact boundary of the one known
// difference between x/text and CPython's str.lower().
//
// Recorded as a measurement rather than a caveat: "x/text is approximately
// right" is not a fact anyone can act on, whereas "it diverges at exactly 31
// case-ignorable runes" tells a future reader where to look and lets them notice
// if a Go or x/text release moves it.
func TestSigmaLookaheadDivergesAtThirtyOne(t *testing.T) {
	sigmaForm := func(dotCount int) rune {
		input := "AΣ" + strings.Repeat(".", dotCount) + "B"
		for _, character := range pythonLower(input) {
			if character == 'σ' || character == 'ς' {
				return character
			}
		}
		t.Fatalf("no sigma survived lowering %q", input)
		return 0
	}

	// CPython yields the MEDIAL sigma at every length, because the trailing "B"
	// is a cased letter at any distance.
	for _, dotCount := range []int{0, 1, 29, 30} {
		if got := sigmaForm(dotCount); got != 'σ' {
			t.Errorf("with %d case-ignorable runes: x/text gave %q, want medial σ "+
				"(CPython gives medial at every length)", dotCount, got)
		}
	}
	for _, dotCount := range []int{31, 32, 50} {
		if got := sigmaForm(dotCount); got != 'ς' {
			t.Errorf("with %d case-ignorable runes: x/text gave %q, expected the "+
				"known FINAL-sigma divergence ς. If x/text has been fixed, delete "+
				"this half of the test and the containment comment in "+
				"telemetrylabels.go -- do not just widen the bound", dotCount, got)
		}
	}
}

// TestSigmaFormCannotChangeABucket is the containment proof that makes the
// divergence above acceptable.
//
// Both sigma spellings are non-ASCII; every allow-list entry and every
// ModelBucket prefix is ASCII. So no bucket decision can depend on which form
// appears. This test fails the moment that stops being true -- a non-ASCII
// allow-list entry, a non-ASCII prefix, or pythonLower being exported to a
// caller that does not bound its output.
func TestSigmaFormCannotChangeABucket(t *testing.T) {
	replaceSigma := func(value string, with rune) string {
		var builder strings.Builder
		for _, character := range value {
			if character == 'Σ' {
				builder.WriteRune(with)
				continue
			}
			builder.WriteRune(character)
		}
		return builder.String()
	}

	for _, dotCount := range []int{0, 30, 31, 50} {
		dots := strings.Repeat(".", dotCount)
		for _, template := range []string{
			"AΣ" + dots + "B",
			"openaiΣ" + dots + "B",
			"gpt-5-nanoΣ" + dots + "B",
			"claudeΣ" + dots + "B",
			"Σ" + dots + "Bopenai",
		} {
			medial := replaceSigma(template, 'σ')
			final := replaceSigma(template, 'ς')
			if ProviderBucket(medial) != ProviderBucket(final) {
				t.Errorf("ProviderBucket depends on the sigma form for %q: %q vs %q",
					template, ProviderBucket(medial), ProviderBucket(final))
			}
			if ModelBucket(medial) != ModelBucket(final) {
				t.Errorf("ModelBucket depends on the sigma form for %q: %q vs %q",
					template, ModelBucket(medial), ModelBucket(final))
			}
		}
	}
}
