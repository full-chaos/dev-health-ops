package workgraph

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// rawErrorAttr matches a log attribute under any key whose value is an error
// or its text: slog.Any with an err-named value, or an "error"/"err" key
// followed by a value. \s spans newlines, so a wrapped call is matched too.
var rawErrorAttr = regexp.MustCompile(`slog\.Any\(\s*"[^"]*"\s*,\s*[^)"]*[eE]rr|"(error|err)"\s*,\s*[^"\s]`)

func TestRawErrorAttrPatternMatchesTheShapesItGuards(t *testing.T) {
	for _, shape := range []string{
		`slog.Any("error", err)`,
		`slog.Any("err", errors.New("x"))`,
		"slog.Any(\n\t\"cause\",\n\terr,\n)",
		`append([]any{"org_id", orgID, "error", err}, attrs...)`,
		`"error", err.Error()`,
	} {
		if !rawErrorAttr.MatchString(shape) {
			t.Fatalf("pattern misses %q", shape)
		}
	}
	for _, safe := range []string{
		`slog.Any("window_from", window.From)`,
		`workgraph.ErrorAttr(err)`,
		`slog.String("error_class", retryableClass(err))`,
		`slog.Any("failed_steps", failedSteps)`,
	} {
		if rawErrorAttr.MatchString(safe) {
			t.Fatalf("pattern flags %q", safe)
		}
	}
}

// slogAnyKeyAllowlist names the only slog.Any keys whose values are known to
// carry no error text: every other slog.Any in production source is flagged,
// whatever its key or value spelling, so a new raw error under a fresh key
// cannot pass unnoticed.
var slogAnyKeyAllowlist = map[string]bool{
	"window_from": true, "window_to": true, "failed_steps": true, "failed_step_evidence": true,
}

var slogAnyCall = regexp.MustCompile(`slog\.Any\(\s*"([^"]*)"`)

func TestEverySlogAnyKeyIsAllowlisted(t *testing.T) {
	var offenders []string
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		source := string(raw)
		for _, match := range slogAnyCall.FindAllStringSubmatchIndex(source, -1) {
			if key := source[match[2]:match[3]]; !slogAnyKeyAllowlist[key] {
				offenders = append(offenders, fmt.Sprintf("%s:%d: slog.Any(%q)", path, strings.Count(source[:match[0]], "\n")+1, key))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("slog.Any with a key outside the allowlist (use ErrorAttr for errors):\n%s", strings.Join(offenders, "\n"))
	}
}

func TestSlogAnyCallPatternSeesAnyKey(t *testing.T) {
	for _, shape := range []string{`slog.Any("cause", cause)`, "slog.Any(\n\t\"cause\",\n\tcause)"} {
		if !slogAnyCall.MatchString(shape) {
			t.Fatalf("pattern misses %q", shape)
		}
	}
}

func TestNoWorkgraphLogCallCarriesARawError(t *testing.T) {
	var offenders []string
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		source := string(raw)
		for _, span := range rawErrorAttr.FindAllStringIndex(source, -1) {
			line := strings.Count(source[:span[0]], "\n") + 1
			offenders = append(offenders, fmt.Sprintf("%s:%d: %s", path, line, strings.TrimSpace(source[span[0]:span[1]])))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("a raw error reaches a log attribute (use ErrorAttr / withErrorAttrs):\n%s", strings.Join(offenders, "\n"))
	}
}
