package complexity

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ScanConfig is the file-selection half of Python's ComplexityScanner,
// loaded from src/dev_health_ops/config/complexity.yaml.
//
// This config does more than pick files for this family: its include_globs
// ALSO gate provider file-content ingestion (`_fetch_scannable_contents`), so
// an extension absent from it never gets `git_files.contents` and can never
// produce complexity data at all. Diverging here does not merely scan the
// wrong set — it silently changes which files exist to be scanned.
type ScanConfig struct {
	IncludeGlobs []string   `yaml:"include_globs"`
	ExcludeGlobs []string   `yaml:"exclude_globs"`
	Thresholds   Thresholds `yaml:"-"`
}

// rawScanConfig mirrors the YAML file exactly, including the two threshold
// keys, which the Python scanner reads from the same document
// (analytics/complexity.py:91-92).
type rawScanConfig struct {
	IncludeGlobs         []string `yaml:"include_globs"`
	ExcludeGlobs         []string `yaml:"exclude_globs"`
	HighComplexity       *int     `yaml:"high_complexity_threshold"`
	VeryHighComplexity   *int     `yaml:"very_high_threshold"`
}

// LoadScanConfig reads complexity.yaml.
//
// The defaults on a missing key match Python's `.get(...)` fallbacks exactly:
// include_globs defaults to ["**/*.py"], exclude_globs to empty, and the
// thresholds to 15 and 25. Those defaults are reproduced rather than replaced
// with something more sensible — a Go executor that silently scanned a
// different set when a key is absent would diverge precisely when the config
// is malformed, which is when divergence is hardest to notice.
func LoadScanConfig(path string) (ScanConfig, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return ScanConfig{}, fmt.Errorf("read complexity config: %w", err)
	}

	var raw rawScanConfig
	if err := yaml.Unmarshal(contents, &raw); err != nil {
		return ScanConfig{}, fmt.Errorf("parse complexity config %s: %w", path, err)
	}

	config := ScanConfig{
		IncludeGlobs: raw.IncludeGlobs,
		ExcludeGlobs: raw.ExcludeGlobs,
		Thresholds:   DefaultThresholds(),
	}
	if config.IncludeGlobs == nil {
		// Python: self.config.get("include_globs", ["**/*.py"]).
		config.IncludeGlobs = []string{"**/*.py"}
	}
	if config.ExcludeGlobs == nil {
		config.ExcludeGlobs = []string{}
	}
	if raw.HighComplexity != nil {
		config.Thresholds.High = *raw.HighComplexity
	}
	if raw.VeryHighComplexity != nil {
		config.Thresholds.VeryHigh = *raw.VeryHighComplexity
	}
	return config, nil
}

// ShouldProcess ports ComplexityScanner.should_process
// (analytics/complexity.py:104-112).
//
// Order matters and is not symmetric: EXCLUDES are checked first and win
// outright, then includes, and anything matching neither is rejected. So the
// default is DENY, not allow.
//
// Every comparison goes through pythonparity.FnMatch rather than path.Match,
// because the config depends on fnmatch's `*` crossing `/`. The file says so
// itself ("fnmatch '*' crosses '/' so \"*.py\" matches root-level and nested
// paths"), which is why include_globs are written `*.py` rather than
// `**/*.py`. Under path.Match those globs would match only root-level files
// and the scan would collapse to a handful of paths while still succeeding.
//
// The exclude list pairs `**/x/**` with `x/**` for the same reason in reverse:
// `**/migrations/**` does NOT match a top-level `migrations/foo.py`, because
// the leading `**/` requires a separator before it. Dropping either half of a
// pair silently stops excluding one of the two shapes.
func (c ScanConfig) ShouldProcess(filePath string) bool {
	for _, pattern := range c.ExcludeGlobs {
		if pythonparity.FnMatch(filePath, pattern) {
			return false
		}
	}
	for _, pattern := range c.IncludeGlobs {
		if pythonparity.FnMatch(filePath, pattern) {
			return true
		}
	}
	return false
}
