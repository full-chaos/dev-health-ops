package decisioneval

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// ErrDefsMarker is returned by the arm D prompt wrapper when the production
// prompt does not hold the insertion marker exactly one time. The harness fails
// instead of guessing where to put the block.
var ErrDefsMarker = fmt.Errorf("decisioneval: arm D: the insertion marker must occur exactly one time in the production prompt")

// IncumbentConfig configures the incumbent arms. Model and parameters come from
// config; the production provider code is used unchanged.
type IncumbentConfig struct {
	BaseURL         string
	APIKey          secrets.Hidden
	Model           string
	MaxOutputTokens int
	// Defs selects arm D (incumbent+defs).
	Defs   bool
	Rubric *Rubric
}

// DefaultIncumbentModel is the production model (design 9.1): the requested id
// is gpt-5-nano and the provider returns gpt-5-nano-2025-08-07.
const DefaultIncumbentModel = "gpt-5-nano"

// DefsBlock renders the arm D block from the rubric's incumbent_defs data:
//
//	"\n\n" + header + "\n" + join("- <key>: <text>", "\n") + "\n" + footer
//
// The block is inserted before insert_before (see InsertDefs).
func DefsBlock(r *Rubric) string {
	d := r.IncumbentDefs
	var b strings.Builder
	b.WriteString("\n\n" + d.Header + "\n")
	lines := make([]string, 0, len(d.Lines))
	for _, l := range d.Lines {
		line := strings.NewReplacer("<key>", l.Key, "<text>", l.Text).Replace(d.LineFormat)
		lines = append(lines, line)
	}
	b.WriteString(strings.Join(lines, "\n"))
	if d.Footer != "" {
		b.WriteString("\n" + d.Footer)
	}
	return b.String()
}

// InsertDefs builds prompt_D: the first occurrence of insert_before in the
// production prompt is replaced by the block followed by insert_before. The
// marker must occur exactly one time.
func InsertDefs(r *Rubric, prompt string) (string, error) {
	marker := r.IncumbentDefs.InsertBefore
	if marker == "" || strings.Count(prompt, marker) != 1 {
		return "", ErrDefsMarker
	}
	return strings.Replace(prompt, marker, DefsBlock(r)+marker, 1), nil
}

type defsProvider struct {
	inner  categorize.Provider
	rubric *Rubric
}

func (d defsProvider) Complete(ctx context.Context, req categorize.CompletionRequest) (categorize.CompletionResult, error) {
	prompt, err := InsertDefs(d.rubric, req.Prompt)
	if err != nil {
		return categorize.CompletionResult{}, err
	}
	req.Prompt = prompt
	return d.inner.Complete(ctx, req)
}
func (d defsProvider) Close() error  { return d.inner.Close() }
func (d defsProvider) Model() string { return d.inner.Model() }

// IncumbentProvider builds the incumbent provider over an http client (the
// recording client in a live run, a replay client offline).
func IncumbentProvider(cfg IncumbentConfig, client *http.Client) categorize.Provider {
	model := cfg.Model
	if model == "" {
		model = DefaultIncumbentModel
	}
	var p categorize.Provider = categorize.NewOpenAIProvider(categorize.OpenAIProviderConfig{
		APIKey: cfg.APIKey, BaseURL: cfg.BaseURL, Model: model, MaxOutputTokens: cfg.MaxOutputTokens, HTTPClient: client,
	})
	if cfg.Defs {
		p = defsProvider{inner: p, rubric: cfg.Rubric}
	}
	return p
}

// IncumbentCategorize runs the real CategorizeTextBundle (one call + at most
// one repair) with the incumbent provider. Nothing is reimplemented.
func IncumbentCategorize(ctx context.Context, bundle units.TextBundle, cfg IncumbentConfig, client *http.Client, col *Collector) (categorize.CategorizationOutcome, error) {
	p := IncumbentProvider(cfg, client)
	defer p.Close()
	if col != nil {
		ctx = WithCollector(ctx, col)
	}
	return categorize.CategorizeTextBundle(ctx, bundle, categorize.CategorizeOptions{Provider: p, ProviderName: "openai", Model: p.Model()})
}

// IncumbentDefsVersion versions the arm D wiring in the stamp.
const IncumbentDefsVersion = "incumbent-defs"

// IncumbentPromptVersion is the prompt stamp of an incumbent arm. Arm D also
// records defs=<rubric_version>.
func IncumbentPromptVersion(defs bool, rubricVersion string) string {
	if defs {
		return categorize.PromptVersion + ";defs=" + rubricVersion
	}
	return categorize.PromptVersion
}
