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

// IncumbentDefsVersion versions the experiment prompt of the diagnostic arm.
// The production prompt constant is never touched: the arm wraps the provider
// and inserts a definitions block into the prompt the production code built.
const IncumbentDefsVersion = "incumbent-defs-v1"

// sourceTextMarker is the line of the production prompt that introduces the
// source text (prompts.go BuildPrompt). The definitions go before it.
const sourceTextMarker = "\n\nSource text (quotes must be exact substrings):\n"

// IncumbentConfig configures the incumbent arms. Model and parameters come from
// config; the production provider code is used unchanged.
type IncumbentConfig struct {
	BaseURL         string
	APIKey          secrets.Hidden
	Model           string
	MaxOutputTokens int
	// Defs selects the diagnostic arm incumbent+defs.
	Defs   bool
	Rubric *Rubric
}

// DefaultIncumbentModel is the local resolved incumbent (ops/.env). The
// production reference is gpt-5-nano-2025-08-07 (pin it with the config).
const DefaultIncumbentModel = "gpt-5-nano"

// IncumbentDefsText renders the category definitions of the rubric as one
// block. It is a pure function of the rubric file (definitions, inclusions,
// exclusions); it holds no scale, no shared rules and no question text.
func IncumbentDefsText(r *Rubric) string {
	var b strings.Builder
	b.WriteString("Category definitions (use them to judge how strongly the evidence supports each subcategory):\n")
	for _, k := range SortedKeys() {
		c := r.Category(k)
		fmt.Fprintf(&b, "- %s (%s): %s", c.Key, c.Name, c.Definition)
		if len(c.Inclusions) > 0 {
			b.WriteString(" Counts: " + strings.Join(c.Inclusions, "; ") + ".")
		}
		if len(c.Exclusions) > 0 {
			var ex []string
			for _, e := range c.Exclusions {
				if e.UseInstead != "" {
					ex = append(ex, fmt.Sprintf("%s (use %s)", e.Text, e.UseInstead))
				} else {
					ex = append(ex, e.Text)
				}
			}
			b.WriteString(" Not this category: " + strings.Join(ex, "; ") + ".")
		}
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// InsertDefs puts the block before the source text of a production prompt (or
// at the end when the marker is absent).
func InsertDefs(prompt, block string) string {
	if i := strings.Index(prompt, sourceTextMarker); i >= 0 {
		return prompt[:i] + "\n\n" + block + prompt[i:]
	}
	return prompt + "\n\n" + block
}

type defsProvider struct {
	inner categorize.Provider
	block string
}

func (d defsProvider) Complete(ctx context.Context, req categorize.CompletionRequest) (categorize.CompletionResult, error) {
	req.Prompt = InsertDefs(req.Prompt, d.block)
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
		p = defsProvider{inner: p, block: IncumbentDefsText(cfg.Rubric)}
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

// IncumbentPromptVersion is the prompt stamp of an incumbent arm.
func IncumbentPromptVersion(defs bool, rubricVersion string) string {
	if defs {
		return categorize.PromptVersion + "+" + IncumbentDefsVersion + "/" + rubricVersion
	}
	return categorize.PromptVersion
}
