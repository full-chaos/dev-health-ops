package decision

import "github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"

// Identity constants of a decision classification. Each one is a part of the
// stamp, so a change of any of them makes a new result-reuse key.
const (
	// ProviderName is the provider kind of the backend.
	ProviderName = "typesafe"
	// APIMode is the protocol; a provider can serve one model name on two
	// protocols with different prices.
	APIMode = "systemone"
	// DefaultModel is the pinned, versioned model id. Never an alias.
	DefaultModel = "jev-1.13.0"
	// RubricVersion is the rubric_version of the embedded rubric file.
	RubricVersion = "decision-support-v1f"
	// RubricSHA256 is the digest of the embedded rubric file bytes. The loader
	// compares it at construction; a mismatch is a construction error.
	RubricSHA256 = "eac20c674565a7b017450e3b6a9dba7926a9f085f60ac28e35b2b7c2b990762d"
	// AdapterVersion names the span rule, the validity rules and the state
	// rules of this package. v3 is the first production version (the experiment
	// ran decision-adapter-v2; the rubric file still names v2 because its bytes
	// are the evaluated bytes).
	AdapterVersion = "decision-adapter-v3"
	// MapVersion is the level -> weight map (0 / 1 / 2 / 4).
	MapVersion = "support-map-v1"
	// SpanVersion is the evidence span candidate rule.
	SpanVersion = "span-candidates-v1"
	// LevelRule is the configured level rule.
	LevelRule = "presence-floor:0.4"
	// presenceFloor is the threshold of LevelRule: a key is supported only when
	// P(level 0) is below it.
	presenceFloor = 0.4
)

// Identity is what makes two decision classifications of one input
// comparable: same identity, same rules.
type Identity struct {
	Provider       string
	API            string
	Model          string // the REQUESTED model id
	Taxonomy       string
	RubricVersion  string
	RubricSHA256   string
	AdapterVersion string
	MapVersion     string
	LevelRule      string
}

// IdentityFor returns the identity of this package's configuration with the
// given requested model ("" selects DefaultModel).
func IdentityFor(model string) Identity {
	if model == "" {
		model = DefaultModel
	}
	return Identity{
		Provider: ProviderName, API: APIMode, Model: model, Taxonomy: categorize.TaxonomyVersion,
		RubricVersion: RubricVersion, RubricSHA256: RubricSHA256, AdapterVersion: AdapterVersion,
		MapVersion: MapVersion, LevelRule: LevelRule,
	}
}

// Stamp is the one stamp string of a decision classification:
//
//	provider=typesafe;api=systemone;model=jev-1.13.0;taxonomy=investment-taxonomy-v1;prompt=decision-support-v1f@eac20c674565;adapter=decision-adapter-v3;map=support-map-v1;level=presence-floor:0.4
//
// The rubric is the prompt, so the prompt part holds the first 12 hex digits
// of the rubric digest: a rubric edit changes the key even when the version
// label was forgotten. The stamp differs from categorize.EffectiveModelVersion
// of every generative provider in its provider part and in its shape, so a
// decision row can never match an incumbent key.
func (i Identity) Stamp() string {
	digest := i.RubricSHA256
	if len(digest) > 12 {
		digest = digest[:12]
	}
	return "provider=" + i.Provider + ";api=" + i.API + ";model=" + i.Model +
		";taxonomy=" + i.Taxonomy + ";prompt=" + i.RubricVersion + "@" + digest +
		";adapter=" + i.AdapterVersion + ";map=" + i.MapVersion + ";level=" + i.LevelRule
}
