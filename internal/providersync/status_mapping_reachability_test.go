package providersync

import (
	"os"
	"sort"
	"testing"

	"gopkg.in/yaml.v3"
)

// Two-directional tripwires for the deliberately-mirrored quirks.
//
// WHY THESE EXIST ALONGSIDE THE ORACLES. The differential pairs compare Go
// against Python, so they catch one engine drifting from the other -- but they
// go GREEN if BOTH sides are "fixed" together. A quirk this port mirrors on
// purpose would then disappear with nothing failing. These tests assert the
// quirk itself, against the REAL config and this engine, so they fail in BOTH
// directions: if the quirk becomes reachable (someone fixes it), and if the
// config section the pin depends on is renamed or deleted (the pin silently
// stops meaning anything).
//
// Each dead arm is paired with a COUNTERPART showing the same arm works when
// fed, so "this never fires" reads as a property of the CONFIG rather than as
// this engine being broken.
//
// They also need no Python, so the quirks stay pinned in the fast CI lane.

func loadRealStatusMapping(t *testing.T) *StatusMapping {
	t.Helper()
	// Neutralize quirk 7: an ambient STATUS_MAPPING_PATH would replace the
	// explicit argument and silently point every assertion below at another
	// file.
	t.Setenv(statusMappingPathEnvVar, "")
	mapping, err := LoadStatusMapping(resolveStatusMappingConfig(t, "real"))
	if err != nil {
		t.Fatalf("loading the real config: %v", err)
	}
	return mapping
}

func realConfigProviderSection(t *testing.T, provider string) *yaml.Node {
	t.Helper()
	contents, err := os.ReadFile(resolveStatusMappingConfig(t, "real"))
	if err != nil {
		t.Fatalf("reading the real config: %v", err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(contents, &document); err != nil {
		t.Fatalf("parsing the real config: %v", err)
	}
	if len(document.Content) == 0 {
		t.Fatal("the real config is empty")
	}
	section := mappingValue(mappingValue(document.Content[0], "providers"), provider)
	if section == nil {
		t.Fatalf("the real config has no %q provider section -- these tripwires pin "+
			"behaviour that depends on it, so its removal must fail loudly rather "+
			"than quietly make the pin vacuous", provider)
	}
	return section
}

// TestRealConfigDeclaresLinearButTheLoaderIgnoresIt pins CHAOS-3505.
func TestRealConfigDeclaresLinearButTheLoaderIgnoresIt(t *testing.T) {
	// Direction 1: the config really does declare linear, with real rules. If
	// the section is deleted or renamed, this pin stops describing anything and
	// must fail rather than pass vacuously.
	linear := realConfigProviderSection(t, "linear")
	labels := mappingValue(linear, "type_labels")
	if len(mappingEntries(labels)) == 0 {
		t.Fatal("the real config's linear section no longer declares type_labels; " +
			"CHAOS-3505 is about those rules being silently ignored, so this pin " +
			"is now meaningless and needs revisiting")
	}

	// Direction 2: none of it is reachable, because the provider tuple omits
	// linear. If someone adds it -- which is the eventual FIX -- this fails, so
	// the fix cannot land silently inside a bug-for-bug port.
	mapping := loadRealStatusMapping(t)
	for name, index := range map[string]map[string]map[string]string{
		"status_by_provider":       mapping.StatusByProvider,
		"label_status_by_provider": mapping.LabelStatusByProvider,
		"type_by_provider":         mapping.TypeByProvider,
		"label_type_by_provider":   mapping.LabelTypeByProvider,
	} {
		if _, present := index[linearProviderName]; present {
			t.Errorf("%s now has a %q entry: CHAOS-3505 appears to be fixed. That is a "+
				"BEHAVIOUR CHANGE, not a cleanup -- Linear currently falls through to "+
				"linear/normalize.py's hand-rolled _type_from_labels, which disagrees "+
				"with the config path on priority (bug before incident), on default "+
				"(task vs unknown) and on normalization (bare lower vs _norm_key). "+
				"Update the port and the ticket together.", name, linearProviderName)
		}
	}

	// COUNTERPART: the same builder DOES index type_labels for a provider that
	// is in the tuple, so the emptiness above is the tuple's doing, not a broken
	// builder.
	if len(mapping.LabelTypeByProvider["github"]) == 0 {
		t.Error("github has no type_labels index either -- the label-type builder is " +
			"broken, and the linear assertions above prove nothing")
	}
}

const linearProviderName = "linear"

// TestTypePriorityOmitsExactlyPrAndMergeRequest pins quirk 8: the label arm can
// match a valid type that no priority list mentions, and then falls through.
func TestTypePriorityOmitsExactlyPrAndMergeRequest(t *testing.T) {
	prioritized := map[string]bool{}
	for _, name := range typePriority {
		prioritized[name] = true
	}
	var unprioritized []string
	for name := range validWorkItemTypes {
		if !prioritized[name] {
			unprioritized = append(unprioritized, name)
		}
	}
	sort.Strings(unprioritized)

	// Two-directional: adding pr/merge_request to typePriority fails this (the
	// quirk was fixed), and adding a NEW WorkItemType without prioritizing it
	// also fails it (a new instance of the same quirk that nobody noticed).
	want := []string{"merge_request", "pr"}
	if len(unprioritized) != len(want) {
		t.Fatalf("valid work-item types absent from _TYPE_PRIORITY = %v, want %v -- "+
			"either quirk 8 changed or a new type joined the vocabulary without a "+
			"priority, which silently reopens the same fall-through", unprioritized, want)
	}
	for i, name := range want {
		if unprioritized[i] != name {
			t.Fatalf("unprioritized types = %v, want %v", unprioritized, want)
		}
	}

	// The asymmetry is the evidence that this is an oversight rather than a
	// design: the status side has no such gap.
	for name := range validStatusCategories {
		found := false
		for _, candidate := range statusPriority {
			if candidate == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("status category %q has no entry in _STATUS_PRIORITY; the status "+
				"label arm can now fall through the same way the type arm does, and "+
				"NormalizeStatus's fall-through comment is no longer accurate", name)
		}
	}
}

// TestRealConfigGithubTypeBugLabelIsAMisparsedMapping pins the highest-impact
// quirk: status_mapping.yaml's github `bug` list contains `- type: bug` WITH A
// SPACE, which YAML reads as a mapping. Python indexes it under str(dict), so
// the conventional GitHub label `type:bug` matches nothing.
func TestRealConfigGithubTypeBugLabelIsAMisparsedMapping(t *testing.T) {
	github := realConfigProviderSection(t, "github")
	bugValues := sequenceItems(mappingValue(mappingValue(github, "type_labels"), "bug"))
	if len(bugValues) == 0 {
		t.Fatal("the real config's github type_labels.bug list is gone; this pin " +
			"depends on it")
	}
	sawMapping := false
	for _, item := range bugValues {
		if resolveAlias(item).Kind == yaml.MappingNode {
			sawMapping = true
		}
	}
	if !sawMapping {
		t.Fatal("github's type_labels.bug list no longer contains a YAML MAPPING. " +
			"If the `- type: bug` typo was corrected to `- type:bug`, that is a " +
			"REAL BEHAVIOUR CHANGE: GitHub items labelled type:bug start classifying " +
			"as bug instead of issue. Fix the port, the oracle cases and the ticket " +
			"together -- do not just delete this test.")
	}

	mapping := loadRealStatusMapping(t)
	githubTypeLabels := mapping.LabelTypeByProvider["github"]
	if got := githubTypeLabels[pythonDictReprBugKey]; got != "bug" {
		t.Errorf("github type-label index has no %q entry (got %q); reproducing "+
			"Python's dict repr byte-for-byte is what keeps this port faithful",
			pythonDictReprBugKey, got)
	}
	if _, present := githubTypeLabels["type:bug"]; present {
		t.Error("github now maps the conventional label \"type:bug\"; that is the " +
			"upstream FIX, and this port must be updated deliberately rather than " +
			"drifting into it")
	}

	// COUNTERPART: GitLab wrote the same intent as `type::bug`, which YAML keeps
	// as a string -- proving the label arm works fine when the config is
	// well-formed, so github's miss is the FILE's defect, not the engine's.
	if got := mapping.LabelTypeByProvider["gitlab"]["type::bug"]; got != "bug" {
		t.Errorf("gitlab's type::bug label maps to %q, want \"bug\" -- the counterpart "+
			"that proves the github miss above is a config defect is gone", got)
	}
}

const pythonDictReprBugKey = "{'type': 'bug'}"

// TestRealConfigTypeSectionsThatDoNotExist pins the two structural dead arms:
// github/gitlab declare no `types`, and jira declares no `type_labels`.
func TestRealConfigTypeSectionsThatDoNotExist(t *testing.T) {
	mapping := loadRealStatusMapping(t)

	for _, provider := range []string{"github", "gitlab"} {
		if mappingValue(realConfigProviderSection(t, provider), "types") != nil {
			t.Errorf("%s now declares a `types` section; its type_raw arm was dead and "+
				"NormalizeType's default was the only reachable answer for a raw type",
				provider)
		}
		if len(mapping.TypeByProvider[provider]) != 0 {
			t.Errorf("type_by_provider[%q] is no longer empty", provider)
		}
	}

	if mappingValue(realConfigProviderSection(t, "jira"), "type_labels") != nil {
		t.Error("jira now declares `type_labels`; NormalizeType's label arm could not " +
			"fire for jira before, and any test relying on that is now weaker")
	}
	if len(mapping.LabelTypeByProvider["jira"]) != 0 {
		t.Error("label_type_by_provider[\"jira\"] is no longer empty")
	}

	// COUNTERPART for both: jira DOES declare `types`, and it is indexed. The
	// builders work; the emptiness above is what the file says.
	if len(mapping.TypeByProvider["jira"]) == 0 {
		t.Error("jira's `types` index is empty too -- the type builder is broken and " +
			"the emptiness assertions above prove nothing about the config")
	}
	if len(mapping.LabelStatusByProvider["github"]) == 0 {
		t.Error("github's status_labels index is empty -- the label-status builder is " +
			"broken and the jira type_labels assertion above proves nothing")
	}
}
