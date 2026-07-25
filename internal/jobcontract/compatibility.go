package jobcontract

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// BreakingChange is a deterministic, path-addressed incompatibility.
type BreakingChange struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// CompareTrees reports breaking edits made in place to an existing envelope
// or kind/version schema. Adding a new optional property is compatible.
func CompareTrees(baseRoot, candidateRoot string) ([]BreakingChange, error) {
	baseRegistry, err := loadRegistry(baseRoot, false)
	if err != nil {
		return nil, fmt.Errorf("load base registry: %w", err)
	}
	candidateRegistry, err := loadRegistry(candidateRoot, false)
	if err != nil {
		return nil, fmt.Errorf("load candidate registry: %w", err)
	}

	var changes []BreakingChange
	baseEnvelope, err := loadSchema(baseRoot, baseRegistry.EnvelopeSchema)
	if err != nil {
		return nil, err
	}
	candidateEnvelope, err := loadSchema(candidateRoot, candidateRegistry.EnvelopeSchema)
	if err != nil {
		return nil, err
	}
	compareSchema("envelope", baseEnvelope, candidateEnvelope, &changes)

	candidates := make(map[string]JobDefinition, len(candidateRegistry.Jobs))
	for _, job := range candidateRegistry.Jobs {
		candidates[job.Kind] = job
	}
	for _, baseJob := range baseRegistry.Jobs {
		candidateJob, ok := candidates[baseJob.Kind]
		if !ok {
			changes = append(changes, BreakingChange{Path: baseJob.Kind, Reason: "registered kind was removed"})
			continue
		}
		if candidateJob.CurrentVersion < baseJob.CurrentVersion {
			changes = append(changes, BreakingChange{Path: baseJob.Kind, Reason: "current_version decreased"})
		}
		if candidateJob.DomainLink != baseJob.DomainLink {
			changes = append(changes, BreakingChange{Path: baseJob.Kind + ".domain_link", Reason: "domain link changed"})
		}
		if candidateJob.OrganizationScope != baseJob.OrganizationScope {
			changes = append(changes, BreakingChange{Path: baseJob.Kind + ".organization_scope", Reason: "organization scope changed"})
		}
		for versionKey, baseSchemaPath := range baseJob.SchemaVersions {
			candidateSchemaPath, exists := candidateJob.SchemaVersions[versionKey]
			if !exists {
				changes = append(changes, BreakingChange{Path: baseJob.Kind + "@" + versionKey, Reason: "versioned schema was removed"})
				continue
			}
			baseSchema, err := loadSchema(baseRoot, baseSchemaPath)
			if err != nil {
				return nil, err
			}
			candidateSchema, err := loadSchema(candidateRoot, candidateSchemaPath)
			if err != nil {
				return nil, err
			}
			compareSchema(baseJob.Kind+"@"+versionKey, baseSchema, candidateSchema, &changes)
		}
	}
	sort.Slice(changes, func(left, right int) bool {
		if changes[left].Path == changes[right].Path {
			return changes[left].Reason < changes[right].Reason
		}
		return changes[left].Path < changes[right].Path
	})
	return changes, nil
}

func loadSchema(root, relative string) (map[string]any, error) {
	data, err := readContractFile(root, relative)
	if err != nil {
		return nil, err
	}
	var schema map[string]any
	if err := decodeGeneric(data, &schema); err != nil {
		return nil, fmt.Errorf("decode schema %s: %w", relative, err)
	}
	return schema, nil
}

func compareSchema(path string, base, candidate map[string]any, changes *[]BreakingChange) {
	baseRequired := stringSet(base["required"])
	candidateRequired := stringSet(candidate["required"])
	if !reflect.DeepEqual(baseRequired, candidateRequired) {
		*changes = append(*changes, BreakingChange{Path: path + ".required", Reason: "required fields changed"})
	}

	baseProperties, _ := base["properties"].(map[string]any)
	candidateProperties, _ := candidate["properties"].(map[string]any)
	for _, property := range sortedKeys(baseProperties) {
		candidateValue, exists := candidateProperties[property]
		if !exists {
			*changes = append(*changes, BreakingChange{Path: path + ".properties." + property, Reason: "property was removed"})
			continue
		}
		baseValue, baseObject := baseProperties[property].(map[string]any)
		candidateValueObject, candidateObject := candidateValue.(map[string]any)
		if baseObject && candidateObject {
			compareSchema(path+".properties."+property, baseValue, candidateValueObject, changes)
		} else if !reflect.DeepEqual(baseProperties[property], candidateValue) {
			*changes = append(*changes, BreakingChange{Path: path + ".properties." + property, Reason: "property schema changed"})
		}
	}
	for _, property := range sortedKeys(candidateProperties) {
		if _, existed := baseProperties[property]; existed {
			continue
		}
		if _, required := candidateRequired[property]; required {
			*changes = append(*changes, BreakingChange{Path: path + ".properties." + property, Reason: "new property is required"})
		}
	}

	ignored := map[string]struct{}{
		"$id": {}, "$schema": {}, "description": {}, "examples": {},
		"properties": {}, "required": {}, "title": {},
	}
	allKeys := make(map[string]struct{}, len(base)+len(candidate))
	for key := range base {
		allKeys[key] = struct{}{}
	}
	for key := range candidate {
		allKeys[key] = struct{}{}
	}
	keys := make([]string, 0, len(allKeys))
	for key := range allKeys {
		if _, skip := ignored[key]; !skip {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !reflect.DeepEqual(base[key], candidate[key]) {
			*changes = append(*changes, BreakingChange{Path: path + "." + key, Reason: "schema constraint changed"})
		}
	}
}

func stringSet(value any) map[string]struct{} {
	result := make(map[string]struct{})
	values, ok := value.([]any)
	if !ok {
		return result
	}
	for _, raw := range values {
		if item, ok := raw.(string); ok {
			result[item] = struct{}{}
		}
	}
	return result
}

// FormatBreakingChanges emits stable one-line diagnostics suitable for CI.
func FormatBreakingChanges(changes []BreakingChange) string {
	lines := make([]string, 0, len(changes))
	for _, change := range changes {
		lines = append(lines, change.Path+": "+change.Reason)
	}
	return strings.Join(lines, "\n")
}
