package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// ClassMarker is one way a credential class announces itself on the request
// head, in the vocabulary of endpoint-auth-profile.v1's class_disambiguator.
type ClassMarker struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// RegistryClass is the part of a credential-classes.json entry the profile
// selector gate reads.
type RegistryClass struct {
	ClassID           string        `json:"class_id"`
	Markers           []ClassMarker `json:"markers"`
	MarkersNoneReason string        `json:"markers_none_reason"`
}

// LoadRegistryClasses reads credential-classes.json keyed by class_id.
func LoadRegistryClasses(root string) (map[string]RegistryClass, error) {
	path := filepath.Join(ContractsDir(root), "credential-classes.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var document struct {
		Classes []RegistryClass `json:"classes"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	classes := make(map[string]RegistryClass, len(document.Classes))
	for _, class := range document.Classes {
		classes[class.ClassID] = class
	}
	return classes, nil
}

// CheckClassSelector is the profile registry gate for a route that accepts
// more than one credential class (TRD section 10). It joins the profile's
// class_disambiguator with each accepted class's markers in the registry.
func CheckClassSelector(profile map[string]any, registry map[string]RegistryClass) error {
	accepted, _ := profile["accepted_credential_classes"].([]any)
	if len(accepted) < 2 {
		return nil
	}
	selector, ok := profile["class_disambiguator"].(map[string]any)
	if !ok {
		return fmt.Errorf("route accepts %d classes and declares no class_disambiguator", len(accepted))
	}
	kind, _ := selector["kind"].(string)
	name, _ := selector["name"].(string)
	var owners []string
	for _, entry := range accepted {
		id, _ := entry.(string)
		class, found := registry[id]
		if !found {
			return fmt.Errorf("accepted class %q is not in credential-classes.json", id)
		}
		if len(class.Markers) == 0 {
			return fmt.Errorf("accepted class %q has no marker (%s), so no selector can pick it", id, class.MarkersNoneReason)
		}
		if slices.ContainsFunc(class.Markers, func(marker ClassMarker) bool { return markerMatches(marker, kind, name) }) {
			owners = append(owners, id)
		}
	}
	switch len(owners) {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("selector %s/%s is declared by none of the accepted classes %v", kind, name, accepted)
	default:
		return fmt.Errorf("selector %s/%s is shared by accepted classes %v, so it does not distinguish them", kind, name, owners)
	}
}

// Header and scheme names are case-insensitive on the wire; a credential
// prefix and a JWT header alg are compared exactly.
func markerMatches(marker ClassMarker, kind, name string) bool {
	if marker.Kind != kind {
		return false
	}
	if kind == "credential_prefix" || kind == "jwt_header_alg" {
		return marker.Name == name
	}
	return strings.EqualFold(marker.Name, name)
}

// LoadPinnedClasses reads, from the profile schema itself, the credential
// classes whose token pins a route must state.
func LoadPinnedClasses(root string) ([]string, error) {
	raw, err := os.ReadFile(SchemaPath(root, EndpointAuthProfileSurface))
	if err != nil {
		return nil, err
	}
	var document struct {
		AllOf []struct {
			If struct {
				Properties struct {
					Classes struct {
						Contains struct {
							Enum []string `json:"enum"`
						} `json:"contains"`
					} `json:"accepted_credential_classes"`
				} `json:"properties"`
			} `json:"if"`
		} `json:"allOf"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("parsing profile schema: %w", err)
	}
	for _, rule := range document.AllOf {
		if enum := rule.If.Properties.Classes.Contains.Enum; len(enum) > 0 {
			return enum, nil
		}
	}
	return nil, fmt.Errorf("profile schema lists no pinned credential classes")
}

// CheckTokenPins is the half of the `tokens` rule JSON Schema cannot state: the
// keys of `tokens` must be exactly the pinned classes the route accepts.
func CheckTokenPins(profile map[string]any, pinned []string) error {
	tokens, ok := profile["tokens"].(map[string]any)
	if !ok {
		return nil
	}
	accepted, _ := profile["accepted_credential_classes"].([]any)
	var want []string
	for _, entry := range accepted {
		if id, _ := entry.(string); slices.Contains(pinned, id) && !slices.Contains(want, id) {
			want = append(want, id)
		}
	}
	var got []string
	for id := range tokens {
		got = append(got, id)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !slices.Equal(want, got) {
		return fmt.Errorf("tokens keys %v differ from the pinned accepted classes %v", got, want)
	}
	return nil
}

// ValidateEndpointAuthProfile is the one entry for judging a profile: the wire
// schema first, then the selector gate over the credential-class registry.
func ValidateEndpointAuthProfile(root string, document any) error {
	if err := Validate(root, EndpointAuthProfileSurface, document); err != nil {
		return err
	}
	profile, ok := document.(map[string]any)
	if !ok {
		return fmt.Errorf("profile is %T, not an object", document)
	}
	registry, err := LoadRegistryClasses(root)
	if err != nil {
		return err
	}
	if err := CheckClassSelector(profile, registry); err != nil {
		return err
	}
	pinned, err := LoadPinnedClasses(root)
	if err != nil {
		return err
	}
	return CheckTokenPins(profile, pinned)
}
