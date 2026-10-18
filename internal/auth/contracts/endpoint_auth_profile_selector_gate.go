package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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
	return CheckClassSelector(profile, registry)
}
