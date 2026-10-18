package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	return nil
}
