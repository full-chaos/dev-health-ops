package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
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

// mediaType is a Content-Type value's media type: parameters (; charset=...) dropped, trimmed, lower-cased.
func mediaType(value string) string {
	base, _, _ := strings.Cut(value, ";")
	return strings.ToLower(strings.TrimSpace(base))
}

// Header and scheme names are case-insensitive on the wire; a credential
// prefix and a JWT header alg or typ are compared exactly against the one
// spelling the contract files carry.
func markerMatches(marker ClassMarker, kind, name string) bool {
	if marker.Kind != kind {
		return false
	}
	if kind == "content_type" {
		return mediaType(marker.Name) == mediaType(name)
	}
	if kind == "credential_prefix" || kind == "jwt_header_alg" || kind == "jwt_header_typ" {
		return marker.Name == name
	}
	return strings.EqualFold(marker.Name, name)
}

// pinMarkerKinds maps a token pin field to the registry marker kind it must equal.
var pinMarkerKinds = []struct{ field, kind string }{
	{"algorithm", "jwt_header_alg"},
	{"token_type", "jwt_header_typ"},
}

// CheckTokenPins holds every accepted class's token pins to that class's JWT
// header markers in the registry: the profile's algorithm must be the class's
// jwt_header_alg marker and its token_type the jwt_header_typ marker, compared
// exactly. The pins come from `token` when one pinned class is accepted and
// from the class's own `tokens` block otherwise. A class with no such marker,
// or not in the registry, has nothing to join.
func CheckTokenPins(profile map[string]any, registry map[string]RegistryClass) error {
	accepted, _ := profile["accepted_credential_classes"].([]any)
	single, _ := profile["token"].(map[string]any)
	perClass, _ := profile["tokens"].(map[string]any)
	for _, entry := range accepted {
		id, _ := entry.(string)
		pins, _ := perClass[id].(map[string]any)
		if pins == nil {
			pins = single
		}
		if pins == nil {
			continue
		}
		class := registry[id]
		for _, pin := range pinMarkerKinds {
			for _, marker := range class.Markers {
				if marker.Kind != pin.kind {
					continue
				}
				if value, _ := pins[pin.field].(string); value != marker.Name {
					return fmt.Errorf("class %q pins %s %q; its %s marker is %q", id, pin.field, value, pin.kind, marker.Name)
				}
			}
		}
	}
	return nil
}

// ValidateEndpointAuthProfile is the one entry for judging a profile: the wire
// schema first, then the selector gate and the token pin join over the
// credential-class registry.
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
	if err := CheckTokenPins(profile, registry); err != nil {
		return err
	}
	return CheckActionCachePolicy(profile)
}

// CheckActionCachePolicy joins a profile's cache_class to the action catalog:
// a profile that caches must name an action, and a high-risk catalog action is
// never cached (CHAOS-10641). The error names the rule and the action id only.
func CheckActionCachePolicy(profile map[string]any) error {
	cache, _ := profile["cache_class"].(string)
	if cache == "" || cache == "none" {
		return nil
	}
	action, _ := profile["action"].(string)
	if action == "" {
		return fmt.Errorf("action cache policy: a cached profile must name an action")
	}
	if highRiskActions()[action] {
		return fmt.Errorf("action cache policy: action %q is high risk and its profile cache_class must be none", action)
	}
	return nil
}

// highRiskActions reads the embedded actions.v1 catalog directly:
// internal/auth/policycatalog's own tests import this package, so importing it
// here would be a cycle.
var highRiskActions = sync.OnceValue(func() map[string]bool {
	var catalog struct {
		Actions []struct {
			Action    string `json:"action"`
			RiskClass string `json:"risk_class"`
		} `json:"actions"`
	}
	if err := json.Unmarshal(vocabulary.ActionsV1, &catalog); err != nil {
		panic(fmt.Sprintf("embedded actions.v1 is not JSON: %v", err))
	}
	high := map[string]bool{}
	for _, a := range catalog.Actions {
		if a.RiskClass == "high" {
			high[a.Action] = true
		}
	}
	return high
})
