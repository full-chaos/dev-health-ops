// Package ingressplanes reads the route-plane contract and writes the nginx
// configuration of the self-hosted router from it (CHAOS-8363).
//
// The contract (contracts/ingress/v1/planes.json) is the rule table that prod's
// Ingress carries for a host that serves the api: one row per Ingress path
// (path, path type, plane). The self-hosted compose stack has no Ingress, so
// Render writes one nginx location per row, in the form and in the order
// ingress-nginx writes them for a host in regex mode. The file is generated
// here and checked in; nothing generates it at run time.
//
// The design, the comparison the deploy repository must make, and every point
// that was read from source and not measured are in
// contracts/ingress/v1/README.md.
package ingressplanes

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

const (
	// ContractPath is the contract, relative to the module root.
	ContractPath = "contracts/ingress/v1/planes.json"
	// RouterConfigPath is the generated nginx configuration, relative to the
	// module root. The compose router mounts this file.
	RouterConfigPath = "contracts/ingress/v1/compose-router.nginx.conf"

	// SchemaVersion is the only contract version this package reads.
	SchemaVersion = 1

	// PlaneGoAPI and PlaneQueryAPI are the planes a rule can name: the Go api
	// (REST) and the Go query-api (/graphql and the query REST paths).
	PlaneGoAPI    = "go-api"
	PlaneQueryAPI = "query-api"

	// The path types of a Kubernetes Ingress path.
	PathTypePrefix                 = "Prefix"
	PathTypeExact                  = "Exact"
	PathTypeImplementationSpecific = "ImplementationSpecific"

	// defaultPath is the path of the one rule that gives every other request
	// its plane (prod: "/" Prefix, the default backend).
	defaultPath = "/"
	// wildcard is the one regex token a rule path may hold: one or more
	// characters of one path segment.
	wildcard = "[^/]+"
)

// Planes is every plane a rule can name, sorted.
var Planes = []string{PlaneGoAPI, PlaneQueryAPI}

// Contract is the rule table of one api host.
type Contract struct {
	SchemaVersion int `json:"schema_version"`
	// RegexMode is true when an Ingress of the host carries
	// nginx.ingress.kubernetes.io/use-regex: "true". ingress-nginx then writes
	// EVERY path of the host as a case-insensitive regex location.
	RegexMode bool   `json:"regex_mode"`
	Rules     []Rule `json:"rules"`
}

// Rule is one Ingress path of the host: the path as the Ingress object holds
// it (after the chart rendered it), its path type, and the plane of its
// backend Service.
type Rule struct {
	Path     string `json:"path"`
	PathType string `json:"path_type"`
	Plane    string `json:"plane"`
}

// anchoredPath is the one shape of a rule that is not the default: segments of
// literal text (letters, digits, "_", "-", and `\.` for a dot) or the wildcard,
// then exactly one "$". It is the union of what the charts render for a host in
// regex mode, and its match result is the same in PCRE (nginx) and in RE2.
var anchoredPath = regexp.MustCompile(`^(/(\[\^/\]\+|([A-Za-z0-9_-]|\\\.)+))+\$$`)

// Load reads and validates the contract at path.
func Load(path string) (Contract, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Contract{}, fmt.Errorf("read the route-plane contract: %w", err)
	}
	return Parse(data)
}

// Parse decodes and validates a contract. An unknown key and trailing data are
// errors: the deploy repository compares its rendered rules with these rows,
// so a row this package does not fully read must not load.
func Parse(data []byte) (Contract, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var contract Contract
	if err := decoder.Decode(&contract); err != nil {
		return Contract{}, fmt.Errorf("decode the route-plane contract: %w", err)
	}
	if decoder.More() {
		return Contract{}, errors.New("decode the route-plane contract: data after the document")
	}
	if err := contract.Validate(); err != nil {
		return Contract{}, err
	}
	return contract, nil
}

// Validate refuses every table whose nginx form this package cannot write with
// the match result ingress-nginx gives it. A refusal is by design: a shape that
// would have to be guessed is not read.
func (c Contract) Validate() error {
	if c.SchemaVersion != SchemaVersion {
		return fmt.Errorf("route-plane contract: schema_version is %d, this package reads %d", c.SchemaVersion, SchemaVersion)
	}
	if !c.RegexMode {
		return errors.New("route-plane contract: regex_mode is false: only a host in ingress-nginx regex mode is modelled (prod's api hosts are in it); a table without it matches paths by other rules")
	}
	defaults := 0
	seen := map[string]int{}
	type template struct {
		index    int
		segments []string
	}
	var templates []template
	for index, rule := range c.Rules {
		where := fmt.Sprintf("route-plane contract: rule %d (%s %s)", index, rule.PathType, rule.Path)
		if rule.Plane != PlaneGoAPI && rule.Plane != PlaneQueryAPI {
			return fmt.Errorf("%s: plane %q is not one of %s", where, rule.Plane, strings.Join(Planes, ", "))
		}
		key := strings.ToLower(rule.Path)
		if first, repeated := seen[key]; repeated {
			return fmt.Errorf("%s: the path repeats rule %d (paths are compared without case, as regex mode matches them)", where, first)
		}
		seen[key] = index
		if rule.Path == defaultPath {
			if rule.PathType != PathTypePrefix {
				return fmt.Errorf("%s: the default rule is path %q with path_type %s", where, defaultPath, PathTypePrefix)
			}
			defaults++
			continue
		}
		switch rule.PathType {
		case PathTypeImplementationSpecific:
		case PathTypePrefix, PathTypeExact:
			return fmt.Errorf("%s: in regex mode ingress-nginx writes an Exact or Prefix path as a regex with no end anchor, so it also matches every longer path; write the rule anchored (path_type %s, path ending in \"$\")", where, PathTypeImplementationSpecific)
		default:
			return fmt.Errorf("%s: path_type is not %s, %s or %s", where, PathTypePrefix, PathTypeExact, PathTypeImplementationSpecific)
		}
		if !anchoredPath.MatchString(rule.Path) {
			return fmt.Errorf("%s: the path is not an anchored path: segments of letters, digits, \"_\", \"-\" and `\\.`, or the whole-segment token %s, then one \"$\"", where, wildcard)
		}
		segments := pathSegments(rule.Path)
		for _, segment := range segments {
			if segment == "." || segment == ".." {
				return fmt.Errorf("%s: the path has a . or .. segment", where)
			}
		}
		templates = append(templates, template{index, segments})
	}
	if defaults != 1 {
		return fmt.Errorf("route-plane contract: %d default rules, want exactly one (path %q, path_type %s)", defaults, defaultPath, PathTypePrefix)
	}
	// One request path, one rule. Two rules that can match the same path would
	// make the answer depend on the order of the locations.
	for i, a := range templates {
		for _, b := range templates[i+1:] {
			if segmentsOverlap(a.segments, b.segments) {
				return fmt.Errorf("route-plane contract: rule %d (%s) and rule %d (%s) can match the same path; a path has exactly one rule", a.index, c.Rules[a.index].Path, b.index, c.Rules[b.index].Path)
			}
		}
	}
	return nil
}

// DefaultPlane is the plane of the default rule: the plane of every request
// that no other rule matches. The contract must be valid.
func (c Contract) DefaultPlane() string {
	for _, rule := range c.Rules {
		if rule.Path == defaultPath {
			return rule.Plane
		}
	}
	return ""
}

// wildcardSegment stands for the wildcard in the segments of a path. The token
// itself holds a "/", so it is replaced before the path is split; no literal
// segment can spell the mark.
const wildcardSegment = "\x00"

// pathSegments returns the segments of an anchored path as regex mode compares
// them: without the "$", `\.` as a dot, lower case, a token as wildcardSegment.
func pathSegments(path string) []string {
	trimmed := strings.TrimPrefix(strings.TrimSuffix(path, "$"), "/")
	segments := strings.Split(strings.ReplaceAll(trimmed, wildcard, wildcardSegment), "/")
	for i, segment := range segments {
		segments[i] = strings.ToLower(strings.ReplaceAll(segment, `\.`, "."))
	}
	return segments
}

// segmentsOverlap reports whether some request path matches both anchored
// paths: the same number of segments, and at each position equal text or a
// token on either side (a token never crosses a "/").
func segmentsOverlap(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] && a[i] != wildcardSegment && b[i] != wildcardSegment {
			return false
		}
	}
	return true
}
