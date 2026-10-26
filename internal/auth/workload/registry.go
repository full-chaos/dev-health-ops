package workload

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	registrySchemaVersion = "workload-registry.v1"
	classWorkloadToken    = "workload_token"
)

var ErrRegistryInvalid = errors.New("workload: invalid registry contract")

type registryFile struct {
	SchemaVersion string `json:"schema_version"`
	Description   string `json:"description"`
	Workloads     []struct {
		Name           string   `json:"name"`
		Audiences      []string `json:"audiences"`
		AllowedActions []string `json:"allowed_actions"`
	} `json:"workloads"`
}

type audiencesFile struct {
	Description string `json:"description"`
	Audiences   []struct {
		Audience string   `json:"audience"`
		Classes  []string `json:"classes"`
		Services []string `json:"services"`
	} `json:"audiences"`
}

func strictDecode(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after the document")
	}
	return nil
}

func bad(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRegistryInvalid, fmt.Sprintf(format, args...))
}

// LoadRegistry parses and validates the registry contract against the audience
// vocabulary and the action catalog. A row may carry only an audience whose
// classes include workload_token, only catalog actions, and no duplicate name.
func LoadRegistry(registryJSON, audiencesJSON []byte, cat *policycatalog.Catalog) ([]Spec, error) {
	var rf registryFile
	if err := strictDecode(registryJSON, &rf); err != nil {
		return nil, bad("registry: %v", err)
	}
	var af audiencesFile
	if err := json.Unmarshal(audiencesJSON, &af); err != nil {
		return nil, bad("audiences: %v", err)
	}
	if rf.SchemaVersion != registrySchemaVersion {
		return nil, bad("schema_version must be %s", registrySchemaVersion)
	}
	carriers := map[string]bool{}
	for _, a := range af.Audiences {
		for _, c := range a.Classes {
			if c == classWorkloadToken {
				carriers[a.Audience] = true
			}
		}
	}
	if len(rf.Workloads) == 0 {
		return nil, bad("no workloads")
	}
	seen := map[string]bool{}
	out := make([]Spec, 0, len(rf.Workloads))
	for _, w := range rf.Workloads {
		if ValidateName(w.Name) != nil || seen[w.Name] {
			return nil, bad("workload name %q is invalid or repeated", w.Name)
		}
		seen[w.Name] = true
		if w.Audiences == nil || w.AllowedActions == nil {
			return nil, bad("workload %q must list audiences and allowed_actions, empty or not", w.Name)
		}
		if ValidateAudiences(w.Audiences) != nil {
			return nil, bad("workload %q has a malformed or repeated audience", w.Name)
		}
		for _, a := range w.Audiences {
			if !carriers[a] {
				return nil, bad("workload %q names audience %q, which no workload_token audience row carries", w.Name, a)
			}
		}
		if ValidateActions(cat, w.AllowedActions) != nil {
			return nil, bad("workload %q has an allowed action outside the catalog", w.Name)
		}
		out = append(out, Spec{Name: w.Name, Audiences: w.Audiences, AllowedActions: w.AllowedActions})
	}
	return out, nil
}

// DefaultRegistry is the embedded contract, validated against the embedded
// audience vocabulary and the default catalog.
func DefaultRegistry() ([]Spec, error) {
	return LoadRegistry(vocabulary.WorkloadRegistryV1, vocabulary.Audiences, policycatalog.Default())
}
