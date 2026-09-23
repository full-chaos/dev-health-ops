package goapiproof

import (
	"fmt"
	"sort"
)

// RESTService names the Go service a corpus entry's candidate leg is sent
// to. A prover run targets exactly one service: the build every receipt names
// comes from that one service's /buildinfo, so a run that mixed services
// could not say which build a receipt belongs to.
type RESTService string

const (
	// RESTServiceQueryAPI is query-api, the default: every entry that leaves
	// RESTEndpointSpec.Service empty.
	RESTServiceQueryAPI RESTService = "query-api"
	// RESTServiceDHOAPI is the dho api service (internal/apiservice).
	RESTServiceDHOAPI RESTService = "dho-api"
)

// ParseRESTService resolves a -service flag value; the empty string is
// query-api, the prover's behaviour before the flag existed.
func ParseRESTService(value string) (RESTService, error) {
	switch RESTService(value) {
	case "", RESTServiceQueryAPI:
		return RESTServiceQueryAPI, nil
	case RESTServiceDHOAPI:
		return RESTServiceDHOAPI, nil
	}
	return "", fmt.Errorf("goapiproof: unknown REST service %q, want %q or %q", value, RESTServiceQueryAPI, RESTServiceDHOAPI)
}

// EffectiveService is the service the spec targets: query-api unless it names
// another.
func (s RESTEndpointSpec) EffectiveService() RESTService {
	if s.Service == "" {
		return RESTServiceQueryAPI
	}
	return s.Service
}

// RESTRunOrderFor is RESTRunOrder restricted to the operations whose spec
// targets service, in the same order.
func RESTRunOrderFor(service RESTService) []string {
	if service == "" {
		service = RESTServiceQueryAPI
	}
	var out []string
	for _, operation := range restRunOrder {
		if spec, ok := restEndpointSpecs[operation]; ok && spec.EffectiveService() == service {
			out = append(out, operation)
		}
	}
	return out
}

// KnownRESTPathsFor is KnownRESTPaths restricted to service, sorted.
func KnownRESTPathsFor(service RESTService) []string {
	if service == "" {
		service = RESTServiceQueryAPI
	}
	seen := map[string]bool{}
	for _, spec := range restEndpointSpecs {
		if spec.EffectiveService() == service {
			seen[spec.Path] = true
		}
	}
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// validateRESTServices refuses a spec naming a service this package does not
// know, and an operation whose spec is missing from restRunOrder or vice
// versa only for non-default services: a dho-api entry that is never run
// would read as covered while measuring nothing.
func validateRESTServices() error {
	inOrder := map[string]bool{}
	for _, operation := range restRunOrder {
		inOrder[operation] = true
	}
	for operation, spec := range restEndpointSpecs {
		if _, err := ParseRESTService(string(spec.Service)); err != nil {
			return fmt.Errorf("goapiproof: REST corpus entry %q: %w", operation, err)
		}
		if spec.EffectiveService() != RESTServiceQueryAPI && !inOrder[operation] {
			return fmt.Errorf("goapiproof: REST corpus entry %q targets %s but is absent from restRunOrder, so no run would ever send it", operation, spec.EffectiveService())
		}
	}
	return nil
}
