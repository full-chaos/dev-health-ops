package goapiproof

import (
	"fmt"
	"sort"
	"strings"
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

// RESTCredentialKind says which credential a corpus entry's legs send. The
// default is the run's own pair of bearers (-candidate/-baseline-bearer-exec);
// a push-token entry sends the external-ingest push token from
// -push-token-file on BOTH legs, because that API owns its own bearer
// authentication and accepts nothing else.
type RESTCredentialKind string

const (
	// RESTCredentialRun is the default: the run's own bearers.
	RESTCredentialRun RESTCredentialKind = ""
	// RESTCredentialPushToken is the external-ingest push token.
	RESTCredentialPushToken RESTCredentialKind = "push_token"
	// RESTCredentialOrgAdmin is the access token of the proof principal that
	// holds an Admin membership in the proof org (the org-admin admin routes).
	RESTCredentialOrgAdmin RESTCredentialKind = "org_admin"
	// RESTCredentialPlatformSuperadmin is the access token of the dedicated
	// platform-superadmin proof principal (the Superuser admin routes).
	RESTCredentialPlatformSuperadmin RESTCredentialKind = "platform_superadmin"
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
	if err := validateRESTCredentialKinds(); err != nil {
		return err
	}
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

// PlansPushTokenEntries says whether a run of service sends any entry that
// needs the push token, so a run without -push-token-file can refuse at
// startup instead of failing request by request.
func PlansPushTokenEntries(service RESTService) bool {
	return PlansCredentialKind(service, RESTCredentialPushToken)
}

// PlansCredentialKind says whether a run of service sends any entry of that
// credential kind, so a run without the matching token file can refuse at
// startup instead of failing request by request.
func PlansCredentialKind(service RESTService, kind RESTCredentialKind) bool {
	for _, operation := range RESTRunOrderFor(service) {
		if restEndpointSpecs[operation].Credential == kind {
			return true
		}
	}
	return false
}

// adminPersistNothingPOSTs are the only POST operations an admin credential may
// carry: each was read in the Python handler to persist nothing and call
// nothing outside the process (the GitHub install-url route only signs a state
// and builds a URL), so a case is a read in effect, like the external-ingest
// validate case. Anything else that is not a GET is a write: real use only
// (R402/R406), never a synthetic corpus case.
var adminPersistNothingPOSTs = map[string]bool{
	"REST:POST:/api/v1/admin/integrations/github/install-url": true,
}

// validateRESTCredentialKinds refuses an unknown credential kind, a file-fed
// credential entry outside the dho api (only that service authenticates it), an
// admin credential on anything but a GET (bar the persist-nothing allowlist,
// judged on the method and path an entry really sends, which must equal its
// key), and a PathLiterals key that is not a {placeholder} of the entry's path.
func validateRESTCredentialKinds() error {
	for operation, spec := range restEndpointSpecs {
		switch spec.Credential {
		case RESTCredentialRun:
		case RESTCredentialPushToken, RESTCredentialOrgAdmin, RESTCredentialPlatformSuperadmin:
			// Every file-fed kind is authenticated only by the dho api, and
			// an entry that sends no credential cannot also name one.
			if spec.EffectiveService() != RESTServiceDHOAPI {
				return fmt.Errorf("goapiproof: REST corpus entry %q sends the %s credential but targets %s, not %s", operation, spec.Credential, spec.EffectiveService(), RESTServiceDHOAPI)
			}
			if spec.PublicNoAuth {
				return fmt.Errorf("goapiproof: REST corpus entry %q is both PublicNoAuth and a %s-credential entry", operation, spec.Credential)
			}
			if spec.Credential != RESTCredentialPushToken {
				// The request an admin entry sends is its Method and Path, not
				// its registry key: the allowlist is judged on the identity those
				// two make, and the key must equal it, so a key cannot borrow the
				// allowlisted name for a different route.
				identity := "REST:" + spec.Method + ":" + spec.Path
				if operation != identity {
					return fmt.Errorf("goapiproof: REST corpus entry %q sends the %s credential but its key does not match its method and path (%s)", operation, spec.Credential, identity)
				}
				if spec.Method != "GET" && !adminPersistNothingPOSTs[identity] {
					return fmt.Errorf("goapiproof: REST corpus entry %q sends the %s credential on a %s request; admin credentials are for read-only GETs (R402/R406)", operation, spec.Credential, spec.Method)
				}
			}
		default:
			return fmt.Errorf("goapiproof: REST corpus entry %q has an unknown credential kind %q", operation, spec.Credential)
		}
		for _, request := range spec.Requests {
			for name := range request.PathLiterals {
				if !strings.Contains(spec.Path, "{"+name+"}") {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q gives a PathLiterals value for {%s}, which is not a placeholder of %s", operation, request.Name, name, spec.Path)
				}
			}
		}
	}
	return nil
}
