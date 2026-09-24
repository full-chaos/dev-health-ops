package goapiproof

// This file holds the corpus entries whose candidate is the dho api
// (RESTServiceDHOAPI) rather than query-api: the read-only routes that
// have a live Python baseline. init below registers them into
// restEndpointSpecs, restRunOrder and restOperatorSuppliedProducers, so the
// query-api corpus in restcorpus.go is untouched.
//
// Every entry here is a GET answered 200 by both planes. A route whose Python
// body is deleted has no baseline to compare with and is not in this file.
//
// Bodies fall in two classes:
//
//   - Data the route reads and both planes read the same way (`/ready`,
//     orgs/me, licensing entitlements, telemetry status): compared as JSON,
//     no declared divergence.
//   - Reports of the serving deployment's own wiring (`/health`,
//     `/health/workers`, webhook `/health`): the body names which
//     dependencies and secrets THIS process has configured, so two
//     differently configured Deployments legitimately answer differently.
//     They are RESTBodyModeCandidateShape: the candidate must answer 200 with
//     a live JSON object; the two bodies are not compared. This is not an
//     excuse for a difference: the wiring difference is read from the two
//     stored response bodies of the run, never assumed.

// dhoAPIOrgIDProducer is the operator-supplied id the entitlements route
// binds into its path (-bind org_id=<the -org value>). The route serves only
// the caller's own org, so the operator names it.
const dhoAPIOrgIDProducer = "org_id"

var dhoAPIRunOrder = []string{
	"REST:GET:/ready",
	"REST:GET:/health",
	"REST:GET:/health/workers",
	"REST:GET:/api/v1/webhooks/health",
	"REST:GET:/api/v1/orgs/me",
	"REST:GET:/api/v1/licensing/entitlements/{org_id}",
	"REST:GET:/api/v1/telemetry/status",
}

var dhoAPIEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/ready": {
		Method: "GET", Path: "/ready", Service: RESTServiceDHOAPI, PublicNoAuth: true,
		Requests: []RESTRequest{{
			Name:                "ready",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}},
	},
	"REST:GET:/health": {
		Method: "GET", Path: "/health", Service: RESTServiceDHOAPI, PublicNoAuth: true,
		Requests: []RESTRequest{{
			Name:                "health",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeCandidateShape,
		}},
	},
	"REST:GET:/health/workers": {
		Method: "GET", Path: "/health/workers", Service: RESTServiceDHOAPI, PublicNoAuth: true,
		Requests: []RESTRequest{{
			Name:                "health_workers",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeCandidateShape,
		}},
	},
	"REST:GET:/api/v1/webhooks/health": {
		Method: "GET", Path: "/api/v1/webhooks/health", Service: RESTServiceDHOAPI, PublicNoAuth: true,
		Requests: []RESTRequest{{
			Name:                "webhooks_health",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeCandidateShape,
		}},
	},
	"REST:GET:/api/v1/orgs/me": {
		Method: "GET", Path: "/api/v1/orgs/me", Service: RESTServiceDHOAPI,
		Requests: []RESTRequest{{
			Name:                "own_org",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}},
	},
	"REST:GET:/api/v1/licensing/entitlements/{org_id}": {
		Method: "GET", Path: "/api/v1/licensing/entitlements/{org_id}", Service: RESTServiceDHOAPI,
		Requests: []RESTRequest{{
			Name:                "own_org_entitlements",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode:   RESTBodyModeJSON,
			IDBindings: []RESTIDBinding{{Producer: dhoAPIOrgIDProducer, PathParam: "org_id"}},
		}},
	},
	"REST:GET:/api/v1/telemetry/status": {
		Method: "GET", Path: "/api/v1/telemetry/status", Service: RESTServiceDHOAPI,
		Requests: []RESTRequest{{
			Name:                "status",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}},
	},
}

func init() {
	for operation, spec := range dhoAPIEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIRunOrder...)
	restOperatorSuppliedProducers[dhoAPIOrgIDProducer] = true
}
