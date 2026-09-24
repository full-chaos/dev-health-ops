package goapiproof

import (
	"encoding/json"
	"strings"
)

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

// External-ingest entries authenticate with the push token (see
// RESTCredentialPushToken), not the run's bearers, and are compared against
// the Python api on the same token. POST /batches is deliberately not here: it
// persists a batch, and a synthetic batch in the proof org is not made (real
// use only; its parity proof is the venue oracle).
const (
	ingestSchemaVersion   = "external-ingest.v1"
	ingestMissingBatchID  = "00000000-0000-4000-8000-000000000000"
	ingestValidateIdemKey = "probe-validate-only"
)

// ingestValidateBody is a validate-only request (it persists nothing): one
// work_item record whose payload carries a single numeric literal `n`, which
// both planes must reject the same way. lit is written into the body verbatim.
func ingestValidateBody(lit string) map[string]any {
	return map[string]any{
		"schemaVersion":  ingestSchemaVersion,
		"idempotencyKey": ingestValidateIdemKey,
		"source":         map[string]any{"system": "custom", "instance": "probe"},
		"records": []any{map[string]any{
			"kind": "work_item.v1", "externalId": "probe-1",
			"payload": map[string]any{"n": json.RawMessage(lit)},
		}},
	}
}

var dhoAPIRunOrder = []string{
	"REST:GET:/api/v1/external-ingest/schemas",
	"REST:GET:/api/v1/external-ingest/schemas/{schema_version}",
	"REST:GET:/api/v1/external-ingest/availability",
	"REST:GET:/api/v1/external-ingest/batches",
	"REST:GET:/api/v1/external-ingest/batches/{ingestion_id}",
	"REST:POST:/api/v1/external-ingest/validate",
	"REST:GET:/ready",
	"REST:GET:/health",
	"REST:GET:/health/workers",
	"REST:GET:/api/v1/webhooks/health",
	"REST:GET:/api/v1/orgs/me",
	"REST:GET:/api/v1/licensing/entitlements/{org_id}",
	"REST:GET:/api/v1/telemetry/status",
}

var dhoAPIEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/external-ingest/schemas": {
		Method: "GET", Path: "/api/v1/external-ingest/schemas", Service: RESTServiceDHOAPI, Credential: RESTCredentialPushToken,
		Requests: []RESTRequest{{
			Name:                "schemas",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}},
	},
	"REST:GET:/api/v1/external-ingest/schemas/{schema_version}": {
		Method: "GET", Path: "/api/v1/external-ingest/schemas/{schema_version}", Service: RESTServiceDHOAPI, Credential: RESTCredentialPushToken,
		Requests: []RESTRequest{{
			Name:                "schema_v1",
			PathLiterals:        map[string]string{"schema_version": ingestSchemaVersion},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}},
	},
	"REST:GET:/api/v1/external-ingest/availability": {
		Method: "GET", Path: "/api/v1/external-ingest/availability", Service: RESTServiceDHOAPI, Credential: RESTCredentialPushToken,
		Requests: []RESTRequest{{
			Name:                "availability",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}},
	},
	"REST:GET:/api/v1/external-ingest/batches": {
		Method: "GET", Path: "/api/v1/external-ingest/batches", Service: RESTServiceDHOAPI, Credential: RESTCredentialPushToken,
		Requests: []RESTRequest{{
			Name:                "list",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}},
	},
	"REST:GET:/api/v1/external-ingest/batches/{ingestion_id}": {
		Method: "GET", Path: "/api/v1/external-ingest/batches/{ingestion_id}", Service: RESTServiceDHOAPI, Credential: RESTCredentialPushToken,
		Requests: []RESTRequest{{
			Name:                "missing",
			PathLiterals:        map[string]string{"ingestion_id": ingestMissingBatchID},
			WantCandidateStatus: 404, WantBaselineStatus: 404,
			BodyMode: RESTBodyModeJSON,
		}},
	},
	"REST:POST:/api/v1/external-ingest/validate": {
		Method: "POST", Path: "/api/v1/external-ingest/validate", Service: RESTServiceDHOAPI, Credential: RESTCredentialPushToken,
		Requests: []RESTRequest{
			{
				// An integer literal of 4301 digits: beyond CPython's default
				// int-to-str digit limit, so both planes answer the same 500.
				Name:                "bigint_4301_digits",
				Body:                ingestValidateBody(strings.Repeat("9", 4301)),
				WantCandidateStatus: 500, WantBaselineStatus: 500,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// A float literal that overflows to infinity in JSON.
				Name:                "float_1e1000",
				Body:                ingestValidateBody("1e1000"),
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
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
