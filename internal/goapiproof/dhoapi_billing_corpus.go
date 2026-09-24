package goapiproof

// This file adds the dho api's billing READ routes and the HEAD method of the
// three probe paths to the dho-api corpus (registered by init below, beside
// dhoapi_corpus.go's own). Everything here is read-only and persists nothing:
//
//   - list and own-org routes are compared as JSON;
//   - every {id} route is exercised with an id that does not exist (404 on both
//     planes, and no Stripe call is made for it);
//   - HEAD is compared on status only (a HEAD answer has no body), which is the
//     method-completeness evidence for /health, /ready and /health/workers.
//
// Billing's mutating routes (checkout, portal, refunds, subscription changes,
// plan writes and syncs, reconcile, audit resolve, invoice void) are real use
// only and have no synthetic case.

const billingMissingID = "00000000-0000-4000-8000-000000000000"

// billingReadCase is one GET entry: a route, its request name, the status both
// planes must answer, and any fixed path literal / operator binding it needs.
type billingReadCase struct {
	path     string
	name     string
	status   int
	literals map[string]string
	bindOrg  bool // the org id fills the route's {org_id} path parameter
	queryOrg bool // the org id is sent as the org_id query parameter
}

// The statuses are what the Python api answers the proof principal (a viewer
// or member, not a superuser), read from a live capture of both planes; the Go
// plane must answer the same. Refunds and the audit trail are superuser-only
// in Python, so a non-superuser reads 403 there (and the audit LIST first
// requires org_id, answering 422 without it).
var billingReadCases = []billingReadCase{
	{path: "/api/v1/billing/plans", name: "list", status: 200},
	{path: "/api/v1/billing/plans/{plan_id}", name: "missing", status: 404, literals: map[string]string{"plan_id": billingMissingID}},
	{path: "/api/v1/billing/invoices", name: "list", status: 200},
	{path: "/api/v1/billing/invoices/{invoice_id}", name: "missing", status: 404, literals: map[string]string{"invoice_id": billingMissingID}},
	{path: "/api/v1/billing/refunds", name: "not_superuser", status: 403},
	{path: "/api/v1/billing/refunds/{refund_id}", name: "not_superuser", status: 403, literals: map[string]string{"refund_id": billingMissingID}},
	{path: "/api/v1/billing/entitlements/{org_id}", name: "own_org", status: 200, bindOrg: true},
	{path: "/api/v1/billing/subscriptions/list", name: "list", status: 200},
	{path: "/api/v1/billing/subscriptions", name: "no_subscription", status: 404},
	{path: "/api/v1/billing/subscriptions/history", name: "history", status: 200},
	{path: "/api/v1/billing/audit", name: "missing_org_id", status: 422},
	{path: "/api/v1/billing/audit", name: "not_superuser", status: 403, queryOrg: true},
	{path: "/api/v1/billing/audit/{audit_id}", name: "not_superuser", status: 403, literals: map[string]string{"audit_id": billingMissingID}},
}

// headProbePaths are the paths whose HEAD method is measured.
var headProbePaths = []string{"/health", "/ready", "/health/workers"}

func init() {
	for _, c := range billingReadCases {
		request := RESTRequest{
			Name:                c.name,
			PathLiterals:        c.literals,
			WantCandidateStatus: c.status, WantBaselineStatus: c.status,
			BodyMode: RESTBodyModeJSON,
		}
		if c.bindOrg {
			request.IDBindings = []RESTIDBinding{{Producer: dhoAPIOrgIDProducer, PathParam: "org_id"}}
		}
		if c.queryOrg {
			request.IDBindings = []RESTIDBinding{{Producer: dhoAPIOrgIDProducer, QueryParam: "org_id"}}
		}
		if c.status != 200 {
			request.StatusDivergenceReason = ""
		}
		operation := "REST:GET:" + c.path
		spec, exists := restEndpointSpecs[operation]
		if !exists {
			spec = RESTEndpointSpec{Method: "GET", Path: c.path, Service: RESTServiceDHOAPI}
			restRunOrder = append(restRunOrder, operation)
		}
		spec.Requests = append(spec.Requests, request)
		restEndpointSpecs[operation] = spec
	}
	for _, path := range headProbePaths {
		operation := "REST:HEAD:" + path
		restEndpointSpecs[operation] = RESTEndpointSpec{
			Method: "HEAD", Path: path, Service: RESTServiceDHOAPI, PublicNoAuth: true,
			Requests: []RESTRequest{{
				Name:                "head",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeStatusOnly,
			}},
		}
		restRunOrder = append(restRunOrder, operation)
	}
}
