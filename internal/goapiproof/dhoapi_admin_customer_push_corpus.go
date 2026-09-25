package goapiproof

// Batch 5 of the admin corpus (CHAOS-6618): the org-admin READ-ONLY routes of
// the customer-push admin area (sources, their tokens and batches, the
// schemas), registered into the dho-api corpus by init below. Same rules as
// batch 1 (dhoapi_admin_access_corpus.go): the org-admin token on both legs,
// statuses from a live capture of BOTH planes on bigboy (ops a97b97ed, with and
// without an Origin header; status, body and headers identical on all 22 rows;
// record _records/bigboy-a97b97ed/corpus-baseline-admin5), writes are real use
// only (R402/R406), bigboy is the leg of record until CHAOS-6570.
//
// Tokens: a token body carries a token_prefix and NO token value, hash or
// secret field, on either plane (checked by key names in the capture). The
// run's echo guard would in any case refuse a body that carries the token this
// prover sent; that is the admin credential, not a customer-push token.
//
// What a case does NOT show, by design:
//
//   - Writes (source create/update/validate, token create/revoke/rotate) are
//     real use only. The validate route is not treated as read-only here.
//   - The proof org has one source and no batch, so the source routes have a
//     produced-id case (the id is read from the sources list) and the batch
//     routes only a missing case: no produced batch id exists.

// adminCustomerPushSourceIDProducer names the id read from the sources list's
// first source and bound into sources/{source_id} and its sub-routes.
const adminCustomerPushSourceIDProducer = "admin_customer_push_source_id"

var dhoAPIAdminCustomerPushRunOrder = []string{
	"REST:GET:/api/v1/admin/customer-push/schemas",
	"REST:GET:/api/v1/admin/customer-push/schemas/{schema_version}",
	"REST:GET:/api/v1/admin/customer-push/sources",
	"REST:GET:/api/v1/admin/customer-push/sources/{source_id}",
	"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/batches",
	"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/tokens",
	"REST:GET:/api/v1/admin/customer-push/tokens",
	"REST:GET:/api/v1/admin/customer-push/batches/{ingestion_id}",
}

// adminSourceScoped is a {source_id} entry with a missing (404) and a produced
// (200) request.
func adminSourceScoped(path string) RESTEndpointSpec {
	return withSecondRequest(
		adminGET(path, "missing", 404, map[string]string{"source_id": adminMissingID}),
		RESTRequest{
			Name:                "produced",
			IDBindings:          []RESTIDBinding{{Producer: adminCustomerPushSourceIDProducer, PathParam: "source_id"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		})
}

var dhoAPIAdminCustomerPushEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/admin/customer-push/schemas": adminGET("/api/v1/admin/customer-push/schemas", "list", 200, nil),
	"REST:GET:/api/v1/admin/customer-push/schemas/{schema_version}": adminGET("/api/v1/admin/customer-push/schemas/{schema_version}", "v1", 200,
		map[string]string{"schema_version": ingestSchemaVersion}),
	// The sources list answers a bare JSON array: the producer reads the root.
	"REST:GET:/api/v1/admin/customer-push/sources": withProducer(adminGET("/api/v1/admin/customer-push/sources", "list", 200, nil),
		RESTIDProducer{Name: adminCustomerPushSourceIDProducer, IDField: "id"}),
	"REST:GET:/api/v1/admin/customer-push/sources/{source_id}":         adminSourceScoped("/api/v1/admin/customer-push/sources/{source_id}"),
	"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/batches": adminSourceScoped("/api/v1/admin/customer-push/sources/{source_id}/batches"),
	"REST:GET:/api/v1/admin/customer-push/sources/{source_id}/tokens":  adminSourceScoped("/api/v1/admin/customer-push/sources/{source_id}/tokens"),
	"REST:GET:/api/v1/admin/customer-push/tokens":                      adminGET("/api/v1/admin/customer-push/tokens", "list", 200, nil),
	"REST:GET:/api/v1/admin/customer-push/batches/{ingestion_id}": adminGET("/api/v1/admin/customer-push/batches/{ingestion_id}", "missing", 404,
		map[string]string{"ingestion_id": adminMissingID}),
}

func init() {
	for operation, spec := range dhoAPIAdminCustomerPushEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIAdminCustomerPushRunOrder...)
}
