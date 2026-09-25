package goapiproof

// Batch 2 of the admin corpus (CHAOS-6610): the org-admin READ-ONLY routes of
// users, org members and credentials, registered into the dho-api corpus by
// init below. Same rules as batch 1 (dhoapi_admin_access_corpus.go): the
// org-admin token on both legs, statuses from a live capture of BOTH planes on
// bigboy (ops a97b97ed, with and without an Origin header, status/body/headers
// identical on every row; record
// _records/bigboy-a97b97ed/corpus-baseline-admin2), writes are real use only
// (R402/R406), bigboy is the leg of record until CHAOS-6570.
//
// What a case does NOT show, by design:
//
//   - The proof org has no credential row, so credentials/{provider}/{name} has
//     only the missing case; a produced-id case needs a row a producer POST
//     leg would create, and writes have no synthetic case. A credential body
//     is never in a corpus case for another reason too: it must never carry a
//     secret, and the run's echo guard refuses a body that contains the
//     credential it was sent.
//   - discover-members / infer-members are not here: they may call an external
//     provider, and no case is added until that is proven not to happen.

const (
	// adminUserIDProducer names the id read from the users list's first user
	// and bound into users/{user_id}.
	adminUserIDProducer = "admin_user_id"
	// adminMissingCredentialProvider / Name address a credential that does not
	// exist (404 on both planes).
	adminMissingCredentialProvider = "github"
	adminMissingCredentialName     = "zz-missing"
)

var dhoAPIAdminOrgRunOrder = []string{
	"REST:GET:/api/v1/admin/users",
	"REST:GET:/api/v1/admin/users/{user_id}",
	"REST:GET:/api/v1/admin/orgs/{org_id}/members",
	"REST:GET:/api/v1/admin/credentials",
	"REST:GET:/api/v1/admin/credentials/{provider}/{name}",
}

var dhoAPIAdminOrgEndpointSpecs = map[string]RESTEndpointSpec{
	// The users list answers a bare JSON array (no wrapper key): the producer
	// reads the root.
	"REST:GET:/api/v1/admin/users": withProducer(adminGET("/api/v1/admin/users", "list", 200, nil),
		RESTIDProducer{Name: adminUserIDProducer, IDField: "id"}),
	"REST:GET:/api/v1/admin/users/{user_id}": withSecondRequest(
		adminGET("/api/v1/admin/users/{user_id}", "missing", 404, map[string]string{"user_id": adminMissingID}),
		RESTRequest{
			Name:                "produced",
			IDBindings:          []RESTIDBinding{{Producer: adminUserIDProducer, PathParam: "user_id"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}),
	// The org id is the operator-supplied one (-bind org_id=<the -org value>),
	// the same binding the entitlements entry uses: the proof principal is an
	// admin of that org.
	"REST:GET:/api/v1/admin/orgs/{org_id}/members": {
		Method: "GET", Path: "/api/v1/admin/orgs/{org_id}/members", Service: RESTServiceDHOAPI, Credential: RESTCredentialOrgAdmin,
		Requests: []RESTRequest{{
			Name:                "own_org_members",
			IDBindings:          []RESTIDBinding{{Producer: dhoAPIOrgIDProducer, PathParam: "org_id"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}},
	},
	"REST:GET:/api/v1/admin/credentials": adminGET("/api/v1/admin/credentials", "list", 200, nil),
	"REST:GET:/api/v1/admin/credentials/{provider}/{name}": adminGET("/api/v1/admin/credentials/{provider}/{name}", "missing", 404,
		map[string]string{"provider": adminMissingCredentialProvider, "name": adminMissingCredentialName}),
}

func init() {
	for operation, spec := range dhoAPIAdminOrgEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIAdminOrgRunOrder...)
}
