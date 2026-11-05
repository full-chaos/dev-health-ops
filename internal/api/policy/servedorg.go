package policy

import (
	"net/http"

	"github.com/google/uuid"
)

// servedOrgParam names where a row's handler reads the organization it serves when that is not the caller's
// session organization: the path segment, or the last value of the query parameter.
type servedOrgParam struct {
	path, query string
}

// goAPIServedOrgRows are the go-api action rows whose handler serves an organization a path or query parameter
// names: the organization_from_path rows and the billing rows whose handler reads an org_id query parameter
// (resolveOrg). The profile still names those billing rows organization_from_session, so this table, not the
// resolver, is their source.
var goAPIServedOrgRows = map[string]servedOrgParam{
	"GET /api/v1/admin/orgs/{org_id}/members":              {path: "org_id"},
	"POST /api/v1/admin/orgs/{org_id}/members":             {path: "org_id"},
	"POST /api/v1/admin/orgs/{org_id}/invites":             {path: "org_id"},
	"PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}":  {path: "org_id"},
	"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}": {path: "org_id"},
	"POST /api/v1/admin/orgs/{org_id}/transfer-ownership":  {path: "org_id"},
	"GET /api/v1/billing/entitlements/{org_id}":            {path: "org_id"},
	"GET /api/v1/licensing/entitlements/{org_id}":          {path: "org_id"},
	"GET /api/v1/billing/invoices":                         {query: "org_id"},
	"GET /api/v1/billing/invoices/{invoice_id}":            {query: "org_id"},
	"POST /api/v1/billing/invoices/{invoice_id}/void":      {query: "org_id"},
	"GET /api/v1/billing/subscriptions":                    {query: "org_id"},
	"GET /api/v1/billing/subscriptions/list":               {query: "org_id"},
	"GET /api/v1/billing/subscriptions/history":            {query: "org_id"},
	"POST /api/v1/billing/subscriptions/change-plan":       {query: "org_id"},
	"POST /api/v1/billing/subscriptions/cancel":            {query: "org_id"},
	"POST /api/v1/billing/subscriptions/reactivate":        {query: "org_id"},
}

// servedOrg is the organization a request names for its handler to serve in place of the session organization.
type servedOrg struct {
	named bool
	// org is the canonical lowercase id; "" when the named value parses as no UUID.
	org string
}

// servedOrgOf reads the organization r names on a row of goAPIServedOrgRows. A value is parsed by every form a
// handler of those rows accepts (uuid.Parse, ParsePyUUID), so no spelling a handler serves escapes the compare.
func servedOrgOf(r *http.Request) servedOrg {
	param, ok := goAPIServedOrgRows[r.Pattern]
	if !ok {
		return servedOrg{}
	}
	var raw string
	if param.query != "" {
		values := r.URL.Query()[param.query]
		if len(values) == 0 {
			return servedOrg{}
		}
		raw = values[len(values)-1]
	} else {
		raw = r.PathValue(param.path)
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		parsed, ok := ParsePyUUID(raw)
		if !ok {
			return servedOrg{named: true}
		}
		id = parsed
	}
	return servedOrg{named: true, org: id.String()}
}

// servedOrgVerdict is how a served organization compares with the organization the decision point judges.
type servedOrgVerdict int

const (
	// servedOrgJudged: the request names none, or names the judged organization.
	servedOrgJudged servedOrgVerdict = iota
	// servedOrgUnparsed: it names a value that parses as no UUID; the handler refuses it.
	servedOrgUnparsed
	// servedOrgOther: it names another organization, which the seam refuses before the handler.
	servedOrgOther
)

// against compares s with judged, the organization id the decision point is asked about.
func (s servedOrg) against(judged string) servedOrgVerdict {
	switch {
	case !s.named:
		return servedOrgJudged
	case s.org == "":
		return servedOrgUnparsed
	case s.org != judged:
		return servedOrgOther
	}
	return servedOrgJudged
}
