package policy

import (
	"net/http"

	"github.com/google/uuid"
)

const (
	// pathOrgSegment is the path segment a row whose resolver is organization_from_path serves.
	pathOrgSegment = "org_id"
	// queryOrgParam is the query parameter a row whose resolver is organization_from_query_or_session serves, its
	// last value.
	queryOrgParam = "org_id"
	// queryOrgResolver is the profile resolver kind of a row whose handler serves the organization queryOrgParam names.
	queryOrgResolver = "organization_from_query_or_session:"
)

// servedOrg is the organization a request names for its handler to serve in place of the token organization.
type servedOrg struct {
	named bool
	raw   string
}

// servedOrgOf reads the organization r names on row: the {org_id} path value of a path row, the last org_id query
// value of a query row.
func servedOrgOf(r *http.Request, row ShadowRow) servedOrg {
	switch {
	case row.OrgFromPath:
		return servedOrg{named: true, raw: r.PathValue(pathOrgSegment)}
	case row.OrgFromQuery:
		values := r.URL.Query()[queryOrgParam]
		if len(values) == 0 {
			return servedOrg{}
		}
		return servedOrg{named: true, raw: values[len(values)-1]}
	}
	return servedOrg{}
}

// servedOrgVerdict is how a served organization compares with the organization the decision point judges (the
// token's).
type servedOrgVerdict int

const (
	// servedOrgJudged: the request names none, or names the judged one.
	servedOrgJudged servedOrgVerdict = iota
	// servedOrgUnparsed: it names a value that parses as no UUID; the handler refuses it.
	servedOrgUnparsed
	// servedOrgOther: it names another organization, or the token has none; the seam refuses it.
	servedOrgOther
)

// against compares s with judged, the token organization. Both are parsed by every form a handler of these rows
// accepts (uuid.Parse, ParsePyUUID) and compared as UUIDs, so no spelling a handler serves escapes the compare.
func (s servedOrg) against(judged string) servedOrgVerdict {
	if !s.named {
		return servedOrgJudged
	}
	served, ok := parseOrgID(s.raw)
	if !ok {
		return servedOrgUnparsed
	}
	token, ok := parseOrgID(judged)
	if !ok || token != served {
		return servedOrgOther
	}
	return servedOrgJudged
}

func parseOrgID(raw string) (uuid.UUID, bool) {
	if id, err := uuid.Parse(raw); err == nil {
		return id, true
	}
	return ParsePyUUID(raw)
}
