package goapiproof

import (
	"net/url"
	"strings"
)

// This file lets one REST corpus request bind a path/query id it read
// from an EARLIER request's own BASELINE (Python) response, in the same
// run -- so a live id-scoped route (a team/repo/person quadrant scope, a
// repo-scoped drilldown, a repo/team-scoped explain metric, a person-
// scoped people/{person_id}/summary or /metric) can reach its 200 path
// at all, instead of being refused by name the way
// OperationSpec.InstanceVariable refuses the GraphQL `pr` operation's own
// `$id` gap (operations.go) for want of any id this table could supply.
//
// The id is read from the BASELINE leg specifically -- never candidate --
// because the ruling this file implements is "the same id proves both
// planes were asked about the SAME row", and the baseline (Python) leg is
// the one plane every corpus entry always calls regardless of routeswitch
// enablement, so a producer's id is available even for an operation this
// binary is still rolling out on the candidate side.
//
// An id is never invented: RESTIDBinding names a Producer, resolved at
// run time from a package-level map ("produced") the run loop
// (cmd/go-api-rest-prove) builds up as RESTRunOrder executes -- a
// producer that never ran, or whose response held no usable id, leaves
// its name absent from that map, and ResolveRESTIDBindings reports it in
// `unresolved` rather than substituting a placeholder. The caller then
// refuses the whole request by name (RESTRefusalIDBindingUnresolved) --
// unproven, never a tool failure -- without ever sending it.

// RESTIDProducer names one id a request's BASELINE response makes
// available to a LATER request's own IDBindings, once RESTAdmit has
// admitted the response and its body decoded as JSON.
type RESTIDProducer struct {
	// Name is the id's identity across the whole corpus -- what a later
	// request's RESTIDBinding.Producer names to consume it. Never empty;
	// never produced by two requests (ValidateRESTIDBindingOrder checks
	// both).
	Name string
	// ListPath is a dotted, index-free path (ValidateRESTCorpus's own
	// "data."-free convention: this walks the DECODED BODY directly, not
	// a "data"-prefixed citation path) to a JSON array in the baseline
	// body. Empty names the body's own root value -- GET /api/v1/people
	// answers a bare JSON array with no wrapper key, so its own producer
	// entry (person_id) leaves this empty.
	ListPath string
	// IDField names the string field to read from each array element.
	// Empty when an element IS the id, a bare string -- GET
	// /api/v1/filters/options' team/repo lists are both []string.
	IDField string
}

// RESTIDBinding binds one id an EARLIER request (per RESTRunOrder) has
// Produced into THIS request, applied identically to BOTH legs before
// either is sent -- see ResolveRESTIDBindings. Exactly one of
// QueryParam/PathParam is set (ValidateRESTIDBindingOrder checks this);
// PathParam names a "{name}" placeholder in the route's own Path, for a
// future path-scoped route this corpus does not yet mount live.
type RESTIDBinding struct {
	Producer   string
	QueryParam string
	PathParam  string
}

// ExtractRESTID walks body (typically a decoded Snapshot.Data) via
// producer's ListPath/IDField and returns the FIRST non-empty string id
// it finds, in array order -- "the first person_id", this ticket's own
// ruling. false when the addressed value is not a JSON array, or no
// element yields a non-empty string: a malformed or empty producer
// response is a failed extraction, never a panic, so a downstream
// consumer refuses by name instead of this tool crashing on bad live
// data it does not control.
func ExtractRESTID(body any, producer RESTIDProducer) (string, bool) {
	value := body
	if producer.ListPath != "" {
		for _, segment := range strings.Split(producer.ListPath, ".") {
			obj, ok := value.(map[string]any)
			if !ok {
				return "", false
			}
			value, ok = obj[segment]
			if !ok {
				return "", false
			}
		}
	}
	list, ok := value.([]any)
	if !ok {
		return "", false
	}
	for _, element := range list {
		var candidate string
		var ok bool
		if producer.IDField == "" {
			candidate, ok = element.(string)
		} else if obj, isObj := element.(map[string]any); isObj {
			raw, present := obj[producer.IDField]
			if present {
				candidate, ok = raw.(string)
			}
		}
		if ok && candidate != "" {
			return candidate, true
		}
	}
	return "", false
}

// ResolveRESTIDBindings applies request's declared IDBindings against
// produced (a running producer-Name -> id map the run loop builds up
// following RESTRunOrder), returning the resolved path (specPath with
// every PathParam placeholder substituted) and a resolved COPY of
// request.Query with every QueryParam binding set -- the same resolved
// values then build BOTH the candidate and the baseline request, so a
// bound id never differs between the two legs. unresolved names every
// binding whose Producer has not (yet, or ever) produced a usable id in
// this run; the caller refuses the request by name rather than sending
// it with an invented or stale value.
//
// Only called when request.IDBindings is non-empty (see
// cmd/go-api-rest-prove's own run loop): a request with no bindings
// keeps its original request.Query untouched, byte-for-byte, so an
// existing entry's RequestIdentity digest (which folds the query in)
// never shifts just because this mechanism exists.
func ResolveRESTIDBindings(specPath string, request RESTRequest, produced map[string]string) (resolvedPath string, resolvedQuery url.Values, unresolved []string) {
	resolvedPath = specPath
	resolvedQuery = url.Values{}
	for key, values := range request.Query {
		resolvedQuery[key] = append([]string(nil), values...)
	}
	boundIDs := make(map[string]string, len(request.IDBindings))
	for _, binding := range request.IDBindings {
		id, ok := produced[binding.Producer]
		if !ok || id == "" {
			unresolved = append(unresolved, binding.Producer)
			continue
		}
		switch {
		case binding.QueryParam != "":
			resolvedQuery.Set(binding.QueryParam, id)
		case binding.PathParam != "":
			resolvedPath = strings.ReplaceAll(resolvedPath, "{"+binding.PathParam+"}", id)
		}
		boundIDs[binding.Producer] = id
	}
	return resolvedPath, resolvedQuery, unresolved
}

// RESTRefusalIDBindingUnresolved is the named refusal reason for a
// consumer request whose declared IDBindings did not all resolve --
// distinct from every other REST refusal constant (restadmit.go) because
// it fires BEFORE either leg is ever called, never from an HTTP
// response.
const RESTRefusalIDBindingUnresolved = "rest_request_id_binding_unresolved"
