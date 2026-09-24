package goapiproof

import (
	"encoding/json"
	"net/url"
	"strconv"
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
// The id is read from the BASELINE leg for the overwhelming majority of
// producers, because the ruling this file implements is "the same id
// proves both planes were asked about the SAME row", and the baseline
// (Python) leg is the one plane every corpus entry always calls
// regardless of routeswitch enablement, so a producer's id is available
// even for an operation this binary is still rolling out on the
// candidate side. The one exception: a request whose declared
// WantBaselineStatus differs from WantCandidateStatus with the
// candidate's own want at 200 -- the baseline is declared failing in
// production, so cmd/go-api-rest-prove reads that producer's id from the
// CANDIDATE leg instead (proveOneRESTRequest's own doc comment) -- an id
// is a request parameter, not evidence compared between planes, so it is
// honest to read it from whichever leg actually answers with a body.
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
	// JoinField, when set, names a SECOND field to read from the SAME
	// array element and append after IDField's own value, joined by ":"
	// -- flame's own "pr" entity_id shape ("<repo_id>:<number>",
	// parseRepoEntity's own doc comment in internal/queryapi/flame)
	// needs BOTH a repo_id and a PR number from the SAME drilldown item,
	// never two independently-produced ids that could each name a
	// different PR. drilldown/prs' own PRItem.Number (prs.go) is a JSON
	// NUMBER on the wire, not a string -- ExtractRESTID formats it as a
	// base-10 integer via numericRESTIDField, the same "decoded body,
	// never re-typed source" convention IDField's own string read
	// already follows. Empty for every producer that names only one
	// field (the overwhelming majority).
	JoinField string
}

// RESTIDBinding binds one id an EARLIER request (per RESTRunOrder) has
// Produced into THIS request, applied identically to BOTH legs before
// either is sent -- see ResolveRESTIDBindings. Exactly one of
// QueryParam/PathParam/BodyPath is set (ValidateRESTCorpus checks this);
// PathParam names a "{name}" placeholder in the route's own Path, for a
// future path-scoped route this corpus does not yet mount live. BodyPath
// names a dotted, index-free walk (the same convention
// RESTIDProducer.ListPath already uses) into a POST request's own JSON
// Body map, for a route whose scope id travels in the body rather than
// the query string or path -- a POST-only route with no query/path
// binding surface at all otherwise has no way to prove a scoped branch
// live against a real id.
type RESTIDBinding struct {
	Producer   string
	QueryParam string
	PathParam  string
	BodyPath   string

	// Candidates opts this binding into bounded candidate iteration:
	// instead of taking Producer's single first-extracted id (today's
	// only behaviour, unchanged when this is zero), the run loop tries
	// up to Candidates of Producer's own candidate pool
	// (ExtractRESTIDCandidates' full, ordered result, not just its first
	// match), sending this request in full for each, until one
	// candidate's OWN declared Produces all resolve -- that candidate
	// wins -- or the bound is exhausted. Zero, the field every existing
	// binding carries, is the explicit "no iteration" default: this
	// binding keeps single-shot first-element behaviour byte for byte.
	// Required together with ExposeAs (ValidateRESTCorpus checks both
	// directions); at most one iterating binding is supported per
	// request.
	Candidates int
	// ExposeAs, only meaningful with Candidates > 0, names the id under
	// which the WINNING candidate itself -- not anything read from a
	// response body -- becomes available to a LATER request's own
	// IDBindings, exactly like an ordinary RESTIDProducer.Name would.
	// Distinct from Producer's own name on purpose: Producer keeps
	// naming the single first-extracted value every OTHER (non-
	// iterating) consumer still binds to, untouched by this mechanism.
	ExposeAs string
}

// ExtractRESTID walks body (typically a decoded Snapshot.Data) via
// producer's ListPath/IDField and returns the FIRST non-empty string id
// it finds, in array order -- "the first person_id", this ticket's own
// ruling. false when the addressed value is not a JSON array, or no
// element yields a non-empty string: a malformed or empty producer
// response is a failed extraction, never a panic, so a downstream
// consumer refuses by name instead of this tool crashing on bad live
// data it does not control. When JoinField is set, BOTH fields must
// extract from the SAME element for that element to count -- an element
// carrying IDField but missing or malformed under JoinField is skipped,
// never joined with an empty tail.
func ExtractRESTID(body any, producer RESTIDProducer) (string, bool) {
	list, ok := restIDListAt(body, producer)
	if !ok {
		return "", false
	}
	for _, element := range list {
		if candidate, ok := restIDFromElement(element, producer); ok {
			return candidate, true
		}
	}
	return "", false
}

// ExtractRESTIDCandidates walks body the same way ExtractRESTID does, but
// returns EVERY non-empty id it finds, in array order, instead of only
// the first -- the candidate pool a bounded-iteration consumer
// (RESTIDBinding.Candidates) draws from. nil (never a false-but-non-nil
// distinction ExtractRESTID's own bool return needs) whenever the
// addressed value is not a JSON array, or no element yields a usable id
// -- the same "failed extraction, never a panic" contract ExtractRESTID's
// own doc comment states.
func ExtractRESTIDCandidates(body any, producer RESTIDProducer) []string {
	list, ok := restIDListAt(body, producer)
	if !ok {
		return nil
	}
	var candidates []string
	for _, element := range list {
		if candidate, ok := restIDFromElement(element, producer); ok {
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}

// RESTIDListIsEmpty reports whether body carries producer's declared list
// (ListPath) as a JSON array with no elements -- the one shape that means
// "no rows" to a bounded candidate search. A missing, null or non-array
// value, or a non-empty array, is not empty.
func RESTIDListIsEmpty(body any, producer RESTIDProducer) bool {
	list, ok := restIDListAt(body, producer)
	return ok && len(list) == 0
}

// restIDListAt walks body via producer's ListPath and returns the decoded
// JSON array at that address -- the list-addressing half ExtractRESTID
// and ExtractRESTIDCandidates share, so both read the identical path.
func restIDListAt(body any, producer RESTIDProducer) ([]any, bool) {
	value := body
	if producer.ListPath != "" {
		for _, segment := range strings.Split(producer.ListPath, ".") {
			obj, ok := value.(map[string]any)
			if !ok {
				return nil, false
			}
			value, ok = obj[segment]
			if !ok {
				return nil, false
			}
		}
	}
	list, ok := value.([]any)
	return list, ok
}

// restIDFromElement extracts ONE candidate id from a single list element
// per producer's IDField/JoinField -- the per-element half ExtractRESTID
// and ExtractRESTIDCandidates share, so both apply the identical
// IDField/JoinField rule to every element they consider.
func restIDFromElement(element any, producer RESTIDProducer) (string, bool) {
	var candidate string
	var ok bool
	if producer.IDField == "" {
		candidate, ok = element.(string)
	} else if obj, isObj := element.(map[string]any); isObj {
		raw, present := obj[producer.IDField]
		if present {
			candidate, ok = raw.(string)
		}
		if ok && candidate != "" && producer.JoinField != "" {
			joined, joinOK := numericRESTIDField(obj, producer.JoinField)
			if !joinOK {
				ok = false
			} else {
				candidate = candidate + ":" + joined
			}
		}
	}
	if ok && candidate != "" {
		return candidate, true
	}
	return "", false
}

// numericRESTIDField reads field from obj as a JSON NUMBER and formats it
// as a base-10 integer string. The body ExtractRESTID walks is always
// admission.BaselineSnap.Data, decoded by DecodeRESTSnapshot's own
// json.Decoder with UseNumber set (restadmit.go) -- so a JSON number here
// is an encoding/json.Number (a string under the hood), never a float64;
// this is the ONLY numeric type this decode path ever produces. false for
// a non-json.Number value, an absent field, or a value json.Number.Int64
// rejects (a fractional part or an exponent) -- drilldown/prs' own
// "number" field is always a whole PR number (PRItem.Number is a Go
// uint32), so a rejected value here means the producer's own
// ListPath/IDField named the wrong field, never a real PR number.
func numericRESTIDField(obj map[string]any, field string) (string, bool) {
	raw, present := obj[field]
	if !present {
		return "", false
	}
	num, ok := raw.(json.Number)
	if !ok {
		return "", false
	}
	whole, err := num.Int64()
	if err != nil {
		return "", false
	}
	return strconv.FormatInt(whole, 10), true
}

// setBodyPathID returns a copy of value with the leaf addressed by
// segments (a dotted-path walk already split, the same "data."-free
// convention RESTIDProducer.ListPath uses) replaced by id, and true --
// or (nil, false) when segments does not address an existing map key at
// every level, or the addressed leaf is neither a []string nor a string
// (the only two shapes this corpus's own body literals use for a scope
// id: a single-element list under "ids"/"repos", or a bare string field).
// A []string leaf becomes a single-element []string{id} (a real request
// naming exactly one id); a string leaf becomes id directly. Every map
// ancestor ON the path is shallow-copied so the ORIGINAL corpus literal
// (a package-level var, reused across every run and every request that
// shares that literal) is never mutated in place; every sibling value is
// shared, not copied, since only the walked path is ever written to.
func setBodyPathID(value any, segments []string, id string) (any, bool) {
	if len(segments) == 0 {
		switch value.(type) {
		case []string:
			return []string{id}, true
		case string:
			return id, true
		default:
			return nil, false
		}
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	child, present := obj[segments[0]]
	if !present {
		return nil, false
	}
	updatedChild, ok := setBodyPathID(child, segments[1:], id)
	if !ok {
		return nil, false
	}
	copied := make(map[string]any, len(obj))
	for k, v := range obj {
		copied[k] = v
	}
	copied[segments[0]] = updatedChild
	return copied, true
}

// ResolveRESTIDBindings applies request's declared IDBindings against
// produced (a running producer-Name -> id map the run loop builds up
// following RESTRunOrder), returning the resolved path (specPath with
// every PathParam placeholder substituted), a resolved COPY of
// request.Query with every QueryParam binding set, and a resolved COPY
// of request.Body with every BodyPath binding set -- the same resolved
// values then build BOTH the candidate and the baseline request, so a
// bound id never differs between the two legs. unresolved names every
// binding whose Producer has not (yet, or ever) produced a usable id in
// this run, OR whose BodyPath does not address an existing, id-shaped
// leaf in request.Body; the caller refuses the request by name rather
// than sending it with an invented, stale, or silently-unbound value.
//
// Only called when request.IDBindings is non-empty (see
// cmd/go-api-rest-prove's own run loop): a request with no bindings
// keeps its original request.Query/request.Body untouched, byte-for-
// byte, so an existing entry's RequestIdentity digest (which folds the
// query in) never shifts just because this mechanism exists.
func ResolveRESTIDBindings(specPath string, request RESTRequest, produced map[string]string) (resolvedPath string, resolvedQuery url.Values, resolvedBody any, unresolved []string) {
	resolvedPath = specPath
	resolvedQuery = url.Values{}
	for key, values := range request.Query {
		resolvedQuery[key] = append([]string(nil), values...)
	}
	resolvedBody = request.Body
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
		case binding.BodyPath != "":
			updated, ok := setBodyPathID(resolvedBody, strings.Split(binding.BodyPath, "."), id)
			if !ok {
				unresolved = append(unresolved, binding.Producer)
				continue
			}
			resolvedBody = updated
		}
		boundIDs[binding.Producer] = id
	}
	return resolvedPath, resolvedQuery, resolvedBody, unresolved
}

// RESTRefusalIDBindingUnresolved is the named refusal reason for a
// consumer request whose declared IDBindings did not all resolve --
// distinct from every other REST refusal constant (restadmit.go) because
// it fires BEFORE either leg is ever called, never from an HTTP
// response.
const RESTRefusalIDBindingUnresolved = "rest_request_id_binding_unresolved"

// RESTRefusalCandidateProducerUnresolved is the named refusal reason for
// a StatusOnly, declared-failing-baseline request (ValidateRESTCorpus's
// isBaselineOnlyFailure exception) whose own Produces entries the
// CANDIDATE leg's decoded body did not all yield -- ExtractRESTID found
// no value at a declared entry's path. A body that does not decode at
// all refuses as RESTRefusalCandidateBodyUndecodable instead.
// Fires from cmd/go-api-rest-prove's own proveOneRESTRequest, AFTER the
// candidate leg answered (unlike RESTRefusalIDBindingUnresolved above,
// which fires before either leg of a CONSUMER is ever called): a
// producer that cannot resolve its own declared id would otherwise leave
// a later consumer refused by RESTRefusalIDBindingUnresolved with no
// visible reason on the request that actually failed to produce it.
const RESTRefusalCandidateProducerUnresolved = "rest_candidate_body_did_not_produce_the_declared_id"

// RESTRefusalCandidateIterationExhausted is the named refusal reason for
// an iterating consumer (RESTIDBinding.Candidates > 0) whose own declared
// Produces did not resolve for ANY of the bounded candidates it tried --
// distinct from RESTRefusalCandidateProducerUnresolved (a single-shot
// consumer's own producer failure, one candidate, no alternative tried):
// this name marks that the whole bounded search came up empty, with
// every individual attempt's own reason recorded separately (see
// cmd/go-api-rest-prove's own outcome.Attempts).
const RESTRefusalCandidateIterationExhausted = "rest_candidate_iteration_exhausted_no_candidate_produced_the_declared_ids"

// RESTRefusalNoLegProducedTheDeclaredID is the named refusal reason for
// one attempt of a bounded-candidate request (RESTIDBinding.Candidates >
// 0) in JSON body mode where a declared Produces id is yielded by
// NEITHER leg's body AND the declared list is an empty array on both
// legs (RESTIDListIsEmpty); any other shape without the id refuses as
// RESTRefusalDeclaredIDListUnrecognised instead. The attempt loses before its bodies are compared
// and writes no receipt, and the search moves to the next candidate: a
// candidate with no rows on either plane (a repository with no data on
// this route, for example) is not the candidate the request's siblings
// need. An id yielded by the candidate leg alone does not refuse: the
// attempt is compared, so that difference between the planes surfaces.
const RESTRefusalNoLegProducedTheDeclaredID = "rest_neither_leg_produced_the_declared_id"

// RESTRefusalCandidateBodyUndecodable is the named refusal reason for a
// StatusOnly, declared-failing-baseline request whose CANDIDATE leg
// answered its wanted status with a body that does not decode as JSON.
// Distinct from RESTRefusalCandidateProducerUnresolved (a decoded body
// with no value at a declared path, i.e. no data): a body that does not
// decode is a failure of the candidate plane, and a bounded candidate
// search ends on it instead of moving to the next candidate.
const RESTRefusalCandidateBodyUndecodable = "rest_candidate_body_did_not_decode"

// RESTRefusalDeclaredIDListUnrecognised is the named refusal reason for a
// request whose declared Produces id is carried by neither leg it reads,
// where the body does NOT carry the declared list as an empty array: the
// list is missing, null or not an array, or its elements lack the id. It
// is a failure of a plane, never "no rows", so a bounded candidate search
// ends on it instead of moving to the next candidate.
const RESTRefusalDeclaredIDListUnrecognised = "rest_body_did_not_carry_the_declared_id_list"
