package goapiproof

import (
	"fmt"
	"strings"
)

// restDedupKeySeparator matches orderInsensitiveKey's own separator
// (0x1f, ASCII unit separator): it cannot appear in a %v-formatted JSON
// scalar, so joined key parts cannot collide with each other.
const restDedupKeySeparator = "\x1f"

// RESTDedupKeyField is the synthetic object field InjectRESTDedupKeys
// writes. Never a real wire field name (every REST response type in this
// service is a Go struct with an explicit json tag, none of which is
// this string), so it cannot collide with one.
const RESTDedupKeyField = "_rest_dedup_key"

// InjectRESTDedupKeys decorates every object element of the list at
// listPath (dotted, index-free, matching BaselineDefect.Paths' own form,
// e.g. "items") with a synthetic RESTDedupKeyField built by joining
// keyFields' values -- so a REST BaselineDefect can point
// WorkGraphEdgeDedupShape (built for a GraphQL list with a single id
// field) at a REST list whose natural identity is a COMPOSITE, e.g.
// drilldown/prs's PRItem, identified by (repo_id, number) and carrying no
// single id field of its own. WorkGraphEdgeDedupShape itself needs no
// change: it reads whatever field name its IDField names, and this
// function's whole job is to make that field exist.
//
// Mutates data in place (list elements are shared map[string]any values)
// and also returns it, so a caller can chain it directly onto
// DecodeRESTSnapshot's result. Must be called identically on BOTH the
// baseline and candidate snapshots before Compare runs, or the injected
// key itself would read as a one-sided extra field.
//
// An element that is not an object, or is missing any of keyFields, is
// left undecorated: WorkGraphEdgeDedupShape's own edgeObjectAndID then
// reports it as missing the id field, the same safe "explains nothing"
// default every shape in this package uses rather than guessing a key.
func InjectRESTDedupKeys(data any, listPath string, keyFields []string) any {
	if listPath == "" || len(keyFields) == 0 {
		return data
	}
	// listAtDottedPath (workgraphedgedup.go) navigates via citedSegments,
	// which requires and strips a "data." prefix -- the convention every
	// BaselineDefect.Paths/Shape path in this package uses because it
	// navigates the SNAPSHOT's Data field as if rooted at "$.data".
	// listPath itself stays bare ("items", not "data.items") because a
	// REST corpus entry has no such envelope to name; the prefix is
	// added here, once, rather than pushed onto every caller.
	list, ok := listAtDottedPath(data, "data."+listPath)
	if !ok {
		return data
	}
	for _, element := range list {
		object, isObject := element.(map[string]any)
		if !isObject {
			continue
		}
		parts := make([]string, len(keyFields))
		complete := true
		for i, field := range keyFields {
			value, present := object[field]
			if !present {
				complete = false
				break
			}
			parts[i] = fmt.Sprintf("%v", value)
		}
		if complete {
			object[RESTDedupKeyField] = strings.Join(parts, restDedupKeySeparator)
		}
	}
	return data
}
