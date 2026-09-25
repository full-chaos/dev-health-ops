package apiservice

import (
	"fmt"
	"regexp"
)

// registerResponseModelRoutes adds routes to responseModelRoutes. It is how a
// route family declares which of its routes FastAPI writes as a response_model,
// WITHOUT editing the shared table in responsemodel.go: put the entries in a
// per-family file (responsemodel_<family>.go) and register them from its init():
//
//	func init() {
//		registerResponseModelRoutes(map[string]bool{
//			"GET /api/v1/things":  true,
//			"POST /api/v1/things": true,
//		})
//	}
//
// (CHAOS-6722.) responsemodel.go's single table was touched by 25 of the last
// 120 merged PRs, and gofmt re-aligns every line of a map literal when one key
// gets longer, so each touch conflicted with the next PR. A per-family file is
// touched only by that family's PRs.
//
// Registration runs at init time, before any request reads the table (package
// variables are initialised before every init function). A malformed key, or a
// key already registered anywhere (this table or another family's file), panics
// at startup with the key named: a duplicate would otherwise let one file's
// value silently win.
func registerResponseModelRoutes(routes map[string]bool) {
	for key, model := range routes {
		if !responseModelKeyPattern.MatchString(key) {
			panic(fmt.Sprintf("apiservice: response model route %q is not \"METHOD /path\"", key))
		}
		if existing, dup := responseModelRoutes[key]; dup {
			panic(fmt.Sprintf("apiservice: response model route %q registered twice (already %t, now %t)", key, existing, model))
		}
		responseModelRoutes[key] = model
	}
}

// responseModelKeyPattern is the shape of every key of the table: an upper-case
// HTTP method, one space, an absolute path.
var responseModelKeyPattern = regexp.MustCompile(`^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS) /\S*$`)
