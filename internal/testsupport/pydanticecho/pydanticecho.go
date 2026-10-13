// Package pydanticecho strips what the Go api deliberately does not send from
// a FastAPI/pydantic validation body: the "input" and "ctx" members of each
// error. FastAPI echoes the submitted value there, which can be a password or
// the whole request, so the Go api never writes them. A parity test compares
// the frozen or live Python body after Strip with the Go body after Strip.
package pydanticecho

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// Strip removes input and ctx from every pydantic error (an object with "type"
// and "loc") anywhere in a JSON body, and re-renders it. A body that is not
// JSON is returned unchanged.
func Strip(body string) string {
	if !strings.Contains(body, `"loc"`) {
		return body
	}
	value, err := pyjson.DecodeString(body)
	if err != nil {
		return body
	}
	if !strip(value) {
		return body // nothing to remove: keep the bytes, so a spacing difference still shows
	}
	// Render in the spacing the body came in, so a spacing difference between
	// two planes still shows after the strip.
	if strings.Contains(body, `"loc": `) {
		out, err := pyjson.Dumps(value)
		if err != nil {
			return body
		}
		return out
	}
	out, err := pyjson.Marshal(value)
	if err != nil {
		return body
	}
	return string(out)
}

func strip(value pyjson.Value) (changed bool) {
	switch typed := value.(type) {
	case *pyjson.Object:
		_, hasType := typed.Get("type")
		_, hasLoc := typed.Get("loc")
		if hasType && hasLoc {
			for _, key := range []string{"input", "ctx"} {
				if _, present := typed.Get(key); present {
					typed.Delete(key)
					changed = true
				}
			}
		}
		for _, key := range typed.Keys() {
			member, _ := typed.Get(key)
			if strip(member) {
				changed = true
			}
		}
	case []pyjson.Value:
		for _, item := range typed {
			if strip(item) {
				changed = true
			}
		}
	}
	return changed
}
