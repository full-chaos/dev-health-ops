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
	strip(value)
	out, err := pyjson.Dumps(value)
	if err != nil {
		return body
	}
	return out
}

func strip(value pyjson.Value) {
	switch typed := value.(type) {
	case *pyjson.Object:
		_, hasType := typed.Get("type")
		_, hasLoc := typed.Get("loc")
		if hasType && hasLoc {
			typed.Delete("input")
			typed.Delete("ctx")
		}
		for _, key := range typed.Keys() {
			member, _ := typed.Get(key)
			strip(member)
		}
	case []pyjson.Value:
		for _, item := range typed {
			strip(item)
		}
	}
}
