// Package control holds legal code the item route census must not refuse (CHAOS-14292).
package control

import (
	"encoding/json"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type item struct{ A int }

const route = "/control"

func decodeItem(raw json.RawMessage) (item, error) {
	var out item
	if json.Unmarshal(raw, &out) != nil {
		return out, providerfoundation.ErrNormalizationInvalid
	}
	return out, nil
}

// C1: the decode in the if initializer, the failure returns RouteFailure.
func routedIfInit(data json.RawMessage) (item, error) {
	var out item
	if err := json.Unmarshal(data, &out); err != nil {
		return out, providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err)
	}
	return out, nil
}

// C2: the decode in an assignment, the failure returns RouteFailure.
func routedAssign(data json.RawMessage) (item, error) {
	var out item
	err := json.Unmarshal(data, &out)
	if err != nil {
		return out, providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err)
	}
	return out, nil
}

// C3: the failure in else and in a switch, every failure arm routed.
func routedElse(data json.RawMessage) (item, error) {
	var out item
	err := json.Unmarshal(data, &out)
	if err == nil {
		return out, nil
	} else {
		return out, providerfoundation.ResponseInvalid(nil, providerfoundation.ReasonBodyDecode)
	}
}

func routedSwitch(data json.RawMessage) (item, error) {
	var out item
	err := json.Unmarshal(data, &out)
	switch {
	case err != nil:
		return out, providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err)
	}
	return out, nil
}

// C4: one outer wrapper that keeps the routed error.
func routedWrapped(data json.RawMessage) error {
	_, err := decodeItem(data)
	if err != nil {
		return fmt.Errorf("page: %w", providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err))
	}
	return nil
}

// C5: a caller of a decoder that returns RouteFailure on err and nil elsewhere.
func callerRoutes(raws []json.RawMessage) ([]item, error) {
	var out []item
	for _, raw := range raws {
		one, err := decodeItem(raw)
		if err != nil {
			return nil, providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err)
		}
		out = append(out, one)
	}
	return out, nil
}

// C6: a shared helper (listed as an origin by the test) whose caller wraps the route.
func sharedHelper(raw []byte, target any) error {
	if err := json.Unmarshal(raw, target); err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func sharedCallerRoutes(raw []byte) error {
	var v map[string]any
	if err := sharedHelper(raw, &v); err != nil {
		return providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err)
	}
	return nil
}

// C7: an edge listed by the test (the identity compare) returns the bare error.
func identityEdge(raw []byte) error {
	var v map[string]any
	return sharedHelper(raw, &v)
}

// C8: a plain validation sentinel in a caller that also routes a decode.
func validate(it item) error {
	if it.A == 0 {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func callerRoutesAndValidates(data json.RawMessage) error {
	it, err := decodeItem(data)
	if err != nil {
		return providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err)
	}
	return validate(it)
}

// C9: a decode whose failure is ignored (a probe), with a plain envelope sentinel next to it.
func decodeIgnored(data json.RawMessage, envelope *bool) error {
	var desk item
	if json.Unmarshal(data, &desk) == nil && desk.A == 1 {
		return nil
	}
	if envelope == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

// C10: a function listed as absorbed by the test (it cannot fail) and its caller.
func cannotFail(data json.RawMessage) error {
	var out item
	if err := json.Unmarshal(data, &out); err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func absorbedCaller(data json.RawMessage) error {
	return cannotFail(data)
}
