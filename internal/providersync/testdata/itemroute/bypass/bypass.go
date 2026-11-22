// Package bypass holds the bypass forms of CHAOS-14292: each returns the bare sentinel of a decode failure that the AST census accepted.
package bypass

import (
	"bytes"
	"encoding/json"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type item struct{ A int }

const route = "/bypass"

// decodeItem is an item decoder by signature: an allowed origin, its callers are checked.
func decodeItem(raw json.RawMessage) (item, error) {
	var out item
	if json.Unmarshal(raw, &out) != nil {
		return out, providerfoundation.ErrNormalizationInvalid
	}
	return out, nil
}

// Form 1: the decode in an assignment before the if, success first or the failure in else or switch.
func assignSuccessFirst(data json.RawMessage) (item, error) {
	var out item
	err := json.Unmarshal(data, &out)
	if err == nil {
		return out, nil
	}
	return out, providerfoundation.ErrNormalizationInvalid
}

func assignDecode(data json.RawMessage) (item, error) {
	var out item
	err := json.NewDecoder(bytes.NewReader(data)).Decode(&out)
	if err == nil {
		return out, nil
	}
	return out, providerfoundation.ErrNormalizationInvalid
}

func assignElse(data json.RawMessage) (item, error) {
	var out item
	err := json.NewDecoder(bytes.NewReader(data)).Decode(&out)
	if err == nil {
		return out, nil
	} else {
		return out, providerfoundation.ErrNormalizationInvalid
	}
}

func assignSwitch(data json.RawMessage) (item, error) {
	var out item
	err := json.Unmarshal(data, &out)
	switch {
	case err != nil:
		return out, providerfoundation.ErrNormalizationInvalid
	}
	return out, nil
}

// Form 2: the sentinel returned through a local binding or two forwarding helpers.
func sentinelLocal(data json.RawMessage) (item, error) {
	var out item
	if err := json.Unmarshal(data, &out); err != nil {
		bad := providerfoundation.ErrNormalizationInvalid
		return out, bad
	}
	return out, nil
}

func forwardOne(err error) error { return err }
func forwardTwo(err error) error { return forwardOne(err) }

func sentinelNested(data json.RawMessage) (item, error) {
	var out item
	if err := json.Unmarshal(data, &out); err != nil {
		return out, forwardTwo(forwardOne(providerfoundation.ErrNormalizationInvalid))
	}
	return out, nil
}

func phiMerge(data json.RawMessage, flag bool) (item, error) {
	var out item
	err := json.Unmarshal(data, &out)
	var result error
	if err != nil {
		result = providerfoundation.ErrNormalizationInvalid
	} else if flag {
		result = providerfoundation.ErrInvalidScope
	}
	return out, result
}

// Form 3: an item decoder reached through a var initializer, a function-value alias or a helper that returns its result.
var initItem, initErr = decodeItem(json.RawMessage(`[]`))

var aliasDecode = decodeItem

func callerVar(data json.RawMessage) error {
	_, err := aliasDecode(data)
	return err
}

func callerDirect(data json.RawMessage) (item, error) {
	return decodeItem(data)
}

// Form 4: a caller of a shared decoder (listed as an origin by the test) that returns the bare error.
func sharedHelper(raw []byte, target any) error {
	if err := json.Unmarshal(raw, target); err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func newSharedCaller(raw []byte) error {
	var v map[string]any
	if err := sharedHelper(raw, &v); err != nil {
		return err
	}
	return nil
}

// Form 5: a RouteFailure the decode failure does not reach, or an outer wrapper that converts back to the sentinel.
func callerRouteConditional(data json.RawMessage, flag bool) error {
	_, err := decodeItem(data)
	if flag {
		return providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err)
	}
	return err
}

func backToSentinel(error) error { return providerfoundation.ErrNormalizationInvalid }

func callerRouteDropped(data json.RawMessage) error {
	_, err := decodeItem(data)
	if err != nil {
		return backToSentinel(providerfoundation.RouteFailure(route, providerfoundation.ReasonBodyDecode, err))
	}
	return nil
}
