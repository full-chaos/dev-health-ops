package externalingest

import (
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// ErrStoredJSONColumn is where Python's status.py raises while it reads a batch
// row (`_parse_json`, `_recompute_scope_response`): an unhandled exception, the
// api's generic 500. It is a typed refusal here, answered by unhandledError().
var ErrStoredJSONColumn = errors.New("external_ingest_batches JSON column is not readable as status.py reads it")

// parseStoredDict is status.py's `_parse_json(m[column])` for one JSON column of
// an external_ingest_batches row: SQL NULL and JSON null are None, a JSON object
// is itself, and any other value raises in Python. The driver hands Python an
// already-parsed value, so only a JSON *string* goes through `json.loads` (and
// then `dict()`); a list, number or boolean reaches `json.loads` as a non-str
// and raises TypeError.
//
// Named divergence: Python's `dict(json.loads(text))` also builds a dict from a
// non-empty JSON list of pairs held in a stored JSON string (`[["k", 1]]`,
// `["ab"]`); no writer stores that shape (they all store an object), and it is
// refused here (the unhandled 500) rather than ported. An empty list or empty
// string held in a string is `{}` in both.
func parseStoredDict(raw []byte) (*pyjson.Object, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	decoded, err := pyjson.Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStoredJSONColumn, err)
	}
	switch typed := decoded.(type) {
	case nil:
		return nil, nil
	case *pyjson.Object:
		return typed, nil
	case string:
		inner, err := pyjson.DecodeString(typed)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrStoredJSONColumn, err)
		}
		switch value := inner.(type) {
		case *pyjson.Object:
			return value, nil
		case []pyjson.Value:
			if len(value) == 0 {
				return pyjson.NewObject(), nil
			}
		case string:
			if value == "" {
				return pyjson.NewObject(), nil
			}
		}
		return nil, fmt.Errorf("%w: a stored string that is not an object", ErrStoredJSONColumn)
	default:
		return nil, fmt.Errorf("%w: not an object", ErrStoredJSONColumn)
	}
}

// recomputeScopeResponse is status.py's `_recompute_scope_response(scope)`: None
// stays None, and a dict is read lazily per field, as Python does, in the model's
// declared field order:
//
//	repo_ids / team_ids = list(scope.get(k) or [])  (a str is its characters, a
//	                       dict its keys; a number or true raises TypeError) and
//	                       pydantic then requires every item to be a str;
//	window_*            = _parse_dt(scope.get(k)): None stays None, anything else
//	                       datetime.fromisoformat(str(value)), which raises when
//	                       it does not parse; a naive value is UTC;
//	capped_*            = bool(scope.get(k, False)) over any JSON value.
func recomputeScopeResponse(scope *pyjson.Object) (pyjson.Value, error) {
	if scope == nil {
		return nil, nil
	}
	object := pyjson.NewObject()
	for _, key := range []string{"repoIds", "teamIds"} {
		list, err := scopeStringList(scope, key)
		if err != nil {
			return nil, err
		}
		object.Set(key, list)
	}
	for _, key := range []string{"windowStartedAt", "windowEndedAt"} {
		value, err := scopeDatetime(scope, key)
		if err != nil {
			return nil, err
		}
		object.Set(key, value)
	}
	for _, key := range []string{"cappedDays", "cappedRepos"} {
		value, _ := scope.Get(key)
		object.Set(key, pyjson.Truthy(value))
	}
	return object, nil
}

func scopeStringList(scope *pyjson.Object, key string) ([]pyjson.Value, error) {
	value, _ := scope.Get(key)
	if !pyjson.Truthy(value) {
		return []pyjson.Value{}, nil
	}
	var items []pyjson.Value
	switch typed := value.(type) {
	case []pyjson.Value:
		items = typed
	case string:
		for _, r := range pyjson.Runes(typed) {
			items = append(items, pyjson.FromRunes([]rune{r}))
		}
	case *pyjson.Object:
		for _, name := range typed.Keys() {
			items = append(items, name)
		}
	default:
		return nil, fmt.Errorf("%w: %s is not iterable", ErrStoredJSONColumn, key)
	}
	out := make([]pyjson.Value, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%w: %s holds a value that is not a str", ErrStoredJSONColumn, key)
		}
		out = append(out, text)
	}
	return out, nil
}

func scopeDatetime(scope *pyjson.Object, key string) (pyjson.Value, error) {
	value, _ := scope.Get(key)
	if value == nil {
		return nil, nil
	}
	parsed, ok := pytime.FromISOFormat(pyjson.Str(value))
	if !ok {
		return nil, fmt.Errorf("%w: %s is not an ISO 8601 datetime", ErrStoredJSONColumn, key)
	}
	if !parsed.Aware {
		// _parse_dt_required: `dt.replace(tzinfo=timezone.utc)` on a naive value.
		parsed.Aware, parsed.Offset, parsed.OffsetMicro = true, 0, 0
	}
	return pytime.Pydantic(parsed), nil
}
