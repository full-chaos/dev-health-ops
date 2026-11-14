package pybody

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// MaxSurrogateErrors caps the 422 entries one body can raise: a body of
// millions of bad strings must not become a megabyte answer. pydantic has no
// cap; this is a deliberate departure.
const MaxSurrogateErrors = 100

// ReadChecked is Read for the go-api body entry points. A decoded body that
// holds a lone surrogate anywhere (a string value, a list item, a dict key or
// value, at any depth) is refused with one string_unicode error per position,
// in document order, before any field validation runs. The errors never carry
// the input and never name a surrogate key in loc ("[key]" stands for it).
// auth-service reads its bodies with Read and is unchanged.
func ReadChecked(r *http.Request) (Body, Outcome, []Error, error) {
	body, outcome, failure, err := Read(r)
	if err != nil {
		return body, outcome, nil, err
	}
	if outcome != Ready {
		if failure != nil {
			return body, outcome, []Error{*failure}, nil
		}
		return body, outcome, nil, nil
	}
	if errs := RefuseLoneSurrogates(body.Value); len(errs) > 0 {
		return Body{}, DecodeFailed, errs, nil
	}
	return body, Ready, nil, nil
}

// RefuseLoneSurrogates walks a decoded JSON value and returns a string_unicode
// error for each string or object key that holds a lone surrogate (at most
// MaxSurrogateErrors). The loc starts at "body".
func RefuseLoneSurrogates(value pyjson.Value) []Error {
	w := surrogateWalk{}
	w.walk(value, []pyjson.Value{"body"})
	return w.errs
}

type surrogateWalk struct{ errs []Error }

func (w *surrogateWalk) full() bool { return len(w.errs) >= MaxSurrogateErrors }

func (w *surrogateWalk) refuse(loc []pyjson.Value) {
	w.errs = append(w.errs, Error{Type: "string_unicode", Loc: loc,
		Msg: "Input should be a valid string, unable to parse raw data as a unicode string"})
}

func at(loc []pyjson.Value, part pyjson.Value) []pyjson.Value {
	out := make([]pyjson.Value, len(loc)+1)
	copy(out, loc)
	out[len(loc)] = part
	return out
}

func (w *surrogateWalk) walk(value pyjson.Value, loc []pyjson.Value) {
	if w.full() {
		return
	}
	switch typed := value.(type) {
	case string:
		if pyjson.HasSurrogate(typed) {
			w.refuse(loc)
		}
	case []pyjson.Value:
		for index, item := range typed {
			w.walk(item, at(loc, int64(index)))
		}
	case *pyjson.Object:
		for _, key := range typed.Keys() {
			if w.full() {
				return
			}
			next := at(loc, key)
			if pyjson.HasSurrogate(key) {
				next = at(loc, "[key]")
				w.refuse(next)
			}
			item, _ := typed.Get(key)
			w.walk(item, next)
		}
	}
}
