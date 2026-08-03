package providersync

import (
	"reflect"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type singleErrorUnwrapper interface {
	Unwrap() error
}

type multiErrorUnwrapper interface {
	Unwrap() []error
}

// gitLabErrorTreeOnlyProviderClasses reports whether every leaf in err's full
// wrapping/join tree is a ProviderError whose class is explicitly allowed.
// Inspecting only the first errors.As match is unsafe: HTTP reservation cleanup
// can join a soft request failure with a fatal budget or lease failure.
func gitLabErrorTreeOnlyProviderClasses(
	err error,
	allowedClasses ...providerfoundation.ErrorClass,
) bool {
	allowed := make(map[providerfoundation.ErrorClass]struct{}, len(allowedClasses))
	for _, class := range allowedClasses {
		allowed[class] = struct{}{}
	}
	return gitLabErrorTreeOnlyLeaves(err, func(leaf error) bool {
		providerErr, ok := leaf.(*providerfoundation.ProviderError)
		if !ok {
			return false
		}
		_, ok = allowed[providerErr.Class]
		return ok
	})
}

func gitLabErrorTreeOnlyLeaves(err error, allowed func(error) bool) bool {
	if gitLabErrorIsNil(err) || allowed == nil {
		return false
	}
	switch wrapped := err.(type) {
	case multiErrorUnwrapper:
		children := wrapped.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !gitLabErrorTreeOnlyLeaves(child, allowed) {
				return false
			}
		}
		return true
	case singleErrorUnwrapper:
		if child := wrapped.Unwrap(); child != nil {
			return gitLabErrorTreeOnlyLeaves(child, allowed)
		}
	}
	return allowed(err)
}

// gitLabErrorIsNil rejects typed-nil error implementations before invoking
// an Unwrap method on them. Interface comparison alone cannot detect a nil
// pointer stored in an error interface, and a custom unwrapper may dereference
// its receiver and panic instead of allowing the route to fail closed.
func gitLabErrorIsNil(err error) bool {
	if err == nil {
		return true
	}
	value := reflect.ValueOf(err)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
