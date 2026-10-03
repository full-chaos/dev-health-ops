package logging

import "reflect"

// An error chain is walked with a bound and under recover (CHAOS-7933): an error whose Unwrap returns itself (or a cycle) would
// make errors.Is / errors.As / a plain unwrap loop never return, and an Unwrap, Is or SQLState method on a typed nil receiver
// panics. The classifiers of this package see the chain only through boundedChain and the direct-comparison helpers below, so a
// hostile error cannot stall or crash a log call.
const (
	maxChainDepth = 32
	maxChainNodes = 64
)

// boundedChain lists the errors reachable from err by Unwrap (single or multiple), outermost first, at most maxChainNodes of them,
// each at most once; a panicking Unwrap ends that branch.
func boundedChain(err error) []error {
	if err == nil {
		return nil
	}
	chain := make([]error, 0, 8)
	type entry struct {
		err   error
		depth int
	}
	queue := []entry{{err, 0}}
	for len(queue) > 0 && len(chain) < maxChainNodes {
		current := queue[0]
		queue = queue[1:]
		if current.err == nil || current.depth > maxChainDepth || seen(chain, current.err) {
			continue
		}
		chain = append(chain, current.err)
		for _, next := range unwrapped(current.err) {
			queue = append(queue, entry{next, current.depth + 1})
		}
	}
	return chain
}

// seen reports whether err is already in chain, comparing by identity where the dynamic type is comparable (a panic on an
// uncomparable one counts as not seen: the node bound still stops the walk).
func seen(chain []error, err error) (found bool) {
	defer func() {
		if recover() != nil {
			found = false
		}
	}()
	for _, existing := range chain {
		if existing == err {
			return true
		}
	}
	return false
}

// unwrapped calls err's Unwrap method (either form) under recover.
func unwrapped(err error) (next []error) {
	defer func() {
		if recover() != nil {
			next = nil
		}
	}()
	switch typed := err.(type) {
	case interface{ Unwrap() error }:
		if inner := typed.Unwrap(); inner != nil {
			return []error{inner}
		}
	case interface{ Unwrap() []error }:
		return typed.Unwrap()
	}
	return nil
}

// chainIs is errors.Is over a bounded chain: each element is compared directly and through its own Is method, never through
// its Unwrap.
func chainIs(chain []error, target error) bool {
	for _, err := range chain {
		if directIs(err, target) {
			return true
		}
	}
	return false
}

func directIs(err, target error) (matched bool) {
	defer func() {
		if recover() != nil {
			matched = false
		}
	}()
	if err == target {
		return true
	}
	if is, ok := err.(interface{ Is(error) bool }); ok {
		return is.Is(target)
	}
	return false
}

// chainAs is errors.As over a bounded chain: target is a non-nil pointer to an interface or to a type implementing error; the
// first element assignable to it is stored.
func chainAs(chain []error, target any) bool {
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return false
	}
	want := value.Type().Elem()
	for _, err := range chain {
		if reflect.TypeOf(err).AssignableTo(want) {
			value.Elem().Set(reflect.ValueOf(err))
			return true
		}
		if as, ok := err.(interface{ As(any) bool }); ok && callAs(as, target) {
			return true
		}
	}
	return false
}

func callAs(as interface{ As(any) bool }, target any) (matched bool) {
	defer func() {
		if recover() != nil {
			matched = false
		}
	}()
	return as.As(target)
}

// safeSQLState reads a SQLSTATE under recover.
func safeSQLState(state sqlStater) (code string) {
	defer func() {
		if recover() != nil {
			code = ""
		}
	}()
	return state.SQLState()
}

// innermost is the last element of the first chain of single Unwraps: the Go type that names the failure.
func innermost(err error) error {
	for depth := 0; depth < maxChainDepth; depth++ {
		next := unwrappedOne(err)
		if next == nil {
			break
		}
		err = next
	}
	return err
}

func unwrappedOne(err error) (next error) {
	defer func() {
		if recover() != nil {
			next = nil
		}
	}()
	if typed, ok := err.(interface{ Unwrap() error }); ok {
		return typed.Unwrap()
	}
	return nil
}

// isLeaf reports whether err unwraps to nothing in either form: its Unwrap() error answers nil (or it has none) and its
// Unwrap() []error holds no non-nil element (or it has none). errors.Unwrap alone answers nil for every multi-error
// (errors.Join, fmt.Errorf with two %w), so it cannot decide this. A panicking Unwrap is not a leaf (CHAOS-8127 r2).
func isLeaf(err error) (leaf bool) {
	defer func() {
		if recover() != nil {
			leaf = false
		}
	}()
	if single, ok := err.(interface{ Unwrap() error }); ok && single.Unwrap() != nil {
		return false
	}
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		for _, inner := range multi.Unwrap() {
			if inner != nil {
				return false
			}
		}
	}
	return true
}
