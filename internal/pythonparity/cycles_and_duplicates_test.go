package pythonparity

import (
	"strings"
	"testing"
)

// TestCircularReferenceIsAnErrorNotACrash pins the P1 fix.
//
// json.dumps defaults to check_circular=True and raises
// `ValueError: Circular reference detected`. Before this, Go recursed until
// `fatal error: stack overflow` -- which is NOT a panic. It cannot be
// recovered, so it takes the whole process down. A CPython ValueError had
// become a process kill, and no caller could defend against it.
func TestCircularReferenceIsAnErrorNotACrash(t *testing.T) {
	t.Run("slice containing itself", func(t *testing.T) {
		cyclic := make([]any, 1)
		cyclic[0] = cyclic

		_, err := MarshalPythonJSONInsertionOrder(cyclic)
		if err == nil {
			t.Fatal("expected a circular-reference error; got a successful encode")
		}
		if !strings.Contains(err.Error(), "circular reference") {
			t.Errorf("error should name the circular reference so the caller knows "+
				"what to fix; got %q", err)
		}
	})

	t.Run("object reachable from itself through a slice", func(t *testing.T) {
		inner := make([]any, 1)
		object := OrderedObject{{Key: "self", Value: inner}}
		inner[0] = object

		if _, err := MarshalPythonJSONInsertionOrder(object); err == nil {
			t.Error("expected a circular-reference error through an OrderedObject; " +
				"the cycle does not have to be slice-to-slice")
		}
	})
}

// TestSharedContainersAreNotCycles is the other half of the P1 fix, and the
// half a naive implementation gets wrong.
//
// A visited-set that never removed entries would reject these: the same
// container appears twice, but nested inside ITSELF nowhere. CPython's
// `markers` dict adds on entry and deletes on exit for exactly this reason, and
// json.dumps encodes both of these happily. Rejecting them would be a
// regression dressed as a safety check.
func TestSharedContainersAreNotCycles(t *testing.T) {
	shared := []any{1, 2}

	t.Run("same slice twice as siblings", func(t *testing.T) {
		encoded, err := MarshalPythonJSONInsertionOrder([]any{shared, shared})
		if err != nil {
			t.Fatalf("shared sibling containers are not a cycle, but were "+
				"rejected: %v", err)
		}
		if got, want := string(encoded), "[[1, 2], [1, 2]]"; got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("prefix re-slice nested inside its parent", func(t *testing.T) {
		// `parent[:1]` shares a backing POINTER with `parent`. Keying cycle
		// detection on the pointer alone would call this a cycle; it is not,
		// and json.dumps encodes it. Length is part of the key for this case.
		parent := make([]any, 2)
		parent[0] = 1
		parent[1] = 2
		nested := []any{parent[:1], parent}

		if _, err := MarshalPythonJSONInsertionOrder(nested); err != nil {
			t.Errorf("a prefix re-slice shares a backing pointer with its parent "+
				"but is not a cycle; got %v", err)
		}
	})

	t.Run("same object twice as siblings", func(t *testing.T) {
		object := OrderedObject{{Key: "k", Value: 1}}
		if _, err := MarshalPythonJSONInsertionOrder([]any{object, object}); err != nil {
			t.Errorf("the same OrderedObject twice is not a cycle; got %v", err)
		}
	})

	// The PRODUCTION shape, and a different code path from the one above.
	//
	// An evidence list is a []OrderedObject, not a []any, so it takes the typed
	// slice branch. A Go port that REUSES a row value rather than rebuilding it
	// would produce exactly this, and an over-eager cycle check would reject a
	// payload CPython accepts -- silently breaking the real evidence path while
	// every data-shaped corpus stayed green.
	//
	// The distinction this pins is "no cycle" versus "no repetition". They are
	// easy to conflate when touching the marker discipline, and only one of them
	// is what json.dumps enforces. lane-3092 probed this against its own port;
	// it is a permanent test rather than a throwaway because the next person to
	// touch enterContainer needs it.
	t.Run("shared rows in a typed evidence list", func(t *testing.T) {
		row := OrderedObject{{Key: "a", Value: 1.0}}
		encoded, err := MarshalPythonJSONInsertionOrder([]OrderedObject{row, row})
		if err != nil {
			t.Fatalf("a reused evidence row is repetition, not a cycle: %v", err)
		}
		if got, want := string(encoded), `[{"a": 1.0}, {"a": 1.0}]`; got != want {
			t.Errorf("got %s, want %s", got, want)
		}

		nested := OrderedObject{{Key: "n", Value: 2.0}}
		outer := []OrderedObject{{{Key: "wrap", Value: nested}}, {{Key: "wrap", Value: nested}}}
		if _, err := MarshalPythonJSONInsertionOrder(outer); err != nil {
			t.Errorf("a reused NESTED object is also repetition, not a cycle: %v", err)
		}
	})
}

// TestDuplicateKeysAreRefused pins the P2 fix.
//
// A Python dict cannot hold duplicate keys, so an OrderedObject that does is
// not a dict and there is NO json.dumps call these bytes could equal. Before
// this, `{"a": 1, "a": 2}` encoded successfully -- output CPython cannot
// produce, from an encoder whose entire contract is equalling one CPython call.
//
// Refused rather than collapsed, and the error says what a dict would have
// done, because "collapse it yourself" is only actionable if the caller knows
// which value and which position survive.
func TestDuplicateKeysAreRefused(t *testing.T) {
	_, err := MarshalPythonJSONInsertionOrder(OrderedObject{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
		{Key: "a", Value: 3},
	})
	if err == nil {
		t.Fatal("expected duplicate keys to be refused; got a successful encode " +
			"of bytes no json.dumps call can produce")
	}
	for _, want := range []string{"duplicate key", `"a"`, "0", "2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name the key and BOTH positions so the caller "+
				"can find them; %q missing from %q", want, err)
		}
	}

	// Keys that differ in Go but are INDISTINGUISHABLE once encoded.
	//
	// AppendPythonJSONString maps every invalid UTF-8 byte to U+FFFD, so these
	// two distinct Go strings emit the same key. Scanning the raw strings let
	// them through and produced `{"\ufffd": 1, "\ufffd": 2}`; CPython, where
	// both decode to the same character, collapses to `{"\ufffd": 2}`.
	//
	// The general rule this pins: two keys that cannot be told apart in the
	// OUTPUT are duplicates in the output, whatever they were on the way in.
	t.Run("keys that collide only after encoding", func(t *testing.T) {
		_, err := MarshalPythonJSONInsertionOrder(OrderedObject{
			{Key: string([]byte{0xff}), Value: 1},
			{Key: string([]byte{0xfe}), Value: 2},
		})
		if err == nil {
			t.Error("two invalid-UTF-8 keys both encode as \\ufffd and must be " +
				"refused as duplicates; emitting them produces bytes no Python " +
				"dict can hold")
		}
	})

	// The single-member and distinct-key cases must stay unaffected -- the
	// duplicate scan is skipped for len <= 1, and that shortcut is easy to get
	// wrong in the direction of skipping too much.
	for _, testCase := range []struct {
		name  string
		value OrderedObject
	}{
		{"empty", OrderedObject{}},
		{"single member", OrderedObject{{Key: "a", Value: 1}}},
		{"distinct keys", OrderedObject{{Key: "a", Value: 1}, {Key: "b", Value: 2}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := MarshalPythonJSONInsertionOrder(testCase.value); err != nil {
				t.Errorf("unexpected refusal: %v", err)
			}
		})
	}
}
