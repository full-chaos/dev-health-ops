package pydanticecho

import "testing"

func TestStripRemovesInputAndCtxOnly(t *testing.T) {
	body := `{"detail":[{"type":"missing","loc":["body","x"],"msg":"Field required","input":{"a":1},"ctx":{"k":"v"}}]}`
	want := `{"detail":[{"type":"missing","loc":["body","x"],"msg":"Field required"}]}`
	if got := Strip(body); got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestStripKeepsBodiesThatCarryNothingToRemove(t *testing.T) {
	for _, body := range []string{
		`{"input":1}`,
		`{"detail":[{"type":"missing","loc":["a"],"msg":"m"}]}`,
		`{"detail": [{"type": "missing", "loc": ["a"], "msg": "m"}]}`,
		`not json "loc"`,
	} {
		if got := Strip(body); got != body {
			t.Fatalf("changed %s to %s", body, got)
		}
	}
}

func TestStripKeepsTheSpacingItCameIn(t *testing.T) {
	body := `{"detail": [{"type": "missing", "loc": ["a"], "msg": "m", "input": null}]}`
	want := `{"detail": [{"type": "missing", "loc": ["a"], "msg": "m"}]}`
	if got := Strip(body); got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}
