package goapiproof

import (
	"context"
	"testing"
)

func TestMintViaAllowlistedHelper_RefusesAnUnknownName(t *testing.T) {
	if _, err := MintViaAllowlistedHelper(context.Background(), "not-a-real-helper", nil); err == nil {
		t.Fatal("want an error for a helper name outside the allowlist")
	}
}

func TestMintViaAllowlistedHelper_RefusesAPathLookingName(t *testing.T) {
	// A name that would be a valid, executable path if this function ever
	// treated helperName as one (it does not -- see its own doc comment:
	// only the two literal names below select a mintcli package to call).
	if _, err := MintViaAllowlistedHelper(context.Background(), "/bin/echo", []string{"should never run"}); err == nil {
		t.Fatal("want an error: a path is not an allowlisted helper name, however valid it looks as a path")
	}
}
