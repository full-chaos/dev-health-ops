package goapiproof

import (
	"bytes"
	"context"
	"testing"
)

func TestMintViaAllowlistedHelper_RefusesAnUnknownName(t *testing.T) {
	if _, err := MintViaAllowlistedHelper(context.Background(), "not-a-real-helper", nil); err == nil {
		t.Fatal("want an error for a helper name outside the allowlist")
	}
}

func TestMintViaAllowlistedHelper_NeverReachesExecForAnUnknownName(t *testing.T) {
	// A name that would be a valid, executable path if it ever reached
	// exec.CommandContext (it does not: MintViaAllowlistedHelper never
	// treats helperName as a path at all -- see its own doc comment).
	if _, err := MintViaAllowlistedHelper(context.Background(), "/bin/echo", []string{"should never run"}); err == nil {
		t.Fatal("want an error: a path is not an allowlisted helper name, however valid it looks as a path")
	}
}

func TestMintLimitedWriter_CapsBytesWithoutError(t *testing.T) {
	var buf bytes.Buffer
	lw := &mintLimitedWriter{w: &buf, max: 4}
	if _, err := lw.Write([]byte("abcdefgh")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if buf.String() != "abcd" {
		t.Fatalf("captured = %q, want %q", buf.String(), "abcd")
	}
}

func TestMintLimitedWriter_WritesUnderTheCapUnchanged(t *testing.T) {
	var buf bytes.Buffer
	lw := &mintLimitedWriter{w: &buf, max: 100}
	if _, err := lw.Write([]byte("token-value")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if buf.String() != "token-value" {
		t.Fatalf("captured = %q, want %q", buf.String(), "token-value")
	}
}
