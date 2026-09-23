package cli

import (
	"errors"
	"flag"
	"testing"
)

func TestWrapFlagParseError_NilStaysNil(t *testing.T) {
	if err := WrapFlagParseError(nil); err != nil {
		t.Fatalf("WrapFlagParseError(nil) = %v, want nil", err)
	}
}

func TestWrapFlagParseError_HelpStaysUnwrapped(t *testing.T) {
	err := WrapFlagParseError(flag.ErrHelp)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("WrapFlagParseError(flag.ErrHelp) = %v, want flag.ErrHelp", err)
	}
	var usage *FlagUsageError
	if errors.As(err, &usage) {
		t.Fatalf("WrapFlagParseError(flag.ErrHelp) wrapped as FlagUsageError, want the bare sentinel")
	}
}

func TestWrapFlagParseError_OtherErrorsWrap(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(nil)
	parseErr := fs.Parse([]string{"-not-a-real-flag"})
	if parseErr == nil {
		t.Fatal("fs.Parse([\"-not-a-real-flag\"]) = nil, want an unknown-flag error")
	}
	err := WrapFlagParseError(parseErr)
	var usage *FlagUsageError
	if !errors.As(err, &usage) {
		t.Fatalf("WrapFlagParseError(%v) = %v (%T), want a *FlagUsageError", parseErr, err, err)
	}
	if !errors.Is(err, parseErr) {
		t.Fatalf("WrapFlagParseError result does not unwrap to the original Parse error")
	}
	if err.Error() != parseErr.Error() {
		t.Fatalf("Error() = %q, want the original Parse error text %q", err.Error(), parseErr.Error())
	}
}

func TestExitForVerbError(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(nil)
	unknownFlagErr := WrapFlagParseError(fs.Parse([]string{"-not-a-real-flag"}))

	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, ExitOK},
		{"help", flag.ErrHelp, ExitOK},
		{"unknown flag", unknownFlagErr, ExitUsage},
		{"other failure", errors.New("dependency down"), ExitFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ExitForVerbError(tc.err); got != tc.want {
				t.Fatalf("ExitForVerbError(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}
