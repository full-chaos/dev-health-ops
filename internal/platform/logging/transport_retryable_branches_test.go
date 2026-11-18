package logging

import (
	"errors"
	"net"
	"testing"
)

type stubTimeout struct{ timeout bool }

func (s stubTimeout) Error() string { return "stub" }
func (s stubTimeout) Timeout() bool { return s.timeout }

func TestRetryableTransportWalksEveryBranchInEitherOrder(t *testing.T) {
	var nilOp *net.OpError
	dial := &net.OpError{Op: "dial", Err: errors.New("refused")}
	read := &net.OpError{Op: "read", Err: errors.New("reset")}
	rows := map[string][]error{
		"typed-nil op / dial":            {nilOp, dial},
		"dial / typed-nil op":            {dial, nilOp},
		"non-timeout / timeout":          {stubTimeout{false}, stubTimeout{true}},
		"timeout / non-timeout":          {stubTimeout{true}, stubTimeout{false}},
		"typed-nil op / timeout":         {nilOp, stubTimeout{true}},
		"timeout / typed-nil op":         {stubTimeout{true}, nilOp},
		"non-dial op / dial op":          {read, dial},
		"dial op / non-dial op":          {dial, read},
		"non-dial op / typed-nil / dial": {read, nilOp, dial},
	}
	for name, errs := range rows {
		if !RetryableTransport(errors.Join(errs...)) {
			t.Errorf("%s: RetryableTransport = false, want true", name)
		}
	}
	for name, errs := range map[string][]error{
		"typed-nil / non-dial":   {nilOp, read},
		"non-timeout / non-dial": {stubTimeout{false}, read},
	} {
		if RetryableTransport(errors.Join(errs...)) {
			t.Errorf("%s: RetryableTransport = true, want false", name)
		}
	}
}
