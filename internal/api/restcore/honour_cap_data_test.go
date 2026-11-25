package restcore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestARefusedWaitIsCarriedAsDataAndNeverInTheText(t *testing.T) {
	for _, wait := range []time.Duration{6 * time.Second, 24 * time.Hour} {
		var sleeps []time.Duration
		transport := &answerTransport{answers: []int{429}}
		_, err := waitCore(wait, transport, &sleeps).Get(context.Background(), "http://provider.test/x", "list")
		var typed *Error
		if !errors.As(err, &typed) {
			t.Fatalf("wait %s: err = %v", wait, err)
		}
		if typed.RetryAfter != wait {
			t.Fatalf("wait %s: RetryAfter %s, WANT the provider's wait", wait, typed.RetryAfter)
		}
		if want := "gitlab rate limit exceeded on list"; typed.Message != want {
			t.Fatalf("wait %s: message %q, want %q", wait, typed.Message, want)
		}
	}
}

func TestASleptWaitLeavesNoRetryAfterOnALaterError(t *testing.T) {
	var sleeps []time.Duration
	transport := &answerTransport{answers: []int{429, 404}}
	_, err := waitCore(2*time.Second, transport, &sleeps).Get(context.Background(), "http://provider.test/x", "list")
	var typed *Error
	if !errors.As(err, &typed) || typed.RetryAfter != 0 {
		t.Fatalf("err = %v, want an error with no RetryAfter", err)
	}
}
