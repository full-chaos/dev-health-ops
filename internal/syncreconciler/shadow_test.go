package syncreconciler

import (
	"context"
	"testing"
	"time"
)

func TestShadowUsesObserverContractWithoutRetainingBeginPath(t *testing.T) {
	stepper := &kernelStepper{}
	shadow, err := newShadow(testRegistry(t, ""), stepper)
	if err != nil {
		t.Fatal(err)
	}
	if shadow.kernel.mode != KernelModeShadow || shadow.kernel.begin != nil {
		t.Fatalf("shadow kernel = %#v", shadow.kernel)
	}

	now := time.Date(2026, time.July, 23, 0, 0, 0, 0, time.UTC)
	observation, err := shadow.Step(context.Background(), now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if stepper.calls != 1 {
		t.Fatalf("observer calls = %d, want 1", stepper.calls)
	}
	if observation.Limit != 1 {
		t.Fatalf("observation = %#v, want limit 1", observation)
	}
}

func TestShadowRejectsMissingKernel(t *testing.T) {
	var shadow *Shadow
	_, err := shadow.Step(context.Background(), time.Now().UTC(), 1)
	if err != ErrInvalidConfiguration {
		t.Fatalf("error = %v, want %v", err, ErrInvalidConfiguration)
	}
}
