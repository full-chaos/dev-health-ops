package deploymentstatus

import "testing"

func TestClassifyEveryStoredSpelling(t *testing.T) {
	for status, want := range map[string]Class{
		"success": Succeeded, "SUCCESS": Succeeded, " success ": Succeeded,
		"failure": Failed, "failed": Failed, "error": Failed, " Failed ": Failed, "ERROR": Failed,
		"canceled": Canceled, "cancelled": Canceled, "Canceled": Canceled,
		"inactive": Other, "blocked": Other, "created": Other, "running": Other, "pending": Other,
		"queued": Other, "in_progress": Other, "waiting": Other, "": Other, "weird": Other,
	} {
		if got := Classify(status); got != want {
			t.Errorf("Classify(%q) = %s, want %s", status, got, want)
		}
	}
}

func TestOnlyFinalOutcomesAreTerminal(t *testing.T) {
	for class, want := range map[Class]bool{Succeeded: true, Failed: true, Canceled: false, Other: false} {
		if got := class.Terminal(); got != want {
			t.Errorf("%s.Terminal() = %v, want %v", class, got, want)
		}
	}
}
