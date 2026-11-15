package deploymentstatus

import (
	"slices"
	"strings"
	"testing"
)

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

// The SQL side of the canceled test binds CanceledStatuses, so the list must be
// exactly the set Classify calls Canceled, in normalized form.
func TestCanceledStatusesAreExactlyWhatClassifyCancels(t *testing.T) {
	for _, status := range CanceledStatuses {
		if status != strings.ToLower(strings.TrimSpace(status)) {
			t.Errorf("CanceledStatuses entry %q is not normalized", status)
		}
		if Classify(status) != Canceled {
			t.Errorf("CanceledStatuses entry %q classifies as %s", status, Classify(status))
		}
	}
	for _, status := range []string{
		"canceled", "cancelled", "Canceled", " CANCELLED\t", "cancel", "aborted", "abort", "skipped", "stopped",
		"revoked", "superseded", "inactive", "success", "failure", "failed", "error", "", "weird",
	} {
		inList := slices.Contains(CanceledStatuses, strings.ToLower(strings.TrimSpace(status)))
		if canceled := Classify(status) == Canceled; canceled != inList {
			t.Errorf("Classify(%q) canceled=%t, in CanceledStatuses=%t", status, canceled, inList)
		}
	}
}
