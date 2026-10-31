package restorefence

import (
	"errors"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func declared(refusals []error, err error) bool {
	wrapped := fmt.Errorf("%w: applying the state mutation: %w", audit.ErrMutationFailed, err)
	for _, r := range refusals {
		if errors.Is(wrapped, r) {
			return true
		}
	}
	return false
}

func TestApplyDeclaresReApplyAndDryRunAsRefusals(t *testing.T) {
	for name, sentinel := range map[string]error{"already applied": errAlready, "dry run": errDryRun} {
		t.Run(name, func(t *testing.T) {
			if !declared(applyRefusals(), sentinel) {
				t.Errorf("%v is a normal exit and must log at WARN, not ERROR", sentinel)
			}
		})
	}
}

func TestApplyRealFailuresStayErrors(t *testing.T) {
	for name, failure := range map[string]error{
		"privileges": ErrPrivileges, "apply": ErrApply, "manifest": ErrManifest,
		"sql": errors.New("recording the fence (sqlstate 42501)"), "init exists": errInitExists,
	} {
		t.Run(name, func(t *testing.T) {
			if declared(applyRefusals(), failure) {
				t.Errorf("%v is a real failure and must stay at ERROR", failure)
			}
		})
	}
}

func TestInitDeclaresAnExistingFenceAsRefusal(t *testing.T) {
	if !declared(initRefusals(), errInitExists) {
		t.Error("an existing fence row is an expected refusal and must log at WARN, not ERROR")
	}
}

func TestInitRealFailuresStayErrors(t *testing.T) {
	for name, failure := range map[string]error{
		"apply": ErrApply, "recovery": errors.New("reading the timeline"), "already applied": errAlready, "dry run": errDryRun,
	} {
		t.Run(name, func(t *testing.T) {
			if declared(initRefusals(), failure) {
				t.Errorf("%v must stay at ERROR for init", failure)
			}
		})
	}
}
