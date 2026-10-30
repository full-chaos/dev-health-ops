package audit_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

func tries(revoked, disabled drillAttemptOutcome) []drillAttemptOutcome {
	revoked.Name, disabled.Name = drillAttemptRevokedName, drillAttemptDisabledName
	return []drillAttemptOutcome{revoked, disabled}
}

func refused(reason workload.Reason) drillAttemptOutcome {
	return drillAttemptOutcome{Reason: string(reason)}
}

var accepted = drillAttemptOutcome{Accepted: true}

// Where a refusal is required (the end-of-WAL copy, or any fenced copy) each secret must be refused for ITS reason: a
// store error, a wrong reason or an acceptance is a failure.
func TestARequiredRefusalNeedsTheRightReasonPerSecret(t *testing.T) {
	for _, mode := range []struct {
		name, expect string
		fenced       bool
	}{{"end-of-WAL copy", "F", false}, {"fenced PITR copy", "T", true}, {"fenced end-of-WAL copy", "F", true}} {
		t.Run(mode.name, func(t *testing.T) {
			if err := checkAttempts(mode.expect, mode.fenced, tries(refused(workload.ReasonRevoked), refused(workload.ReasonDisabled))); err != nil {
				t.Fatalf("revoked + disabled refusals: %v", err)
			}
			for name, bad := range map[string][]drillAttemptOutcome{
				"store error on the revoked secret":  tries(drillAttemptOutcome{Reason: string(workload.ReasonStoreError)}, refused(workload.ReasonDisabled)),
				"store error on the disabled secret": tries(refused(workload.ReasonRevoked), drillAttemptOutcome{Reason: string(workload.ReasonStoreError)}),
				"swapped reasons":                    tries(refused(workload.ReasonDisabled), refused(workload.ReasonRevoked)),
				"both revoked":                       tries(refused(workload.ReasonRevoked), refused(workload.ReasonRevoked)),
				"unknown reason":                     tries(refused(workload.ReasonUnknown), refused(workload.ReasonDisabled)),
				"empty reason":                       tries(drillAttemptOutcome{}, refused(workload.ReasonDisabled)),
				"revoked secret accepted":            tries(accepted, refused(workload.ReasonDisabled)),
				"disabled secret accepted":           tries(refused(workload.ReasonRevoked), accepted),
				"both accepted":                      tries(accepted, accepted),
			} {
				err := checkAttempts(mode.expect, mode.fenced, bad)
				if err == nil {
					t.Errorf("%s: no error", name)
					continue
				}
				// the message names the secret label and the reason, never a value
				if !strings.Contains(err.Error(), "drill-11232-wl-") {
					t.Errorf("%s: the error does not name the secret: %v", name, err)
				}
			}
		})
	}
}

func TestAStoreErrorReasonIsNamedInTheFailure(t *testing.T) {
	err := checkAttempts("F", false, tries(drillAttemptOutcome{Reason: string(workload.ReasonStoreError)}, refused(workload.ReasonDisabled)))
	if err == nil || !strings.Contains(err.Error(), string(workload.ReasonStoreError)) || !strings.Contains(err.Error(), drillAttemptRevokedName) {
		t.Fatalf("err = %v, want it to name %q and %q", err, drillAttemptRevokedName, workload.ReasonStoreError)
	}
}

// The unfenced PITR copy is the measured gap: BOTH secrets must be accepted, or the drill did not reproduce the gap.
func TestTheUnfencedPITRCopyMustReproduceTheGapWithBothSecretsAccepted(t *testing.T) {
	if err := checkAttempts("T", false, tries(accepted, accepted)); err != nil {
		t.Fatalf("both accepted: %v", err)
	}
	for name, bad := range map[string][]drillAttemptOutcome{
		"only the revoked secret accepted":  tries(accepted, refused(workload.ReasonDisabled)),
		"only the disabled secret accepted": tries(refused(workload.ReasonRevoked), accepted),
		"none accepted":                     tries(refused(workload.ReasonRevoked), refused(workload.ReasonDisabled)),
		"none accepted, store errors":       tries(drillAttemptOutcome{Reason: string(workload.ReasonStoreError)}, drillAttemptOutcome{Reason: string(workload.ReasonStoreError)}),
	} {
		err := checkAttempts("T", false, bad)
		if err == nil {
			t.Errorf("%s: no error", name)
		} else if !strings.Contains(err.Error(), "gap") {
			t.Errorf("%s: the error does not say the gap was not reproduced: %v", name, err)
		}
	}
}

func TestTheOutcomeListMustHoldExactlyTheTwoSecretsInOrder(t *testing.T) {
	good := tries(refused(workload.ReasonRevoked), refused(workload.ReasonDisabled))
	for name, bad := range map[string][]drillAttemptOutcome{
		"none":        nil,
		"one":         good[:1],
		"three":       append(append([]drillAttemptOutcome(nil), good...), good[0]),
		"wrong names": {{Name: "x", Reason: "revoked"}, {Name: "y", Reason: "disabled"}},
		"swapped":     {good[1], good[0]},
	} {
		if err := checkAttempts("F", false, bad); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
