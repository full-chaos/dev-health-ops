package apiservice

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
)

var acrPrincipalA = uuid.MustParse("00000000-0000-0000-0000-000000000000")

func liveRow(id uuid.UUID, audiences ...string) acrWorkloadRow {
	return acrWorkloadRow{principal: id, audiences: audiences}
}

// Exactly one live acr-api row gives its wire principal id; none or several is not configured.
func TestACRSubjectOfNeedsExactlyOneLiveRow(t *testing.T) {
	other := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	both := []string{"auth-service", "dev-health-api"}
	for name, tc := range map[string]struct {
		rows []acrWorkloadRow
		want string
		err  error
	}{
		"one live row":                   {[]acrWorkloadRow{liveRow(acrPrincipalA, both...)}, "prn_AAAAAAAAAAAAAAAAAAAAAA", nil},
		"no rows":                        {nil, "", acr.ErrSubjectNotConfigured},
		"two live rows":                  {[]acrWorkloadRow{liveRow(acrPrincipalA, both...), liveRow(other, both...)}, "", acr.ErrSubjectNotConfigured},
		"workload or principal disabled": {[]acrWorkloadRow{{principal: acrPrincipalA, audiences: both, disabled: true}}, "", acr.ErrSubjectNotConfigured},
		"audience lacks dev-health-api":  {[]acrWorkloadRow{liveRow(acrPrincipalA, "auth-service")}, "", acr.ErrSubjectNotConfigured},
		"no audiences":                   {[]acrWorkloadRow{liveRow(acrPrincipalA)}, "", acr.ErrSubjectNotConfigured},
		"a disabled twin is not live":    {[]acrWorkloadRow{{principal: other, audiences: both, disabled: true}, liveRow(acrPrincipalA, both...)}, "prn_AAAAAAAAAAAAAAAAAAAAAA", nil},
		"live row, other audience twin":  {[]acrWorkloadRow{liveRow(other, "auth-service"), liveRow(acrPrincipalA, both...)}, "prn_AAAAAAAAAAAAAAAAAAAAAA", nil},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := acrSubjectOf(tc.rows)
			if !errors.Is(err, tc.err) || got != tc.want {
				t.Fatalf("got %q %v, want %q %v", got, err, tc.want, tc.err)
			}
		})
	}
}

func TestNoPoolMeansNoRegistrySource(t *testing.T) {
	if source := acrSubjectSourceOf(nil, "auth"); source != nil {
		t.Fatalf("a nil pool gave a source %v: the routes must refuse every workload token as not_configured", source)
	}
}
