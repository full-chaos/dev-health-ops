package gitlabcode

import (
	"context"
	"math/big"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries PRIVATE-TOKEN to another origin: the base
// answers a redirect and the other origin sees no request at all (D4124 class sweep).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	client := Client{BaseURL: probe.Base.URL, Token: "SECRET-TOKEN", HTTP: probe.Client()}
	_, _ = client.ListProjects(context.Background(), ListOptions{MaxProjects: big.NewInt(100)})
	probe.Assert(t)
}

// The client restcore builds when none is supplied (production: syncadmin GitLabHTTP and credentials repoClient pass
// through; nil here) follows no redirect (restcore.go default branch).
func TestTheDefaultClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	client := Client{Token: "SECRET-TOKEN", BaseURL: probe.Base.URL}
	_, _ = client.ListProjects(context.Background(), ListOptions{MaxProjects: big.NewInt(100)})
	probe.Assert(t)
}
