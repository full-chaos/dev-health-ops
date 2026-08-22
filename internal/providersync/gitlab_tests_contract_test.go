package providersync

import (
	"errors"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabCICDAndTestsAliasesShareOneCompleteRouteContract(t *testing.T) {
	wantDestinations := []string{
		"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
		"test_suite_results", "test_case_results", "coverage_snapshots",
	}
	cicd, ok := Descriptor("gitlab", "cicd")
	if !ok || !cicd.RouteReady || !cicd.Plannable || cicd.Executor != ExecutorNativeGo {
		t.Fatalf("gitlab/cicd descriptor=%+v ok=%v", cicd, ok)
	}
	// `tests` is the non-plannable alias of the canonical `cicd` writer
	// (CHAOS-4054): RouteReady stays true for audit/watermark visibility, but
	// only `cicd` may be independently planned.
	tests, ok := Descriptor("gitlab", "tests")
	if !ok || !tests.RouteReady || tests.Plannable || tests.Executor != ExecutorNativeGo {
		t.Fatalf("gitlab/tests descriptor=%+v ok=%v", tests, ok)
	}
	if !reflect.DeepEqual(cicd.Destinations, wantDestinations) ||
		!reflect.DeepEqual(tests.Destinations, wantDestinations) {
		t.Fatalf("complete destinations diverged\ncicd=%v\ntests=%v", cicd.Destinations, tests.Destinations)
	}
	if !reflect.DeepEqual(
		ProviderRequestPlan("gitlab", "cicd", 3, nil),
		ProviderRequestPlan("gitlab", "tests", 3, nil),
	) {
		t.Fatal("shared aliases have different admission identities")
	}
}

func TestTestOpsRowsBindPersistedProviderToClaim(t *testing.T) {
	for _, test := range []struct {
		provider  string
		persisted string
		wantErr   bool
	}{
		{provider: "github", persisted: "github_actions"},
		{provider: "gitlab", persisted: "gitlab_ci"},
		{provider: "github", persisted: "gitlab_ci", wantErr: true},
		{provider: "gitlab", persisted: "github_actions", wantErr: true},
	} {
		t.Run(test.provider+"/"+test.persisted, func(t *testing.T) {
			err := validateTestOpsProvider(Claim{Unit: Unit{Provider: test.provider}}, test.persisted)
			if test.wantErr != errors.Is(err, providerfoundation.ErrInvalidScope) {
				t.Fatalf("error=%v wantErr=%t", err, test.wantErr)
			}
		})
	}
}
