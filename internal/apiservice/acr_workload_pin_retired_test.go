package apiservice

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The AUTH_ACR_WORKLOAD_SUBJECT pin is retired: no source file names it, its gauge series is not declared, its
// chart guard is gone and the exception ledger holds no row for it.
func TestTheACRWorkloadSubjectPinIsRetired(t *testing.T) {
	root := filepath.Join("..", "..")
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if strings.Contains(read("ci/auth_exception_ledger.tsv"), "acr-workload-subject-pin") {
		t.Error("the exception ledger still has the acr-workload-subject-pin row")
	}
	if strings.Contains(read("internal/platform/otlpmetrics/labels.tsv"), "acr_workload_subject_config_invalid") {
		t.Error("labels.tsv still declares the retired gauge")
	}
	for _, gone := range []string{
		"deploy/helm/dev-health/templates/go-api-acr-workload-subject-guard.yaml",
		"deploy/helm/dev-health/go_api_acr_workload_subject_test.go",
	} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(gone))); err == nil {
			t.Errorf("%s still exists", gone)
		}
	}
	for _, source := range []string{"internal/apiservice/accessverify.go", "internal/apiservice/acr/workloadauth.go", "internal/apiservice/service.go"} {
		if strings.Contains(read(source), "AUTH_ACR_WORKLOAD_SUBJECT") {
			t.Errorf("%s still names AUTH_ACR_WORKLOAD_SUBJECT", source)
		}
	}
}
