package contracts

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// Lines inserted at the top of the provisioning files move every line in them and change no code. The provisioning
// consumer check must measure as many anchors on such a tree as on the real one (a cited line that moved must not be
// skipped).
func TestProvisioningConsumerAnchorsSurviveALineShift(t *testing.T) {
	document := loadClassRegistryDocument(t)
	var files []string
	for _, class := range document.Classes {
		for _, consumer := range class.Consumers {
			if consumer.Anchor != nil && consumer.Anchor.Repo == "ops" {
				files = append(files, consumer.Anchor.Path)
			}
		}
	}
	root := routescan.ShiftedTree(t, testRoot(t), nil, files, map[string]string{
		"internal/auth/authapi/provisioning_serviceaccount.go":      "package ",
		"internal/auth/authapi/provisioning_serviceaccount_keys.go": "package ",
		"internal/auth/authapi/provisioning_grant.go":               "package ",
		"internal/auth/authapi/provisioning_read.go":                "package ",
	})
	_, real := provisioningConsumerProblems(t, document, testRoot(t))
	problems, checked := provisioningConsumerProblems(t, document, root)
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d provisioning consumer anchors:\n%s", len(problems), strings.Join(problems, "\n"))
	}
	if checked["workload_token"] < 7 || checked["workload_token"] != real["workload_token"] {
		t.Fatalf("%d workload_token provisioning consumers were measured on the shifted tree, %d on the real tree: a moved anchor was skipped", checked["workload_token"], real["workload_token"])
	}
}
