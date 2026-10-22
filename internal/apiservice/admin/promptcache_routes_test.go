package admin_test

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
)

func TestPromptCacheRoutesAreMounted(t *testing.T) {
	mounted := map[string]bool{}
	for _, route := range admin.Routes(admin.Deps{Guard: policy.NewGuard(nil, nil)}) {
		mounted[route.Method+" "+route.Pattern] = true
	}
	for _, want := range []string{"GET /api/v1/admin/llm-settings/prompt-cache", "PUT /api/v1/admin/llm-settings/prompt-cache"} {
		if !mounted[want] {
			t.Errorf("admin.Routes does not mount %q", want)
		}
	}
}
