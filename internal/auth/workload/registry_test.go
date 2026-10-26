package workload

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

func TestDefaultRegistryIsTheFiveSeededWorkloads(t *testing.T) {
	specs, err := DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range specs {
		names = append(names, s.Name)
		if strings.Contains(s.Name, "worker") || strings.Contains(s.Name, "billing") {
			t.Errorf("registry row %q: no worker or billing workload until it makes a credentialed call", s.Name)
		}
		if len(s.AllowedActions) != 0 {
			t.Errorf("registry row %q holds actions %v; the catalog has no workload action yet", s.Name, s.AllowedActions)
		}
	}
	want := []string{"api", "auth-service", "acr-api", "web", "query-api"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("registry names = %v, want %v", names, want)
	}
	for _, s := range specs {
		switch s.Name {
		case "web", "query-api", "auth-service":
			if len(s.Audiences) != 0 {
				t.Errorf("%q is a registry identity and holds no workload audience, got %v", s.Name, s.Audiences)
			}
		}
	}
}

func TestRegistryAudiencesAreOnlyWorkloadTokenAudiences(t *testing.T) {
	var af audiencesFile
	if err := json.Unmarshal(vocabulary.Audiences, &af); err != nil {
		t.Fatal(err)
	}
	specs, _ := DefaultRegistry()
	for _, s := range specs {
		for _, a := range s.Audiences {
			ok := false
			for _, row := range af.Audiences {
				for _, c := range row.Classes {
					if row.Audience == a && c == "workload_token" {
						ok = true
					}
				}
			}
			if !ok {
				t.Errorf("%q names audience %q, which is not a workload_token audience", s.Name, a)
			}
		}
	}
}

func registryWith(t *testing.T, workloads string) []byte {
	t.Helper()
	return []byte(`{"schema_version":"workload-registry.v1","description":"d","workloads":` + workloads + `}`)
}

func TestLoadRegistryRefusesEveryDefect(t *testing.T) {
	cat := policycatalog.Default()
	cases := map[string]string{
		"audience that is not a workload_token audience": `[{"name":"x1","audiences":["query-api"],"allowed_actions":[]}]`,
		"unknown audience":        `[{"name":"x1","audiences":["nope"],"allowed_actions":[]}]`,
		"unknown action":          `[{"name":"x1","audiences":[],"allowed_actions":["a.b"]}]`,
		"wildcard action":         `[{"name":"x1","audiences":[],"allowed_actions":["*"]}]`,
		"duplicate name":          `[{"name":"x1","audiences":[],"allowed_actions":[]},{"name":"x1","audiences":[],"allowed_actions":[]}]`,
		"bad name":                `[{"name":"X","audiences":[],"allowed_actions":[]}]`,
		"missing audiences":       `[{"name":"x1","allowed_actions":[]}]`,
		"missing allowed_actions": `[{"name":"x1","audiences":[]}]`,
		"duplicate audience":      `[{"name":"x1","audiences":["auth-service","auth-service"],"allowed_actions":[]}]`,
		"unknown member":          `[{"name":"x1","audiences":[],"allowed_actions":[],"secret":"s"}]`,
		"no workloads":            `[]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadRegistry(registryWith(t, body), vocabulary.Audiences, cat); !errors.Is(err, ErrRegistryInvalid) {
				t.Fatalf("LoadRegistry accepted %s: %v", name, err)
			}
		})
	}
	if _, err := LoadRegistry([]byte(`{"schema_version":"workload-registry.v2","workloads":[]}`), vocabulary.Audiences, cat); !errors.Is(err, ErrRegistryInvalid) {
		t.Fatalf("wrong schema_version accepted: %v", err)
	}
	good := `[{"name":"x1","audiences":["auth-service"],"allowed_actions":["org.read"]}]`
	if _, err := LoadRegistry(registryWith(t, good), vocabulary.Audiences, cat); err != nil {
		t.Fatalf("the accepting row was refused: %v", err)
	}
}

func TestRegistryHoldsNoSecretShape(t *testing.T) {
	for _, bad := range []string{"svc_", "fcacr_", "fcpush_", "eyJ", "BEGIN ", "password", "secret", "hash"} {
		if strings.Contains(string(vocabulary.WorkloadRegistryV1), bad) {
			t.Errorf("the registry contract contains %q", bad)
		}
	}
}
