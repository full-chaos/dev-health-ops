package providerfoundation

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// mappingRejections reads credential_mapping_rejected_total: "provider/field" -> count.
func mappingRejections(t *testing.T) map[string]int64 {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := meterReader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != credentialMappingRejectedName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				provider, _ := point.Attributes.Value("provider")
				field, _ := point.Attributes.Value("missing_field")
				out[provider.AsString()+"/"+field.AsString()] += point.Value
			}
		}
	}
	return out
}

func movedBy(t *testing.T, act func()) map[string]int64 {
	t.Helper()
	before := mappingRejections(t)
	act()
	after := mappingRejections(t)
	moved := map[string]int64{}
	for key, value := range after {
		if delta := value - before[key]; delta != 0 {
			moved[key] = delta
		}
	}
	return moved
}

// TestRecordCredentialMappingRejectedNamesTheFirstAbsentField pins
// _record_mapping_rejected: the label is the first required field that is
// absent, in the caller's order, and "unknown" when none is.
func TestRecordCredentialMappingRejectedNamesTheFirstAbsentField(t *testing.T) {
	present := func(name string) MappingField { return MappingField{Name: name, Present: true} }
	absent := func(name string) MappingField { return MappingField{Name: name} }
	for _, c := range []struct {
		name   string
		fields []MappingField
		want   string
	}{
		{"first absent", []MappingField{absent("a"), absent("b")}, "x/a"},
		{"later absent", []MappingField{present("a"), absent("b"), absent("c")}, "x/b"},
		{"none absent", []MappingField{present("a"), present("b")}, "x/unknown"},
		{"no fields", nil, "x/unknown"},
	} {
		moved := movedBy(t, func() { RecordCredentialMappingRejected(context.Background(), "x", c.fields...) })
		if len(moved) != 1 || moved[c.want] != 1 {
			t.Errorf("%s: moved %v, want only %s", c.name, moved, c.want)
		}
	}
}

// TestNewJiraClientCountsEveryMappingItRejects: jira_credentials_from_mapping
// counts a mapping it cannot build against the first of api_token, email and
// base_url that is absent, under any accepted spelling; a complete mapping and
// another provider count nothing.
func TestNewJiraClientCountsEveryMappingItRejects(t *testing.T) {
	for _, c := range []struct {
		name     string
		provider string
		values   map[string]string
		config   map[string]string
		want     map[string]int64
	}{
		{"nothing", "jira", map[string]string{"unrelated": "x"}, nil, map[string]int64{"jira/api_token": 1}},
		{"only a token", "jira", map[string]string{"token": "t"}, nil, map[string]int64{"jira/email": 1}},
		{"token and email", "jira", map[string]string{"apiToken": "t", "email": "e@x.test"}, nil, map[string]int64{"jira/base_url": 1}},
		{"email and base url", "jira", map[string]string{"email": "e@x.test", "server_url": "https://x.test"}, nil, map[string]int64{"jira/api_token": 1}},
		{"empty token value", "jira", map[string]string{"api_token": "", "email": "e@x.test", "url": "https://x.test"}, nil, map[string]int64{"jira/api_token": 1}},
		{"base url from the config column", "jira", map[string]string{"token": "t", "email": "e@x.test"}, map[string]string{"base_url": "https://x.test"}, nil},
		{"complete", "jira", map[string]string{"token": "t", "email": "e@x.test", "url": "https://x.test"}, nil, nil},
		{"another provider", "github", map[string]string{"unrelated": "x"}, nil, nil},
	} {
		credential := testCredential(c.provider, c.values)
		credential.Config = c.config
		moved := movedBy(t, func() { _, _ = NewJiraClient(credential, &headerCaptureDoer{}, jiraTestRetry(), jiraTestLease()) })
		if len(moved) != len(c.want) {
			t.Errorf("%s: moved %v, want %v", c.name, moved, c.want)
			continue
		}
		for key, want := range c.want {
			if moved[key] != want {
				t.Errorf("%s: moved %v, want %v", c.name, moved, c.want)
			}
		}
	}
}
