package devhealth_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// CHAOS-13130: authService.networkPolicy.workloadClients admits named workloads outside the release (acr-api, which
// calls the workload-token exchange) on auth-service's INTERNAL port alone, as more `from` entries of the
// <fullname>-auth-service-internal policy. The API port's policy never changes: it stays go-api's while go-api forwards
// the auth routes.

const workloadClientsKey = "authService.networkPolicy.workloadClients"

// acrClient is the acr-api pods in the acr release's namespace.
const acrClient = `{"podSelector":{"matchLabels":{"app.kubernetes.io/name":"acr","app.kubernetes.io/component":"api"}},` +
	`"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"dev-health-acr"}}}`

// acrClientLines is acrClient as the internal policy renders it, one `from` entry.
const acrClientLines = `        - podSelector:
            matchLabels:
              app.kubernetes.io/component: api
              app.kubernetes.io/name: acr
          namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: dev-health-acr
`

func workloadClients(value string) []string {
	return []string{"--set-json", workloadClientsKey + "=" + value}
}

func renderAuthText(t *testing.T, args []string) string {
	t.Helper()
	_, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, out)
	}
	return out
}

// The default is the empty list: at every phase 0-4, and at phases 2-4 with the internal listener on (its test base,
// internalOn, needs phase >= 2), the render with the key unset, set to [] and set to null are byte-equal.
func TestAuthWorkloadClientsDefaultIsTheEmptyList(t *testing.T) {
	type form struct {
		name string
		args func(extra ...string) []string
	}
	var forms []form
	for phase := 0; phase <= 4; phase++ {
		forms = append(forms, form{fmt.Sprintf("phase %d", phase), func(extra ...string) []string { return phaseArgs(phase, extra...) }})
	}
	for _, phase := range []int{2, 3, 4} {
		forms = append(forms, form{fmt.Sprintf("phase %d internal on", phase), func(extra ...string) []string {
			return phaseArgs(phase, internalOn(`["web"]`, extra...)...)
		}})
	}
	for _, f := range forms {
		t.Run(f.name, func(t *testing.T) {
			plain := renderAuthText(t, f.args())
			for _, value := range []string{`[]`, `null`} {
				if got := renderAuthText(t, f.args(workloadClients(value)...)); got != plain {
					t.Errorf("%s=%s changes the render", workloadClientsKey, value)
				}
			}
		})
	}
}

// One entry adds exactly its lines to the internal policy's one `from`, after the peers and before the port: the rest
// of the render, the API port's policy included, is byte-equal.
func TestAuthWorkloadClientsAddOneFromEntryOnTheInternalPortOnly(t *testing.T) {
	for _, phase := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("phase %d", phase), func(t *testing.T) {
			without := renderAuthText(t, phaseArgs(phase, internalOn(`["web"]`)...))
			with := renderAuthText(t, phaseArgs(phase, internalOn(`["web"]`, workloadClients("["+acrClient+"]")...)...))
			want := insertInternalFrom(t, without, acrClientLines)
			if with != want {
				t.Errorf("the render is not the plain one plus the client's lines:\n%s", lineDiff(want, with))
			}
			assertAPIPortUnchanged(t, renderPhase(t, phase, internalOn(`["web"]`, workloadClients("["+acrClient+"]")...)...),
				renderPhase(t, phase, internalOn(`["web"]`)...))
		})
	}
}

// A podSelector alone stays in the release's namespace (no namespaceSelector line), matchExpressions render as given,
// and the entries keep their listed order.
func TestAuthWorkloadClientsRenderInOrderWithoutANamespaceSelector(t *testing.T) {
	without := renderAuthText(t, phaseArgs(3, internalOn(`["web"]`)...))
	clients := `[{"podSelector":{"matchExpressions":[{"key":"app","operator":"In","values":["tools"]}]}},` + acrClient + `]`
	with := renderAuthText(t, phaseArgs(3, internalOn(`["web"]`, workloadClients(clients)...)...))
	want := insertInternalFrom(t, without, `        - podSelector:
            matchExpressions:
            - key: app
              operator: In
              values:
              - tools
`+acrClientLines)
	if with != want {
		t.Errorf("the render is not the plain one plus both clients' lines in order:\n%s", lineDiff(want, with))
	}
	docs := renderPhase(t, 3, internalOn(`["web"]`, workloadClients(clients)...)...)
	rules, _ := at(authDoc(docs, "NetworkPolicy", internalListenerDoc), "spec", "ingress").([]any)
	if len(rules) != 1 || !reflect.DeepEqual(at(rules[0], "ports"), []any{map[string]any{"protocol": "TCP", "port": 8097}}) {
		t.Errorf("internal policy ingress = %v, want one rule on 8097 alone", rules)
	}
}

// assertAPIPortUnchanged: the API port's policy (the <fullname>-auth-service NetworkPolicy, port 8095) and Service are
// the same with and without workload clients. A client is never admitted on the API port.
func assertAPIPortUnchanged(t *testing.T, with, without []map[string]any) {
	t.Helper()
	for _, kind := range []string{"NetworkPolicy", "Service"} {
		got, want := authDoc(with, kind, "a-dev-health-auth-service"), authDoc(without, kind, "a-dev-health-auth-service")
		if want == nil || !reflect.DeepEqual(got, want) {
			t.Errorf("the API %s changes with workload clients:\n got %v\nwant %v", kind, got, want)
		}
	}
	for _, rule := range toAnySlice(at(authDoc(with, "NetworkPolicy", "a-dev-health-auth-service"), "spec", "ingress")) {
		if strings.Contains(fmt.Sprint(at(rule, "from")), "acr") {
			t.Errorf("an API port rule admits the acr client: %v", rule)
		}
	}
}

func toAnySlice(v any) []any {
	s, _ := v.([]any)
	return s
}

// Clients alone (internal.enabled, peers []) render: the internal policy's one rule admits exactly the client on the
// internal port, with no peer entry and never an empty `from` (which would admit every source); the API port is as
// it is with the internal listener off.
func TestAuthWorkloadClientsAloneOnTheInternalPort(t *testing.T) {
	for _, phase := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("phase %d", phase), func(t *testing.T) {
			docs := renderPhase(t, phase, internalOn(`[]`, workloadClients("["+acrClient+"]")...)...)
			policy := authDoc(docs, "NetworkPolicy", internalListenerDoc)
			if policy == nil {
				t.Fatal("no internal listener NetworkPolicy")
			}
			wantIngress := []any{map[string]any{
				"from": []any{map[string]any{
					"podSelector":       map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/name": "acr", "app.kubernetes.io/component": "api"}},
					"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": "dev-health-acr"}},
				}},
				"ports": []any{map[string]any{"protocol": "TCP", "port": 8097}},
			}}
			if got := at(policy, "spec", "ingress"); !reflect.DeepEqual(got, wantIngress) {
				t.Errorf("internal policy ingress = %v, want the acr client alone on 8097: %v", got, wantIngress)
			}
			if got := at(policy, "spec", "policyTypes"); !reflect.DeepEqual(got, []any{"Ingress"}) {
				t.Errorf("internal policy policyTypes = %v, want [Ingress] alone", got)
			}
			assertAPIPortUnchanged(t, docs, renderPhase(t, phase, internalBase()...))
		})
	}
}

// insertInternalFrom puts lines after the last `from` entry of the internal policy, before its ports.
func insertInternalFrom(t *testing.T, render, lines string) string {
	t.Helper()
	docs := strings.Split(render, "\n---\n")
	found := 0
	for i, doc := range docs {
		if !strings.Contains(doc, "\nkind: NetworkPolicy\n") || !strings.Contains(doc, "\n  name: "+internalListenerDoc+"\n") {
			continue
		}
		found++
		// helm 3 ends the last document without a newline, helm 4 with one.
		body, last := doc, !strings.HasSuffix(doc, "\n")
		if last {
			body += "\n"
		}
		const ports = "      ports:\n        - protocol: TCP\n          port: 8097\n"
		if strings.Count(body, ports) != 1 {
			t.Fatalf("the internal policy has %d port blocks on 8097, want 1:\n%s", strings.Count(body, ports), doc)
		}
		body = strings.Replace(body, ports, lines+ports, 1)
		if last {
			body = strings.TrimSuffix(body, "\n")
		}
		docs[i] = body
	}
	if found != 1 {
		t.Fatalf("the render has %d internal policies, want 1", found)
	}
	return strings.Join(docs, "\n---\n")
}

// lineDiff names the first line where want and got part.
func lineDiff(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")
	for i := 0; i < len(w) || i < len(g); i++ {
		var wl, gl string
		if i < len(w) {
			wl = w[i]
		}
		if i < len(g) {
			gl = g[i]
		}
		if wl != gl {
			return fmt.Sprintf("line %d: want %q, got %q", i+1, wl, gl)
		}
	}
	return "no line differs"
}

func TestAuthWorkloadClientsRefusals(t *testing.T) {
	const pods = `"podSelector":{"matchLabels":{"app":"acr"}}`
	on := func(value string) []string { return phaseArgs(3, internalOn(`["web"]`, workloadClients(value)...)...) }
	on2 := func(peers, value string) []string {
		return phaseArgs(3, internalOn(peers, workloadClients(value)...)...)
	}
	cases := map[string]struct {
		args []string
		want string
	}{
		"not a list (string)":     {on(`"acr"`), workloadClientsKey + " must be a list"},
		"not a list (map)":        {on(`{` + pods + `}`), workloadClientsKey + " must be a list"},
		"not a list (phase 0)":    {phaseArgs(0, workloadClients(`"acr"`)...), workloadClientsKey + " must be a list"},
		"entry not a map":         {on(`["acr"]`), workloadClientsKey + "[0] must be an object"},
		"no podSelector":          {on(`[{"namespaceSelector":{"matchLabels":{"a":"b"}}}]`), workloadClientsKey + "[0] needs a podSelector"},
		"empty entry":             {on(`[{}]`), workloadClientsKey + "[0] needs a podSelector"},
		"empty podSelector":       {on(`[{"podSelector":{}}]`), workloadClientsKey + "[0].podSelector needs a non-empty matchLabels or matchExpressions"},
		"empty matchLabels":       {on(`[{"podSelector":{"matchLabels":{}}}]`), workloadClientsKey + "[0].podSelector needs a non-empty"},
		"empty matchExpressions":  {on(`[{"podSelector":{"matchExpressions":[]}}]`), workloadClientsKey + "[0].podSelector needs a non-empty"},
		"podSelector not a map":   {on(`[{"podSelector":"acr"}]`), workloadClientsKey + "[0].podSelector needs a non-empty"},
		"empty namespaceSelector": {on(`[{` + pods + `,"namespaceSelector":{}}]`), workloadClientsKey + "[0].namespaceSelector needs a non-empty"},
		"null namespaceSelector":  {on(`[{` + pods + `,"namespaceSelector":null}]`), workloadClientsKey + "[0].namespaceSelector needs a non-empty"},
		"second entry checked":    {on(`[{` + pods + `},{"podSelector":{}}]`), workloadClientsKey + "[1].podSelector needs a non-empty"},
		"port key (API port)":     {on(`[{` + pods + `,"port":8095}]`), workloadClientsKey + `[0] takes only podSelector and namespaceSelector, not "port"`},
		"ports key (API port)":    {on(`[{` + pods + `,"ports":[{"protocol":"TCP","port":8095}]}]`), workloadClientsKey + `[0] takes only podSelector and namespaceSelector, not "ports"`},
		"ipBlock key":             {on(`[{` + pods + `,"ipBlock":{"cidr":"10.0.0.0/8"}}]`), workloadClientsKey + `[0] takes only podSelector and namespaceSelector, not "ipBlock"`},
		"internal listener off":   {phaseArgs(3, internalBase(workloadClients(`[{`+pods+`}]`)...)...), workloadClientsKey + " needs authService.internal.enabled"},
		"no peer and no client":   {phaseArgs(3, internalOn(`[]`)...), "authService.internal.peers or " + workloadClientsKey + " must be non-empty"},
		"no peer, clients []":     {on2(`[]`, `[]`), "authService.internal.peers or " + workloadClientsKey + " must be non-empty"},
		"no peer, clients null":   {on2(`[]`, `null`), "authService.internal.peers or " + workloadClientsKey + " must be non-empty"},
		"service off (phase 0)":   {phaseArgs(0, workloadClients(`[{`+pods+`}]`)...), workloadClientsKey + " needs authService.internal.enabled"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, c.args...)
			if err == nil || !strings.Contains(out, c.want) {
				t.Fatalf("not refused with %q (err %v): %.400s", c.want, err, out)
			}
		})
	}
}
