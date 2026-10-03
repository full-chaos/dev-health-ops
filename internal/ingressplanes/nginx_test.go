package ingressplanes

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"
)

func mustRender(t *testing.T, c Contract, o Options) string {
	t.Helper()
	out, err := Render(c, o)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return string(out)
}

// TestLocationsAreWrittenAsIngressNginxWritesThem: table in, locations out.
// The form is `~* "^<path>"` for every rule, the default rule too, and the
// order is the longest path first, ties by the greater path first.
func TestLocationsAreWrittenAsIngressNginxWritesThem(t *testing.T) {
	contract := Contract{SchemaVersion: SchemaVersion, RegexMode: true, Rules: []Rule{
		anchored("/graphql$", PlaneQueryAPI),
		rule("/", PathTypePrefix, PlaneGoAPI),
		anchored("/api/v1/people/[^/]+/metric$", PlaneQueryAPI),
		anchored("/ready$", PlaneGoAPI),
		anchored("/a/bb$", PlaneQueryAPI),
		anchored("/a/ba$", PlaneGoAPI),
		anchored(`/a\.b$`, PlaneGoAPI),
	}}
	want := []Location{
		{`~* "^/api/v1/people/[^/]+/metric$"`, PlaneQueryAPI},
		{`~* "^/graphql$"`, PlaneQueryAPI},
		{`~* "^/ready$"`, PlaneGoAPI},
		{`~* "^/a\\.b$"`, PlaneGoAPI}, // six characters as written, like the two below; `\` sorts above `/`
		{`~* "^/a/bb$"`, PlaneQueryAPI},
		{`~* "^/a/ba$"`, PlaneGoAPI},
		{`~* "^/"`, PlaneGoAPI},
	}
	got := Locations(contract)
	if len(got) != len(want) {
		t.Fatalf("got %d locations, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("location %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestRenderIsTheSameForAnyRuleOrder: the order of the rules in the contract
// has no meaning, so every order of one table gives the same bytes.
func TestRenderIsTheSameForAnyRuleOrder(t *testing.T) {
	contract := checkedInContract(t)
	first := mustRender(t, contract, DefaultOptions())
	if again := mustRender(t, contract, DefaultOptions()); again != first {
		t.Fatalf("two renders of one contract differ")
	}
	shuffled := Contract{SchemaVersion: contract.SchemaVersion, RegexMode: contract.RegexMode, Rules: append([]Rule(nil), contract.Rules...)}
	random := rand.New(rand.NewSource(8363))
	for round := 0; round < 5; round++ {
		random.Shuffle(len(shuffled.Rules), func(i, j int) { shuffled.Rules[i], shuffled.Rules[j] = shuffled.Rules[j], shuffled.Rules[i] })
		if mustRender(t, shuffled, DefaultOptions()) != first {
			t.Fatalf("round %d: another order of the same rules gives other bytes", round)
		}
	}
}

// TestRenderWritesEachPlaneToItsOwnUpstream: a location of a plane proxies to
// that plane's upstream and to no other, with the options the caller gives.
func TestRenderWritesEachPlaneToItsOwnUpstream(t *testing.T) {
	options := Options{Listen: "127.0.0.1:18000", Upstreams: map[string]string{PlaneGoAPI: "10.0.0.1:1", PlaneQueryAPI: "10.0.0.2:2"}, RunDir: "/run/router"}
	file := readRouterFile(t, mustRender(t, validContract(), options))
	want := map[string]string{
		`^/api/v1/people/[^/]+/metric$`: "10.0.0.2:2",
		`^/api/v1/auth/login$`:          "10.0.0.1:1",
		`^/graphql$`:                    "10.0.0.2:2",
		`^/a\.b$`:                       "10.0.0.1:1",
		`^/`:                            "10.0.0.1:1",
	}
	if len(file.locations) != len(want) {
		t.Fatalf("got %d locations, want %d", len(file.locations), len(want))
	}
	for _, location := range file.locations {
		if location.modifier != "~*" {
			t.Errorf("location %s: modifier %q, want ~*", location.pattern, location.modifier)
		}
		if upstream, ok := want[location.pattern]; !ok || upstream != location.upstream {
			t.Errorf("location %s: upstream %q, want %q", location.pattern, location.upstream, upstream)
		}
	}
	if listens := children(file.server, "listen"); len(listens) != 1 || len(listens[0].args) != 1 || listens[0].args[0] != "127.0.0.1:18000" {
		t.Errorf("listen: got %+v", listens)
	}
	text := mustRender(t, validContract(), options)
	if strings.Contains(text, "resolver") {
		t.Errorf("an empty Resolver must write no resolver directive")
	}
	if !strings.Contains(text, "pid /run/router/nginx.pid;") || !strings.Contains(text, "proxy_temp_path /run/router/proxy_temp;") {
		t.Errorf("RunDir must hold the pid file and the temporary paths")
	}
}

// TestRenderRefusesIncompleteOptions: a file with no listen address, no run
// directory or a plane with no upstream must not be written.
func TestRenderRefusesIncompleteOptions(t *testing.T) {
	for name, change := range map[string]func(*Options){
		"no listen":           func(o *Options) { o.Listen = "" },
		"no run directory":    func(o *Options) { o.RunDir = "" },
		"no go-api upstream":  func(o *Options) { delete(o.Upstreams, PlaneGoAPI) },
		"no query upstream":   func(o *Options) { o.Upstreams[PlaneQueryAPI] = "" },
		"no upstreams at all": func(o *Options) { o.Upstreams = nil },
	} {
		options := DefaultOptions()
		change(&options)
		if out, err := Render(validContract(), options); err == nil {
			t.Errorf("%s: Render must refuse, got %d bytes", name, len(out))
		}
	}
	if out, err := Render(validContract(), DefaultOptions()); err != nil || !bytes.Contains(out, []byte("resolver "+DockerResolver+" ")) {
		t.Errorf("the default options must render with Docker's resolver: err=%v", err)
	}
}
