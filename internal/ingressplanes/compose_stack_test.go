package ingressplanes

import (
	"fmt"
	"net/netip"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// The root compose.yml is the self-hosted stack (CHAOS-8361). It runs no
// Python api: the router of this package holds host port 8000 and sends each
// request to go-api or query-api. These tests read compose.yml and hold the
// parts of that shape a routing test of the file alone cannot see: which
// service has the port, what the router mounts, what a plane publishes, who
// mounts the envelope key, what each plane waits for, and how go-api gets its
// own ClickHouse login.
//
// composeStackFindings is the check. TestComposeStackRunsTheRouterAndNoPythonAPI
// runs it on the checked-in file. TestComposeStackCheckSeesEachDefect plants
// one defect at a time in the text of the checked-in file and requires the
// finding that names it, so a check that stops seeing a defect fails here.
//
// What the check does NOT hold (a defect of these kinds leaves both tests
// green; the measurement for "every service becomes ready" is a real fresh
// start of the stack, not this file): the environment names of a service
// other than the two database role names of go-river-migrate and the names of
// go-api's ClickHouse login; the dependency edges of the services that are
// not a plane (migrate, go-river-provision, the workers); and the router's
// healthcheck and its read_only flag.

const composeFilePath = "compose.yml"

// routerService and planesNetwork are the compose names of the router and of
// the network it shares with the two planes.
const (
	routerService      = "router"
	planesNetwork      = "planes"
	keysInitService    = "envelope-keys-init"
	keysVolume         = "envelope_keys"
	routerConfigTarget = "/etc/nginx/nginx.conf"
)

// The api's own ClickHouse login: a users.d file that ClickHouse reads, with
// the password in a variable of the `clickhouse` service. They are written
// here, not taken from the file's generator (internal/storage/clickhouse).
const (
	clickHouseService    = "clickhouse"
	apiUsersFileSource   = "./docker/clickhouse-users.d/dho_api_ch.xml"
	apiUsersFileTarget   = "/etc/clickhouse-server/users.d/dho_api_ch.xml"
	apiUsersPasswordEnv  = "DHO_API_CH_PASSWORD"
	apiClickHouseUser    = "dho_api_ch"
	apiClickHouseUserKey = "DEV_HEALTH_CH_API_USER"
)

// planeHealthcheck is the one probe the planes have: the distroless images
// hold no shell and no curl, so the probe is the binary itself.
var planeHealthcheck = []string{"CMD", "/usr/local/bin/dho", "reconciler", "healthcheck"}

// proofRouteSwitches are the two switches of query-api that are not routes of
// the product: they mount measurement routes for the proof verbs, which a
// stack that serves clients does not mount.
var proofRouteSwitches = map[string]bool{
	"GO_API_PROOF_ROUTE_ENABLED":       true,
	"GO_API_PROOF_WRITE_ROUTE_ENABLED": true,
}

var routeSwitchName = regexp.MustCompile(`^GO_API_[A-Z0-9_]+_ENABLED$`)

type composeStack struct {
	Services map[string]composeStackService `yaml:"services"`
	Networks map[string]struct {
		IPAM struct {
			Config []struct {
				Subnet  string `yaml:"subnet"`
				IPRange string `yaml:"ip_range"`
			} `yaml:"config"`
		} `yaml:"ipam"`
	} `yaml:"networks"`
}

type composeStackService struct {
	Image string `yaml:"image"`
	Build *struct {
		Dockerfile string `yaml:"dockerfile"`
	} `yaml:"build"`
	Command     yaml.Node         `yaml:"command"`
	Ports       []yaml.Node       `yaml:"ports"`
	Volumes     []yaml.Node       `yaml:"volumes"`
	Networks    yaml.Node         `yaml:"networks"`
	Environment map[string]string `yaml:"environment"`
	DependsOn   map[string]struct {
		Condition string `yaml:"condition"`
	} `yaml:"depends_on"`
	Healthcheck *struct {
		Test []string `yaml:"test"`
	} `yaml:"healthcheck"`
	Restart string `yaml:"restart"`
}

// composeMount is one volume entry, in the short or the long form.
type composeMount struct {
	source, target, subpath string
	readOnly                bool
}

func (s composeStackService) mounts() ([]composeMount, error) {
	var mounts []composeMount
	for _, node := range s.Volumes {
		switch node.Kind {
		case yaml.ScalarNode:
			parts := strings.Split(node.Value, ":")
			if len(parts) < 2 || len(parts) > 3 {
				return nil, fmt.Errorf("volume %q is not source:target[:mode]", node.Value)
			}
			mount := composeMount{source: parts[0], target: parts[1]}
			if len(parts) == 3 {
				for _, option := range strings.Split(parts[2], ",") {
					if option == "ro" {
						mount.readOnly = true
					}
				}
			}
			mounts = append(mounts, mount)
		case yaml.MappingNode:
			var long struct {
				Source   string `yaml:"source"`
				Target   string `yaml:"target"`
				ReadOnly bool   `yaml:"read_only"`
				Volume   struct {
					Subpath string `yaml:"subpath"`
				} `yaml:"volume"`
			}
			if err := node.Decode(&long); err != nil {
				return nil, err
			}
			mounts = append(mounts, composeMount{source: long.Source, target: long.Target, subpath: long.Volume.Subpath, readOnly: long.ReadOnly})
		default:
			return nil, fmt.Errorf("a volume entry is neither a string nor a mapping")
		}
	}
	return mounts, nil
}

// publishedPorts returns host port -> container port of the service.
func (s composeStackService) publishedPorts() (map[string]string, error) {
	published := map[string]string{}
	for _, node := range s.Ports {
		switch node.Kind {
		case yaml.ScalarNode:
			parts := strings.Split(node.Value, ":")
			if len(parts) < 2 {
				return nil, fmt.Errorf("port %q names no host port", node.Value)
			}
			published[parts[len(parts)-2]] = strings.SplitN(parts[len(parts)-1], "/", 2)[0]
		case yaml.MappingNode:
			var long struct {
				Published string `yaml:"published"`
				Target    string `yaml:"target"`
			}
			if err := node.Decode(&long); err != nil {
				return nil, err
			}
			published[long.Published] = long.Target
		default:
			return nil, fmt.Errorf("a ports entry is neither a string nor a mapping")
		}
	}
	return published, nil
}

// networkAddresses returns network -> the fixed address the service asks for
// ("" for none). A service with no networks key is on the default network.
func (s composeStackService) networkAddresses() (map[string]string, error) {
	switch s.Networks.Kind {
	case 0:
		return map[string]string{"default": ""}, nil
	case yaml.SequenceNode:
		var names []string
		if err := s.Networks.Decode(&names); err != nil {
			return nil, err
		}
		out := map[string]string{}
		for _, name := range names {
			out[name] = ""
		}
		return out, nil
	case yaml.MappingNode:
		var long map[string]*struct {
			IPv4 string `yaml:"ipv4_address"`
		}
		if err := s.Networks.Decode(&long); err != nil {
			return nil, err
		}
		out := map[string]string{}
		for name, settings := range long {
			out[name] = ""
			if settings != nil {
				out[name] = settings.IPv4
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("networks is neither a list nor a mapping")
}

func (s composeStackService) arguments() []string {
	var arguments []string
	if s.Command.Kind == yaml.SequenceNode {
		_ = s.Command.Decode(&arguments)
	}
	return arguments
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// queryAPIRouteSwitches is every route switch of query-api, from the option
// registry `dho query-api` is configured through: a switch that is added
// there is required here with no edit of this test.
func queryAPIRouteSwitches() []string {
	var names []string
	for _, option := range config.OptionsFor(config.QueryAPIServiceName, false) {
		if routeSwitchName.MatchString(option.Env) && !proofRouteSwitches[option.Env] {
			names = append(names, option.Env)
		}
	}
	sort.Strings(names)
	return names
}

// composeStackFindings returns what is wrong with a compose file as the
// self-hosted stack, one finding per defect. An empty list is a pass.
func composeStackFindings(text []byte) ([]string, error) {
	var stack composeStack
	if err := yaml.Unmarshal(text, &stack); err != nil {
		return nil, err
	}
	if len(stack.Services) == 0 {
		return nil, fmt.Errorf("the compose file has no services")
	}
	var findings []string
	found := func(format string, args ...any) { findings = append(findings, fmt.Sprintf(format, args...)) }

	goAPIHost, goAPIPort, _ := strings.Cut(GoAPIUpstream, ":")
	queryAPIHost, queryAPIPort, _ := strings.Cut(QueryAPIUpstream, ":")
	planeListener := map[string]string{goAPIHost: goAPIPort, queryAPIHost: queryAPIPort}
	planeAddrFlag := map[string]string{goAPIHost: "--api-addr=:" + goAPIPort, queryAPIHost: "--query-addr=:" + queryAPIPort}

	names := make([]string, 0, len(stack.Services))
	for name := range stack.Services {
		names = append(names, name)
	}
	sort.Strings(names)

	// No Python api, by name, by image and by build.
	for _, name := range names {
		service := stack.Services[name]
		if name == "api" || name == "metrics-api" {
			found("python api: the service %q is back", name)
		}
		if strings.Contains(service.Image, "dev-hops-api") {
			found("python api: %s runs the Python api image %q", name, service.Image)
		}
		if service.Build != nil && strings.TrimPrefix(service.Build.Dockerfile, "./") == "docker/Dockerfile" {
			found("python api: %s builds docker/Dockerfile, the Python api image", name)
		}
	}

	// Host port 8000 is the router's, and a plane publishes no plane listener.
	var keyMounters, privateMounters []string
	planeMembers := []string{}
	for _, name := range names {
		service := stack.Services[name]
		published, err := service.publishedPorts()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for hostPort, containerPort := range published {
			if hostPort == RouterPort && name != routerService {
				found("host port: %s publishes host port %s, which is the router's", name, RouterPort)
			}
			if listener, isPlane := planeListener[name]; isPlane && containerPort == listener {
				found("plane port: %s publishes its plane listener (%s) on host port %s; only the router reaches a plane", name, listener, hostPort)
			}
		}
		mounts, err := service.mounts()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for _, mount := range mounts {
			if mount.source != keysVolume {
				continue
			}
			keyMounters = append(keyMounters, name)
			if mount.subpath == "private" {
				privateMounters = append(privateMounters, name)
			}
		}
		networks, err := service.networkAddresses()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if _, member := networks[planesNetwork]; member {
			planeMembers = append(planeMembers, name)
		}
	}
	wantMembers := []string{goAPIHost, queryAPIHost, routerService}
	sort.Strings(wantMembers)
	if !equalStrings(planeMembers, wantMembers) {
		found("planes network: its members are %v, want exactly %v", planeMembers, wantMembers)
	}

	// The router: the tested image, the checked-in generated file, read-only.
	router, hasRouter := stack.Services[routerService]
	routerAddress := ""
	if !hasRouter {
		found("router: there is no %q service", routerService)
	} else {
		if router.Image != RouterImage {
			found("router image: %q is not ingressplanes.RouterImage, the nginx build the routing test proves", router.Image)
		}
		if router.Build != nil {
			found("router: it has a build; the router runs the tested image as it is")
		}
		if len(router.Environment) != 0 {
			found("router: it has an environment; the router takes no setting and no credential")
		}
		published, _ := router.publishedPorts()
		if len(published) != 1 || published[RouterPort] != RouterPort {
			found("router port: it publishes %v, want host port %s to its port %s and nothing else", published, RouterPort, RouterPort)
		}
		mounts, _ := router.mounts()
		wantSource := "./" + RouterConfigPath
		switch {
		case len(mounts) != 1:
			found("router config: the router has %d mounts, want the generated file alone", len(mounts))
		case mounts[0].source != wantSource || mounts[0].target != routerConfigTarget:
			found("router config: the router mounts %s at %s, want the checked-in generated file %s at %s", mounts[0].source, mounts[0].target, wantSource, routerConfigTarget)
		case !mounts[0].readOnly:
			found("router config: the generated file is mounted read-write")
		}
		networks, _ := router.networkAddresses()
		if len(networks) != 1 {
			found("router network: the router is on %d networks, want the planes network alone: on a second network its address to a plane is not the trusted one", len(networks))
		}
		routerAddress = networks[planesNetwork]
		if _, err := netip.ParseAddr(routerAddress); err != nil {
			found("router address: the router has no fixed address on the planes network (%q)", routerAddress)
		}
		for _, plane := range []string{goAPIHost, queryAPIHost} {
			if router.DependsOn[plane].Condition != "service_healthy" {
				found("router order: the router waits for %s with %q, want service_healthy", plane, router.DependsOn[plane].Condition)
			}
		}
	}

	// The planes network gives the router's address to no plane.
	if network, ok := stack.Networks[planesNetwork]; !ok || len(network.IPAM.Config) != 1 {
		found("planes network: it has no single fixed subnet")
	} else if address, err := netip.ParseAddr(routerAddress); err == nil {
		subnet, subnetErr := netip.ParsePrefix(network.IPAM.Config[0].Subnet)
		dynamic, rangeErr := netip.ParsePrefix(network.IPAM.Config[0].IPRange)
		switch {
		case subnetErr != nil || rangeErr != nil:
			found("planes network: subnet %q or ip_range %q is not a prefix", network.IPAM.Config[0].Subnet, network.IPAM.Config[0].IPRange)
		case !subnet.Contains(address):
			found("planes network: the router address %s is outside the subnet %s", address, subnet)
		case dynamic.Contains(address):
			found("planes network: the router address %s is inside the ip_range %s, so a plane can take it first", address, dynamic)
		}
	}

	// The planes: where the router sends, healthy before the router starts,
	// after the key and the grants.
	for _, plane := range []string{goAPIHost, queryAPIHost} {
		service, ok := stack.Services[plane]
		if !ok {
			found("plane: the router sends to %s and there is no such service", plane)
			continue
		}
		hasAddr := false
		for _, argument := range service.arguments() {
			hasAddr = hasAddr || argument == planeAddrFlag[plane]
		}
		if !hasAddr {
			found("plane listener: %s does not run with %s, the port the router sends to", plane, planeAddrFlag[plane])
		}
		if service.Healthcheck == nil || !equalStrings(service.Healthcheck.Test, planeHealthcheck) {
			found("plane health: %s has no `dho reconciler healthcheck` probe, so the router cannot wait for it", plane)
		}
		for _, before := range []string{keysInitService, "go-river-provision", "go-river-migrate"} {
			if service.DependsOn[before].Condition != "service_completed_successfully" {
				found("plane order: %s waits for %s with %q, want service_completed_successfully", plane, before, service.DependsOn[before].Condition)
			}
		}
	}

	// Each plane waits for the stores its readiness reads. go-api's wait for
	// clickhouse is held with its ClickHouse login below. A wait for valkey of
	// any kind passes: the check is that the edge is there.
	for _, wait := range []struct{ plane, store, condition string }{
		{goAPIHost, "postgres", "service_healthy"},
		{goAPIHost, "valkey", ""},
		{queryAPIHost, "postgres", "service_healthy"},
		{queryAPIHost, clickHouseService, "service_healthy"},
	} {
		switch got := stack.Services[wait.plane].DependsOn[wait.store].Condition; {
		case got == "":
			found("plane stores: %s does not wait for %s", wait.plane, wait.store)
		case wait.condition != "" && got != wait.condition:
			found("plane stores: %s waits for %s with %q, want %s", wait.plane, wait.store, got, wait.condition)
		}
	}
	// The migration that grants runs after the step that makes the logins: it
	// grants only to a login that exists.
	if got := stack.Services["go-river-migrate"].DependsOn["go-river-provision"].Condition; got != "service_completed_successfully" {
		found("plane grants: go-river-migrate waits for go-river-provision with %q, want service_completed_successfully", got)
	}

	// The envelope key: written by the init service, private half to go-api
	// only, both halves read-only.
	if init, ok := stack.Services[keysInitService]; !ok {
		found("envelope key: there is no %s service", keysInitService)
	} else if init.Restart != "no" {
		found("envelope key: %s has restart %q, want \"no\": a refusal must not loop", keysInitService, init.Restart)
	}
	sort.Strings(keyMounters)
	wantMounters := []string{keysInitService, goAPIHost, queryAPIHost}
	sort.Strings(wantMounters)
	if !equalStrings(keyMounters, wantMounters) {
		found("envelope key: the key volume is mounted by %v, want exactly %v", keyMounters, wantMounters)
	}
	if !equalStrings(privateMounters, []string{goAPIHost}) {
		found("envelope key: the private half is mounted by %v, want go-api alone", privateMounters)
	}
	for plane, half := range map[string]string{goAPIHost: "private", queryAPIHost: "public"} {
		mounts, _ := stack.Services[plane].mounts()
		for _, mount := range mounts {
			if mount.source != keysVolume {
				continue
			}
			if mount.subpath != half {
				found("envelope key: %s mounts the %q part of the key volume, want %q only", plane, mount.subpath, half)
			}
			if !mount.readOnly {
				found("envelope key: %s mounts the %s half read-write", plane, half)
			}
		}
	}

	// go-api trusts the forwarded headers of the router's one address.
	if trusted := stack.Services[goAPIHost].Environment["TRUSTED_PROXIES"]; trusted != routerAddress || routerAddress == "" {
		found("trusted proxy: go-api has TRUSTED_PROXIES %q, want the router's one address %q and nothing wider", trusted, routerAddress)
	}

	// go-api logs in to ClickHouse as its own user, which ClickHouse declares
	// from the mounted users.d file, with the password both read from one
	// operator variable.
	clickHouse := stack.Services[clickHouseService]
	clickHouseMounts, err := clickHouse.mounts()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", clickHouseService, err)
	}
	usersFileMounted := false
	for _, mount := range clickHouseMounts {
		if mount.target != apiUsersFileTarget {
			continue
		}
		usersFileMounted = true
		if mount.source != apiUsersFileSource {
			found("api login: clickhouse mounts %s at %s, want the checked-in generated file %s", mount.source, apiUsersFileTarget, apiUsersFileSource)
		}
		if !mount.readOnly {
			found("api login: the users file is mounted read-write")
		}
	}
	if !usersFileMounted {
		found("api login: clickhouse does not mount the users file at %s, so the login %s does not exist", apiUsersFileTarget, apiClickHouseUser)
	}
	goAPIEnvironment := stack.Services[goAPIHost].Environment
	if goAPIEnvironment["DEV_HEALTH_CH_API_HOST"] != clickHouseService || goAPIEnvironment[apiClickHouseUserKey] != apiClickHouseUser || goAPIEnvironment["DEV_HEALTH_CH_API_DB"] == "" {
		found("api login: go-api has DEV_HEALTH_CH_API_HOST %q, %s %q, DEV_HEALTH_CH_API_DB %q, want host %s, user %s and a database: without them the team and identity admin routes are not mounted",
			goAPIEnvironment["DEV_HEALTH_CH_API_HOST"], apiClickHouseUserKey, goAPIEnvironment[apiClickHouseUserKey], goAPIEnvironment["DEV_HEALTH_CH_API_DB"], clickHouseService, apiClickHouseUser)
	}
	if _, both := goAPIEnvironment["API_CLICKHOUSE_URI"]; both {
		found("api login: go-api has API_CLICKHOUSE_URI beside the component names; one form only")
	}
	if password := clickHouse.Environment[apiUsersPasswordEnv]; password == "" || goAPIEnvironment["DEV_HEALTH_CH_API_PASSWORD"] != password {
		found("api login: clickhouse reads the password from %s = %q and go-api logs in with %q; both must be the same operator variable", apiUsersPasswordEnv, password, goAPIEnvironment["DEV_HEALTH_CH_API_PASSWORD"])
	}
	if stack.Services[goAPIHost].DependsOn[clickHouseService].Condition != "service_healthy" {
		found("api login: go-api waits for clickhouse with %q, want service_healthy: its readiness reads the login", stack.Services[goAPIHost].DependsOn[clickHouseService].Condition)
	}

	// The migration that grants is told the names the planes log in as.
	migrate := stack.Services["go-river-migrate"].Environment
	for plane, key := range map[string]string{goAPIHost: "API_DATABASE_ROLE", queryAPIHost: "QUERY_API_DATABASE_ROLE"} {
		role := stack.Services[plane].Environment[key]
		if role == "" || migrate[key] != role {
			found("plane grants: go-river-migrate has %s %q and %s logs in as %q: the migration grants to the name it is told, so %s is ready only when the two are the same", key, migrate[key], plane, role, plane)
		}
	}

	// Every route of query-api is on: the router sends its path there and
	// nothing else serves it.
	switches := queryAPIRouteSwitches()
	if len(switches) == 0 {
		return nil, fmt.Errorf("the option registry names no route switch of query-api: nothing was checked")
	}
	queryEnvironment := stack.Services[queryAPIHost].Environment
	for _, name := range switches {
		if queryEnvironment[name] != "true" {
			found("route switch: query-api has %s %q, want \"true\": the router sends the route's path to query-api", name, queryEnvironment[name])
		}
	}
	for name := range queryEnvironment {
		if proofRouteSwitches[name] {
			found("route switch: query-api sets %s, a proof route that a stack for clients does not mount", name)
		}
	}
	sort.Strings(findings)
	return findings, nil
}

func checkedInComposeFile(t *testing.T) []byte {
	t.Helper()
	text, err := os.ReadFile(repoFile(t, composeFilePath))
	if err != nil {
		t.Fatalf("read %s: %v", composeFilePath, err)
	}
	return text
}

// TestComposeStackRunsTheRouterAndNoPythonAPI holds the shape of the
// self-hosted stack: see composeStackFindings.
func TestComposeStackRunsTheRouterAndNoPythonAPI(t *testing.T) {
	findings, err := composeStackFindings(checkedInComposeFile(t))
	if err != nil {
		t.Fatalf("read %s: %v", composeFilePath, err)
	}
	for _, finding := range findings {
		t.Errorf("%s: %s", composeFilePath, finding)
	}
	// The mounted users file is the one that declares the user go-api logs in
	// as, and it reads the password from the variable compose gives ClickHouse.
	usersFile, err := os.ReadFile(repoFile(t, strings.TrimPrefix(apiUsersFileSource, "./")))
	if err != nil {
		t.Fatalf("read the users file compose mounts: %v", err)
	}
	for _, want := range []string{"<" + apiClickHouseUser + ">", `<password from_env="` + apiUsersPasswordEnv + `"/>`} {
		if !strings.Contains(string(usersFile), want) {
			t.Errorf("%s does not hold %s", apiUsersFileSource, want)
		}
	}
}

// TestComposeStackCheckSeesEachDefect plants one defect at a time in the text
// of the checked-in compose file and requires the finding that names it. A
// plant that changes nothing in the text fails: it measured nothing.
func TestComposeStackCheckSeesEachDefect(t *testing.T) {
	clean := string(checkedInComposeFile(t))
	routerMount := "      - ./" + RouterConfigPath + ":" + routerConfigTarget + ":ro\n"
	for _, plant := range []struct {
		name, old, new, want string
		// old2 and new2 are a second change of the same plant, for a defect
		// that is in two services at once.
		old2, new2 string
	}{
		{
			name: "a plane published on a host port",
			old:  "    ports:\n      - \"8010:8010\"\n",
			new:  "    ports:\n      - \"8010:8010\"\n      - \"8001:8000\"\n",
			want: "plane port: go-api publishes its plane listener",
		},
		{
			name: "the query plane published on a host port",
			old:  "    volumes:\n      # Only the public/ subpath: the JWKS and nothing else.\n",
			new:  "    ports:\n      - \"8090:8090\"\n    volumes:\n      # Only the public/ subpath: the JWKS and nothing else.\n",
			want: "plane port: query-api publishes its plane listener",
		},
		{
			name: "the Python api service back",
			old:  "\n  bugsink:\n",
			new:  "\n  api:\n    image: ${DEV_HEALTH_API_IMAGE:-ghcr.io/full-chaos/dev-hops-api:local}\n    build:\n      context: .\n      dockerfile: ./docker/Dockerfile\n\n  bugsink:\n",
			want: "python api: the service \"api\" is back",
		},
		{
			name: "a second service on host port 8000",
			old:  "      - \"8800:8000\"\n",
			new:  "      - \"8000:8000\"\n",
			want: "host port: bugsink publishes host port 8000",
		},
		{
			name: "the router config path wrong",
			old:  routerMount,
			new:  "      - ./contracts/ingress/v1/router.conf:" + routerConfigTarget + ":ro\n",
			want: "router config: the router mounts ./contracts/ingress/v1/router.conf",
		},
		{
			name: "the router config mounted read-write",
			old:  routerMount,
			new:  "      - ./" + RouterConfigPath + ":" + routerConfigTarget + "\n",
			want: "router config: the generated file is mounted read-write",
		},
		{
			name: "no read-only flag on the private key mount",
			old:  "        target: /run/envelope/private\n        read_only: true\n",
			new:  "        target: /run/envelope/private\n",
			want: "envelope key: go-api mounts the private half read-write",
		},
		{
			name: "the private key mounted into query-api",
			old:  "        target: /run/envelope/public\n        read_only: true\n        volume:\n          subpath: public\n",
			new:  "        target: /run/envelope/public\n        read_only: true\n        volume:\n          subpath: private\n",
			want: "envelope key: the private half is mounted by [go-api query-api]",
		},
		{
			name: "another nginx build",
			old:  "    image: " + RouterImage + "\n",
			new:  "    image: ghcr.io/nginx/nginx-unprivileged:1.30-alpine\n",
			want: "router image:",
		},
		{
			name: "go-api trusts a subnet",
			old:  "      TRUSTED_PROXIES: *router-address\n",
			new:  "      TRUSTED_PROXIES: 10.199.81.0/28\n",
			want: "trusted proxy: go-api has TRUSTED_PROXIES \"10.199.81.0/28\"",
		},
		{
			name: "go-api trusts nobody",
			old:  "      TRUSTED_PROXIES: *router-address\n",
			new:  "",
			want: "trusted proxy: go-api has TRUSTED_PROXIES \"\"",
		},
		{
			name: "the router address inside the range of the planes",
			old:  "          ip_range: 10.199.81.8/29\n",
			new:  "          ip_range: 10.199.81.0/29\n",
			want: "planes network: the router address 10.199.81.2 is inside the ip_range",
		},
		{
			name: "the router on the default network too",
			old:  "    networks:\n      planes:\n        ipv4_address: *router-address\n",
			new:  "    networks:\n      default: {}\n      planes:\n        ipv4_address: *router-address\n",
			want: "router network: the router is on 2 networks",
		},
		{
			name: "the router does not wait for a healthy query-api",
			old:  "      query-api:\n        condition: service_healthy\n",
			new:  "      query-api:\n        condition: service_started\n",
			want: "router order: the router waits for query-api with \"service_started\"",
		},
		{
			name: "the migration does not grant the query-api login",
			old:  "      QUERY_API_DATABASE_ROLE: ${QUERY_API_DATABASE_ROLE:-devhealth_query_api}\n    depends_on:\n      go-river-provision:\n",
			new:  "    depends_on:\n      go-river-provision:\n",
			want: "plane grants: go-river-migrate has QUERY_API_DATABASE_ROLE \"\"",
		},
		{
			name: "a route of query-api left off",
			old:  "      GO_API_HOME_ENABLED: \"true\"\n",
			new:  "",
			want: "route switch: query-api has GO_API_HOME_ENABLED \"\"",
		},
		{
			name: "the api's ClickHouse users file not mounted",
			old:  "      - " + apiUsersFileSource + ":" + apiUsersFileTarget + ":ro\n",
			new:  "",
			want: "api login: clickhouse does not mount the users file",
		},
		{
			name: "the users file mounted read-write",
			old:  "      - " + apiUsersFileSource + ":" + apiUsersFileTarget + ":ro\n",
			new:  "      - " + apiUsersFileSource + ":" + apiUsersFileTarget + "\n",
			want: "api login: the users file is mounted read-write",
		},
		{
			name: "another users file mounted",
			old:  "      - " + apiUsersFileSource + ":" + apiUsersFileTarget + ":ro\n",
			new:  "      - ./docker/users.xml:" + apiUsersFileTarget + ":ro\n",
			want: "api login: clickhouse mounts ./docker/users.xml",
		},
		{
			name: "go-api without its ClickHouse login",
			old:  "      DEV_HEALTH_CH_API_HOST: clickhouse\n",
			new:  "",
			want: "api login: go-api has DEV_HEALTH_CH_API_HOST \"\"",
		},
		{
			name: "go-api logs in as the general user",
			old:  "      DEV_HEALTH_CH_API_USER: dho_api_ch\n",
			new:  "      DEV_HEALTH_CH_API_USER: ${CLICKHOUSE_USER:-ch}\n",
			want: "DEV_HEALTH_CH_API_USER \"${CLICKHOUSE_USER:-ch}\"",
		},
		{
			name: "ClickHouse is not given the password of the login",
			old:  "      DHO_API_CH_PASSWORD: ${API_CLICKHOUSE_PASSWORD:-dho_api_ch}\n",
			new:  "",
			want: "api login: clickhouse reads the password from DHO_API_CH_PASSWORD = \"\"",
		},
		{
			name: "go-api and ClickHouse read two password variables",
			old:  "      DEV_HEALTH_CH_API_PASSWORD: ${API_CLICKHOUSE_PASSWORD:-dho_api_ch}\n",
			new:  "      DEV_HEALTH_CH_API_PASSWORD: ${CLICKHOUSE_PASSWORD:-ch}\n",
			want: "both must be the same operator variable",
		},
		{
			name: "go-api does not wait for clickhouse",
			old:  "      postgres:\n        condition: service_healthy\n      clickhouse:\n        condition: service_healthy\n      valkey:\n",
			new:  "      postgres:\n        condition: service_healthy\n      valkey:\n",
			want: "api login: go-api waits for clickhouse with \"\"",
		},
		{
			name: "go-api does not wait for postgres",
			old:  "      go-river-migrate:\n        condition: service_completed_successfully\n      postgres:\n        condition: service_healthy\n      clickhouse:\n        condition: service_healthy\n      valkey:\n",
			new:  "      go-river-migrate:\n        condition: service_completed_successfully\n      clickhouse:\n        condition: service_healthy\n      valkey:\n",
			want: "plane stores: go-api does not wait for postgres",
		},
		{
			name: "go-api does not wait for valkey",
			old:  "      valkey:\n        condition: service_started\n",
			new:  "",
			want: "plane stores: go-api does not wait for valkey",
		},
		{
			name: "query-api does not wait for clickhouse",
			old:  "      postgres:\n        condition: service_healthy\n      clickhouse:\n        condition: service_healthy\n\n  # The router of the stack",
			new:  "      postgres:\n        condition: service_healthy\n\n  # The router of the stack",
			want: "plane stores: query-api does not wait for clickhouse",
		},
		{
			name: "query-api does not wait for postgres",
			old:  "      go-river-migrate:\n        condition: service_completed_successfully\n      postgres:\n        condition: service_healthy\n      clickhouse:\n        condition: service_healthy\n\n  # The router of the stack",
			new:  "      go-river-migrate:\n        condition: service_completed_successfully\n      clickhouse:\n        condition: service_healthy\n\n  # The router of the stack",
			want: "plane stores: query-api does not wait for postgres",
		},
		{
			name: "query-api waits for a started postgres only",
			old:  "      postgres:\n        condition: service_healthy\n      clickhouse:\n        condition: service_healthy\n\n  # The router of the stack",
			new:  "      postgres:\n        condition: service_started\n      clickhouse:\n        condition: service_healthy\n\n  # The router of the stack",
			want: "plane stores: query-api waits for postgres with \"service_started\"",
		},
		{
			name: "the migration that grants does not wait for the provision step",
			old:  "      QUERY_API_DATABASE_ROLE: ${QUERY_API_DATABASE_ROLE:-devhealth_query_api}\n    depends_on:\n      go-river-provision:\n        condition: service_completed_successfully\n",
			new:  "      QUERY_API_DATABASE_ROLE: ${QUERY_API_DATABASE_ROLE:-devhealth_query_api}\n",
			want: "plane grants: go-river-migrate waits for go-river-provision with \"\"",
		},
		{
			name: "the query-api role named by neither service",
			old:  "      QUERY_API_DATABASE_ROLE: ${QUERY_API_DATABASE_ROLE:-devhealth_query_api}\n      GO_API_ENVELOPE_JWKS_PATH:",
			new:  "      GO_API_ENVELOPE_JWKS_PATH:",
			old2: "      QUERY_API_DATABASE_ROLE: ${QUERY_API_DATABASE_ROLE:-devhealth_query_api}\n    depends_on:\n      go-river-provision:\n",
			new2: "    depends_on:\n      go-river-provision:\n",
			want: "plane grants: go-river-migrate has QUERY_API_DATABASE_ROLE \"\" and query-api logs in as \"\"",
		},
		{
			name: "the ClickHouse password of the login given to neither service",
			old:  "      DHO_API_CH_PASSWORD: ${API_CLICKHOUSE_PASSWORD:-dho_api_ch}\n",
			new:  "",
			old2: "      DEV_HEALTH_CH_API_PASSWORD: ${API_CLICKHOUSE_PASSWORD:-dho_api_ch}\n",
			new2: "",
			want: "api login: clickhouse reads the password from DHO_API_CH_PASSWORD = \"\" and go-api logs in with \"\"",
		},
		{
			name: "a plane that does not wait for the key",
			old:  "    depends_on:\n      envelope-keys-init:\n        condition: service_completed_successfully\n      go-river-provision:\n        condition: service_completed_successfully\n      go-river-migrate:\n        condition: service_completed_successfully\n      postgres:\n        condition: service_healthy\n      clickhouse:\n",
			new:  "    depends_on:\n      go-river-provision:\n        condition: service_completed_successfully\n      go-river-migrate:\n        condition: service_completed_successfully\n      postgres:\n        condition: service_healthy\n      clickhouse:\n",
			want: "plane order: query-api waits for envelope-keys-init with \"\"",
		},
	} {
		t.Run(plant.name, func(t *testing.T) {
			if strings.Count(clean, plant.old) != 1 {
				// This is NOT a finding about the compose file. The self-test
				// could not plant its defect, because the text it changes is
				// no longer in the file once: the file was edited. What the
				// check says about the file is the other test,
				// TestComposeStackRunsTheRouterAndNoPythonAPI. Update old and
				// new of this plant to the file's text.
				t.Fatalf("the plant is not valid, and this says NOTHING about a defect in %s: the text this plant changes is in the file %d times, want once (the file's text changed). The verdict on the file is TestComposeStackRunsTheRouterAndNoPythonAPI; bring this plant's old and new text up to the file", composeFilePath, strings.Count(clean, plant.old))
			}
			planted := strings.Replace(clean, plant.old, plant.new, 1)
			if plant.old2 != "" {
				if strings.Count(planted, plant.old2) != 1 {
					t.Fatalf("the plant is not valid, and this says NOTHING about a defect in %s: the second text this plant changes is in the file %d times, want once", composeFilePath, strings.Count(planted, plant.old2))
				}
				planted = strings.Replace(planted, plant.old2, plant.new2, 1)
			}
			findings, err := composeStackFindings([]byte(planted))
			if err != nil {
				t.Fatalf("the planted file does not read: %v", err)
			}
			for _, finding := range findings {
				if strings.Contains(finding, plant.want) {
					return
				}
			}
			t.Fatalf("the check did not see the defect: want a finding with %q, got %q", plant.want, findings)
		})
	}
}
