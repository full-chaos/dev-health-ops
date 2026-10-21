package devhealth_test

// CHAOS-9177: a rendered or linted chart is not a started one. On 2026-10-10 the chart mounted an emptyDir
// over the WHOLE /etc/clickhouse-server/config.d; that hid the image's own docker_related_config.xml
// (listen_host), so the server listened on loopback only, the kubelet liveness probe on the pod address
// failed and the pod was killed every minute. This test STARTS the real image (compose) with the mounts the
// rendered StatefulSet declares (init containers included) and probes /ping from ANOTHER container on the
// compose network, the same as a kubelet or a client reaching the pod address (never loopback).
//
// Mount model: emptyDir = an EMPTY host directory (never a named volume: docker fills a named volume from
// the image, kubernetes does not); configMap = a host directory holding the key files; subPath = the one
// file; the PVC = an empty host directory.
//
// It needs docker + helm. Without them it skips, and FAILS when CHART_STARTED_TEST_REQUIRED is set (a
// skipped measurement must not read as coverage; set it on any job that is meant to run this test). CHART_STARTED_TEST_IMAGE overrides the rendered image.

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const startedTestUsersd = `<clickhouse><users><acr_ro><password_sha256_hex>` +
	`aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` +
	`</password_sha256_hex><profile>default</profile></acr_ro></users></clickhouse>`

const startedTestConfigd = `<clickhouse><query_log><ttl>event_date + INTERVAL 7 DAY DELETE</ttl>` +
	`<ttl_only_drop_parts>1</ttl_only_drop_parts></query_log></clickhouse>`

type k8sMount struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mountPath"`
	SubPath   string `yaml:"subPath"`
	ReadOnly  bool   `yaml:"readOnly"`
}

type k8sContainer struct {
	Name         string     `yaml:"name"`
	Image        string     `yaml:"image"`
	Command      []string   `yaml:"command"`
	Env          []k8sEnv   `yaml:"env"`
	VolumeMounts []k8sMount `yaml:"volumeMounts"`
}

type k8sEnv struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type k8sVolume struct {
	Name      string                 `yaml:"name"`
	ConfigMap *struct{ Name string } `yaml:"configMap"`
	EmptyDir  *struct{}              `yaml:"emptyDir"`
}

func needStartedTestTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"helm", "docker"} {
		_, err := exec.LookPath(tool)
		if err == nil && tool == "docker" {
			err = exec.Command("docker", "compose", "version").Run() // the compose subcommand, not only the binary
		}
		if err != nil {
			if os.Getenv("CHART_STARTED_TEST_REQUIRED") != "" {
				t.Fatalf("%s is not usable and CHART_STARTED_TEST_REQUIRED is set: the started-chart test cannot be skipped", tool)
			}
			t.Skipf("%s is not usable", tool)
		}
	}
}

func TestClickHouseConfigdStartsAndAnswersOnThePodAddress(t *testing.T) {
	needStartedTestTools(t)

	chart := os.Getenv("CHART_DIR")
	if chart == "" {
		chart = "."
	}
	out, err := exec.Command("helm", "template", "t", chart,
		"--show-only", "templates/clickhouse.yaml",
		"--set-string", `clickhouse.configd.system-logs\.xml=`+startedTestConfigd,
		"--set-string", `clickhouse.usersd.acr_ro\.xml=`+startedTestUsersd,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v: %.600s", err, out)
	}

	var sts struct {
		Kind string `yaml:"kind"`
		Spec struct {
			Template struct {
				Spec struct {
					InitContainers []k8sContainer `yaml:"initContainers"`
					Containers     []k8sContainer `yaml:"containers"`
					Volumes        []k8sVolume    `yaml:"volumes"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	var cms = map[string]map[string]string{}
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var raw map[string]interface{}
		if err := dec.Decode(&raw); err != nil {
			break
		}
		b, _ := yaml.Marshal(raw)
		switch raw["kind"] {
		case "StatefulSet":
			if err := yaml.Unmarshal(b, &sts); err != nil {
				t.Fatal(err)
			}
		case "ConfigMap":
			var cm struct {
				Metadata struct{ Name string } `yaml:"metadata"`
				Data     map[string]string     `yaml:"data"`
			}
			if err := yaml.Unmarshal(b, &cm); err != nil {
				t.Fatal(err)
			}
			cms[cm.Metadata.Name] = cm.Data
		}
	}
	pod := sts.Spec.Template.Spec
	if len(pod.Containers) != 1 {
		t.Fatalf("want one container, got %d", len(pod.Containers))
	}
	main := pod.Containers[0]

	root := t.TempDir()
	hostDir := func(vol string) string { return filepath.Join(root, "vol-"+vol) }
	mk := func(p string, mode os.FileMode) {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range pod.Volumes {
		if v.EmptyDir != nil {
			mk(hostDir(v.Name), 0o777)
		}
	}
	mk(hostDir("clickhouse-data"), 0o777) // the PVC

	// ConfigMap files go in as compose configs (content sent through the API), never as host bind mounts:
	// on the ARC pool the daemon is a DinD sidecar that cannot see this pod's TempDir, so a bind mount of
	// a file there arrives as an empty directory (CHAOS-10294).
	cmByVolume := map[string]map[string]string{}
	for _, v := range pod.Volumes {
		if v.ConfigMap != nil {
			cmByVolume[v.Name] = cms[v.ConfigMap.Name]
		}
	}
	var topConfigs []string
	volumesOf := func(c k8sContainer) (vols, cfgs []string) {
		for _, m := range c.VolumeMounts {
			if data, ok := cmByVolume[m.Name]; ok {
				files := map[string]string{}
				if m.SubPath != "" {
					files[m.MountPath] = data[m.SubPath]
				} else {
					for k, content := range data {
						files[m.MountPath+"/"+k] = content
					}
				}
				var targets []string
				for target := range files {
					targets = append(targets, target)
				}
				sort.Strings(targets)
				for _, target := range targets {
					name := fmt.Sprintf("cfg%d", len(topConfigs))
					topConfigs = append(topConfigs, fmt.Sprintf("  %s:\n    content: %s", name, strings.ReplaceAll(fmt.Sprintf("%q", files[target]), "$", "$$")))
					cfgs = append(cfgs, fmt.Sprintf("      - source: %s\n        target: %q", name, target))
				}
				continue
			}
			spec := fmt.Sprintf("      - %s:%s", hostDir(m.Name), m.MountPath)
			if m.ReadOnly {
				spec += ":ro"
			}
			vols = append(vols, spec)
		}
		return vols, cfgs
	}
	image := main.Image
	if o := os.Getenv("CHART_STARTED_TEST_IMAGE"); o != "" {
		image = o
	}

	var sb strings.Builder
	sb.WriteString("services:\n")
	var inits []string
	for _, ic := range pod.InitContainers {
		inits = append(inits, ic.Name)
		vols, cfgs := volumesOf(ic)
		fmt.Fprintf(&sb, "  %s:\n    image: %s\n    user: \"0\"\n    command: %s\n    volumes:\n%s\n",
			ic.Name, image, jsonList(ic.Command), strings.Join(vols, "\n"))
		if len(cfgs) > 0 {
			sb.WriteString("    configs:\n" + strings.Join(cfgs, "\n") + "\n")
		}
	}
	sb.WriteString("  clickhouse:\n    image: " + image + "\n    environment:\n")
	for _, e := range main.Env {
		fmt.Fprintf(&sb, "      %s: %q\n", e.Name, e.Value)
	}
	mainVols, mainCfgs := volumesOf(main)
	sb.WriteString("    volumes:\n" + strings.Join(mainVols, "\n") + "\n")
	if len(mainCfgs) > 0 {
		sb.WriteString("    configs:\n" + strings.Join(mainCfgs, "\n") + "\n")
	}
	if len(inits) > 0 {
		sb.WriteString("    depends_on:\n")
		sort.Strings(inits)
		for _, n := range inits {
			fmt.Fprintf(&sb, "      %s:\n        condition: service_completed_successfully\n", n)
		}
	}
	// The prober is ANOTHER container: it reaches the server on the service address, never on loopback.
	sb.WriteString("  prober:\n    image: " + image + "\n    profiles: [probe]\n    entrypoint: [\"sh\",\"-c\"]\n" +
		"    command: ['for i in $$(seq 1 20); do wget -q -T 2 -O /dev/null http://clickhouse:8123/ping && echo PING_OK && exit 0; sleep 2; done; echo PING_FAILED; exit 1']\n")

	if len(topConfigs) > 0 {
		sb.WriteString("configs:\n" + strings.Join(topConfigs, "\n") + "\n")
	}
	compose := filepath.Join(root, "compose.yml")
	if err := os.WriteFile(compose, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	project := fmt.Sprintf("chart9177-%d", time.Now().UnixNano()%1_000_000_000)
	dc := func(args ...string) ([]byte, error) {
		return exec.Command("docker", append([]string{"compose", "-p", project, "-f", compose}, args...)...).CombinedOutput()
	}
	t.Cleanup(func() {
		_, _ = dc("--profile", "probe", "down", "-v", "--remove-orphans", "-t", "2")
		// the server wrote files as its own uid into the host directories: open them for TempDir's removal
		_, _ = dc("run", "--rm", "--no-deps", "-T", "-v", root+":/r", "--user", "0", "--entrypoint", "sh", "clickhouse",
			"-c", "chmod -R a+rwX /r; rm -rf /r/vol-*")
		_, _ = dc("--profile", "probe", "down", "-v", "--remove-orphans", "-t", "2")
	})

	if o, err := dc("up", "-d", "clickhouse"); err != nil {
		logs, _ := dc("logs", "--no-color")
		t.Fatalf("compose up: %v\n%.1500s\n%.2500s", err, o, logs)
	}
	if o, err := dc("--profile", "probe", "run", "--rm", "-T", "prober"); err != nil {
		logs, _ := dc("logs", "--no-color", "clickhouse")
		files, _ := dc("exec", "-T", "clickhouse", "ls", "/etc/clickhouse-server/config.d")
		t.Fatalf("/ping from another container did not answer on the service address: %v\n%s\nconfig.d: %s\n%.2000s", err, o, files, tail(string(logs), 2000))
	}

	t.Logf("PING_OK: started %s and /ping answered from another container on the service address", image)

	// The image's own file and every chart file are all in config.d / users.d.
	ls := func(dir string) string {
		o, err := dc("exec", "-T", "clickhouse", "ls", dir)
		if err != nil {
			t.Fatalf("ls %s: %v: %s", dir, err, o)
		}
		return string(o)
	}
	cfgd := ls("/etc/clickhouse-server/config.d")
	for _, want := range []string{"docker_related_config.xml", "system-logs.xml"} {
		if !strings.Contains(cfgd, want) {
			t.Fatalf("config.d lacks %s: %q", want, cfgd)
		}
	}
	usd := ls("/etc/clickhouse-server/users.d")
	for _, want := range []string{"default-user.xml", "acr_ro.xml"} {
		if !strings.Contains(usd, want) {
			t.Fatalf("users.d lacks %s: %q", want, usd)
		}
	}
	// The chart file arrived with its content (the state the mount exists to reach).
	// stdout only: docker's own stderr warnings must not enter the compared bytes
	got, err := exec.Command("docker", "compose", "-p", project, "-f", compose, "exec", "-T", "clickhouse",
		"cat", "/etc/clickhouse-server/config.d/system-logs.xml").Output()
	if err != nil || strings.TrimSpace(string(got)) != startedTestConfigd {
		t.Fatalf("system-logs.xml content differs: %v: %q", err, got)
	}
}

func jsonList(items []string) string {
	var parts []string
	for _, s := range items {
		parts = append(parts, fmt.Sprintf("%q", strings.ReplaceAll(s, "$", "$$")))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
