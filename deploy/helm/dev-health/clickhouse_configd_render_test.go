package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-9177: the rendered form of clickhouse.configd. Each file is ONE subPath mount into the image's own
// config.d; no volume is ever mounted over config.d (it holds the image's docker_related_config.xml) and no
// init container exists for it. The started image is TestClickHouseConfigdStartsAndAnswersOnThePodAddress.
func TestClickHouseConfigdRendersOneSubPathMountPerFile(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	out, err := exec.Command("helm", "template", "t", ".", "--show-only", "templates/clickhouse.yaml",
		"--set-string", `clickhouse.configd.b\.xml=<clickhouse/>`,
		"--set-string", `clickhouse.configd.a\.xml=<clickhouse/>`).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v: %.500s", err, out)
	}
	var sts struct {
		Spec struct {
			Template struct {
				Spec struct {
					InitContainers []struct{ Name string } `yaml:"initContainers"`
					Containers     []struct {
						VolumeMounts []k8sMount `yaml:"volumeMounts"`
					} `yaml:"containers"`
					Volumes []k8sVolume `yaml:"volumes"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var raw map[string]interface{}
		if err := dec.Decode(&raw); err != nil {
			break
		}
		if raw["kind"] == "StatefulSet" {
			b, _ := yaml.Marshal(raw)
			if err := yaml.Unmarshal(b, &sts); err != nil {
				t.Fatal(err)
			}
		}
	}
	pod := sts.Spec.Template.Spec
	if len(pod.InitContainers) != 0 {
		t.Fatalf("configd needs no init container: %+v", pod.InitContainers)
	}
	var subPaths []string
	for _, m := range pod.Containers[0].VolumeMounts {
		if m.MountPath == "/etc/clickhouse-server/config.d" || m.MountPath == "/etc/clickhouse-server" {
			t.Fatalf("a volume is mounted over an image directory: %+v", m)
		}
		if strings.HasPrefix(m.MountPath, "/etc/clickhouse-server/config.d/") {
			if m.Name != "clickhouse-configd" || !m.ReadOnly || m.SubPath != strings.TrimPrefix(m.MountPath, "/etc/clickhouse-server/config.d/") {
				t.Fatalf("config.d file mount is not a read-only subPath of its own name: %+v", m)
			}
			subPaths = append(subPaths, m.SubPath)
		}
	}
	if strings.Join(subPaths, ",") != "a.xml,b.xml" {
		t.Fatalf("want one mount per file in name order, got %v", subPaths)
	}
	for _, v := range pod.Volumes {
		if v.EmptyDir != nil {
			t.Fatalf("configd must leave no emptyDir: %+v", v)
		}
	}
}

// CHAOS-9177: a configd file named like the image's own docker_related_config.xml would replace it
// (the mount hides the image file): the render refuses, observed failing by naming the file.
func TestClickHouseConfigdRefusesTheImageFileName(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	out, err := exec.Command("helm", "template", "t", ".", "--show-only", "templates/clickhouse.yaml",
		"--set-string", `clickhouse.configd.docker_related_config\.xml=<clickhouse/>`).CombinedOutput()
	if err == nil {
		t.Fatalf("a configd file named docker_related_config.xml rendered")
	}
	if !strings.Contains(string(out), "docker_related_config.xml") || !strings.Contains(string(out), "listen_host") {
		t.Fatalf("the refusal does not name the file and the reason: %.400s", out)
	}
}
