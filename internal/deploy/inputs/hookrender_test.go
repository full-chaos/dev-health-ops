package inputs

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// chartRender is a real client-only `helm template` of deploy/helm/dev-health
// under one release name, with every hook of the chart on: the Kind/name of
// each doc it renders.
func chartRender(t *testing.T, release string) []string {
	t.Helper()
	out := chartRenderBytes(t, release)
	var docs []string
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var d struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
		}
		if err := dec.Decode(&d); err != nil {
			break
		}
		if d.Kind != "" {
			docs = append(docs, d.Kind+"/"+d.Metadata.Name)
		}
	}
	if len(docs) < 20 {
		t.Fatalf("helm template %s rendered %d docs", release, len(docs))
	}
	return docs
}

// chartRenderBytes is the raw client-only `helm template` output.
func chartRenderBytes(t *testing.T, release string) []byte {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Fatal("helm is required: CI installs it for the Go test and race legs (a missing helm fails this cell, it never skips)")
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("no go.mod above the test directory")
		}
		root = parent
	}
	chart := filepath.Join(root, "deploy", "helm", "dev-health")
	// helm reads the chart in its own process; read it here too, so a chart-only
	// change invalidates the go test result cache.
	if err := filepath.WalkDir(chart, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			_, _ = os.ReadFile(p)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(helm, "template", release, chart,
		"--set", "web.env.BACKEND_URL=http://backend.test:8000",
		"--set", "web.authSecret.secretName=web-auth-placeholder",
		"--set", "postgresql.enabled=true",
		"--set", "goApi.enabled=true",
		"--set", "authService.phase=1",
		"--set", "authService.database.secretName=auth-db",
		"--set", "authService.migrate.secretName=auth-migrate-db",
		"--set", "authService.signingKey.secretName=auth-key",
		"--set", "authService.signingKey.keyId=k1",
		"--set", "authService.networkPolicy.enabled=false",
		"--set", "migrations.hook.provisionRoles.enabled=true",
		"--set", "migrations.hook.riverMigrate.enabled=true",
		"--set", "migrations.hook.riverMigrate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
		"--set", "migrations.hook.routeActivate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
		"--set", "migrations.hook.streakIndex.enabled=true",
		"--set", "migrations.hook.openUnitsIndex.enabled=true")
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		t.Fatalf("helm template %s: %v: %.400s", release, err, errOut.String())
	}
	return out.Bytes()
}

// (a) on the real producer: under the docs release name (dev-health, no
// fullnameOverride), the prod name (dev-health-ops) and one other prefix, the
// 7 migrate, data and grant hook Jobs of the render are refused on any kind,
// and the render's app-roll rows (every non-auth Deployment, route-activate)
// still pass.
func TestTheSevenHooksOfARealRenderAreRefusedUnderAnyRelease(t *testing.T) {
	img := ".spec.template.spec.containers[0].image"
	for _, release := range []string{"dev-health", "dev-health-ops", "acme"} {
		docs := chartRender(t, release)
		t.Run(release+"/hooks refused", func(t *testing.T) {
			found := map[string]bool{}
			for _, doc := range docs {
				kind, name, _ := strings.Cut(doc, "/")
				if kind != "Job" {
					continue
				}
				best := ""
				for _, h := range []string{"river-migrate", "provision-roles", "migrate", "streak-index", "open-units-index", "auth-migrate", "auth-grants"} {
					if strings.HasSuffix(name, "-"+h) && len(h) > len(best) {
						best = h
					}
				}
				if best == "" {
					continue
				}
				found[best] = true
				for _, k := range []string{"Job", "NetworkPolicy", "ServiceAccount", "ConfigMap"} {
					path := img
					if k != "Job" {
						path = ".metadata.labels.x"
					}
					_, err := Parse([]byte(goodDev + row(k+"/"+name, path, oldDig, newDig)))
					var r *Refusal
					if !errors.As(err, &r) || !r.NotBuilt || !strings.Contains(r.Reason, "migrate, data or grant hook") {
						t.Errorf("%s: a row on %s/%s is not refused as a hook doc: %v", release, k, name, err)
					}
				}
			}
			if len(found) != 7 {
				t.Fatalf("%s: the render holds %d of the 7 hook Jobs: %v", release, len(found), found)
			}
		})
		t.Run(release+"/app-roll rows pass", func(t *testing.T) {
			n := 0
			for _, doc := range docs {
				kind, name, _ := strings.Cut(doc, "/")
				if !(kind == "Deployment" && !strings.Contains(name, "-auth-")) && !(kind == "Job" && strings.HasSuffix(name, "-route-activate")) {
					continue
				}
				buf := dhHead + dhScal + row(doc, img, oldDig, newDig)
				if kind == "Job" {
					buf = dhHead + dhScal + row("Deployment/app", img, oldDig, newDig) + row(doc, img, oldDig, newDig)
				}
				if _, err := Parse([]byte(buf)); err != nil {
					t.Errorf("%s: the app-roll row on %s is refused: %v", release, doc, err)
				}
				n++
			}
			if n < 10 {
				t.Fatalf("%s: only %d app-roll docs in the render", release, n)
			}
		})
	}
}
