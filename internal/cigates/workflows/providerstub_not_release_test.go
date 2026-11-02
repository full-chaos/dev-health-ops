package workflows_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The corpus provider stub (docker/providerstub.Dockerfile) is a test tool that answers provider hostnames: it must never
// be part of an image that is built for release. A release image is built from docker/go-worker.Dockerfile (or
// docker/go-api-tools.Dockerfile) by a workflow or a GitLab job; none of them may name the stub's binary or Dockerfile.

const providerStubName = "providerstub"

// corpusToolNames are the corpus-only images: the provider stub and the mail sink (docker/smtpsink.Dockerfile).
var corpusToolNames = []string{providerStubName, "smtpsink", "idpstub"}

// providerStubInRelease is every way the release inputs name the stub: texts maps a path to its content.
func providerStubInRelease(texts map[string]string) []string {
	var problems []string
	for path, text := range texts {
		for _, name := range corpusToolNames {
			if strings.Contains(text, name) {
				problems = append(problems, path+" names "+name+": a release image input must not carry a corpus tool")
			}
		}
	}
	return problems
}

func releaseInputs(t *testing.T, root string) map[string]string {
	t.Helper()
	paths := []string{"docker/go-worker.Dockerfile", "docker/go-api-tools.Dockerfile", ".gitlab-ci.yml"}
	workflows, err := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.y*ml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, workflow := range workflows {
		rel, err := filepath.Rel(root, workflow)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, rel)
	}
	texts := map[string]string{}
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		texts[path] = string(raw)
	}
	return texts
}

func TestProviderStubIsNotInAReleaseImage(t *testing.T) {
	root := repoRoot(t)
	texts := releaseInputs(t, root)
	if problems := providerStubInRelease(texts); len(problems) > 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
	dockerfile, err := os.ReadFile(filepath.Join(root, "docker", "providerstub.Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dockerfile), "AS providerstub") || !strings.Contains(string(dockerfile), "corpus") {
		t.Error("docker/providerstub.Dockerfile must stay the corpus-only stage and say so")
	}
	sink, err := os.ReadFile(filepath.Join(root, "docker", "smtpsink.Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sink), "AS smtpsink") || !strings.Contains(string(sink), "Corpus-only") {
		t.Error("docker/smtpsink.Dockerfile must stay the corpus-only stage and say so")
	}
}

func TestProviderStubReleaseCheckFailsWhenAReleaseInputNamesIt(t *testing.T) {
	texts := releaseInputs(t, repoRoot(t))
	plants := map[string]func(path string) (string, string){
		"the release Dockerfile copies the binary": func(string) (string, string) {
			return "docker/go-worker.Dockerfile", texts["docker/go-worker.Dockerfile"] + "\nCOPY --from=build /out/providerstub /usr/local/bin/providerstub\n"
		},
		"a release workflow points at the corpus Dockerfile": func(string) (string, string) {
			text := texts[".github/workflows/docker-images.yml"]
			return ".github/workflows/docker-images.yml", strings.Replace(text, "file: docker/go-worker.Dockerfile", "file: docker/providerstub.Dockerfile", 1)
		},
		"a GitLab release job builds the corpus Dockerfile": func(string) (string, string) {
			return ".gitlab-ci.yml", strings.Replace(texts[".gitlab-ci.yml"], "docker/go-worker.Dockerfile", "docker/providerstub.Dockerfile", 1)
		},
	}
	for name, plant := range plants {
		path, planted := plant("")
		if planted == texts[path] {
			t.Errorf("%s: the plant did not change %s", name, path)
			continue
		}
		mutated := map[string]string{}
		for p, text := range texts {
			mutated[p] = text
		}
		mutated[path] = planted
		if problems := providerStubInRelease(mutated); len(problems) == 0 {
			t.Errorf("%s: the check passed", name)
		}
	}
}
