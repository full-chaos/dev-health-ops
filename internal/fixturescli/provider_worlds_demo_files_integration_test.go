//go:build integration

package fixturescli

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The demo world's files: the tree of each repository's default branch at the instant, and the text of
// its Go files, answered to the files sync routes (GitHub's tree and blob reads, GitLab's tree and blob
// reads) so the complexity job scans them as it scans a real repository. A repository holds the files its
// commits touch plus a README, a go.mod and its command's main.go; a Go file grows more branches the more
// commits touch it, so the files a team changes most are the complex ones.

// demoRepoFile is one file of a repository's tree.
type demoRepoFile struct {
	Path, Text string
}

// demoRepoFiles is a repository's tree from its commits, sorted by path.
func demoRepoFiles(repo string, commits []demoCommit) []demoRepoFile {
	module := repo[strings.LastIndex(repo, "/")+1:]
	touches := map[string]int{"README.md": 0, "go.mod": 0, "cmd/" + module + "/main.go": 0}
	for _, commit := range commits {
		for _, file := range commit.Files {
			touches[file]++
		}
	}
	paths := make([]string, 0, len(touches))
	for path := range touches {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	files := make([]demoRepoFile, 0, len(paths))
	for _, path := range paths {
		files = append(files, demoRepoFile{Path: path, Text: demoFileText(repo, path, touches[path])})
	}
	return files
}

// demoFileText is a file's text: Go source for a .go file, a short text otherwise.
func demoFileText(repo, path string, touches int) string {
	module := repo[strings.LastIndex(repo, "/")+1:]
	switch {
	case path == "go.mod":
		return "module example.com/" + repo + "\n\ngo 1.22\n"
	case strings.HasSuffix(path, ".go"):
		return demoGoSource(repo, path, touches)
	default:
		return "# " + module + " " + path + "\n"
	}
}

// demoGoSource is a Go file with 1 + touches functions (at most 8), each a loop with one branch more
// than the one before it, seeded by the repository and path.
func demoGoSource(repo, path string, touches int) string {
	sum := sha256.Sum256([]byte("dev-health-demo-file:" + repo + ":" + path))
	pkg := "main"
	if dir := path[:max(strings.LastIndex(path, "/"), 0)]; !strings.HasPrefix(path, "cmd/") && dir != "" {
		pkg = demoGoIdent(dir[strings.LastIndex(dir, "/")+1:])
	}
	var out strings.Builder
	fmt.Fprintf(&out, "package %s\n\n", pkg)
	functions := min(1+touches, 8)
	for index := 0; index < functions; index++ {
		branches := 1 + index + int(sum[index%len(sum)])%3
		fmt.Fprintf(&out, "// step%d folds the input for %s.\nfunc step%d(input []int) int {\n\ttotal := 0\n\tfor _, value := range input {\n", index, path, index)
		for branch := 0; branch < branches; branch++ {
			keyword := "if"
			if branch > 0 {
				keyword = "} else if"
			}
			fmt.Fprintf(&out, "\t\t%s value%%%d == %d && total < %d {\n\t\t\ttotal += value * %d\n", keyword, branch+2, branch%2, 1000*(branch+1), branch+1)
		}
		out.WriteString("\t\t} else {\n\t\t\ttotal--\n\t\t}\n\t}\n")
		demoPadding(&out, sum, index, 34*branches+60)
		out.WriteString("\treturn total\n}\n\n")
	}
	if pkg == "main" {
		out.WriteString("func main() {\n\t_ = step0([]int{1, 2, 3})\n")
		demoPadding(&out, sum, 99, 70)
		out.WriteString("}\n")
	}
	return out.String()
}

// demoPadding writes that many straight-line assignments with no branch, seeded by the file and the
// function. A function gets about thirty-four per branch it has, so the measured cyclomatic complexity per
// thousand lines of every file lands where real code does (about 30 to 60) and not at the 400 to 500 that
// files of nothing but branches measure. The complexity job counts every line of a file, so the padding is
// real statements and not comments.
func demoPadding(out *strings.Builder, sum [32]byte, salt, statements int) {
	for line := 0; line < statements; line++ {
		fmt.Fprintf(out, "\ttotal += %d * (total %% %d)\n", 1+int(sum[(line+salt)%len(sum)])%9, 3+line%7)
	}
}

// demoGoIdent is a Go package name for a directory name: its letters and digits, lower case.
func demoGoIdent(name string) string {
	var out strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (out.Len() > 0 && r >= '0' && r <= '9') {
			out.WriteRune(r)
		}
	}
	if out.Len() == 0 {
		return "app"
	}
	return out.String()
}

var gitHubBlobAlias = regexp.MustCompile(`f(\d+): object\(expression: ("(?:[^"\\]|\\.)*")\)`)

// demoFileAnswers answer the files sync of every demo repository: the tree at the newest commit (the
// commit the route resolves the default branch to at the instant) and the text of each file.
func demoFileAnswers(build *demoBuild) []providerAnswer {
	var answers []providerAnswer
	for _, name := range demoGitHubRepoNames {
		repo := build.GitHub[name]
		if len(repo.Commits) == 0 {
			continue
		}
		files := demoRepoFiles(name, repo.Commits)
		ref := newestFirst(repo.Commits)[0].SHA
		entries := []any{}
		texts := map[string]string{}
		for _, file := range files {
			entries = append(entries, map[string]any{"path": file.Path, "mode": "100644", "type": "blob",
				"sha": demoSHA("blob", name, file.Path), "size": len(file.Text)})
			texts[file.Path] = file.Text
		}
		root := "/repos/" + githubOrg + "/" + name
		answers = append(answers, get(githubHost, root+"/git/trees/"+ref, "recursive=true", map[string]any{"sha": ref, "tree": entries, "truncated": false}))
		repoName := name
		answers = append(answers, providerAnswer{Method: "POST", Host: githubHost, Path: "/graphql", Status: 200,
			Respond: func(request replayRequest) (any, bool) {
				var vars struct{ Owner, Repo string }
				if json.Unmarshal(request.Variables, &vars) != nil || vars.Owner != githubOrg || vars.Repo != repoName || !strings.Contains(request.Document, "... on Blob {") {
					return nil, false
				}
				repository := map[string]any{}
				for _, match := range gitHubBlobAlias.FindAllStringSubmatch(request.Document, -1) {
					var expression string
					if json.Unmarshal([]byte(match[2]), &expression) != nil {
						return nil, false
					}
					treeRef, path, _ := strings.Cut(expression, ":")
					text, ok := texts[path]
					if !ok || treeRef != ref {
						return nil, false
					}
					repository["f"+match[1]] = map[string]any{"text": text, "isBinary": false, "isTruncated": false}
				}
				return map[string]any{"data": map[string]any{"repository": repository}}, true
			}})
	}
	for _, path := range demoGitLabProjectPaths {
		project := build.GitLab[path]
		if len(project.Commits) == 0 {
			continue
		}
		files := demoRepoFiles(path, project.Commits)
		entries := []any{}
		texts := map[string]string{}
		for _, file := range files {
			entries = append(entries, map[string]any{"id": demoSHA("blob", path, file.Path), "name": file.Path[strings.LastIndex(file.Path, "/")+1:],
				"type": "blob", "path": file.Path, "mode": "100644"})
			texts[file.Path] = file.Text
		}
		root := "/api/v4/projects/" + strconv.Itoa(gitlabProjects[path].ID)
		answers = append(answers, glList(root+"/repository/tree", "*", entries))
		fullPath := path
		answers = append(answers, providerAnswer{Method: "POST", Host: gitlabHost, Path: "/api/graphql", Status: 200,
			Respond: func(request replayRequest) (any, bool) {
				var vars struct {
					FullPath string   `json:"fullPath"`
					Paths    []string `json:"paths"`
				}
				if json.Unmarshal(request.Variables, &vars) != nil || vars.FullPath != fullPath || !strings.Contains(request.Document, "blobs(ref: $ref, paths: $paths)") {
					return nil, false
				}
				nodes := []any{}
				for _, blobPath := range vars.Paths {
					text, ok := texts[blobPath]
					if !ok {
						return nil, false
					}
					node := map[string]any{"path": blobPath}
					if strings.Contains(request.Document, "rawTextBlob") {
						node["rawTextBlob"] = text
					} else {
						node["rawSize"] = len(text)
					}
					nodes = append(nodes, node)
				}
				return map[string]any{"data": map[string]any{"project": map[string]any{"repository": map[string]any{"blobs": map[string]any{"nodes": nodes}}}}}, true
			}})
	}
	return answers
}
