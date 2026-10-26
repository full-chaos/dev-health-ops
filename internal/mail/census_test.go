package mail

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// Census of every code path that builds a mail body or embeds an HTML template.
// A new one fails here until it is added with the reason it is safe: each listed
// file renders through an escaping renderer (RenderTemplate or RenderBillingEmail).
func TestEveryMailBodyBuilderIsKnown(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	messageBuilders := map[string]string{
		"internal/api/authmail/authmail.go":           "HTML from mail.RenderTemplate (escapes every value)",
		"internal/auth/linkmail/linkmail.go":          "HTML from mail.RenderTemplate (escapes every value)",
		"internal/jobs/operational/billinghandler.go": "HTML from RenderBillingEmail (escapes every value)",
	}
	htmlEmbeds := map[string]string{
		"internal/mail/template.go":                 "RenderTemplate",
		"internal/jobs/operational/billingemail.go": "RenderBillingEmail",
	}
	builder := regexp.MustCompile(`mail\.Message\{[^}]*\bHTML:`)
	embed := regexp.MustCompile(`//go:embed .*\.html`)
	foundBuilders, foundEmbeds := map[string]bool{}, map[string]bool{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "third_party", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if builder.Match(data) {
			foundBuilders[rel] = true
		}
		if embed.Match(data) {
			foundEmbeds[rel] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	compare := func(label string, found map[string]bool, known map[string]string) {
		var got, want []string
		for file := range found {
			got = append(got, file)
		}
		for file := range known {
			want = append(want, file)
		}
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s: found\n%s\nknown\n%s", label, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
	compare("mail.Message builders with an HTML body", foundBuilders, messageBuilders)
	compare("embedded HTML templates", foundEmbeds, htmlEmbeds)
}
