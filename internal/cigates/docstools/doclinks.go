package docstools

import (
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/text/unicode/norm"
)

// check-docs-links: every relative Markdown link and anchor under docs/ resolves. Was scripts/check_docs_links.py; the
// regexes keep Python's Unicode \s and \w (a1PySpace, puWord) and the (?<!!) and \b assertions are checked by hand.

func init() { register("check-docs-links", docLinksVerb) }

const dlS = a1PySpace

var (
	dlInlineLink = regexp.MustCompile(`\[[^\]\n]+\]\(([^)` + dlS + `]+)(?:[` + dlS + `]+"[^"]*")?\)`)
	dlRefDef     = regexp.MustCompile(`(?m)^\[[^\]]+\]:[` + dlS + `]+([^` + dlS + `]+)`)
	dlHTMLID     = regexp.MustCompile(`id=["']([^"']+)["']`)
	dlHeading    = regexp.MustCompile(`^(#{1,6})[` + dlS + `]+(.+?)[` + dlS + `]*#*[` + dlS + `]*$`)
	dlFence      = regexp.MustCompile("^[" + dlS + "]*(```|~~~)")
	dlTag        = regexp.MustCompile(`<[^>]+>`)
	dlCode       = regexp.MustCompile("`([^`]*)`")
	dlPunct      = regexp.MustCompile(`[^A-Za-z0-9_\t\n\v\f\r\x1c-\x1f -]`)
	dlRuns       = regexp.MustCompile(`[-\t\n\v\f\r\x1c-\x1f ]+`)
)

// dlSlugify is markdown.extensions.toc.slugify with unicode=False after the script's own tag and backtick strip.
// After the ASCII fold the text is ASCII, where Python's \w and \s are the ASCII classes above.
func dlSlugify(heading string) string {
	heading = dlTag.ReplaceAllString(heading, "")
	heading = dlCode.ReplaceAllString(heading, "$1")
	heading = norm.NFKD.String(heading)
	var b strings.Builder
	for _, r := range heading {
		if r < 0x80 {
			b.WriteRune(r)
		}
	}
	heading = strings.ToLower(a1Strip(dlPunct.ReplaceAllString(b.String(), "")))
	return dlRuns.ReplaceAllString(heading, "-")
}

// dlSplitLines is str.splitlines() of text that already has universal newlines.
func dlSplitLines(s string) []string {
	var lines []string
	start := 0
	for i, r := range s {
		switch r {
		case '\n', '\v', '\f', 0x1c, 0x1d, 0x1e, 0x85, 0x2028, 0x2029:
			lines = append(lines, s[start:i])
			start = i + len(string(r))
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func dlAnchors(path string) (map[string]bool, error) {
	text, err := a1ReadText(path)
	if err != nil {
		return nil, err
	}
	anchors := map[string]bool{"": true}
	counts := map[string]int{}
	fence := ""
	for _, line := range dlSplitLines(text) {
		if m := dlFence.FindStringSubmatch(line); m != nil {
			switch fence {
			case m[1]:
				fence = ""
			case "":
				fence = m[1]
			}
			continue
		}
		if fence != "" {
			continue
		}
		m := dlHeading.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		base := dlSlugify(m[2])
		if base == "" {
			continue
		}
		n := counts[base]
		counts[base] = n + 1
		if n == 0 {
			anchors[base] = true
		} else {
			anchors[fmt.Sprintf("%s-%d", base, n)] = true
		}
	}
	for _, id := range puFindAllAfterBoundary(dlHTMLID, text) {
		anchors[id] = true
	}
	return anchors, nil
}

// dlLinks is INLINE_LINK_RE.findall (image links excluded by the (?<!!) look-behind) then REFERENCE_DEF_RE.findall.
func dlLinks(path string) ([]string, error) {
	text, err := a1ReadText(path)
	if err != nil {
		return nil, err
	}
	var out []string
	for pos := 0; pos <= len(text); {
		loc := dlInlineLink.FindStringSubmatchIndex(text[pos:])
		if loc == nil {
			break
		}
		start := pos + loc[0]
		if start > 0 && text[start-1] == '!' {
			pos = start + 1
			continue
		}
		out = append(out, text[pos+loc[2]:pos+loc[3]])
		pos += loc[1]
	}
	for _, m := range dlRefDef.FindAllStringSubmatch(text, -1) {
		out = append(out, m[1])
	}
	return out, nil
}

func dlShouldSkip(target string) (bool, error) {
	if target == "" {
		return true, nil
	}
	for _, p := range []string{"http://", "https://", "mailto:", "tel:"} {
		if strings.HasPrefix(target, p) {
			return true, nil
		}
	}
	if strings.HasPrefix(target, "#") {
		return false, nil
	}
	u, err := puURLSplit(target)
	if err != nil {
		return false, err
	}
	return u.scheme != "" || u.netloc != "" || strings.HasPrefix(target, "/"), nil
}

type dlChecker struct {
	docsRoot, root string
	cache          map[string]map[string]bool
}

func (c dlChecker) check(source, raw string) (string, error) {
	skip, err := dlShouldSkip(raw)
	if err != nil || skip {
		return "", err
	}
	u, err := puURLSplit(raw)
	if err != nil {
		return "", err
	}
	targetPath, anchor := puUnquote(u.path), puUnquote(u.fragment)
	if targetPath != "" && !strings.HasSuffix(targetPath, ".md") {
		return "", nil
	}
	dest := source
	if targetPath != "" {
		if dest, err = puRealpath(puJoin(filepath.Dir(source), targetPath)); err != nil {
			return "", err
		}
	}
	if !puWithin(dest, c.docsRoot) {
		return "", nil
	}
	rel := strings.TrimPrefix(source, c.root+"/")
	if !puExists(dest) {
		return fmt.Sprintf("%s -> %s: missing file", rel, raw), nil
	}
	if puSuffix(dest) != ".md" || anchor == "" {
		return "", nil
	}
	anchors, ok := c.cache[dest]
	if !ok {
		if anchors, err = dlAnchors(dest); err != nil {
			return "", err
		}
		c.cache[dest] = anchors
	}
	if !anchors[anchor] {
		return fmt.Sprintf("%s -> %s: missing anchor", rel, raw), nil
	}
	return "", nil
}

// dlGlobMD is sorted(docs_root.rglob("*.md")): every entry named *.md (directories too), without following symlinked
// directories, in Path order.
func dlGlobMD(docsRoot string) ([]string, error) {
	var files []string
	// The trailing slash makes a symlinked docs root a directory to walk, as rglob does.
	walkRoot := docsRoot + "/"
	err := filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == walkRoot {
				return fs.SkipAll
			}
			return err
		}
		if path != walkRoot && strings.HasSuffix(d.Name(), ".md") {
			files = append(files, path)
		}
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return a1PartsLess(files[i], files[j]) })
	return files, err
}

func dlCheckDocs(docsRoot, root string) ([]string, error) {
	files, err := dlGlobMD(docsRoot)
	if err != nil {
		return nil, err
	}
	c := dlChecker{docsRoot: docsRoot, root: root, cache: map[string]map[string]bool{}}
	var errs []string
	for _, path := range files {
		links, err := dlLinks(path)
		if err != nil {
			return nil, err
		}
		for _, target := range links {
			msg, err := c.check(path, target)
			if err != nil {
				return nil, err
			}
			if msg != "" {
				errs = append(errs, msg)
			}
		}
	}
	return errs, nil
}

func docLinksVerb(args []string, stdout, stderr io.Writer) int {
	// The script read no arguments; one here is refused rather than ignored.
	if len(args) > 0 {
		fmt.Fprintf(stderr, "cigate docs check-docs-links: takes no arguments, got %s\n", strings.Join(args, " "))
		return 2
	}
	root, err := puRealpath(".")
	if err != nil {
		return puTracebackError(stderr, err)
	}
	errs, err := dlCheckDocs(root+"/docs", root)
	if err != nil {
		return puTracebackError(stderr, err)
	}
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(stdout, "ERROR: %s\n", e)
		}
		return 1
	}
	fmt.Fprintln(stdout, "Docs link check passed")
	return 0
}
