package docstools

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// check-built-site-links: was scripts/check_built_site_links.py (CHAOS-10435). Crawls a built MkDocs site's HTML for
// links, anchors and assets that do not resolve inside the site. Python is the contract (D6129); the stricter places
// are in testdata/README.md and the refusal table in cmd/cigate/docs_checks_refusals_test.go.

func init() { register("check-built-site-links", dcLinksVerb) }

var (
	dcIDRe  = regexp.MustCompile(`id="([^"]+)"`)
	dcRefRe = regexp.MustCompile(`(?:href|src)="([^"]+)"`)
)

// dcIsPyWordRune is the character class of Python's str \w: letters, numbers and the underscore.
func dcIsPyWordRune(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r) }

// dcFindWithBoundary is re.findall(r'\b<re>') for a pattern that starts with a word character: a match counts only
// where the character before it is not a word character (Python's \b is Unicode, Go's is ASCII). After a rejected
// match the search restarts one character on, as Python's scanner does.
func dcFindWithBoundary(re *regexp.Regexp, text string) []string {
	var out []string
	for pos := 0; pos <= len(text); {
		loc := re.FindStringSubmatchIndex(text[pos:])
		if loc == nil {
			break
		}
		start := pos + loc[0]
		if start > 0 {
			if prev, _ := utf8.DecodeLastRuneInString(text[:start]); dcIsPyWordRune(prev) {
				_, size := utf8.DecodeRuneInString(text[start:])
				pos = start + size
				continue
			}
		}
		out = append(out, text[pos+loc[2]:pos+loc[3]])
		pos += loc[1]
	}
	return out
}

func dcShouldSkip(target string) (bool, error) {
	for _, p := range []string{"http://", "https://", "mailto:", "tel:", "javascript:"} {
		if strings.HasPrefix(target, p) {
			return true, nil
		}
	}
	if strings.Contains(target, "{{") || strings.Contains(target, "{%") || strings.Contains(target, "{#") {
		return true, nil
	}
	u, err := dcURLSplit(target)
	if err != nil {
		return false, err
	}
	return u.scheme != "" || u.netloc != "", nil
}

// dcRelativeTo is PurePath.relative_to as a yes/no: the paths are both absolute or both relative, and base's parts
// open path's parts.
func dcRelativeTo(path, base string) bool {
	if strings.HasPrefix(path, "/") != strings.HasPrefix(base, "/") {
		return false
	}
	pp, bp := a1PathParts(path), a1PathParts(base)
	if len(bp) > len(pp) {
		return false
	}
	for i := range bp {
		if pp[i] != bp[i] {
			return false
		}
	}
	return true
}

func dcParent(p string) string {
	s := a1PathStr(p)
	root := ""
	switch {
	case strings.HasPrefix(s, "//"):
		root = "//"
	case strings.HasPrefix(s, "/"):
		root = "/"
	}
	rest := strings.TrimLeft(s, "/")
	i := strings.LastIndex(rest, "/")
	switch {
	case i >= 0:
		return root + rest[:i]
	case root != "":
		return root
	}
	return "."
}

func dcLinksVerb(args []string, stdout, stderr io.Writer) int {
	flags, code := dcParseFlags("check-built-site-links", args, []dcFlag{{name: "site-dir", required: true}}, stderr)
	if flags == nil {
		return code
	}
	siteDir := a1PathStr(flags["site-dir"])
	isDir, err := dcIsDirPy(siteDir)
	if err != nil {
		return dcFail(stderr, err)
	}
	if !isDir { // refusal table: L93-97
		fmt.Fprintf(stderr, "ERROR: built site directory not found: %s\n", siteDir)
		return 1
	}
	errs, err := dcCheckBuiltSite(siteDir)
	if err != nil {
		return dcFail(stderr, err)
	}
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(stdout, "ERROR: %s\n", e)
		}
		return 1
	}
	fmt.Fprintln(stdout, "Built-site link check passed")
	return 0
}

func dcCheckBuiltSite(siteDir string) ([]string, error) {
	var errs []string
	anchorCache := map[string]map[string]bool{}
	anchorsFor := func(path string) (map[string]bool, error) {
		if got, ok := anchorCache[path]; ok {
			return got, nil
		}
		text, err := a1ReadText(path)
		if err != nil {
			return nil, err
		}
		set := map[string]bool{}
		for _, id := range dcFindWithBoundary(dcIDRe, text) {
			set[id] = true
		}
		anchorCache[path] = set
		return set, nil
	}
	for _, rel := range dcRglob(siteDir, ".html") {
		if !utf8.ValidString(rel) { // L59 STRICTER
			return nil, dcValueError{"ValueError", "a file name that is not UTF-8 is refused (STRICTER): " + a1Repr(strings.ToValidUTF8(rel, "�"))}
		}
		htmlPath := dcJoin(siteDir, rel)
		if fi, serr := os.Stat(htmlPath); serr == nil && !fi.Mode().IsRegular() && !fi.IsDir() {
			return nil, dcValueError{"OSError", "a special file named *.html is refused (STRICTER): " + a1Repr(htmlPath)}
		}
		text, err := a1ReadText(htmlPath) // L60
		if err != nil {
			return nil, err
		}
		for _, raw := range dcFindWithBoundary(dcRefRe, text) {
			skip, err := dcShouldSkip(raw) // L35: urlsplit
			if err != nil {
				return nil, err
			}
			if skip {
				continue
			}
			u, err := dcURLSplit(raw)
			if err != nil {
				return nil, err
			}
			targetPath, anchor := dcUnquote(u.path), dcUnquote(u.fragment)
			if targetPath == "" && anchor == "" {
				continue
			}
			dest, ok, err := dcResolveDestination(htmlPath, siteDir, targetPath) // L39-56: resolve, relative_to, is_dir
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			exists, err := dcExistsPy(dest)
			if err != nil {
				return nil, err
			}
			if !exists {
				errs = append(errs, fmt.Sprintf("%s -> %s: missing built asset", rel, raw))
				continue
			}
			if anchor != "" && dcPathSuffix(dest) == ".html" {
				anchors, err := anchorsFor(dest)
				if err != nil {
					return nil, err
				}
				if !anchors[anchor] {
					errs = append(errs, fmt.Sprintf("%s -> %s: missing anchor '%s'", rel, raw, anchor))
				}
			}
		}
	}
	return errs, nil
}

// dcResolveDestination is resolve_destination: ok is false where Python returned None.
func dcResolveDestination(htmlPath, siteDir, targetPath string) (string, bool, error) {
	dest := htmlPath
	if targetPath != "" {
		resolved, err := dcResolve(dcJoin(dcParent(htmlPath), targetPath))
		if err != nil {
			return "", false, err
		}
		dest = resolved
	}
	siteResolved, err := dcResolve(siteDir)
	if err != nil {
		return "", false, err
	}
	if !dcRelativeTo(dest, siteResolved) {
		return "", false, nil
	}
	isDir, err := dcIsDirPy(dest)
	if err != nil {
		return "", false, err
	}
	if isDir {
		dest = dcJoin(dest, "index.html")
	}
	return dest, true, nil
}
