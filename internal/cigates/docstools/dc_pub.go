package docstools

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"
)

// validate-publication: was scripts/validate_docs_v2_publication.py (CHAOS-10500). It validates the canonical
// documentation site (mkdocs.yml navigation, the docs/ tree, the frozen information architecture under docs-data/ia and
// the redirects) and writes the publication inventory under .build. Python 3.14.7 with PyYAML 6.0.3 is the contract
// (D6129); the stricter places are listed in testdata/README.md and the refusal table in
// cmd/cigate/docs_pub_refusals_test.go. Like the script, the verb takes no argument and ignores any it is given.

func init() { register("validate-publication", dcPubVerb) }

var (
	dcPubLinkRe        = regexp.MustCompile(`!?\[[^\]]*\]\(([^)]+)\)`)
	dcPubFrontMatterRe = regexp.MustCompile(`\A---[` + a1PySpace + `]*\n((?s:.*?))\n---[` + a1PySpace + `]*\n`)
)

type dcPubRecord struct{ sourcePath, label, navPath string }

// dcPubFlattenNav is _flatten_nav. A key or a value that Python's str() would refuse is an error.
func dcPubFlattenNav(node any, trail []string) ([]dcPubRecord, error) {
	switch n := node.(type) {
	case string:
		label := ""
		navPath := ""
		if len(trail) > 0 {
			label = trail[len(trail)-1]
			navPath = strings.Join(trail, " > ")
		} else {
			label = dcTitle(strings.ReplaceAll(dcPyStem(n), "-", " "))
			navPath = label
		}
		return []dcPubRecord{{strings.TrimLeft(n, "/"), label, navPath}}, nil
	case *pyList:
		var out []dcPubRecord
		for _, child := range n.items {
			recs, err := dcPubFlattenNav(child, trail)
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
		}
		return out, nil
	case *pyDict:
		var out []dcPubRecord
		for i := range n.keys {
			label, err := pyStr(n.keys[i])
			if err != nil {
				return nil, err
			}
			next := append(append([]string(nil), trail...), label)
			if child, ok := n.vals[i].(string); ok {
				out = append(out, dcPubRecord{strings.TrimLeft(child, "/"), label, strings.Join(next, " > ")})
				continue
			}
			recs, err := dcPubFlattenNav(n.vals[i], next)
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
		}
		return out, nil
	}
	return nil, nil
}

// dcPubLinks is LINK_RE.findall(text) with the href each match gives: raw.strip().split()[0].strip("<>"). An all-blank
// match is the IndexError of Python.
func dcPubLinks(text string) ([][2]string, error) {
	var out [][2]string
	for _, m := range dcPubLinkRe.FindAllStringSubmatch(text, -1) {
		fields := strings.FieldsFunc(a1Strip(m[1]), a1IsPySpace)
		if len(fields) == 0 {
			return nil, dcValueError{"IndexError", "list index out of range"}
		}
		out = append(out, [2]string{m[1], strings.Trim(fields[0], "<>")})
	}
	return out, nil
}

// dcPubFrontMatter is FRONT_MATTER_RE.match(text).group(1): ok is false when the text has no front matter.
func dcPubFrontMatter(text string) (string, bool) {
	m := dcPubFrontMatterRe.FindStringSubmatch(text)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// dcPubCanonicalURL is _canonical_url.
func dcPubCanonicalURL(sourcePath string) string {
	value := strings.TrimLeft(a1Strip(sourcePath), "/")
	if value == "index.md" {
		return "/"
	}
	if strings.HasSuffix(value, "/index.md") {
		return "/" + value[:len(value)-len("index.md")]
	}
	value = strings.TrimSuffix(value, ".md")
	return "/" + strings.Trim(value, "/") + "/"
}

// dcPubLoadYAML is _load_yaml and _front_matter's loader: safe_load, and {} unless the result is a mapping.
func dcPubLoadYAML(text string) (*pyDict, error) {
	v, err := dcYAMLSafeLoad(text)
	if err != nil {
		return nil, err
	}
	if d, ok := v.(*pyDict); ok {
		return d, nil
	}
	return newPyDict(), nil
}

func dcPubIsFile(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().IsRegular()
}

func dcPubGetStr(d *pyDict, key string) any {
	v, _ := d.getStr(key)
	return v
}

// dcPubOr is `a or b`.
func dcPubOr(a, b any) any {
	if pyTruthy(a) {
		return a
	}
	return b
}

// dcPubWithSuffixMD is Path.with_suffix(".md").
func dcPubWithSuffixMD(p string) (string, error) {
	s := a1PathStr(p)
	if s == "." || s == "/" || s == "//" {
		return "", dcValueError{"ValueError", "PosixPath(" + a1Repr(s) + ") has an empty name"}
	}
	name := s[strings.LastIndex(s, "/")+1:]
	if suffix := dcPathSuffix(s); suffix != "" {
		name = strings.TrimSuffix(name, suffix)
	}
	return dcJoin(dcParent(s), name+".md"), nil
}

var dcPubUsesParams = map[string]bool{"": true, "ftp": true, "hdl": true, "prospero": true, "http": true, "imap": true, "https": true, "shttp": true, "rtsp": true, "rtspu": true, "sip": true, "sips": true, "mms": true, "sftp": true, "tel": true}

// dcPubURLParse is the scheme, netloc and path of urllib.parse.urlparse(href): urlsplit, and the path without its
// parameters for the schemes that take them (the empty scheme is one).
func dcPubURLParse(href string) (scheme, netloc, path string, err error) {
	u, err := dcURLSplit(href)
	if err != nil {
		return "", "", "", err
	}
	path = u.path
	if dcPubUsesParams[u.scheme] && strings.Contains(path, ";") {
		start := 0
		if slash := strings.LastIndex(path, "/"); slash >= 0 {
			start = slash
		}
		if i := strings.Index(path[start:], ";"); i >= 0 {
			path = path[:start+i]
		}
	}
	return u.scheme, u.netloc, path, nil
}

// dcPubResolveRelative is _resolve_relative.
func dcPubResolveRelative(page, href, docsDir string) (bool, error) {
	scheme, netloc, path, err := dcPubURLParse(href)
	if err != nil {
		return false, err
	}
	if scheme != "" || netloc != "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "mailto:") || strings.HasPrefix(href, "tel:") {
		return true, nil
	}
	if path == "" {
		return true, nil
	}
	var target string
	if strings.HasPrefix(path, "/") {
		target = dcJoin(docsDir, strings.Trim(path, "/"))
	} else {
		resolved, err := dcResolve(dcJoin(dcParent(page), path))
		if err != nil {
			return false, err
		}
		target = resolved
	}
	candidates := []string{target}
	switch suffix := dcPathSuffix(target); {
	case suffix == "":
		md, err := dcPubWithSuffixMD(target)
		if err != nil {
			return false, err
		}
		candidates = append(candidates, md, dcJoin(target, "index.md"))
	case dcLower(suffix) == ".html" || dcLower(suffix) == ".htm":
		md, err := dcPubWithSuffixMD(target)
		if err != nil {
			return false, err
		}
		candidates = append(candidates, md)
	}
	for _, c := range candidates {
		if ok, _ := dcExistsPy(c); ok {
			return true, nil
		}
	}
	return false, nil
}

// dcPubFail is dcFail for the exceptions of the YAML port too.
func dcPubFail(stderr io.Writer, err error) int {
	var ye *dcYErr
	if errors.As(err, &ye) {
		return a1Traceback(stderr, ye.kind, ye.msg)
	}
	return dcFail(stderr, err)
}

func dcPubNoneStrip() error {
	return dcValueError{"AttributeError", "'NoneType' object has no attribute 'strip'"}
}

func dcPubVerb(args []string, stdout, stderr io.Writer) int {
	root, err := docsRoot("mkdocs.yml")
	if err != nil {
		return dcPubFail(stderr, err)
	}
	problems, err := dcPubRun(root, stdout)
	if err != nil {
		return dcPubFail(stderr, err)
	}
	if len(problems) > 0 {
		if len(problems) > 100 {
			problems = problems[:100]
		}
		fmt.Fprintln(stderr, strings.Join(problems, "\n"))
		return 1
	}
	return 0
}

type dcPubIARow struct{ m dcCSVRowMap }

func (r dcPubIARow) get(key string) (val string, present bool, none bool) {
	v, ok := r.m[key]
	if !ok {
		return "", false, false
	}
	if v == nil {
		return "", true, true
	}
	return *v, true, false
}

// getOrEmpty is row.get(key, "") as a string; None is reported through none.
func (r dcPubIARow) getOrEmpty(key string) (string, bool) {
	v, ok := r.m[key]
	if !ok {
		return "", false
	}
	if v == nil {
		return "", true
	}
	return *v, false
}

func dcPubRun(root string, stdout io.Writer) ([]string, error) {
	var errs []string
	configPath := dcJoin(root, "mkdocs.yml")
	configText, err := a1ReadText(configPath)
	if err != nil {
		return nil, err
	}
	config, err := dcPubLoadYAML(configText)
	if err != nil {
		return nil, err
	}
	docsDirName := "docs"
	if v := dcPubOr(dcPubGetStr(config, "docs_dir"), nil); v != nil {
		if docsDirName, err = pyStr(v); err != nil {
			return nil, err
		}
	}
	docsDir, err := dcResolve(dcJoin(root, docsDirName))
	if err != nil {
		return nil, err
	}
	var navNode any = &pyList{}
	if v, ok := config.getStr("nav"); ok {
		navNode = v
	}
	navRecords, err := dcPubFlattenNav(navNode, nil)
	if err != nil {
		return nil, err
	}
	navPaths := make([]string, len(navRecords))
	for i, r := range navRecords {
		navPaths[i] = r.sourcePath
	}

	counts := map[string]int{}
	var order []string
	for _, p := range navPaths {
		if counts[p] == 0 {
			order = append(order, p)
		}
		counts[p]++
	}
	var dups []string
	for _, p := range order {
		if counts[p] > 1 {
			dups = append(dups, p)
		}
	}
	if len(dups) > 0 {
		errs = append(errs, "duplicate navigation paths: "+a1ListRepr(dups))
	}

	var missing []string
	for _, p := range navPaths {
		if !dcPubIsFile(dcJoin(docsDir, p)) {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		if len(missing) > 30 {
			missing = missing[:30]
		}
		errs = append(errs, "navigation targets do not exist: "+a1ListRepr(missing))
	}

	actual := map[string]bool{}
	for _, rel := range dcRglob(docsDir, ".md") {
		if !utf8.ValidString(rel) {
			return nil, dcValueError{"ValueError", "a file name that is not UTF-8 is refused (STRICTER): " + a1Repr(strings.ToValidUTF8(rel, "�"))}
		}
		actual[rel] = true
	}
	navSet := map[string]bool{}
	for _, p := range navPaths {
		navSet[p] = true
	}
	var offNav []string
	for p := range actual {
		if !navSet[p] {
			offNav = append(offNav, p)
		}
	}
	sort.Strings(offNav)
	if len(offNav) > 0 {
		show := offNav
		if len(show) > 30 {
			show = show[:30]
		}
		errs = append(errs, "unclassified Markdown outside navigation: "+a1ListRepr(show))
	}

	iaRows, err := dcPubLoadIA(root)
	if err != nil {
		return nil, err
	}
	iaByURL := map[string]dcPubIARow{}
	for _, row := range iaRows {
		if u, none := row.getOrEmpty("url"); !none && u != "" {
			iaByURL[u] = row
		}
	}
	navURLs := map[string]bool{}
	for _, p := range navPaths {
		navURLs[dcPubCanonicalURL(p)] = true
	}
	var nonIA []string
	for u := range navURLs {
		if _, ok := iaByURL[u]; !ok {
			nonIA = append(nonIA, u)
		}
	}
	sort.Strings(nonIA)
	if len(nonIA) > 0 {
		show := nonIA
		if len(show) > 30 {
			show = show[:30]
		}
		errs = append(errs, "canonical URLs outside the approved IA: "+a1ListRepr(show))
	}

	type pubRow struct{ fields [9]string }
	pageIDs := map[string]string{}
	var pubRows []pubRow
	for _, rec := range navRecords {
		page := dcJoin(docsDir, rec.sourcePath)
		metadata := newPyDict()
		if dcPubIsFile(page) {
			text, err := a1ReadText(page)
			if err != nil {
				return nil, err
			}
			if body, ok := dcPubFrontMatter(text); ok {
				if metadata, err = dcPubLoadYAML(body); err != nil {
					return nil, err
				}
			}
		}
		url := dcPubCanonicalURL(rec.sourcePath)
		ia := iaByURL[url]
		pick := func(key string, iaKey string, def any) (string, error) {
			v := dcPubGetStr(metadata, key)
			if !pyTruthy(v) && iaKey != "" {
				if t, _ := ia.getOrEmpty(iaKey); t != "" {
					v = t
				}
			}
			if !pyTruthy(v) {
				v = def
			}
			return pyStr(v)
		}
		pageID, err := pick("page_id", "id", "")
		if err != nil {
			return nil, err
		}
		if pageID != "" {
			if prev, ok := pageIDs[pageID]; ok {
				errs = append(errs, fmt.Sprintf("duplicate page_id %s: %s and %s", pageID, prev, rec.sourcePath))
			}
			pageIDs[pageID] = rec.sourcePath
		}
		contentType, err := pick("content_type", "kind", "")
		if err != nil {
			return nil, err
		}
		owner, err := pick("owner", "", "documentation")
		if err != nil {
			return nil, err
		}
		lifecycle, err := pick("lifecycle", "", "active")
		if err != nil {
			return nil, err
		}
		pubRows = append(pubRows, pubRow{[9]string{pageID, url, rec.sourcePath, rec.navPath, rec.label, contentType, owner, lifecycle, "public-canonical"}})
	}

	var sources []string
	for p := range actual {
		sources = append(sources, p)
	}
	sort.Strings(sources)
	for _, sourcePath := range sources {
		page := dcJoin(docsDir, sourcePath)
		text, err := a1ReadText(page)
		if err != nil {
			return nil, err
		}
		links, err := dcPubLinks(text)
		if err != nil {
			return nil, err
		}
		for _, l := range links {
			raw, href := l[0], l[1]
			ok, err := dcPubResolveRelative(page, href, docsDir)
			if err != nil {
				return nil, err
			}
			if !ok {
				errs = append(errs, fmt.Sprintf("broken local link: %s -> %s", sourcePath, raw))
			}
		}
	}

	redirectsPath := dcJoin(root, "docs-data/redirects.tsv")
	var redirects []dcPubIARow
	if dcPubIsFile(redirectsPath) {
		text, err := dcReadRawText(redirectsPath)
		if err != nil {
			return nil, err
		}
		rows, err := dcCSVRows(text)
		if err != nil {
			return nil, err
		}
		seen := map[string]string{}
		for _, m := range rows {
			row := dcPubIARow{m}
			redirects = append(redirects, row)
			source, none := row.getOrEmpty("source_path")
			if none {
				return nil, dcPubNoneStrip()
			}
			target, none := row.getOrEmpty("target_path")
			if none {
				return nil, dcPubNoneStrip()
			}
			source, target = a1Strip(source), a1Strip(target)
			if prev, ok := seen[source]; ok && prev != target {
				errs = append(errs, fmt.Sprintf("redirect conflict for %s: %s vs %s", source, prev, target))
			}
			seen[source] = target
			if _, ok := iaByURL[target]; !ok {
				errs = append(errs, fmt.Sprintf("redirect target outside approved IA: %s -> %s", source, target))
			}
		}
	}

	build := dcJoin(root, ".build")
	if err := dcPubMkdirAll(build); err != nil {
		return nil, err
	}
	sort.SliceStable(pubRows, func(i, j int) bool { return pubRows[i].fields[1] < pubRows[j].fields[1] })
	var inv strings.Builder
	inv.WriteString(dcPubCSVRow([]string{"page_id", "canonical_url", "source_path", "nav_path", "label", "content_type", "owner", "lifecycle", "publication_state"}))
	for _, r := range pubRows {
		inv.WriteString(dcPubCSVRow(r.fields[:]))
	}
	if err := dcPubWriteFile(dcJoin(build, "docs-v2-publication-inventory.tsv"), inv.String()); err != nil {
		return nil, err
	}

	keys := make([]*string, len(iaRows))
	for i, row := range iaRows {
		v, present := row.m["url"]
		if !present {
			s := ""
			keys[i] = &s
			continue
		}
		keys[i] = v
	}
	covHeader := dcPubCSVRow([]string{"page_id", "canonical_url", "label", "kind", "state"})
	if msg := dcSortKeyTypeError(keys); msg != "" {
		// Python opens the file and writes the header before sorted() runs: the header stays behind.
		if err := dcPubWriteFile(dcJoin(build, "docs-v2-ia-coverage.tsv"), covHeader); err != nil {
			return nil, err
		}
		return nil, dcValueError{"TypeError", msg}
	}
	order2 := make([]int, len(iaRows))
	for i := range order2 {
		order2[i] = i
	}
	sort.SliceStable(order2, func(a, b int) bool { return *keys[order2[a]] < *keys[order2[b]] })
	var cov strings.Builder
	cov.WriteString(covHeader)
	for _, i := range order2 {
		row := iaRows[i]
		cell := func(key string) string { v, _ := row.getOrEmpty(key); return v }
		url := cell("url")
		state := "withheld"
		if navURLs[url] {
			state = "implemented"
		}
		cov.WriteString(dcPubCSVRow([]string{cell("id"), url, cell("label"), cell("kind"), state}))
	}
	if err := dcPubWriteFile(dcJoin(build, "docs-v2-ia-coverage.tsv"), cov.String()); err != nil {
		return nil, err
	}

	withheld := 0
	for u := range iaByURL {
		if !navURLs[u] {
			withheld++
		}
	}
	summary := strings.Join([]string{
		"# Canonical documentation publication summary",
		"",
		fmt.Sprintf("- Public pages: **%d**", len(pubRows)),
		fmt.Sprintf("- Frozen IA nodes: **%d**", len(iaRows)),
		fmt.Sprintf("- Published URLs: **%d**", len(navURLs)),
		fmt.Sprintf("- Withheld IA nodes: **%d**", withheld),
		fmt.Sprintf("- Legacy redirect sources: **%d**", len(redirects)),
		fmt.Sprintf("- Unclassified Markdown pages: **%d**", len(offNav)),
		fmt.Sprintf("- Validation errors: **%d**", len(errs)),
		"",
		"The public documentation is sourced from `docs/`; the previous documentation tree has been retired and is available only in git history.",
		"",
	}, "\n")
	if err := dcPubWriteFile(dcJoin(build, "docs-v2-publication-summary.md"), summary); err != nil {
		return nil, err
	}
	if len(errs) > 0 {
		return errs, nil
	}
	fmt.Fprintf(stdout, "Validated %d canonical pages, %d redirects, and %d frozen IA nodes.\n", len(pubRows), len(redirects), len(iaRows))
	return nil, nil
}

// dcPubLoadIA is _load_ia: every row of docs-data/ia/*.tsv, files in Path order.
func dcPubLoadIA(root string) ([]dcPubIARow, error) {
	dir := dcJoin(root, "docs-data/ia")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tsv") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	var out []dcPubIARow
	for _, name := range names {
		text, err := dcReadRawText(dcJoin(dir, name))
		if err != nil {
			return nil, err
		}
		rows, err := dcCSVRows(text)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			out = append(out, dcPubIARow{r})
		}
	}
	return out, nil
}

// dcPubMkdirAll is Path.mkdir(parents=True, exist_ok=True) of the .build directory.
func dcPubMkdirAll(path string) error {
	if fi, err := os.Stat(path); err == nil {
		if fi.IsDir() {
			return nil
		}
		return dcValueError{"FileExistsError", "[Errno 17] File exists: " + a1Repr(path)}
	}
	if err := os.MkdirAll(path, 0o777); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return dcValueError{"FileExistsError", "[Errno 17] File exists: " + a1Repr(path)}
		}
		return err
	}
	return nil
}

// dcPubWriteFile is open(path, "w", encoding="utf-8", newline="") and one write: the file is created or truncated.
func dcPubWriteFile(path, content string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
