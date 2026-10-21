package docstools

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// check-docs-search: was scripts/check_docs_candidate_search.py (CHAOS-10435). Natural-language queries against the
// generated MkDocs search index. Python is the contract (D6129); the stricter places are in testdata/README.md.

func init() { register("check-docs-search", dcSearchVerb) }

var (
	dcTokenRe = regexp.MustCompile(`[a-z0-9]+(?:[._/-][a-z0-9]+)*`)
	dcTagRe   = regexp.MustCompile(`<[^>]+>`)
	dcSlashRe = regexp.MustCompile(`/{2,}`)
)

type dcSearchDoc struct{ location, title, text string }

type dcSearchCase struct {
	query, expected string
	maxRank         *big.Int
}

func dcNormalizeURL(value string) (string, error) {
	u, err := dcURLSplit(value)
	if err != nil {
		return "", err
	}
	path := u.path
	if path == "" {
		path = "/"
	}
	path = strings.TrimSuffix(path, "index.html")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !strings.HasSuffix(path, "/") && dcPathSuffix(path) == "" {
		path += "/"
	}
	return dcSlashRe.ReplaceAllString(path, "/"), nil
}

func dcPlainText(value string) (string, error) {
	un, err := dcUnescape(dcTagRe.ReplaceAllString(value, " "))
	if err != nil {
		return "", err
	}
	return dcLower(un), nil
}

func dcStem(token string) string {
	for _, suffix := range []string{"ing", "ied", "ed", "es", "s"} {
		if strings.HasSuffix(token, suffix) && len(token)-len(suffix) >= 4 {
			if suffix == "ied" {
				return token[:len(token)-3] + "y"
			}
			return token[:len(token)-len(suffix)]
		}
	}
	return token
}

func dcTokens(value string) ([]string, error) {
	plain, err := dcPlainText(value)
	if err != nil {
		return nil, err
	}
	toks := dcTokenRe.FindAllString(plain, -1)
	for i, t := range toks {
		toks[i] = dcStem(t)
	}
	return toks, nil
}

func dcSet(toks []string) map[string]bool {
	set := make(map[string]bool, len(toks))
	for _, t := range toks {
		set[t] = true
	}
	return set
}

func dcScore(query string, doc dcSearchDoc) (float64, error) {
	queryText, err := dcPlainText(query)
	if err != nil {
		return 0, err
	}
	queryTokens, err := dcTokens(query)
	if err != nil {
		return 0, err
	}
	var uniq []string
	seen := map[string]bool{}
	for _, t := range queryTokens {
		if !seen[t] {
			seen[t] = true
			uniq = append(uniq, t)
		}
	}
	title, err := dcPlainText(doc.title)
	if err != nil {
		return 0, err
	}
	text, err := dcPlainText(doc.text)
	if err != nil {
		return 0, err
	}
	location, err := dcPlainText(doc.location)
	if err != nil {
		return 0, err
	}
	sets := make([]map[string]bool, 0, 3)
	for _, field := range []string{title, text, location} {
		toks, err := dcTokens(field)
		if err != nil {
			return 0, err
		}
		sets = append(sets, dcSet(toks))
	}
	titleTokens, textTokens, locationTokens := sets[0], sets[1], sets[2]

	score := 0.0
	if queryText != "" && strings.Contains(title, queryText) {
		score += 30.0
	} else if queryText != "" && strings.Contains(text, queryText) {
		score += 10.0
	}
	matched := 0
	for _, token := range uniq {
		switch {
		case titleTokens[token]:
			score += 7.0
			matched++
		case locationTokens[token]:
			score += 4.0
			matched++
		case textTokens[token]:
			score += 1.5
			matched++
		}
	}
	if len(uniq) > 0 && matched == len(uniq) {
		score += 12.0
	} else if len(uniq) > 0 {
		score += 4.0 * (float64(matched) / float64(len(uniq)))
	}
	return score, nil
}

// dcStringField is str(raw.get(key) or ""): an absent or falsy value is "", a string is itself. Python also took
// str() of a truthy number, list or object; this port refuses those (STRICTER).
func dcStringField(raw *dcJValue, key string) (string, error) {
	v, ok := raw.get(key)
	if !ok || !v.truthy() {
		return "", nil
	}
	if v.kind != dcJString {
		return "", dcValueError{"ValueError", fmt.Sprintf("%s must be a string, got %s (STRICTER: Python took str() of it)", key, v.typeName())}
	}
	return v.s, nil
}

func dcLoadDocuments(path string) ([]dcSearchDoc, error) {
	text, err := a1ReadText(path)
	if err != nil {
		return nil, err
	}
	payload, err := dcParseJSON(text)
	if err != nil {
		return nil, err
	}
	if payload.kind != dcJObject {
		return nil, a1Exception{"AttributeError", "'" + payload.typeName() + "' object has no attribute 'get'"}
	}
	rawDocs, _ := payload.get("docs")
	if rawDocs == nil || rawDocs.kind != dcJArray {
		return nil, dcValueError{"ValueError", "search index has no docs array: " + path}
	}
	var docs []dcSearchDoc
	for _, raw := range rawDocs.arr {
		if raw.kind != dcJObject {
			continue
		}
		location, err := dcStringField(raw, "location")
		if err != nil {
			return nil, err
		}
		title, err := dcStringField(raw, "title")
		if err != nil {
			return nil, err
		}
		body, err := dcStringField(raw, "text")
		if err != nil {
			return nil, err
		}
		if location != "" {
			docs = append(docs, dcSearchDoc{location, title, body})
		}
	}
	return docs, nil
}

func dcLoadCases(path string) ([]dcSearchCase, error) {
	text, err := a1ReadText(path)
	if err != nil {
		return nil, err
	}
	payload, err := dcParseJSON(text)
	if err != nil {
		return nil, err
	}
	if payload.kind != dcJObject {
		return nil, a1Exception{"AttributeError", "'" + payload.typeName() + "' object has no attribute 'get'"}
	}
	rawCases, _ := payload.get("queries")
	if rawCases == nil || rawCases.kind != dcJArray {
		return nil, dcValueError{"ValueError", "query file has no queries array: " + path}
	}
	var cases []dcSearchCase
	for _, raw := range rawCases.arr {
		if raw.kind != dcJObject {
			continue
		}
		q, err := dcStringField(raw, "query")
		if err != nil {
			return nil, err
		}
		query := a1Strip(q)
		exp, err := dcStringField(raw, "expected")
		if err != nil {
			return nil, err
		}
		expected, err := dcNormalizeURL(exp)
		if err != nil {
			return nil, err
		}
		maxRank, err := dcMaxRank(raw)
		if err != nil {
			return nil, err
		}
		if query == "" || expected == "/" {
			return nil, dcValueError{"ValueError", "invalid search case: " + raw.repr()}
		}
		cases = append(cases, dcSearchCase{query, expected, maxRank})
	}
	return cases, nil
}

// dcMaxRank is int(raw.get("max_rank") or 5). A number gives its integer part, true gives 1, a falsy value gives 5.
// A non-empty string, list or object is refused (STRICTER for strings, which int() parsed; a TypeError in Python for
// the others), as are NaN and infinity (ValueError / OverflowError in Python).
func dcMaxRank(raw *dcJValue) (*big.Int, error) {
	v, ok := raw.get("max_rank")
	if !ok || !v.truthy() {
		return big.NewInt(5), nil
	}
	switch v.kind {
	case dcJBool:
		return big.NewInt(1), nil
	case dcJInt:
		return v.i, nil
	case dcJFloat:
		if math.IsNaN(v.f) {
			return nil, dcValueError{"ValueError", "cannot convert float NaN to integer"}
		}
		if math.IsInf(v.f, 0) {
			return nil, dcValueError{"OverflowError", "cannot convert float infinity to integer"}
		}
		n, _ := new(big.Float).SetFloat64(v.f).Int(nil)
		return n, nil
	case dcJString:
		return nil, dcValueError{"ValueError", "max_rank must be a number (STRICTER: Python parsed the string)"}
	}
	return nil, dcValueError{"TypeError", "int() argument must be a string, a bytes-like object or a real number, not '" + v.typeName() + "'"}
}

type dcRanked struct {
	url   string
	score float64
}

func dcRank(query string, docs []dcSearchDoc) ([]dcRanked, error) {
	best := map[string]float64{}
	for _, d := range docs {
		canonical, err := dcNormalizeURL(d.location)
		if err != nil {
			return nil, err
		}
		score, err := dcScore(query, d)
		if err != nil {
			return nil, err
		}
		best[canonical] = math.Max(best[canonical], score)
	}
	out := make([]dcRanked, 0, len(best))
	for u, s := range best {
		out = append(out, dcRanked{u, s})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		return out[i].url < out[j].url
	})
	return out, nil
}

func dcSearchVerb(args []string, stdout, stderr io.Writer) int {
	flags, code := dcParseFlags("check-docs-search", args, []dcFlag{
		{name: "site-dir", required: true},
		{name: "queries", required: true},
		{name: "report", def: ".build/docs-search-acceptance.tsv"},
	}, stderr)
	if flags == nil {
		return code
	}
	siteDir, queries, report := a1PathStr(flags["site-dir"]), a1PathStr(flags["queries"]), a1PathStr(flags["report"])
	indexPath := dcJoin(dcJoin(siteDir, "search"), "search_index.json")
	if fi, err := os.Stat(indexPath); err != nil || !fi.Mode().IsRegular() { // S164-166
		if err != nil && !dcIgnorableStat(err) {
			return dcFail(stderr, err)
		}
		fmt.Fprintf(stderr, "ERROR: search index not found: %s\n", indexPath)
		return 1
	}
	loadFail := func(err error) int {
		var ve dcValueError
		var de a1DecodeError
		var pe *os.PathError
		switch {
		case errors.As(err, &ve) && (ve.kind == "ValueError" || ve.kind == "JSONDecodeError"):
			fmt.Fprintf(stderr, "ERROR: %s\n", ve.msg)
		case errors.As(err, &de):
			fmt.Fprintf(stderr, "ERROR: %s\n", de.Error())
		case errors.As(err, &pe):
			_, msg := a1OSError(err)
			fmt.Fprintf(stderr, "ERROR: %s\n", msg)
		default:
			return dcFail(stderr, err)
		}
		return 1
	}
	docs, err := dcLoadDocuments(indexPath) // S105-118
	if err != nil {
		return loadFail(err)
	}
	cases, err := dcLoadCases(queries) // S123-140
	if err != nil {
		return loadFail(err)
	}
	var tsv strings.Builder
	tsv.WriteString(dcCSVRow([]string{"query", "expected", "rank", "max_rank", "top_result", "status"}))
	var failures []string
	for _, c := range cases {
		ranked, err := dcRank(c.query, docs)
		if err != nil {
			return dcFail(stderr, err)
		}
		rank := 0
		for i, r := range ranked {
			if r.url == c.expected {
				rank = i + 1
				break
			}
		}
		top := ""
		if len(ranked) > 0 {
			top = ranked[0].url
		}
		passed := rank != 0 && c.maxRank.Cmp(big.NewInt(int64(rank))) >= 0
		status, rankText := "fail", "missing"
		if passed {
			status = "pass"
		}
		if rank != 0 {
			rankText = fmt.Sprint(rank)
		}
		tsv.WriteString(dcCSVRow([]string{c.query, c.expected, rankText, c.maxRank.String(), top, status}))
		if !passed {
			failures = append(failures, fmt.Sprintf("%s: expected %s at rank <= %s, found %s; top=%s", a1Repr(c.query), c.expected, c.maxRank, rankText, top))
		}
	}
	if err := os.MkdirAll(filepath.Dir(report), 0o777); err != nil { // S175
		return dcFail(stderr, err)
	}
	if err := os.WriteFile(report, []byte(tsv.String()), 0o666); err != nil { // S177
		return dcFail(stderr, err)
	}
	if len(failures) > 0 {
		for _, f := range failures {
			fmt.Fprintf(stdout, "ERROR: %s\n", f)
		}
		fmt.Fprintf(stdout, "Search acceptance failed; report: %s\n", report)
		return 1
	}
	fmt.Fprintf(stdout, "Search acceptance passed for %d task queries\n", len(cases))
	fmt.Fprintf(stdout, "Report: %s\n", report)
	return 0
}
