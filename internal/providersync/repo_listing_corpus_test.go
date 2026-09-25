package providersync

import (
	"fmt"
	"net/url"
)

func intPtr(n int) *int { return &n }

func setMax(listing map[string]any, max *int) {
	if max != nil {
		listing["max"] = float64(*max)
	}
}

func maxLabel(max *int) string {
	if max == nil {
		return "none"
	}
	return fmt.Sprint(*max)
}

func ghItem(name, fullName string) map[string]any {
	item := map[string]any{"name": name, "full_name": fullName}
	return item
}

// ghPages returns scripted pages of a GitHub listing at path: page 1 is the
// initial request (?per_page=100), later pages are the absolute rel=next URLs.
func ghPages(path string, pages [][]any) []listingScriptEntry {
	var script []listingScriptEntry
	for i, items := range pages {
		uri := path + "?per_page=100"
		if i > 0 {
			uri = fmt.Sprintf("%s?page=%d&per_page=100", path, i+1)
		}
		entry := listingScriptEntry{URI: uri, Body: items}
		if i+1 < len(pages) {
			entry.Link = fmt.Sprintf(`<https://api.github.com%s?per_page=100&page=%d>; rel="next", <https://api.github.com%s?per_page=100&page=%d>; rel="last"`, path, i+2, path, len(pages))
		}
		script = append(script, entry)
	}
	return script
}

func glPages(path string, pages [][]any, perPageFull bool) []listingScriptEntry {
	var script []listingScriptEntry
	for i, items := range pages {
		uri := fmt.Sprintf("/api/v4%s?page=%d&per_page=100", path, i+1)
		entry := listingScriptEntry{URI: uri, Body: items}
		if !perPageFull && i+1 < len(pages) {
			entry.Headers = map[string]string{"X-Next-Page": fmt.Sprint(i + 2)}
		}
		script = append(script, entry)
	}
	return script
}

func repoListingCorpus() []listingCase {
	var corpus []listingCase
	add := func(name, provider string, listing map[string]any, script []listingScriptEntry) {
		corpus = append(corpus, listingCase{Name: name, Provider: provider, Listing: listing, Script: script})
	}

	// --- GitHub -------------------------------------------------------------
	universe := [][]any{
		{ghItem("web", "acme/web"), ghItem("API", "Acme/API"), ghItem("docs", "acme/docs")},
		{ghItem("Web-Tools", "ACME/Web-Tools"), ghItem("mobile", "acme/mobile"), 42, "text", nil},
		{ghItem("x", "acme/x"), map[string]any{"name": "nofull"}, map[string]any{"full_name": "acme/noname"}, ghItem("İstanbul", "acme/İstanbul")},
	}
	odd := [][]any{
		{map[string]any{"name": 7, "full_name": 8}, map[string]any{"name": true, "full_name": "acme/true"}, map[string]any{"name": []any{1}, "full_name": "acme/list"}, ghItem("ok", "acme/ok")},
	}
	pathFor := func(kind, owner string) string {
		switch kind {
		case "org":
			return "/orgs/" + url.PathEscape(owner) + "/repos"
		case "user":
			return "/users/" + url.PathEscape(owner) + "/repos"
		}
		return "/user/repos"
	}
	scriptAll := func() []listingScriptEntry {
		var s []listingScriptEntry
		s = append(s, ghPages("/orgs/acme/repos", universe)...)
		s = append(s, ghPages("/users/acme/repos", universe)...)
		s = append(s, ghPages("/users/bob/repos", universe)...)
		s = append(s, ghPages("/user/repos", universe)...)
		s = append(s, ghPages("/orgs/Acme%20Corp/repos", universe)...)
		s = append(s, ghPages("/users/a%2Fb/repos", universe)...)
		s = append(s, ghPages("/orgs/odd/repos", odd)...)
		return s
	}
	patterns := []string{"", "*", "acme/*", "ACME/WEB*", "acme/web", "*/api", "*/w*", "acme/[wa]*", "acme/?eb", "?cme/*", "acme/[!w]*", "*Tools", "acme/İ*", "acme/i*", "nomatch*", "acme/*/x", "a[", "acme/*[", "  acme/*", "acme/"}
	// nil is no cap (the key is absent); 0 and negatives are caps in Python too.
	maxes := []*int{nil, intPtr(0), intPtr(1), intPtr(2), intPtr(3), intPtr(4), intPtr(8), intPtr(-1), intPtr(-5)}
	owners := []map[string]any{{}, {"org": "acme"}, {"user": "bob"}, {"org": "acme", "user": "bob"}, {"org": "Acme Corp"}, {"user": "a/b"}, {"org": "odd"}}
	for _, owner := range owners {
		for _, pattern := range patterns {
			for _, max := range maxes {
				listing := map[string]any{"pattern": pattern}
				setMax(listing, max)
				for k, v := range owner {
					listing[k] = v
				}
				add(fmt.Sprintf("gh owner=%v pattern=%q max=%s", owner, pattern, maxLabel(max)), "github", listing, scriptAll())
			}
		}
	}
	// the owner extracted from a pattern
	for _, pattern := range []string{"acme/*", "bob/*", "a?me/*", "*/x", "Acme/Web*", "/x", "acme*/x"} {
		add("gh extracted owner "+pattern, "github", map[string]any{"pattern": pattern}, scriptAll())
	}
	// errors: a missing page, a non-list body, a status error
	broken := ghPages("/orgs/acme/repos", universe)
	add("gh page 2 not scripted (404)", "github", map[string]any{"org": "acme"}, broken[:1])
	add("gh non-list body", "github", map[string]any{"org": "acme"}, []listingScriptEntry{{URI: "/orgs/acme/repos?per_page=100", Body: map[string]any{"message": "x"}}})
	add("gh forbidden", "github", map[string]any{"org": "acme"}, []listingScriptEntry{{URI: "/orgs/acme/repos?per_page=100", Status: 403, Body: map[string]any{"message": "no"}}})
	add("gh empty org", "github", map[string]any{"org": "acme"}, []listingScriptEntry{{URI: "/orgs/acme/repos?per_page=100", Body: []any{}}})
	// the page cap: 101 one-item pages
	var long [][]any
	for i := 0; i < 101; i++ {
		long = append(long, []any{ghItem(fmt.Sprintf("r%d", i), fmt.Sprintf("acme/r%d", i))})
	}
	for _, max := range []*int{nil, intPtr(0), intPtr(50), intPtr(100), intPtr(101)} {
		listing := map[string]any{"org": "acme"}
		setMax(listing, max)
		add(fmt.Sprintf("gh 101 pages max=%s", maxLabel(max)), "github", listing, ghPages("/orgs/acme/repos", long))
	}
	add("gh 101 pages pattern", "github", map[string]any{"org": "acme", "pattern": "acme/r9*"}, ghPages("/orgs/acme/repos", long))
	_ = pathFor

	// --- GitLab -------------------------------------------------------------
	proj := func(id any, name, path, ns any) map[string]any {
		item := map[string]any{"id": id, "name": name}
		if path != nil {
			item["path"] = path
		}
		if ns != nil {
			item["path_with_namespace"] = ns
		}
		return item
	}
	glUniverse := [][]any{
		{proj(1, "web", "web", "Acme/web"), proj(2, "api", "api", "acme/API"), proj(3, "docs", "docs", "acme/sub/docs")},
		{proj("4", "mobile", "mobile", "acme/mobile"), proj(5, "onlypath", "onlypath", nil), proj(6, nil, nil, nil), "text", 7, proj(8, "x", "x", "other/x")},
		{proj(9, "y", "y", "acme/y"), proj(nil, "noid", "noid", "acme/noid"), proj("abc", "badid", "badid", "acme/badid"), proj(2.0, "float", "float", "acme/float")},
	}
	glScript := func() []listingScriptEntry {
		var s []listingScriptEntry
		s = append(s, glPages("/projects", glUniverse, false)...)
		s = append(s, glPages("/groups/acme/projects", glUniverse, false)...)
		s = append(s, glPages("/groups/Acme%20Corp/projects", glUniverse, false)...)
		s = append(s, glPages("/groups/a%2Fb/projects", glUniverse, false)...)
		return s
	}
	glPatterns := []string{"", "*", "acme/*", "ACME/*", "acme/sub/*", "*/api", "*/*/docs", "acme/[wa]*", "?cme/*", "nomatch", "acme/*y", "*x"}
	glGroups := []string{"", "acme", "Acme Corp", "a/b"}
	for _, group := range glGroups {
		for _, pattern := range glPatterns {
			for _, max := range maxes {
				listing := map[string]any{"pattern": pattern}
				setMax(listing, max)
				if group != "" {
					listing["group"] = group
				}
				add(fmt.Sprintf("gl group=%q pattern=%q max=%s", group, pattern, maxLabel(max)), "gitlab", listing, glScript())
			}
		}
	}
	// pagination: a full page continues without a header, X-Next-Page wins, a malformed header stops
	var full []any
	for i := 0; i < 100; i++ {
		full = append(full, proj(1000+i, fmt.Sprintf("p%d", i), fmt.Sprintf("p%d", i), fmt.Sprintf("acme/p%d", i)))
	}
	fullPages := [][]any{full, {proj(2000, "tail", "tail", "acme/tail")}}
	for _, max := range []*int{nil, intPtr(0), intPtr(1), intPtr(99), intPtr(100), intPtr(101), intPtr(150), intPtr(-1), intPtr(-99), intPtr(-150)} {
		plain := map[string]any{"group": "acme"}
		setMax(plain, max)
		add(fmt.Sprintf("gl full page heuristic max=%s", maxLabel(max)), "gitlab", plain, glPages("/groups/acme/projects", fullPages, true))
		patterned := map[string]any{"group": "acme", "pattern": "acme/*"}
		setMax(patterned, max)
		add(fmt.Sprintf("gl full page heuristic pattern max=%s", maxLabel(max)), "gitlab", patterned, glPages("/groups/acme/projects", fullPages, true))
	}
	malformed := glPages("/groups/acme/projects", glUniverse, false)
	malformed[0].Headers = map[string]string{"X-Next-Page": "nope"}
	add("gl malformed X-Next-Page", "gitlab", map[string]any{"group": "acme"}, malformed)
	skip := glPages("/groups/acme/projects", glUniverse, false)
	skip[0].Headers = map[string]string{"X-Next-Page": "3"}
	skip = append(skip[:1], listingScriptEntry{URI: "/api/v4/groups/acme/projects?page=3&per_page=100", Body: glUniverse[2]})
	add("gl X-Next-Page skips ahead", "gitlab", map[string]any{"group": "acme"}, skip)
	add("gl 404", "gitlab", map[string]any{"group": "acme"}, []listingScriptEntry{})
	add("gl non-list", "gitlab", map[string]any{"group": "acme"}, []listingScriptEntry{{URI: "/api/v4/groups/acme/projects?page=1&per_page=100", Body: map[string]any{"message": "x"}}})
	add("gl empty", "gitlab", map[string]any{}, []listingScriptEntry{{URI: "/api/v4/projects?page=1&per_page=100", Body: []any{}}})
	return corpus
}
