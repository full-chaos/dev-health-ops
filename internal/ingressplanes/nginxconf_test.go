package ingressplanes

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// This file is a SECOND reading of the generated file, written without the
// generator: a small parser of nginx's configuration syntax, and nginx's rule
// for choosing a location. The tests compare what this reading finds with the
// contract, so a defect of the generator is not hidden by the generator.

// directive is one nginx directive: its name, its arguments and, for a block
// directive, the directives inside the braces.
type directive struct {
	name  string
	args  []string
	block []directive
}

type nginxToken struct {
	text   string
	quoted bool
}

// nginxTokens splits configuration text as nginx does: words, quoted strings
// (a backslash keeps the next quote or backslash), `;`, `{`, `}`, and `#`
// comments to the end of the line.
func nginxTokens(text string) ([]nginxToken, error) {
	var tokens []nginxToken
	for i := 0; i < len(text); {
		c := text[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#':
			for i < len(text) && text[i] != '\n' {
				i++
			}
		case c == ';' || c == '{' || c == '}':
			tokens = append(tokens, nginxToken{text: string(c)})
			i++
		case c == '"' || c == '\'':
			var word strings.Builder
			j := i + 1
			for ; j < len(text) && text[j] != c; j++ {
				if text[j] == '\\' && j+1 < len(text) && (text[j+1] == '"' || text[j+1] == '\'' || text[j+1] == '\\') {
					j++
				}
				word.WriteByte(text[j])
			}
			if j >= len(text) {
				return nil, fmt.Errorf("a quoted string that starts at byte %d has no end", i)
			}
			tokens = append(tokens, nginxToken{text: word.String(), quoted: true})
			i = j + 1
		default:
			j := i
			for j < len(text) && !strings.ContainsRune(" \t\n\r;{", rune(text[j])) {
				j++
			}
			tokens = append(tokens, nginxToken{text: text[i:j]})
			i = j
		}
	}
	return tokens, nil
}

// parseNginx reads configuration text into directives.
func parseNginx(text string) ([]directive, error) {
	tokens, err := nginxTokens(text)
	if err != nil {
		return nil, err
	}
	position := 0
	var block func(depth int) ([]directive, error)
	block = func(depth int) ([]directive, error) {
		var directives []directive
		var words []string
		for position < len(tokens) {
			token := tokens[position]
			position++
			switch {
			case token.quoted:
				words = append(words, token.text)
			case token.text == ";":
				if len(words) == 0 {
					return nil, fmt.Errorf("an empty directive before token %d", position)
				}
				directives = append(directives, directive{name: words[0], args: words[1:]})
				words = nil
			case token.text == "{":
				if len(words) == 0 {
					return nil, fmt.Errorf("a block with no name before token %d", position)
				}
				inner, err := block(depth + 1)
				if err != nil {
					return nil, err
				}
				directives = append(directives, directive{name: words[0], args: words[1:], block: inner})
				words = nil
			case token.text == "}":
				if len(words) != 0 || depth == 0 {
					return nil, fmt.Errorf("an unexpected } at token %d", position)
				}
				return directives, nil
			default:
				words = append(words, token.text)
			}
		}
		if depth != 0 || len(words) != 0 {
			return nil, fmt.Errorf("the text ends inside a block or a directive")
		}
		return directives, nil
	}
	return block(0)
}

func children(directives []directive, name string) []directive {
	var found []directive
	for _, d := range directives {
		if d.name == name {
			found = append(found, d)
		}
	}
	return found
}

// routerLocation is one location of the parsed file.
type routerLocation struct {
	modifier string // "~*", "~", "=", "^~" or "" (a prefix)
	pattern  string
	upstream string // host:port, after the plane variable was resolved
}

// routerFile is what the tests read out of a router configuration.
type routerFile struct {
	server    []directive // the directives of the one server block
	locations []routerLocation
}

// readRouterFile parses a router configuration and resolves the upstream of
// every location. Anything it cannot read is a test failure, never a skip: a
// file this reading does not understand was not checked.
func readRouterFile(t *testing.T, text string) routerFile {
	t.Helper()
	top, err := parseNginx(text)
	if err != nil {
		t.Fatalf("the router configuration does not parse: %v", err)
	}
	https := children(top, "http")
	if len(https) != 1 {
		t.Fatalf("want one http block, got %d", len(https))
	}
	servers := children(https[0].block, "server")
	if len(servers) != 1 {
		t.Fatalf("want one server block, got %d", len(servers))
	}
	file := routerFile{server: servers[0].block}
	variables := map[string]string{}
	for _, set := range children(file.server, "set") {
		if len(set.args) != 2 {
			t.Fatalf("set with %d arguments: %v", len(set.args), set.args)
		}
		variables[set.args[0]] = set.args[1]
	}
	for _, location := range children(file.server, "location") {
		var parsed routerLocation
		switch len(location.args) {
		case 1:
			parsed.pattern = location.args[0]
		case 2:
			parsed.modifier, parsed.pattern = location.args[0], location.args[1]
		default:
			t.Fatalf("location with %d arguments: %v", len(location.args), location.args)
		}
		if len(location.block) != 1 || location.block[0].name != "proxy_pass" || len(location.block[0].args) != 1 {
			t.Fatalf("location %v must hold exactly one proxy_pass, got %+v", location.args, location.block)
		}
		target, ok := strings.CutPrefix(location.block[0].args[0], "http://")
		if !ok {
			t.Fatalf("location %v: proxy_pass %q is not http://", location.args, location.block[0].args[0])
		}
		if strings.HasPrefix(target, "$") {
			value, known := variables[target]
			if !known {
				t.Fatalf("location %v: proxy_pass names %s, which no set directive of the server defines", location.args, target)
			}
			target = value
		}
		parsed.upstream = target
		file.locations = append(file.locations, parsed)
	}
	if len(file.locations) == 0 {
		t.Fatalf("the router configuration has no location: nothing would be checked")
	}
	return file
}

// compiledLocations keeps each location regex compiled once: the routing test
// asks for every row. The tests of this package do not run in parallel.
var compiledLocations = map[string]*regexp.Regexp{}

// chooseLocation is nginx's rule for one request path (ngx_http_core_module,
// "location"): an exact (=) location wins; else the longest matching prefix
// is remembered, and wins at once when it is a ^~ prefix; else the regex
// locations are tried in the order of the file and the first match wins; else
// the remembered prefix. ok is false when no location matches.
//
// It is a model, not nginx: it takes the path as given. nginx first decodes
// percent-escapes, resolves "." and ".." and merges slashes; the real-nginx
// test has rows for those.
func chooseLocation(t *testing.T, locations []routerLocation, path string) (routerLocation, bool) {
	t.Helper()
	var prefix *routerLocation
	for i, location := range locations {
		switch location.modifier {
		case "=":
			if path == location.pattern {
				return location, true
			}
		case "", "^~":
			if strings.HasPrefix(path, location.pattern) && (prefix == nil || len(location.pattern) > len(prefix.pattern)) {
				prefix = &locations[i]
			}
		case "~", "~*":
		default:
			t.Fatalf("location modifier %q is not one this model knows", location.modifier)
		}
	}
	if prefix != nil && prefix.modifier == "^~" {
		return *prefix, true
	}
	for _, location := range locations {
		if location.modifier != "~" && location.modifier != "~*" {
			continue
		}
		expression := location.pattern
		if location.modifier == "~*" {
			expression = "(?i)" + expression
		}
		compiled, known := compiledLocations[expression]
		if !known {
			var err error
			if compiled, err = regexp.Compile(expression); err != nil {
				t.Fatalf("location regex %q does not compile: %v", location.pattern, err)
			}
			compiledLocations[expression] = compiled
		}
		if compiled.MatchString(path) {
			return location, true
		}
	}
	if prefix != nil {
		return *prefix, true
	}
	return routerLocation{}, false
}

// TestNginxReadingKnowsTheSyntaxItMeets pins the parser and the location rule
// on a hand-written file, so a defect of this second reading is not mistaken
// for agreement.
func TestNginxReadingKnowsTheSyntaxItMeets(t *testing.T) {
	const text = `
# a comment with a { brace and a ; semicolon
http {
    server {
        set $a "one:1";
        set $b 'two:2';
        location = /exact { proxy_pass http://$a; }
        location /pre { proxy_pass http://$b; }
        location ^~ /stop { proxy_pass http://$a; }
        location ~* "^/re/[^/]+/x\\.v2$" { proxy_pass http://$b; }
        location ~ "^/Case$" { proxy_pass http://plain:3; }
        location / { proxy_pass http://$a; }
    }
}`
	file := readRouterFile(t, text)
	if got := file.locations[3].pattern; got != `^/re/[^/]+/x\.v2$` {
		t.Fatalf(`a quoted "\\." must read as \.: got %q`, got)
	}
	for path, want := range map[string]string{
		"/exact":         "one:1",
		"/exactly":       "one:1", // no exact match, no /pre: the "/" prefix
		"/pre/x":         "two:2",
		"/stop/re/a/x":   "one:1", // ^~ wins before any regex is tried
		"/re/a/x.v2":     "two:2",
		"/RE/A/X.V2":     "two:2", // ~* ignores case
		"/re/a/xav2":     "one:1", // \. is a dot only
		"/re/a/b/x.v2":   "one:1", // [^/]+ is one segment
		"/Case":          "plain:3",
		"/case":          "one:1", // ~ keeps case
		"/pre/re/a/x.v2": "two:2", // the regex is anchored at its start, so the /pre prefix is the answer
	} {
		location, ok := chooseLocation(t, file.locations, path)
		if !ok || location.upstream != want {
			t.Errorf("%s: got %q (matched %v), want %q", path, location.upstream, ok, want)
		}
	}
	if _, ok := chooseLocation(t, file.locations[:5], "/nothing"); ok {
		t.Errorf("a path that no location matches must not get a location")
	}
	for name, bad := range map[string]string{
		"open string": `http { server { location "x { } } }`,
		"open block":  `http { server {`,
		"stray brace": `http { } }`,
	} {
		if _, err := parseNginx(bad); err == nil {
			t.Errorf("%s: the parser must refuse it", name)
		}
	}
}
