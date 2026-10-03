package venueoracle

import (
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// knownDefect is a defect a venue oracle found in a Go route, planted into a
// copy of a frozen Python answer. A frozen golden replaces the live Python
// plane, so the comparison against it must still see every one of these.
type knownDefect struct {
	name string
	// plant returns the answer with the defect, or false when this answer
	// has nothing the defect could change.
	plant func(Response) (Response, bool)
}

// knownDefects are the defect classes the venue oracles rediscovered against
// the live Python plane. Each one must still be a DIFF against the frozen
// answers.
var knownDefects = []knownDefect{
	// A response body built from a map: its keys come out alphabetized
	// instead of in the model's order (bodies are compared as bytes, never
	// decoded, because a decoding comparison cannot see this).
	{"object keys reordered", func(r Response) (Response, bool) {
		body, ok := swapFirstMembers(r.Body)
		r.Body = body
		return r, ok
	}},
	// A JSON text written with other separators than Python's: pyjson's
	// compact form against json.dumps' ", " and ": ", or the reverse.
	{"JSON separators changed", func(r Response) (Response, bool) {
		for _, swap := range [][2]string{{`", "`, `","`}, {`": "`, `":"`}, {`","`, `", "`}, {`":`, `": `}} {
			if strings.Contains(r.Body, swap[0]) {
				r.Body = strings.Replace(r.Body, swap[0], swap[1], 1)
				return r, true
			}
		}
		return r, false
	}},
	// A string whose character Python keeps (a lone surrogate, written
	// escaped) or writes as is, answered as U+FFFD by Go.
	{"string character replaced by U+FFFD", func(r Response) (Response, bool) {
		start := strings.Index(r.Body, `"`)
		if start < 0 || start+1 >= len(r.Body) || r.Body[start+1] == '"' {
			return r, false
		}
		end := start + 2
		if r.Body[start+1] == '\\' {
			end = start + 3
		}
		r.Body = r.Body[:start+1] + "\ufffd" + r.Body[end:]
		return r, true
	}},
	{"status changed", func(r Response) (Response, bool) {
		r.Status++
		return r, true
	}},
	{"compared header value changed", func(r Response) (Response, bool) {
		name, ok := firstComparedHeader(r)
		if !ok {
			return r, false
		}
		r.Headers = copyHeaders(r.Headers)
		r.Headers[name] += "; changed"
		return r, true
	}},
	{"compared header missing", func(r Response) (Response, bool) {
		name, ok := firstComparedHeader(r)
		if !ok {
			return r, false
		}
		r.Headers = copyHeaders(r.Headers)
		delete(r.Headers, name)
		return r, true
	}},
}

// TestFrozenGoldensRediscoverKnownDefects is the acceptance gate of the
// frozen goldens: for every golden committed under internal/, every frozen
// answer compares SAME with itself and DIFF with each known defect planted in
// a copy, and every frozen row snapshot differs from a copy with one character
// changed. It runs on every comparator change (it is a plain unit test of this
// package). A defect no golden answer could carry, or no golden at all, fails:
// a gate that planted nothing measured nothing.
func TestFrozenGoldensRediscoverKnownDefects(t *testing.T) {
	goldens := committedGoldens(t)
	if len(goldens) == 0 {
		t.Fatal("no committed golden found under internal/: the gate would measure nothing")
	}
	planted := map[string]int{}
	rows := 0
	for _, path := range goldens {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var file goldenFile
		if err := json.Unmarshal(raw, &file); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if len(file.Requests) == 0 {
			t.Errorf("%s holds no answer", path)
		}
		for index, entry := range file.Requests {
			request := Request{Name: entry.Name, Method: entry.Method, Path: entry.Path}
			answer := Response{Status: entry.Status, Headers: entry.Headers, Body: entry.Body}
			if same, _, _, _ := Compare(request, answer, answer, DiffOptions{}); !same {
				t.Fatalf("%s answer %d (%s) differs from itself", path, index, entry.Name)
			}
			for _, defect := range knownDefects {
				mutated, ok := defect.plant(answer)
				if !ok {
					continue
				}
				planted[defect.name]++
				if same, _, _, _ := Compare(request, answer, mutated, DiffOptions{}); same {
					t.Errorf("%s answer %d (%s): %q planted in the Go answer compares SAME", path, index, entry.Name, defect.name)
				}
			}
		}
		for name, snapshot := range file.Rows {
			if snapshot.Rows == "" {
				continue
			}
			rows++
			changed := []rune(snapshot.Rows)
			changed[len(changed)/2]++
			if rowsDiffer(name, snapshot.Rows, string(changed)) == nil {
				t.Errorf("%s rows %q: a changed row compares equal", path, name)
			}
		}
	}
	for _, defect := range knownDefects {
		if planted[defect.name] == 0 {
			t.Errorf("known defect %q was planted in no frozen answer: it is no longer measured", defect.name)
		}
	}
	if rows == 0 {
		t.Error("no frozen row snapshot was found: row drift is no longer measured")
	}
	t.Logf("%d goldens; planted %v; %d row snapshots", len(goldens), planted, rows)
}

// committedGoldens lists every golden file under internal/: a JSON file whose
// header names a 40-hex Python build and a producer digest.
func committedGoldens(t *testing.T) []string {
	t.Helper()
	_, file, _, _ := moduleroot.Caller(0)
	internal := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	var found []string
	err := filepath.WalkDir(internal, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".json") || !strings.Contains(filepath.ToSlash(path), "/testdata/") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(raw), `"producer_digest"`) {
			return nil
		}
		var header struct {
			Header goldenHeader `json:"header"`
		}
		if json.Unmarshal(raw, &header) != nil || !buildPattern.MatchString(header.Header.PythonBuild) || len(header.Header.ProducerDigest) != 64 {
			return nil
		}
		found = append(found, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(found)
	return found
}

func firstComparedHeader(r Response) (string, bool) {
	names := make([]string, 0, len(r.Headers))
	for name := range r.Headers {
		if !Volatile[name] && name != "content-length" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "", false
	}
	sort.Strings(names)
	return names[0], true
}

func copyHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for key, value := range headers {
		out[key] = value
	}
	return out
}

// swapFirstMembers swaps the first two members of the first JSON object in
// body that has two, keeping every other byte, or returns false.
func swapFirstMembers(body string) (string, bool) {
	for start := 0; start < len(body); start++ {
		switch body[start] {
		case '"':
			start = skipString(body, start) - 1
		case '{':
			first, second, ok := objectMembers(body, start)
			if ok && body[first[0]:first[1]] != body[second[0]:second[1]] {
				return body[:first[0]] + body[second[0]:second[1]] + body[first[1]:second[0]] + body[first[0]:first[1]] + body[second[1]:], true
			}
		}
	}
	return body, false
}

// objectMembers returns the spans of the first two members (key through
// value) of the object opening at body[open].
func objectMembers(body string, open int) (first, second [2]int, ok bool) {
	at := skipSpace(body, open+1)
	for member := 0; member < 2; member++ {
		if at >= len(body) || body[at] != '"' {
			return first, second, false
		}
		begin := at
		at = skipSpace(body, skipString(body, at))
		if at >= len(body) || body[at] != ':' {
			return first, second, false
		}
		at = skipValue(body, skipSpace(body, at+1))
		if at < 0 {
			return first, second, false
		}
		if member == 0 {
			first = [2]int{begin, at}
			at = skipSpace(body, at)
			if at >= len(body) || body[at] != ',' {
				return first, second, false
			}
			at = skipSpace(body, at+1)
		} else {
			second = [2]int{begin, at}
		}
	}
	return first, second, true
}

func skipSpace(body string, at int) int {
	for at < len(body) && strings.IndexByte(" \t\r\n", body[at]) >= 0 {
		at++
	}
	return at
}

// skipString returns the index after the string opening at body[at].
func skipString(body string, at int) int {
	for at++; at < len(body); at++ {
		switch body[at] {
		case '\\':
			at++
		case '"':
			return at + 1
		}
	}
	return len(body)
}

// skipValue returns the index after the value starting at body[at], or -1.
func skipValue(body string, at int) int {
	if at >= len(body) {
		return -1
	}
	switch body[at] {
	case '"':
		return skipString(body, at)
	case '{', '[':
		depth := 0
		for ; at < len(body); at++ {
			switch body[at] {
			case '"':
				at = skipString(body, at) - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return at + 1
				}
			}
		}
		return -1
	default:
		for at < len(body) && strings.IndexByte(",}] \t\r\n", body[at]) < 0 {
			at++
		}
		return at
	}
}

func TestSwapFirstMembersKeepsEveryOtherByte(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`{"a":1,"b":[1,{"c":2}],"d":3}`, `{"b":[1,{"c":2}],"a":1,"d":3}`},
		{`{"a": "x,y", "b": {"k": "}"}}`, `{"b": {"k": "}"}, "a": "x,y"}`},
		{`[{"only":1},{"p":"\"q","r":null}]`, `[{"only":1},{"r":null,"p":"\"q"}]`},
		{`{"s":"{\"a\":1,\"b\":2}","t":true}`, `{"t":true,"s":"{\"a\":1,\"b\":2}"}`},
	} {
		got, ok := swapFirstMembers(c.in)
		if !ok || got != c.want {
			t.Errorf("%s: got %s (%v), want %s", c.in, got, ok, c.want)
		}
	}
	for _, body := range []string{`{"a":1}`, `[]`, `"text {\"a\":1,\"b\":2}"`, ``, `{"a":1,"a":1}`} {
		if got, ok := swapFirstMembers(body); ok {
			t.Errorf("%s: swapped to %s", body, got)
		}
	}
}
