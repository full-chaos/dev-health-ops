package workflows_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// A merge queue run of docker-images.yml must build and test the images and must never publish them. The queue builds a
// temporary branch; an image pushed from it is an image of a tree that has not merged and may be ejected.
//
// The test evaluates the workflow's own expressions for event_name=merge_group (and for a main push as the control).

type queueCtx map[string]string

type queueValue struct {
	s      string
	isBool bool
	b      bool
}

func (v queueValue) truthy() bool {
	if v.isBool {
		return v.b
	}
	return v.s != "" && v.s != "0" && v.s != "false"
}

func queueBool(b bool) queueValue { return queueValue{isBool: true, b: b} }

type queueParser struct {
	tokens []string
	pos    int
	ctx    queueCtx
	src    string
	err    error
}

var queueTokenRE = regexp.MustCompile(`'(?:[^']|'')*'|&&|\|\||==|!=|[()!,]|[^\s()!,&|=']+`)

func (p *queueParser) fail(format string, args ...any) queueValue {
	if p.err == nil {
		p.err = fmt.Errorf("expression %q: %s", p.src, fmt.Sprintf(format, args...))
	}
	return queueValue{}
}

func (p *queueParser) peek() string {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return ""
}

func (p *queueParser) or() queueValue {
	v := p.and()
	for p.peek() == "||" {
		p.pos++
		rhs := p.and()
		if !v.truthy() {
			v = rhs
		}
	}
	return v
}

func (p *queueParser) and() queueValue {
	v := p.compare()
	for p.peek() == "&&" {
		p.pos++
		rhs := p.compare()
		if v.truthy() {
			v = rhs
		}
	}
	return v
}

func (p *queueParser) compare() queueValue {
	lhs := p.unary()
	if op := p.peek(); op == "==" || op == "!=" {
		p.pos++
		rhs := p.unary()
		return queueBool((lhs.s == rhs.s && lhs.isBool == rhs.isBool && lhs.b == rhs.b) == (op == "=="))
	}
	return lhs
}

func (p *queueParser) unary() queueValue {
	if p.peek() == "!" {
		p.pos++
		return queueBool(!p.unary().truthy())
	}
	return p.atom()
}

func (p *queueParser) atom() queueValue {
	if p.pos >= len(p.tokens) {
		return p.fail("ended where a value was expected")
	}
	tok := p.tokens[p.pos]
	p.pos++
	switch {
	case tok == "(":
		v := p.or()
		if p.peek() != ")" {
			return p.fail("missing a closing paren")
		}
		p.pos++
		return v
	case strings.HasPrefix(tok, "'"):
		return queueValue{s: strings.ReplaceAll(tok[1:len(tok)-1], "''", "'")}
	case tok == "true":
		return queueBool(true)
	case tok == "false":
		return queueBool(false)
	case p.peek() == "(":
		p.pos++
		var args []queueValue
		for p.peek() != ")" && p.pos < len(p.tokens) {
			args = append(args, p.or())
			if p.peek() == "," {
				p.pos++
			}
		}
		p.pos++
		switch tok {
		case "always":
			return queueBool(true)
		case "contains":
			if len(args) != 2 {
				return p.fail("contains takes two arguments")
			}
			return queueBool(strings.Contains(args[0].s, args[1].s))
		}
		return p.fail("function %s is not modelled", tok)
	}
	value, ok := p.ctx[tok]
	if !ok {
		return p.fail("%q is not in the modelled context; extend it rather than assume", tok)
	}
	return queueValue{s: value}
}

func queueEval(expression string, ctx queueCtx) (queueValue, error) {
	p := &queueParser{tokens: queueTokenRE.FindAllString(expression, -1), ctx: ctx, src: expression}
	v := p.or()
	if p.err == nil && p.pos != len(p.tokens) {
		p.fail("trailing tokens %v", p.tokens[p.pos:])
	}
	return v, p.err
}

var queueBlockRE = regexp.MustCompile(`(?s)\$\{\{(.+?)\}\}`)

type queueExpr struct {
	where string
	text  string
}

// queueExpressions collects every expression of the document: the whole value of an `if` key, and each ${{ }} block.
func queueExpressions(node any, where string, out *[]queueExpr) {
	switch value := node.(type) {
	case yamlMap:
		for _, key := range sortedKeys(value) {
			child := value[key]
			if text, ok := child.(string); ok && key == "if" {
				inner := strings.TrimSpace(text)
				if m := queueBlockRE.FindStringSubmatch(inner); m != nil {
					inner = strings.TrimSpace(m[1])
				}
				*out = append(*out, queueExpr{where + "." + key, inner})
				continue
			}
			queueExpressions(child, where+"."+key, out)
		}
	case []any:
		for index, child := range value {
			queueExpressions(child, fmt.Sprintf("%s[%d]", where, index), out)
		}
	case string:
		for _, m := range queueBlockRE.FindAllStringSubmatch(value, -1) {
			*out = append(*out, queueExpr{where, strings.TrimSpace(m[1])})
		}
	}
}

func queueContext(event, ref string) queueCtx {
	return queueCtx{
		"github.event_name":                             event,
		"github.ref":                                    ref,
		"github.repository":                             "full-chaos/dev-health-ops",
		"github.repository_owner":                       "full-chaos",
		"github.actor":                                  "someone",
		"github.event.action":                           "checks_requested",
		"github.event.label.name":                       "",
		"github.event.inputs.push_images":               "",
		"github.event.inputs.ref":                       "",
		"github.event.pull_request.draft":               "",
		"github.event.pull_request.number":              "",
		"github.event.pull_request.head.repo.full_name": "",
		"github.event.pull_request.labels.*.name":       "",
		"needs.changes.outputs.code":                    "",
		"needs.changes.outputs.docs_only":               "false",
		"needs.changes.result":                          "success",
		"needs.resolve.result":                          "success",
		"needs.resolve.outputs.promoted":                "false",
		"needs.resolve.outputs.hotfix":                  "",
		"needs.dind-smoke-test.result":                  "success",
		"needs.go-build.result":                         "success",
		"needs.go-build-dho-arm64.result":               "success",
		"needs.go-merge.result":                         "success",
		"needs.go-api-tools-build.result":               "success",
		"needs.go-api-tools-merge.result":               "success",
		"vars.SELF_HOSTED_IMAGE_BUILDS":                 "",
		"vars.SELF_HOSTED_RUNNERS":                      "",
	}
}

func TestDockerImagesBuildButNeverPublishOnMergeGroup(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "docker-images.yml")
	var all []queueExpr
	queueExpressions(doc, "", &all)

	isPublishGate := func(e queueExpr) bool {
		return strings.Contains(e.text, "push_images") || (strings.Contains(e.where, "tags") && strings.Contains(e.text, "event_name"))
	}
	// Expressions the context cannot model are reported, not skipped: a skipped one would hide a publish path.
	merge := queueContext("merge_group", "refs/heads/gh-readonly-queue/main/pr-1-abc")
	control := queueContext("push", "refs/heads/main")

	var publishGates []string
	for _, e := range all {
		if !isPublishGate(e) {
			continue
		}
		publishGates = append(publishGates, e.where)
		got, err := queueEval(e.text, merge)
		if err != nil {
			t.Errorf("%s: %v", e.where, err)
			continue
		}
		if got.truthy() {
			t.Errorf("%s publishes on a merge_group run: %s", e.where, e.text)
		}
		// Control: the same gate is open on a main push, so a gate that is shut for every event cannot pass.
		if want, err := queueEval(e.text, control); err != nil || !want.truthy() {
			t.Errorf("%s is not open on a main push (%v): the test would pass on a gate that never publishes", e.where, err)
		}
	}
	sort.Strings(publishGates)
	if len(publishGates) < 12 {
		t.Errorf("found only %d publish publishGates: %v", len(publishGates), publishGates)
	}

	for _, name := range []string{"go-build", "go-build-dho-arm64", "go-api-tools-build"} {
		job := pinJob(doc, name)
		condition := asString(job["if"])
		if condition == "" {
			t.Fatalf("job %s has no if", name)
		}
		got, err := queueEval(strings.TrimSpace(condition), merge)
		if err != nil {
			t.Errorf("job %s: %v", name, err)
			continue
		}
		if !got.truthy() {
			t.Errorf("job %s does not run on a merge_group run, so the queue gets no image build", name)
		}
		// A docs-only queue entry (ci/codeql_docs_only.sh from the base commit) builds nothing.
		docs := queueContext("merge_group", "refs/heads/gh-readonly-queue/main/pr-1-abc")
		docs["needs.changes.outputs.docs_only"] = "true"
		if skip, err := queueEval(strings.TrimSpace(condition), docs); err != nil || skip.truthy() {
			t.Errorf("job %s runs for a docs-only merge_group entry (err %v)", name, err)
		}
	}
}
