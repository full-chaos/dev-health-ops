package renderdiff

import (
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
)

// safeDumpAll is yaml.safe_dump_all(docs, stream, sort_keys=True) of PyYAML 6.0.1: SafeRepresenter, Serializer and
// Emitter (indent 2, width 80, no unicode, '\n' breaks), ported line by line.
func safeDumpAll(docs []any) (string, error) {
	evs := []event{{kind: evStreamStart}}
	for _, d := range docs {
		r := &representer{seen: map[any]*rnode{}}
		root, err := r.represent(d, 1)
		if err != nil {
			return "", err
		}
		s := &serializer{anchors: map[*rnode]string{}, done: map[*rnode]bool{}}
		s.anchorNode(root)
		evs = append(evs, event{kind: evDocStart})
		s.serializeNode(root, &evs)
		evs = append(evs, event{kind: evDocEnd})
	}
	evs = append(evs, event{kind: evStreamEnd})
	e := newEmitter(evs)
	if err := e.run(); err != nil {
		return "", err
	}
	return e.out.String(), nil
}

type rkind int

const (
	rScalar rkind = iota
	rSeq
	rMap
)

type rnode struct {
	kind  rkind
	tag   string
	value string
	style byte
	items []*rnode
	pairs [][2]*rnode
}

type representer struct {
	seen map[any]*rnode
}

// represent is SafeRepresenter.represent_data. Only date and datetime objects can be shared inside one document (a
// YAML alias of a timestamp); every dict and list the kit dumps is a fresh copy made by redact().
// representDepth is the node depth at which the kit's safe_dump_all dies in a RecursionError (three Python frames
// per level: represent_data, represent_dict/list, represent_mapping/sequence; measured on the kit: a value 330 levels
// under a top-level key dumps, 331 does not). The token names main, the kit's innermost frame of its own file.
const representDepth = 332

func (r *representer) represent(v any, depth int) (*rnode, error) {
	if depth >= representDepth {
		return nil, &crashError{msg: "RecursionError: maximum recursion depth exceeded", fn: "main"}
	}
	switch x := v.(type) {
	case nil:
		return &rnode{kind: rScalar, tag: yamlTag + "null", value: "null"}, nil
	case string:
		return &rnode{kind: rScalar, tag: yamlTag + "str", value: x}, nil
	case pyBytes:
		return &rnode{kind: rScalar, tag: yamlTag + "binary", value: pyEncodeBytes([]byte(x)), style: '|'}, nil
	case bool:
		val := "false"
		if x {
			val = "true"
		}
		return &rnode{kind: rScalar, tag: yamlTag + "bool", value: val}, nil
	case *big.Int:
		return &rnode{kind: rScalar, tag: yamlTag + "int", value: pyIntStr(x)}, nil
	case float64:
		var val string
		switch {
		case math.IsNaN(x):
			val = ".nan"
		case math.IsInf(x, 1):
			val = ".inf"
		case math.IsInf(x, -1):
			val = "-.inf"
		default:
			val = strings.ToLower(pyFloatRepr(x))
			if !strings.Contains(val, ".") && strings.Contains(val, "e") {
				val = strings.Replace(val, "e", ".0e", 1)
			}
		}
		return &rnode{kind: rScalar, tag: yamlTag + "float", value: val}, nil
	case *pyDate:
		if n, ok := r.seen[x]; ok {
			return n, nil
		}
		n := &rnode{kind: rScalar, tag: yamlTag + "timestamp", value: pyStr(x)}
		r.seen[x] = n
		return n, nil
	case *pyDateTime:
		if n, ok := r.seen[x]; ok {
			return n, nil
		}
		n := &rnode{kind: rScalar, tag: yamlTag + "timestamp", value: pyStr(x)}
		r.seen[x] = n
		return n, nil
	case []any:
		n := &rnode{kind: rSeq, tag: yamlTag + "seq"}
		for _, e := range x {
			c, err := r.represent(e, depth+1)
			if err != nil {
				return nil, err
			}
			n.items = append(n.items, c)
		}
		return n, nil
	case pyTuple:
		return r.represent([]any(x), depth)
	case *pyDict:
		n := &rnode{kind: rMap, tag: yamlTag + "map"}
		for _, i := range sortedItems(x) {
			k, err := r.represent(x.keys[i], depth+1)
			if err != nil {
				return nil, err
			}
			v, err := r.represent(x.vals[i], depth+1)
			if err != nil {
				return nil, err
			}
			n.pairs = append(n.pairs, [2]*rnode{k, v})
		}
		return n, nil
	}
	return nil, &crashError{msg: fmt.Sprintf("RepresenterError: cannot represent an object %T", v)}
}

// sortedItems is `sorted(mapping.items())` with the TypeError fallback to insertion order.
func sortedItems(d *pyDict) []int {
	idx := make([]int, d.Len())
	for i := range idx {
		idx[i] = i
	}
	if len(idx) < 2 {
		return idx
	}
	for i := 1; i < len(idx); i++ {
		if _, ok := pyLess(d.keys[0], d.keys[i]); !ok {
			return idx
		}
		if _, ok := pyLess(d.keys[i], d.keys[0]); !ok {
			return idx
		}
	}
	sort.SliceStable(idx, func(a, b int) bool {
		lt, _ := pyLess(d.keys[idx[a]], d.keys[idx[b]])
		return lt
	})
	return idx
}

// pyEncodeBytes is base64.encodebytes: 76-character lines, each ending in a newline.
func pyEncodeBytes(b []byte) string {
	enc := base64.StdEncoding.EncodeToString(b)
	var out strings.Builder
	for len(enc) > 76 {
		out.WriteString(enc[:76])
		out.WriteByte('\n')
		enc = enc[76:]
	}
	if enc != "" {
		out.WriteString(enc)
		out.WriteByte('\n')
	}
	return out.String()
}

type evKind int

const (
	evStreamStart evKind = iota
	evStreamEnd
	evDocStart
	evDocEnd
	evAlias
	evScalar
	evSeqStart
	evSeqEnd
	evMapStart
	evMapEnd
)

type event struct {
	kind      evKind
	anchor    string
	tag       string
	implicit  [2]bool
	cimplicit bool
	value     string
	style     byte
}

type serializer struct {
	anchors map[*rnode]string
	done    map[*rnode]bool
	last    int
}

func (s *serializer) anchorNode(n *rnode) {
	if a, ok := s.anchors[n]; ok {
		if a == "" {
			s.last++
			s.anchors[n] = fmt.Sprintf("id%03d", s.last)
		}
		return
	}
	s.anchors[n] = ""
	switch n.kind {
	case rSeq:
		for _, c := range n.items {
			s.anchorNode(c)
		}
	case rMap:
		for _, p := range n.pairs {
			s.anchorNode(p[0])
			s.anchorNode(p[1])
		}
	}
}

func (s *serializer) serializeNode(n *rnode, evs *[]event) {
	alias := s.anchors[n]
	if s.done[n] {
		*evs = append(*evs, event{kind: evAlias, anchor: alias})
		return
	}
	s.done[n] = true
	switch n.kind {
	case rScalar:
		detected := yamlTag + resolveImplicit(n.value)
		implicit := [2]bool{n.tag == detected, n.tag == yamlTag+"str"}
		*evs = append(*evs, event{kind: evScalar, anchor: alias, tag: n.tag, implicit: implicit, value: n.value, style: n.style})
	case rSeq:
		*evs = append(*evs, event{kind: evSeqStart, anchor: alias, tag: n.tag, cimplicit: n.tag == yamlTag+"seq"})
		for _, c := range n.items {
			s.serializeNode(c, evs)
		}
		*evs = append(*evs, event{kind: evSeqEnd})
	case rMap:
		*evs = append(*evs, event{kind: evMapStart, anchor: alias, tag: n.tag, cimplicit: n.tag == yamlTag+"map"})
		for _, p := range n.pairs {
			s.serializeNode(p[0], evs)
			s.serializeNode(p[1], evs)
		}
		*evs = append(*evs, event{kind: evMapEnd})
	}
}

type analysis struct {
	scalar                               []rune
	empty, multiline                     bool
	allowFlowPlain, allowBlockPlain      bool
	allowSingleQuoted, allowDoubleQuoted bool
	allowBlock                           bool
}

type emitter struct {
	events []event
	pos    int
	ev     *event
	out    strings.Builder

	states []func() error
	state  func() error

	indents []int // -1 stands for Python's None
	indent  int

	flowLevel int

	rootContext, sequenceContext, mappingContext, simpleKeyContext bool

	line, column          int
	whitespace, indention bool
	openEnded             bool

	bestIndent, bestWidth int

	preparedAnchor *string
	preparedTag    *string
	analysis       *analysis
	style          *byte
}

func newEmitter(evs []event) *emitter {
	e := &emitter{events: evs, indent: -1, whitespace: true, indention: true, bestIndent: 2, bestWidth: 80}
	e.state = e.expectStreamStart
	return e
}

func (e *emitter) run() error {
	for e.pos < len(e.events) {
		e.ev = &e.events[e.pos]
		e.pos++
		if err := e.state(); err != nil {
			return err
		}
		e.ev = nil
	}
	return nil
}

// next is self.events[0]: the event after the current one.
func (e *emitter) next() *event {
	if e.pos < len(e.events) {
		return &e.events[e.pos]
	}
	return nil
}

func (e *emitter) push(f func() error) { e.states = append(e.states, f) }

func (e *emitter) pop() func() error {
	f := e.states[len(e.states)-1]
	e.states = e.states[:len(e.states)-1]
	return f
}

func (e *emitter) popIndent() {
	e.indent = e.indents[len(e.indents)-1]
	e.indents = e.indents[:len(e.indents)-1]
}

func (e *emitter) increaseIndent(flow, indentless bool) {
	e.indents = append(e.indents, e.indent)
	if e.indent < 0 {
		if flow {
			e.indent = e.bestIndent
		} else {
			e.indent = 0
		}
	} else if !indentless {
		e.indent += e.bestIndent
	}
}

func (e *emitter) expectStreamStart() error {
	if e.ev.kind != evStreamStart {
		return &crashError{msg: "EmitterError: expected StreamStartEvent"}
	}
	e.state = e.expectFirstDocumentStart
	return nil
}

func (e *emitter) expectNothing() error {
	return &crashError{msg: "EmitterError: expected nothing"}
}

func (e *emitter) expectFirstDocumentStart() error { return e.expectDocumentStart(true) }

func (e *emitter) expectDocumentStart(first bool) error {
	switch e.ev.kind {
	case evDocStart:
		implicit := first && !e.checkEmptyDocument()
		if !implicit {
			e.writeIndent()
			e.writeIndicator("---", true, false, false)
		}
		e.state = e.expectDocumentRoot
	case evStreamEnd:
		if e.openEnded {
			e.writeIndicator("...", true, false, false)
			e.writeIndent()
		}
		e.state = e.expectNothing
	default:
		return &crashError{msg: "EmitterError: expected DocumentStartEvent"}
	}
	return nil
}

func (e *emitter) expectDocumentEnd() error {
	if e.ev.kind != evDocEnd {
		return &crashError{msg: "EmitterError: expected DocumentEndEvent"}
	}
	e.writeIndent()
	e.state = func() error { return e.expectDocumentStart(false) }
	return nil
}

func (e *emitter) expectDocumentRoot() error {
	e.push(e.expectDocumentEnd)
	return e.expectNode(true, false, false, false)
}

func (e *emitter) expectNode(root, sequence, mapping, simpleKey bool) error {
	e.rootContext, e.sequenceContext, e.mappingContext, e.simpleKeyContext = root, sequence, mapping, simpleKey
	switch e.ev.kind {
	case evAlias:
		return e.expectAlias()
	case evScalar, evSeqStart, evMapStart:
		if err := e.processAnchor("&"); err != nil {
			return err
		}
		if err := e.processTag(); err != nil {
			return err
		}
		switch e.ev.kind {
		case evScalar:
			e.expectScalar()
		case evSeqStart:
			if e.flowLevel > 0 || e.checkEmptySequence() {
				e.expectFlowSequence()
			} else {
				e.expectBlockSequence()
			}
		case evMapStart:
			if e.flowLevel > 0 || e.checkEmptyMapping() {
				e.expectFlowMapping()
			} else {
				e.expectBlockMapping()
			}
		}
		return nil
	}
	return &crashError{msg: "EmitterError: expected NodeEvent"}
}

func (e *emitter) expectAlias() error {
	if e.ev.anchor == "" {
		return &crashError{msg: "EmitterError: anchor is not specified for alias"}
	}
	if err := e.processAnchor("*"); err != nil {
		return err
	}
	e.state = e.pop()
	return nil
}

func (e *emitter) expectScalar() {
	e.increaseIndent(true, false)
	e.processScalar()
	e.popIndent()
	e.state = e.pop()
}

func (e *emitter) expectFlowSequence() {
	e.writeIndicator("[", true, true, false)
	e.flowLevel++
	e.increaseIndent(true, false)
	e.state = e.expectFirstFlowSequenceItem
}

func (e *emitter) expectFirstFlowSequenceItem() error {
	if e.ev.kind == evSeqEnd {
		e.popIndent()
		e.flowLevel--
		e.writeIndicator("]", false, false, false)
		e.state = e.pop()
		return nil
	}
	if e.column > e.bestWidth {
		e.writeIndent()
	}
	e.push(e.expectFlowSequenceItem)
	return e.expectNode(false, true, false, false)
}

func (e *emitter) expectFlowSequenceItem() error {
	if e.ev.kind == evSeqEnd {
		e.popIndent()
		e.flowLevel--
		e.writeIndicator("]", false, false, false)
		e.state = e.pop()
		return nil
	}
	e.writeIndicator(",", false, false, false)
	if e.column > e.bestWidth {
		e.writeIndent()
	}
	e.push(e.expectFlowSequenceItem)
	return e.expectNode(false, true, false, false)
}

func (e *emitter) expectFlowMapping() {
	e.writeIndicator("{", true, true, false)
	e.flowLevel++
	e.increaseIndent(true, false)
	e.state = e.expectFirstFlowMappingKey
}

func (e *emitter) expectFirstFlowMappingKey() error {
	if e.ev.kind == evMapEnd {
		e.popIndent()
		e.flowLevel--
		e.writeIndicator("}", false, false, false)
		e.state = e.pop()
		return nil
	}
	if e.column > e.bestWidth {
		e.writeIndent()
	}
	if e.checkSimpleKey() {
		e.push(e.expectFlowMappingSimpleValue)
		return e.expectNode(false, false, true, true)
	}
	e.writeIndicator("?", true, false, false)
	e.push(e.expectFlowMappingValue)
	return e.expectNode(false, false, true, false)
}

func (e *emitter) expectFlowMappingKey() error {
	if e.ev.kind == evMapEnd {
		e.popIndent()
		e.flowLevel--
		e.writeIndicator("}", false, false, false)
		e.state = e.pop()
		return nil
	}
	e.writeIndicator(",", false, false, false)
	if e.column > e.bestWidth {
		e.writeIndent()
	}
	if e.checkSimpleKey() {
		e.push(e.expectFlowMappingSimpleValue)
		return e.expectNode(false, false, true, true)
	}
	e.writeIndicator("?", true, false, false)
	e.push(e.expectFlowMappingValue)
	return e.expectNode(false, false, true, false)
}

func (e *emitter) expectFlowMappingSimpleValue() error {
	e.writeIndicator(":", false, false, false)
	e.push(e.expectFlowMappingKey)
	return e.expectNode(false, false, true, false)
}

func (e *emitter) expectFlowMappingValue() error {
	if e.column > e.bestWidth {
		e.writeIndent()
	}
	e.writeIndicator(":", true, false, false)
	e.push(e.expectFlowMappingKey)
	return e.expectNode(false, false, true, false)
}

func (e *emitter) expectBlockSequence() {
	indentless := e.mappingContext && !e.indention
	e.increaseIndent(false, indentless)
	e.state = func() error { return e.expectBlockSequenceItem(true) }
}

func (e *emitter) expectBlockSequenceItem(first bool) error {
	if !first && e.ev.kind == evSeqEnd {
		e.popIndent()
		e.state = e.pop()
		return nil
	}
	e.writeIndent()
	e.writeIndicator("-", true, false, true)
	e.push(func() error { return e.expectBlockSequenceItem(false) })
	return e.expectNode(false, true, false, false)
}

func (e *emitter) expectBlockMapping() {
	e.increaseIndent(false, false)
	e.state = func() error { return e.expectBlockMappingKey(true) }
}

func (e *emitter) expectBlockMappingKey(first bool) error {
	if !first && e.ev.kind == evMapEnd {
		e.popIndent()
		e.state = e.pop()
		return nil
	}
	e.writeIndent()
	if e.checkSimpleKey() {
		e.push(e.expectBlockMappingSimpleValue)
		return e.expectNode(false, false, true, true)
	}
	e.writeIndicator("?", true, false, true)
	e.push(e.expectBlockMappingValue)
	return e.expectNode(false, false, true, false)
}

func (e *emitter) expectBlockMappingSimpleValue() error {
	e.writeIndicator(":", false, false, false)
	e.push(func() error { return e.expectBlockMappingKey(false) })
	return e.expectNode(false, false, true, false)
}

func (e *emitter) expectBlockMappingValue() error {
	e.writeIndent()
	e.writeIndicator(":", true, false, true)
	e.push(func() error { return e.expectBlockMappingKey(false) })
	return e.expectNode(false, false, true, false)
}

func (e *emitter) checkEmptySequence() bool {
	n := e.next()
	return e.ev.kind == evSeqStart && n != nil && n.kind == evSeqEnd
}

func (e *emitter) checkEmptyMapping() bool {
	n := e.next()
	return e.ev.kind == evMapStart && n != nil && n.kind == evMapEnd
}

func (e *emitter) checkEmptyDocument() bool {
	n := e.next()
	if e.ev.kind != evDocStart || n == nil {
		return false
	}
	return n.kind == evScalar && n.anchor == "" && n.tag == "" && n.value == ""
}

func (e *emitter) checkSimpleKey() bool {
	length := 0
	isNode := e.ev.kind == evAlias || e.ev.kind == evScalar || e.ev.kind == evSeqStart || e.ev.kind == evMapStart
	if isNode && e.ev.anchor != "" {
		if e.preparedAnchor == nil {
			a := e.ev.anchor
			e.preparedAnchor = &a
		}
		length += len([]rune(*e.preparedAnchor))
	}
	if (e.ev.kind == evScalar || e.ev.kind == evSeqStart || e.ev.kind == evMapStart) && e.ev.tag != "" {
		if e.preparedTag == nil {
			t := prepareTag(e.ev.tag)
			e.preparedTag = &t
		}
		length += len([]rune(*e.preparedTag))
	}
	if e.ev.kind == evScalar {
		if e.analysis == nil {
			e.analysis = analyzeScalar([]rune(e.ev.value))
		}
		length += len(e.analysis.scalar)
	}
	return length < 128 && (e.ev.kind == evAlias ||
		(e.ev.kind == evScalar && !e.analysis.empty && !e.analysis.multiline) ||
		e.checkEmptySequence() || e.checkEmptyMapping())
}

func (e *emitter) processAnchor(indicator string) error {
	if e.ev.anchor == "" {
		e.preparedAnchor = nil
		return nil
	}
	if e.preparedAnchor == nil {
		a := e.ev.anchor
		e.preparedAnchor = &a
	}
	if *e.preparedAnchor != "" {
		e.writeIndicator(indicator+*e.preparedAnchor, true, false, false)
	}
	e.preparedAnchor = nil
	return nil
}

func (e *emitter) processTag() error {
	tag := e.ev.tag
	if e.ev.kind == evScalar {
		if e.style == nil {
			s := e.chooseScalarStyle()
			e.style = &s
		}
		if (*e.style == 0 && e.ev.implicit[0]) || (*e.style != 0 && e.ev.implicit[1]) {
			e.preparedTag = nil
			return nil
		}
		if e.ev.implicit[0] && tag == "" {
			tag = "!"
			e.preparedTag = nil
		}
	} else if e.ev.cimplicit {
		e.preparedTag = nil
		return nil
	}
	if tag == "" {
		return &crashError{msg: "EmitterError: tag is not specified"}
	}
	if e.preparedTag == nil {
		t := prepareTag(tag)
		e.preparedTag = &t
	}
	if *e.preparedTag != "" {
		e.writeIndicator(*e.preparedTag, true, false, false)
	}
	e.preparedTag = nil
	return nil
}

func (e *emitter) chooseScalarStyle() byte {
	if e.analysis == nil {
		e.analysis = analyzeScalar([]rune(e.ev.value))
	}
	a := e.analysis
	if e.ev.style == '"' {
		return '"'
	}
	if e.ev.style == 0 && e.ev.implicit[0] {
		if !(e.simpleKeyContext && (a.empty || a.multiline)) &&
			(e.flowLevel > 0 && a.allowFlowPlain || (e.flowLevel == 0 && a.allowBlockPlain)) {
			return 0
		}
	}
	if e.ev.style == '|' || e.ev.style == '>' {
		if e.flowLevel == 0 && !e.simpleKeyContext && a.allowBlock {
			return e.ev.style
		}
	}
	if e.ev.style == 0 || e.ev.style == '\'' {
		if a.allowSingleQuoted && !(e.simpleKeyContext && a.multiline) {
			return '\''
		}
	}
	return '"'
}

func (e *emitter) processScalar() {
	if e.analysis == nil {
		e.analysis = analyzeScalar([]rune(e.ev.value))
	}
	if e.style == nil {
		s := e.chooseScalarStyle()
		e.style = &s
	}
	split := !e.simpleKeyContext
	text := e.analysis.scalar
	switch *e.style {
	case '"':
		e.writeDoubleQuoted(text, split)
	case '\'':
		e.writeSingleQuoted(text, split)
	case '>':
		e.writeFolded(text)
	case '|':
		e.writeLiteral(text)
	default:
		e.writePlain(text, split)
	}
	e.analysis = nil
	e.style = nil
}

func prepareTag(tag string) string {
	if tag == "!" {
		return tag
	}
	handle := ""
	suffix := tag
	for _, p := range []string{"!", yamlTag} {
		if strings.HasPrefix(tag, p) && (p == "!" || len(p) < len(tag)) {
			handle = map[string]string{"!": "!", yamlTag: "!!"}[p]
			suffix = tag[len(p):]
		}
	}
	var chunks strings.Builder
	start, end := 0, 0
	rs := []rune(suffix)
	for end < len(rs) {
		ch := rs[end]
		if ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || strings.ContainsRune("-;/?:@&=+$,_.~*'()[]", ch) || (ch == '!' && handle != "!") {
			end++
			continue
		}
		if start < end {
			chunks.WriteString(string(rs[start:end]))
		}
		end++
		start = end
		for _, b := range []byte(string(ch)) {
			fmt.Fprintf(&chunks, "%%%02X", b)
		}
	}
	if start < end {
		chunks.WriteString(string(rs[start:end]))
	}
	if handle != "" {
		return handle + chunks.String()
	}
	return "!<" + chunks.String() + ">"
}

// withEnd is text plus one -1 rune: the emitter loops of PyYAML step one past the last character (Python's ch = None).
func withEnd(text []rune) []rune {
	return append(append(make([]rune, 0, len(text)+1), text...), -1)
}

func isBreakRune(ch rune) bool { return ch == '\n' || ch == 0x85 || ch == 0x2028 || ch == 0x2029 }

func isSpaceOrBreak0(ch rune) bool {
	return ch == 0 || ch == ' ' || ch == '\t' || ch == '\r' || isBreakRune(ch)
}

func analyzeScalar(scalar []rune) *analysis {
	if len(scalar) == 0 {
		return &analysis{scalar: scalar, empty: true, allowBlockPlain: true, allowSingleQuoted: true, allowDoubleQuoted: true}
	}
	var blockIndicators, flowIndicators, lineBreaks, specialCharacters bool
	var leadingSpace, leadingBreak, trailingSpace, trailingBreak, breakSpace, spaceBreak bool
	s := string(scalar)
	if strings.HasPrefix(s, "---") || strings.HasPrefix(s, "...") {
		blockIndicators, flowIndicators = true, true
	}
	precededByWhitespace := true
	followedByWhitespace := len(scalar) == 1 || isSpaceOrBreak0(scalar[1])
	previousSpace, previousBreak := false, false
	for index := 0; index < len(scalar); {
		ch := scalar[index]
		if index == 0 {
			if strings.ContainsRune("#,[]{}&*!|>'\"%@`", ch) {
				flowIndicators, blockIndicators = true, true
			}
			if ch == '?' || ch == ':' {
				flowIndicators = true
				if followedByWhitespace {
					blockIndicators = true
				}
			}
			if ch == '-' && followedByWhitespace {
				flowIndicators, blockIndicators = true, true
			}
		} else {
			if strings.ContainsRune(",?[]{}", ch) {
				flowIndicators = true
			}
			if ch == ':' {
				flowIndicators = true
				if followedByWhitespace {
					blockIndicators = true
				}
			}
			if ch == '#' && precededByWhitespace {
				flowIndicators, blockIndicators = true, true
			}
		}
		if isBreakRune(ch) {
			lineBreaks = true
		}
		if !(ch == '\n' || ch >= 0x20 && ch <= 0x7E) {
			if (ch == 0x85 || ch >= 0xA0 && ch <= 0xD7FF || ch >= 0xE000 && ch <= 0xFFFD || ch >= 0x10000 && ch < 0x10FFFF) && ch != 0xFEFF {
				specialCharacters = true
			} else {
				specialCharacters = true
			}
		}
		switch {
		case ch == ' ':
			if index == 0 {
				leadingSpace = true
			}
			if index == len(scalar)-1 {
				trailingSpace = true
			}
			if previousBreak {
				breakSpace = true
			}
			previousSpace, previousBreak = true, false
		case isBreakRune(ch):
			if index == 0 {
				leadingBreak = true
			}
			if index == len(scalar)-1 {
				trailingBreak = true
			}
			if previousSpace {
				spaceBreak = true
			}
			previousSpace, previousBreak = false, true
		default:
			previousSpace, previousBreak = false, false
		}
		index++
		precededByWhitespace = isSpaceOrBreak0(ch)
		followedByWhitespace = index+1 >= len(scalar) || isSpaceOrBreak0(scalar[index+1])
	}
	a := &analysis{scalar: scalar, multiline: lineBreaks, allowFlowPlain: true, allowBlockPlain: true, allowSingleQuoted: true, allowDoubleQuoted: true, allowBlock: true}
	if leadingSpace || leadingBreak || trailingSpace || trailingBreak {
		a.allowFlowPlain, a.allowBlockPlain = false, false
	}
	if trailingSpace {
		a.allowBlock = false
	}
	if breakSpace {
		a.allowFlowPlain, a.allowBlockPlain, a.allowSingleQuoted = false, false, false
	}
	if spaceBreak || specialCharacters {
		a.allowFlowPlain, a.allowBlockPlain, a.allowSingleQuoted, a.allowBlock = false, false, false, false
	}
	if lineBreaks {
		a.allowFlowPlain, a.allowBlockPlain = false, false
	}
	if flowIndicators {
		a.allowFlowPlain = false
	}
	if blockIndicators {
		a.allowBlockPlain = false
	}
	return a
}

func (e *emitter) write(s string) { e.out.WriteString(s) }

func (e *emitter) writeIndicator(indicator string, needWhitespace, whitespace, indention bool) {
	data := indicator
	if !(e.whitespace || !needWhitespace) {
		data = " " + indicator
	}
	e.whitespace = whitespace
	e.indention = e.indention && indention
	e.column += len([]rune(data))
	e.openEnded = false
	e.write(data)
}

func (e *emitter) writeIndent() {
	indent := e.indent
	if indent < 0 {
		indent = 0
	}
	if !e.indention || e.column > indent || (e.column == indent && !e.whitespace) {
		e.writeLineBreak("")
	}
	if e.column < indent {
		e.whitespace = true
		e.write(strings.Repeat(" ", indent-e.column))
		e.column = indent
	}
}

func (e *emitter) writeLineBreak(data string) {
	if data == "" {
		data = "\n"
	}
	e.whitespace = true
	e.indention = true
	e.line++
	e.column = 0
	e.write(data)
}

func (e *emitter) writeRunes(rs []rune) {
	e.column += len(rs)
	e.write(string(rs))
}

func (e *emitter) writeBreaks(rs []rune) {
	for _, br := range rs {
		if br == '\n' {
			e.writeLineBreak("")
		} else {
			e.writeLineBreak(string(br))
		}
	}
}

func (e *emitter) writeSingleQuoted(text []rune, split bool) {
	e.writeIndicator("'", true, false, false)
	spaces, breaks := false, false
	start, end := 0, 0
	chars := withEnd(text)
	for end < len(chars) {
		ch := chars[end]
		if spaces {
			if ch == -1 || ch != ' ' {
				if start+1 == end && e.column > e.bestWidth && split && start != 0 && end != len(text) {
					e.writeIndent()
				} else {
					e.writeRunes(text[start:end])
				}
				start = end
			}
		} else if breaks {
			if ch == -1 || !isBreakRune(ch) {
				if text[start] == '\n' {
					e.writeLineBreak("")
				}
				e.writeBreaks(text[start:end])
				e.writeIndent()
				start = end
			}
		} else {
			if ch == -1 || ch == ' ' || isBreakRune(ch) || ch == '\'' {
				if start < end {
					e.writeRunes(text[start:end])
					start = end
				}
			}
		}
		if ch == '\'' {
			e.column += 2
			e.write("''")
			start = end + 1
		}
		if ch != -1 {
			spaces = ch == ' '
			breaks = isBreakRune(ch)
		}
		end++
	}
	e.writeIndicator("'", false, false, false)
}

var escapeReplacements = map[rune]string{
	0: "0", 0x07: "a", 0x08: "b", 0x09: "t", 0x0A: "n", 0x0B: "v", 0x0C: "f", 0x0D: "r", 0x1B: "e",
	'"': "\"", '\\': "\\", 0x85: "N", 0xA0: "_", 0x2028: "L", 0x2029: "P",
}

func (e *emitter) writeDoubleQuoted(text []rune, split bool) {
	e.writeIndicator("\"", true, false, false)
	start, end := 0, 0
	chars := withEnd(text)
	for end < len(chars) {
		ch := chars[end]
		if ch == -1 || strings.ContainsRune("\"\\\u0085\u2028\u2029\ufeff", ch) || !(ch >= 0x20 && ch <= 0x7E) {
			if start < end {
				e.writeRunes(text[start:end])
				start = end
			}
			if ch != -1 {
				var data string
				if r, ok := escapeReplacements[ch]; ok {
					data = "\\" + r
				} else if ch <= 0xFF {
					data = fmt.Sprintf("\\x%02X", ch)
				} else if ch <= 0xFFFF {
					data = fmt.Sprintf("\\u%04X", ch)
				} else {
					data = fmt.Sprintf("\\U%08X", ch)
				}
				e.column += len(data)
				e.write(data)
				start = end + 1
			}
		}
		if 0 < end && end < len(text)-1 && (ch == ' ' || start >= end) && e.column+(end-start) > e.bestWidth && split {
			var data []rune
			if start < end {
				data = text[start:end]
			}
			if start < end {
				start = end
			}
			e.column += len(data) + 1
			e.write(string(data) + "\\")
			e.writeIndent()
			e.whitespace = false
			e.indention = false
			if text[start] == ' ' {
				e.column++
				e.write("\\")
			}
		}
		end++
	}
	e.writeIndicator("\"", false, false, false)
}

func (e *emitter) determineBlockHints(text []rune) string {
	hints := ""
	if len(text) > 0 {
		if text[0] == ' ' || isBreakRune(text[0]) {
			hints += fmt.Sprint(e.bestIndent)
		}
		if !isBreakRune(text[len(text)-1]) {
			hints += "-"
		} else if len(text) == 1 || isBreakRune(text[len(text)-2]) {
			hints += "+"
		}
	}
	return hints
}

func (e *emitter) writeFolded(text []rune) {
	hints := e.determineBlockHints(text)
	e.writeIndicator(">"+hints, true, false, false)
	if strings.HasSuffix(hints, "+") {
		e.openEnded = true
	}
	e.writeLineBreak("")
	leadingSpace, spaces, breaks := true, false, true
	start, end := 0, 0
	chars := withEnd(text)
	for end < len(chars) {
		ch := chars[end]
		if breaks {
			if ch == -1 || !isBreakRune(ch) {
				if !leadingSpace && ch != -1 && ch != ' ' && text[start] == '\n' {
					e.writeLineBreak("")
				}
				leadingSpace = ch == ' '
				e.writeBreaks(text[start:end])
				if ch != -1 {
					e.writeIndent()
				}
				start = end
			}
		} else if spaces {
			if ch != ' ' {
				if start+1 == end && e.column > e.bestWidth {
					e.writeIndent()
				} else {
					e.writeRunes(text[start:end])
				}
				start = end
			}
		} else {
			if ch == -1 || ch == ' ' || isBreakRune(ch) {
				e.writeRunes(text[start:end])
				if ch == -1 {
					e.writeLineBreak("")
				}
				start = end
			}
		}
		if ch != -1 {
			breaks = isBreakRune(ch)
			spaces = ch == ' '
		}
		end++
	}
}

func (e *emitter) writeLiteral(text []rune) {
	hints := e.determineBlockHints(text)
	e.writeIndicator("|"+hints, true, false, false)
	if strings.HasSuffix(hints, "+") {
		e.openEnded = true
	}
	e.writeLineBreak("")
	breaks := true
	start, end := 0, 0
	chars := withEnd(text)
	for end < len(chars) {
		ch := chars[end]
		if breaks {
			if ch == -1 || !isBreakRune(ch) {
				e.writeBreaks(text[start:end])
				if ch != -1 {
					e.writeIndent()
				}
				start = end
			}
		} else {
			if ch == -1 || isBreakRune(ch) {
				e.write(string(text[start:end]))
				if ch == -1 {
					e.writeLineBreak("")
				}
				start = end
			}
		}
		if ch != -1 {
			breaks = isBreakRune(ch)
		}
		end++
	}
}

func (e *emitter) writePlain(text []rune, split bool) {
	if e.rootContext {
		e.openEnded = true
	}
	if len(text) == 0 {
		return
	}
	if !e.whitespace {
		e.column++
		e.write(" ")
	}
	e.whitespace = false
	e.indention = false
	spaces, breaks := false, false
	start, end := 0, 0
	chars := withEnd(text)
	for end < len(chars) {
		ch := chars[end]
		if spaces {
			if ch != ' ' {
				if start+1 == end && e.column > e.bestWidth && split {
					e.writeIndent()
					e.whitespace = false
					e.indention = false
				} else {
					e.writeRunes(text[start:end])
				}
				start = end
			}
		} else if breaks {
			if !isBreakRune(ch) {
				if text[start] == '\n' {
					e.writeLineBreak("")
				}
				e.writeBreaks(text[start:end])
				e.writeIndent()
				e.whitespace = false
				e.indention = false
				start = end
			}
		} else {
			if ch == -1 || ch == ' ' || isBreakRune(ch) {
				e.writeRunes(text[start:end])
				start = end
			}
		}
		if ch != -1 {
			spaces = ch == ' '
			breaks = isBreakRune(ch)
		}
		end++
	}
}
