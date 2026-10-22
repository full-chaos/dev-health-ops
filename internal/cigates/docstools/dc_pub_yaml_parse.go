package docstools

import (
	"fmt"
	"regexp"
	"strings"
)

// Parser, composer and resolver of the PyYAML port (see dc_pub_yaml_scan.go).

type dcYEvKind int

const (
	evStreamStart dcYEvKind = iota
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

type dcYEvent struct {
	kind     dcYEvKind
	anchor   *string
	tag      *string
	implicit [2]bool // scalar: (plain, non-plain); collections: [0]
	value    string
	style    rune
}

type dcYParser struct {
	*dcYScanner
	current  *dcYEvent
	state    func() (*dcYEvent, *dcYErr)
	states   []func() (*dcYEvent, *dcYErr)
	haveVer  bool
	handles  map[string]string
	nMarks   int
	composer dcYComposer
}

func newDcYParser(text string) (*dcYParser, *dcYErr) {
	sc, err := newDcYScanner(text)
	if err != nil {
		return nil, err
	}
	p := &dcYParser{dcYScanner: sc, handles: map[string]string{}}
	p.state = p.parseStreamStart
	p.composer.anchors = map[string]*dcYNode{}
	return p, nil
}

func (p *dcYParser) parseErr(context, problem string) *dcYErr {
	return dcYMarked(dcYParserError, context, problem)
}

func (p *dcYParser) peekEvent() (*dcYEvent, *dcYErr) {
	if p.current == nil && p.state != nil {
		ev, err := p.state()
		if err != nil {
			return nil, err
		}
		p.current = ev
	}
	return p.current, nil
}

func (p *dcYParser) checkEvent(kinds ...dcYEvKind) (bool, *dcYErr) {
	ev, err := p.peekEvent()
	if err != nil {
		return false, err
	}
	if ev == nil {
		return false, nil
	}
	for _, k := range kinds {
		if ev.kind == k {
			return true, nil
		}
	}
	return false, nil
}

func (p *dcYParser) getEvent() (*dcYEvent, *dcYErr) {
	ev, err := p.peekEvent()
	if err != nil {
		return nil, err
	}
	p.current = nil
	return ev, nil
}

func (p *dcYParser) pushState(f func() (*dcYEvent, *dcYErr)) { p.states = append(p.states, f) }

func (p *dcYParser) popState() func() (*dcYEvent, *dcYErr) {
	f := p.states[len(p.states)-1]
	p.states = p.states[:len(p.states)-1]
	return f
}

func (p *dcYParser) parseStreamStart() (*dcYEvent, *dcYErr) {
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.state = p.parseImplicitDocumentStart
	return &dcYEvent{kind: evStreamStart}, nil
}

func (p *dcYParser) parseImplicitDocumentStart() (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkDirective, tkDocStart, tkStreamEnd)
	if err != nil {
		return nil, err
	}
	if !ok {
		p.handles = map[string]string{"!": "!", "!!": "tag:yaml.org,2002:"}
		if _, err := p.peekToken(); err != nil {
			return nil, err
		}
		p.pushState(p.parseDocumentEnd)
		p.state = p.parseBlockNode
		return &dcYEvent{kind: evDocStart}, nil
	}
	return p.parseDocumentStart()
}

func (p *dcYParser) parseDocumentStart() (*dcYEvent, *dcYErr) {
	for {
		ok, err := p.checkToken(tkDocEnd)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
	}
	end, err := p.checkToken(tkStreamEnd)
	if err != nil {
		return nil, err
	}
	if !end {
		if _, err := p.peekToken(); err != nil {
			return nil, err
		}
		if err := p.processDirectives(); err != nil {
			return nil, err
		}
		ok, err := p.checkToken(tkDocStart)
		if err != nil {
			return nil, err
		}
		if !ok {
			t, err := p.peekToken()
			if err != nil {
				return nil, err
			}
			return nil, p.parseErr("", fmt.Sprintf("expected '<document start>', but found %s", a1Repr(dcYTokID[t.kind])))
		}
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
		p.pushState(p.parseDocumentEnd)
		p.state = p.parseDocumentContent
		return &dcYEvent{kind: evDocStart}, nil
	}
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.state = nil
	return &dcYEvent{kind: evStreamEnd}, nil
}

func (p *dcYParser) parseDocumentEnd() (*dcYEvent, *dcYErr) {
	if _, err := p.peekToken(); err != nil {
		return nil, err
	}
	ok, err := p.checkToken(tkDocEnd)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
	}
	p.state = p.parseDocumentStart
	return &dcYEvent{kind: evDocEnd}, nil
}

func (p *dcYParser) parseDocumentContent() (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkDirective, tkDocStart, tkDocEnd, tkStreamEnd)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := p.peekToken(); err != nil {
			return nil, err
		}
		ev := dcYEmptyScalar()
		p.state = p.popState()
		return ev, nil
	}
	return p.parseBlockNode()
}

func (p *dcYParser) processDirectives() *dcYErr {
	p.haveVer = false
	p.handles = map[string]string{}
	for {
		ok, err := p.checkToken(tkDirective)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		t, err := p.getToken()
		if err != nil {
			return err
		}
		switch t.value {
		case "YAML":
			if p.haveVer {
				return p.parseErr("", "found duplicate YAML directive")
			}
			if t.major != 1 {
				return p.parseErr("", "found incompatible YAML document (version 1.* is required)")
			}
			p.haveVer = true
		case "TAG":
			if _, dup := p.handles[t.hPref[0]]; dup {
				return p.parseErr("", fmt.Sprintf("duplicate tag handle %s", a1Repr(t.hPref[0])))
			}
			p.handles[t.hPref[0]] = t.hPref[1]
		}
	}
	for k, v := range map[string]string{"!": "!", "!!": "tag:yaml.org,2002:"} {
		if _, ok := p.handles[k]; !ok {
			p.handles[k] = v
		}
	}
	return nil
}

func dcYEmptyScalar() *dcYEvent {
	return &dcYEvent{kind: evScalar, implicit: [2]bool{true, false}}
}

func (p *dcYParser) parseBlockNode() (*dcYEvent, *dcYErr) { return p.parseNode(true, false) }
func (p *dcYParser) parseFlowNode() (*dcYEvent, *dcYErr)  { return p.parseNode(false, false) }
func (p *dcYParser) parseBlockNodeOrIndentlessSequence() (*dcYEvent, *dcYErr) {
	return p.parseNode(true, true)
}

func (p *dcYParser) parseNode(block, indentlessSequence bool) (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkAlias)
	if err != nil {
		return nil, err
	}
	if ok {
		t, err := p.getToken()
		if err != nil {
			return nil, err
		}
		name := t.value
		p.state = p.popState()
		return &dcYEvent{kind: evAlias, anchor: &name}, nil
	}
	var anchor, tagStr *string
	var tagTok *dcYToken
	ok, err = p.checkToken(tkAnchor)
	if err != nil {
		return nil, err
	}
	if ok {
		t, _ := p.getToken()
		a := t.value
		anchor = &a
		isTag, err := p.checkToken(tkTag)
		if err != nil {
			return nil, err
		}
		if isTag {
			tagTok, _ = p.getToken()
		}
	} else {
		isTag, err := p.checkToken(tkTag)
		if err != nil {
			return nil, err
		}
		if isTag {
			tagTok, _ = p.getToken()
			isAnchor, err := p.checkToken(tkAnchor)
			if err != nil {
				return nil, err
			}
			if isAnchor {
				t, _ := p.getToken()
				a := t.value
				anchor = &a
			}
		}
	}
	if tagTok != nil {
		if tagTok.handle != nil {
			prefix, found := p.handles[*tagTok.handle]
			if !found {
				return nil, p.parseErr("while parsing a node", fmt.Sprintf("found undefined tag handle %s", a1Repr(*tagTok.handle)))
			}
			full := prefix + tagTok.suffix
			tagStr = &full
		} else {
			sfx := tagTok.suffix
			tagStr = &sfx
		}
	}
	if _, err := p.peekToken(); err != nil {
		return nil, err
	}
	implicit := tagStr == nil || *tagStr == "!"
	if indentlessSequence {
		ok, err := p.checkToken(tkBlockEntry)
		if err != nil {
			return nil, err
		}
		if ok {
			p.state = p.parseIndentlessSequenceEntry
			return &dcYEvent{kind: evSeqStart, anchor: anchor, tag: tagStr, implicit: [2]bool{implicit}}, nil
		}
	}
	ok, err = p.checkToken(tkScalar)
	if err != nil {
		return nil, err
	}
	if ok {
		t, _ := p.getToken()
		var imp [2]bool
		switch {
		case (t.plain && tagStr == nil) || (tagStr != nil && *tagStr == "!"):
			imp = [2]bool{true, false}
		case tagStr == nil:
			imp = [2]bool{false, true}
		default:
			imp = [2]bool{false, false}
		}
		p.state = p.popState()
		return &dcYEvent{kind: evScalar, anchor: anchor, tag: tagStr, implicit: imp, value: t.value, style: t.style}, nil
	}
	if ok, err = p.checkToken(tkFlowSeqStart); err != nil {
		return nil, err
	} else if ok {
		p.state = p.parseFlowSequenceFirstEntry
		return &dcYEvent{kind: evSeqStart, anchor: anchor, tag: tagStr, implicit: [2]bool{implicit}}, nil
	}
	if ok, err = p.checkToken(tkFlowMapStart); err != nil {
		return nil, err
	} else if ok {
		p.state = p.parseFlowMappingFirstKey
		return &dcYEvent{kind: evMapStart, anchor: anchor, tag: tagStr, implicit: [2]bool{implicit}}, nil
	}
	if block {
		if ok, err = p.checkToken(tkBlockSeqStart); err != nil {
			return nil, err
		} else if ok {
			p.state = p.parseBlockSequenceFirstEntry
			return &dcYEvent{kind: evSeqStart, anchor: anchor, tag: tagStr, implicit: [2]bool{implicit}}, nil
		}
		if ok, err = p.checkToken(tkBlockMapStart); err != nil {
			return nil, err
		} else if ok {
			p.state = p.parseBlockMappingFirstKey
			return &dcYEvent{kind: evMapStart, anchor: anchor, tag: tagStr, implicit: [2]bool{implicit}}, nil
		}
	}
	if anchor != nil || tagStr != nil {
		p.state = p.popState()
		return &dcYEvent{kind: evScalar, anchor: anchor, tag: tagStr, implicit: [2]bool{implicit, false}}, nil
	}
	node := "flow"
	if block {
		node = "block"
	}
	t, err := p.peekToken()
	if err != nil {
		return nil, err
	}
	return nil, p.parseErr("while parsing a "+node+" node", fmt.Sprintf("expected the node content, but found %s", a1Repr(dcYTokID[t.kind])))
}

func (p *dcYParser) parseBlockSequenceFirstEntry() (*dcYEvent, *dcYErr) {
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.nMarks++
	return p.parseBlockSequenceEntry()
}

func (p *dcYParser) parseBlockSequenceEntry() (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkBlockEntry)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
		empty, err := p.checkToken(tkBlockEntry, tkBlockEnd)
		if err != nil {
			return nil, err
		}
		if !empty {
			p.pushState(p.parseBlockSequenceEntry)
			return p.parseBlockNode()
		}
		p.state = p.parseBlockSequenceEntry
		return dcYEmptyScalar(), nil
	}
	end, err := p.checkToken(tkBlockEnd)
	if err != nil {
		return nil, err
	}
	if !end {
		t, err := p.peekToken()
		if err != nil {
			return nil, err
		}
		return nil, p.parseErr("while parsing a block collection", fmt.Sprintf("expected <block end>, but found %s", a1Repr(dcYTokID[t.kind])))
	}
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.state = p.popState()
	p.nMarks--
	return &dcYEvent{kind: evSeqEnd}, nil
}

func (p *dcYParser) parseIndentlessSequenceEntry() (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkBlockEntry)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
		empty, err := p.checkToken(tkBlockEntry, tkKey, tkValue, tkBlockEnd)
		if err != nil {
			return nil, err
		}
		if !empty {
			p.pushState(p.parseIndentlessSequenceEntry)
			return p.parseBlockNode()
		}
		p.state = p.parseIndentlessSequenceEntry
		return dcYEmptyScalar(), nil
	}
	if _, err := p.peekToken(); err != nil {
		return nil, err
	}
	p.state = p.popState()
	return &dcYEvent{kind: evSeqEnd}, nil
}

func (p *dcYParser) parseBlockMappingFirstKey() (*dcYEvent, *dcYErr) {
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.nMarks++
	return p.parseBlockMappingKey()
}

func (p *dcYParser) parseBlockMappingKey() (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkKey)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
		empty, err := p.checkToken(tkKey, tkValue, tkBlockEnd)
		if err != nil {
			return nil, err
		}
		if !empty {
			p.pushState(p.parseBlockMappingValue)
			return p.parseBlockNodeOrIndentlessSequence()
		}
		p.state = p.parseBlockMappingValue
		return dcYEmptyScalar(), nil
	}
	end, err := p.checkToken(tkBlockEnd)
	if err != nil {
		return nil, err
	}
	if !end {
		t, err := p.peekToken()
		if err != nil {
			return nil, err
		}
		return nil, p.parseErr("while parsing a block mapping", fmt.Sprintf("expected <block end>, but found %s", a1Repr(dcYTokID[t.kind])))
	}
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.state = p.popState()
	p.nMarks--
	return &dcYEvent{kind: evMapEnd}, nil
}

func (p *dcYParser) parseBlockMappingValue() (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkValue)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
		empty, err := p.checkToken(tkKey, tkValue, tkBlockEnd)
		if err != nil {
			return nil, err
		}
		if !empty {
			p.pushState(p.parseBlockMappingKey)
			return p.parseBlockNodeOrIndentlessSequence()
		}
		p.state = p.parseBlockMappingKey
		return dcYEmptyScalar(), nil
	}
	p.state = p.parseBlockMappingKey
	if _, err := p.peekToken(); err != nil {
		return nil, err
	}
	return dcYEmptyScalar(), nil
}

func (p *dcYParser) parseFlowSequenceFirstEntry() (*dcYEvent, *dcYErr) {
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.nMarks++
	return p.parseFlowSequenceEntry(true)
}

func (p *dcYParser) parseFlowSequenceEntryNext() (*dcYEvent, *dcYErr) {
	return p.parseFlowSequenceEntry(false)
}

func (p *dcYParser) parseFlowSequenceEntry(first bool) (*dcYEvent, *dcYErr) {
	end, err := p.checkToken(tkFlowSeqEnd)
	if err != nil {
		return nil, err
	}
	if !end {
		if !first {
			ok, err := p.checkToken(tkFlowEntry)
			if err != nil {
				return nil, err
			}
			if ok {
				if _, err := p.getToken(); err != nil {
					return nil, err
				}
			} else {
				t, err := p.peekToken()
				if err != nil {
					return nil, err
				}
				return nil, p.parseErr("while parsing a flow sequence", fmt.Sprintf("expected ',' or ']', but got %s", a1Repr(dcYTokID[t.kind])))
			}
		}
		isKey, err := p.checkToken(tkKey)
		if err != nil {
			return nil, err
		}
		if isKey {
			if _, err := p.peekToken(); err != nil {
				return nil, err
			}
			p.state = p.parseFlowSequenceEntryMappingKey
			return &dcYEvent{kind: evMapStart, implicit: [2]bool{true}}, nil
		}
		again, err := p.checkToken(tkFlowSeqEnd)
		if err != nil {
			return nil, err
		}
		if !again {
			p.pushState(p.parseFlowSequenceEntryNext)
			return p.parseFlowNode()
		}
	}
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.state = p.popState()
	p.nMarks--
	return &dcYEvent{kind: evSeqEnd}, nil
}

func (p *dcYParser) parseFlowSequenceEntryMappingKey() (*dcYEvent, *dcYErr) {
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	stop, err := p.checkToken(tkValue, tkFlowEntry, tkFlowSeqEnd)
	if err != nil {
		return nil, err
	}
	if !stop {
		p.pushState(p.parseFlowSequenceEntryMappingValue)
		return p.parseFlowNode()
	}
	p.state = p.parseFlowSequenceEntryMappingValue
	return dcYEmptyScalar(), nil
}

func (p *dcYParser) parseFlowSequenceEntryMappingValue() (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkValue)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
		stop, err := p.checkToken(tkFlowEntry, tkFlowSeqEnd)
		if err != nil {
			return nil, err
		}
		if !stop {
			p.pushState(p.parseFlowSequenceEntryMappingEnd)
			return p.parseFlowNode()
		}
		p.state = p.parseFlowSequenceEntryMappingEnd
		return dcYEmptyScalar(), nil
	}
	p.state = p.parseFlowSequenceEntryMappingEnd
	if _, err := p.peekToken(); err != nil {
		return nil, err
	}
	return dcYEmptyScalar(), nil
}

func (p *dcYParser) parseFlowSequenceEntryMappingEnd() (*dcYEvent, *dcYErr) {
	p.state = p.parseFlowSequenceEntryNext
	if _, err := p.peekToken(); err != nil {
		return nil, err
	}
	return &dcYEvent{kind: evMapEnd}, nil
}

func (p *dcYParser) parseFlowMappingFirstKey() (*dcYEvent, *dcYErr) {
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.nMarks++
	return p.parseFlowMappingKey(true)
}

func (p *dcYParser) parseFlowMappingKeyNext() (*dcYEvent, *dcYErr) {
	return p.parseFlowMappingKey(false)
}

func (p *dcYParser) parseFlowMappingKey(first bool) (*dcYEvent, *dcYErr) {
	end, err := p.checkToken(tkFlowMapEnd)
	if err != nil {
		return nil, err
	}
	if !end {
		if !first {
			ok, err := p.checkToken(tkFlowEntry)
			if err != nil {
				return nil, err
			}
			if ok {
				if _, err := p.getToken(); err != nil {
					return nil, err
				}
			} else {
				t, err := p.peekToken()
				if err != nil {
					return nil, err
				}
				return nil, p.parseErr("while parsing a flow mapping", fmt.Sprintf("expected ',' or '}', but got %s", a1Repr(dcYTokID[t.kind])))
			}
		}
		isKey, err := p.checkToken(tkKey)
		if err != nil {
			return nil, err
		}
		if isKey {
			if _, err := p.getToken(); err != nil {
				return nil, err
			}
			stop, err := p.checkToken(tkValue, tkFlowEntry, tkFlowMapEnd)
			if err != nil {
				return nil, err
			}
			if !stop {
				p.pushState(p.parseFlowMappingValue)
				return p.parseFlowNode()
			}
			p.state = p.parseFlowMappingValue
			return dcYEmptyScalar(), nil
		}
		again, err := p.checkToken(tkFlowMapEnd)
		if err != nil {
			return nil, err
		}
		if !again {
			p.pushState(p.parseFlowMappingEmptyValue)
			return p.parseFlowNode()
		}
	}
	if _, err := p.getToken(); err != nil {
		return nil, err
	}
	p.state = p.popState()
	p.nMarks--
	return &dcYEvent{kind: evMapEnd}, nil
}

func (p *dcYParser) parseFlowMappingValue() (*dcYEvent, *dcYErr) {
	ok, err := p.checkToken(tkValue)
	if err != nil {
		return nil, err
	}
	if ok {
		if _, err := p.getToken(); err != nil {
			return nil, err
		}
		stop, err := p.checkToken(tkFlowEntry, tkFlowMapEnd)
		if err != nil {
			return nil, err
		}
		if !stop {
			p.pushState(p.parseFlowMappingKeyNext)
			return p.parseFlowNode()
		}
		p.state = p.parseFlowMappingKeyNext
		return dcYEmptyScalar(), nil
	}
	p.state = p.parseFlowMappingKeyNext
	if _, err := p.peekToken(); err != nil {
		return nil, err
	}
	return dcYEmptyScalar(), nil
}

func (p *dcYParser) parseFlowMappingEmptyValue() (*dcYEvent, *dcYErr) {
	p.state = p.parseFlowMappingKeyNext
	if _, err := p.peekToken(); err != nil {
		return nil, err
	}
	return dcYEmptyScalar(), nil
}

// ---- nodes, composer, resolver

type dcYNodeKind int

const (
	nkScalar dcYNodeKind = iota
	nkSeq
	nkMap
)

type dcYNode struct {
	kind  dcYNodeKind
	tag   string
	value string
	items []*dcYNode
	pairs [][2]*dcYNode
}

func (n *dcYNode) id() string {
	switch n.kind {
	case nkScalar:
		return "scalar"
	case nkSeq:
		return "sequence"
	}
	return "mapping"
}

type dcYComposer struct {
	anchors map[string]*dcYNode
	depth   int
}

// dcYMaxComposeDepth bounds the nesting the composer accepts. CPython 3.14.7 stops with RecursionError at a depth the
// recorded probe puts above this number (see testdata/README.md), so Go refuses earlier (STRICTER).
const dcYMaxComposeDepth = 400

func (p *dcYParser) composeErr(context, problem string) *dcYErr {
	return dcYMarked(dcYComposerError, context, problem)
}

func (p *dcYParser) getSingleNode() (*dcYNode, *dcYErr) {
	if _, err := p.getEvent(); err != nil {
		return nil, err
	}
	var doc *dcYNode
	end, err := p.checkEvent(evStreamEnd)
	if err != nil {
		return nil, err
	}
	if !end {
		doc, err = p.composeDocument()
		if err != nil {
			return nil, err
		}
	}
	end, err = p.checkEvent(evStreamEnd)
	if err != nil {
		return nil, err
	}
	if !end {
		if _, err := p.getEvent(); err != nil {
			return nil, err
		}
		return nil, p.composeErr("expected a single document in the stream", "but found another document")
	}
	if _, err := p.getEvent(); err != nil {
		return nil, err
	}
	return doc, nil
}

func (p *dcYParser) composeDocument() (*dcYNode, *dcYErr) {
	if _, err := p.getEvent(); err != nil {
		return nil, err
	}
	node, err := p.composeNode()
	if err != nil {
		return nil, err
	}
	if _, err := p.getEvent(); err != nil {
		return nil, err
	}
	p.composer.anchors = map[string]*dcYNode{}
	return node, nil
}

func (p *dcYParser) composeNode() (*dcYNode, *dcYErr) {
	c := &p.composer
	c.depth++
	defer func() { c.depth-- }()
	if c.depth > dcYMaxComposeDepth {
		return nil, &dcYErr{"RecursionError", "maximum recursion depth exceeded"}
	}
	isAlias, err := p.checkEvent(evAlias)
	if err != nil {
		return nil, err
	}
	if isAlias {
		ev, _ := p.getEvent()
		n, ok := c.anchors[*ev.anchor]
		if !ok {
			return nil, p.composeErr("", fmt.Sprintf("found undefined alias %s", a1Repr(*ev.anchor)))
		}
		return n, nil
	}
	ev, err := p.peekEvent()
	if err != nil {
		return nil, err
	}
	if ev.anchor != nil {
		if _, dup := c.anchors[*ev.anchor]; dup {
			return nil, p.composeErr(fmt.Sprintf("found duplicate anchor %s; first occurrence", a1Repr(*ev.anchor)), "second occurrence")
		}
	}
	switch ev.kind {
	case evScalar:
		ev, _ = p.getEvent()
		tag := ""
		if ev.tag == nil || *ev.tag == "!" {
			tag = dcYResolve(nkScalar, ev.value, ev.implicit)
		} else {
			tag = *ev.tag
		}
		node := &dcYNode{kind: nkScalar, tag: tag, value: ev.value}
		if ev.anchor != nil {
			c.anchors[*ev.anchor] = node
		}
		return node, nil
	case evSeqStart:
		ev, _ = p.getEvent()
		tag := ""
		if ev.tag == nil || *ev.tag == "!" {
			tag = dcYResolve(nkSeq, "", ev.implicit)
		} else {
			tag = *ev.tag
		}
		node := &dcYNode{kind: nkSeq, tag: tag}
		if ev.anchor != nil {
			c.anchors[*ev.anchor] = node
		}
		for {
			end, err := p.checkEvent(evSeqEnd)
			if err != nil {
				return nil, err
			}
			if end {
				break
			}
			child, err := p.composeNode()
			if err != nil {
				return nil, err
			}
			node.items = append(node.items, child)
		}
		if _, err := p.getEvent(); err != nil {
			return nil, err
		}
		return node, nil
	default: // evMapStart
		ev, _ = p.getEvent()
		tag := ""
		if ev.tag == nil || *ev.tag == "!" {
			tag = dcYResolve(nkMap, "", ev.implicit)
		} else {
			tag = *ev.tag
		}
		node := &dcYNode{kind: nkMap, tag: tag}
		if ev.anchor != nil {
			c.anchors[*ev.anchor] = node
		}
		for {
			end, err := p.checkEvent(evMapEnd)
			if err != nil {
				return nil, err
			}
			if end {
				break
			}
			k, err := p.composeNode()
			if err != nil {
				return nil, err
			}
			v, err := p.composeNode()
			if err != nil {
				return nil, err
			}
			node.pairs = append(node.pairs, [2]*dcYNode{k, v})
		}
		if _, err := p.getEvent(); err != nil {
			return nil, err
		}
		return node, nil
	}
}

type dcYImplicit struct {
	tag   string
	re    *regexp.Regexp
	first string // "" means the empty value only is handled through firstEmpty
	empty bool
}

func dcYRe(body string) *regexp.Regexp { return regexp.MustCompile(`\A(?:` + body + `)\z`) }

// Registration order is the order of Resolver.add_implicit_resolver in PyYAML; a value is tried against the
// resolvers listed for its first character in that order.
var dcYImplicits = []dcYImplicit{
	{"tag:yaml.org,2002:bool", dcYRe(`yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF`), "yYnNtTfFoO", false},
	{"tag:yaml.org,2002:float", dcYRe(`[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN)`), "-+0123456789.", false},
	{"tag:yaml.org,2002:int", dcYRe(`[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+`), "-+0123456789", false},
	{"tag:yaml.org,2002:merge", dcYRe(`<<`), "<", false},
	{"tag:yaml.org,2002:null", dcYRe(`~|null|Null|NULL|`), "~nN", true},
	{"tag:yaml.org,2002:timestamp", dcYRe(`[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?`), "0123456789", false},
	{"tag:yaml.org,2002:value", dcYRe(`=`), "=", false},
	{"tag:yaml.org,2002:yaml", dcYRe(`!|&|\*`), "!&*", false},
}

// dcYMatch is re.match of a pattern that ends in "$": the end of the text, or just before one final newline.
func dcYMatch(re *regexp.Regexp, v string) (matched, viaNewline bool) {
	if re.MatchString(v) {
		return true, false
	}
	if strings.HasSuffix(v, "\n") && re.MatchString(v[:len(v)-1]) {
		return true, true
	}
	return false, false
}

// dcYResolve is BaseResolver.resolve.
func dcYResolve(kind dcYNodeKind, value string, implicit [2]bool) string {
	if kind == nkScalar && implicit[0] {
		for _, r := range dcYImplicits {
			if value == "" {
				if !r.empty {
					continue
				}
			} else if !strings.ContainsRune(r.first, []rune(value)[0]) {
				continue
			}
			if ok, via := dcYMatch(r.re, value); ok {
				if via {
					return "\x00trailing-newline " + r.tag
				}
				return r.tag
			}
		}
	}
	switch kind {
	case nkScalar:
		return "tag:yaml.org,2002:str"
	case nkSeq:
		return "tag:yaml.org,2002:seq"
	}
	return "tag:yaml.org,2002:map"
}
