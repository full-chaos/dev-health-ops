// Package renderdiff is a byte-faithful Go port of the prod kit's render-diff gate, full-render-diff.py (sha256
// ecf6771eb509...): the same argv, the same stdout bytes and the same exit codes. The kit's goldens (goldens-1,
// MANIFEST sha12 bc7e47ef923a) are the oracle; a Python traceback is the one form the port does not copy (it prints
// one stderr line and exits 1, as the traceback exits 1).
package renderdiff

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"unicode"
)

type exitCode int

type run struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

func (r *run) print(format string, a ...any) {
	fmt.Fprintf(r.stdout, format+"\n", a...)
}

func (r *run) usage(msg string) {
	if msg != "" {
		r.print("%s", msg)
	}
	r.print("%s", kitUsage)
	panic(exitCode(2))
}

// crashRC is the exit code of a kit crash (the kit's uncaught Python exception exits 1).
const crashRC = 1

// crashToken is the failure class of a kit crash: the Python exception type (every PyYAML error as its base
// class, YAMLError) and the innermost kit function, "<type>@<function>". It is built from fixed text only, so the
// stderr line never holds input bytes.
func crashToken(err error, fn string) string {
	class := "OSError"
	var ye *yamlError
	var ce *crashError
	switch {
	case errors.As(err, &ye):
		class = "YAMLError"
	case errors.As(err, &ce):
		class, _, _ = strings.Cut(ce.msg, ":")
		if ce.fn != "" {
			fn = ce.fn
		}
	}
	return class + "@" + fn
}

// crash ends the run as the kit's traceback does: rc 1, the stdout printed so far, and one stderr line.
func (r *run) crash(err error, fn string) {
	panic(r.crashCode(err, fn))
}

func (r *run) crashCode(err error, fn string) exitCode {
	fmt.Fprintf(r.stderr, "full-render-diff: crash %s\n", crashToken(err, fn))
	return exitCode(crashRC)
}

// Main runs the kit tool on argv (without the program name) and returns its exit code.
func Main(args []string, stdin io.Reader, stdout, stderr io.Writer) (rc int) {
	r := &run{stdin: stdin, stdout: stdout, stderr: stderr}
	defer func() {
		if p := recover(); p != nil {
			if ce, ok := p.(*crashError); ok {
				p = r.crashCode(ce, "main")
			}
			c, ok := p.(exitCode)
			if !ok {
				panic(p)
			}
			rc = int(c)
		}
	}()
	return r.main(args)
}

// strerror is Python's OSError.strerror for an errno: the glibc text.
func strerror(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		s := errno.Error()
		if s == "" {
			return s
		}
		rs := []rune(s)
		rs[0] = unicode.ToUpper(rs[0])
		return string(rs)
	}
	return err.Error()
}

func pyOSErrorStr(err error, path string) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return fmt.Sprintf("[Errno %d] %s: %s", int(errno), strerror(err), pyStrRepr(path))
	}
	return err.Error()
}

func readFile(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			return nil, pe.Err
		}
		return nil, err
	}
	return b, nil
}

func (r *run) readStdinDocs() []*pyDict {
	raw, err := io.ReadAll(r.stdin)
	if err != nil {
		r.crash(err, "docs_of")
	}
	text, err := pythonText(raw)
	if err != nil {
		// sys.stdin decodes with surrogateescape; PyYAML's reader then refuses the escaped byte.
		r.crash(&yamlError{msg: "unacceptable character"}, "docs_of")
	}
	all, err := loadAll(text, composeDepthStdin)
	if err != nil {
		r.crash(err, "docs_of")
	}
	return docsOf(all)
}

func (r *run) main(argv []string) int {
	if len(argv) == 1 && argv[0] == "filter" {
		var kept []any
		for _, d := range r.readStdinDocs() {
			if k, _ := d.getS("kind"); pyEq(k, "Secret") {
				continue
			}
			kept = append(kept, redact(d))
		}
		out, err := safeDumpAll(kept)
		if err != nil {
			r.crash(err, "main")
		}
		_, _ = io.WriteString(r.stdout, out)
		return 0
	}
	if len(argv) == 1 && argv[0] == "names" {
		var names []string
		for _, d := range r.readStdinDocs() {
			k, err := key(d)
			if err != nil {
				r.crash(err, "key")
			}
			names = append(names, k)
		}
		sort.Strings(names)
		for _, n := range names {
			r.print("%s", n)
		}
		return 0
	}
	pos, rcA, rcB, opts := r.parseArgs(argv)
	sa := r.newSide("A", pos[0], rcA)
	sb := r.newSide("B", pos[1], rcB)
	for _, s := range []*side{sa, sb} {
		r.print("render %s: helm rc=%s docs=%d", s.label, pyIntStr(s.rc), s.shown(opts.live))
	}
	var names *[2]map[string]bool
	if opts.names != nil {
		names = &[2]map[string]bool{r.readNames(opts.names[0]), r.readNames(opts.names[1])}
	}
	var reasons []string
	for _, s := range []*side{sa, sb} {
		shown := s.shown(opts.live)
		if s.err != "" {
			reasons = append(reasons, s.err)
		}
		if s.rc.Sign() != 0 {
			reasons = append(reasons, fmt.Sprintf("render %s: helm rc=%s", s.label, pyIntStr(s.rc)))
		}
		if big.NewInt(int64(shown)).Cmp(opts.minDocs) < 0 {
			reasons = append(reasons, fmt.Sprintf("render %s: docs=%d below --min-docs %s", s.label, shown, pyIntStr(opts.minDocs)))
		}
	}
	if names != nil {
		for i, label := range []string{"A", "B"} {
			if len(names[i]) == 0 {
				reasons = append(reasons, fmt.Sprintf("names %s is empty", label))
			}
		}
	}
	final, badWord := "FULL RENDER DIFF", "STOP"
	if opts.expectRows != nil {
		final, badWord = "GATE "+pyBasename(*opts.expectRows), "HOLD"
	}
	if len(reasons) > 0 {
		r.print("%s: %s (render floor: %s)", final, badWord, strings.Join(reasons, "; "))
		return 1
	}

	a, b := sa.docs, sb.docs
	if dups := duplicateLines(a, b); len(dups) > 0 {
		for _, l := range dups {
			r.print("%s", l)
		}
		r.print("%s: %s (duplicate names)", final, badWord)
		return 1
	}
	var added, removed []string
	var rows []row
	for _, k := range unionDocKeys(a, b) {
		da, inA := a.get(k)
		db, inB := b.get(k)
		if !inA {
			added = append(added, k)
			r.print("%s ADDED", k)
			continue
		}
		if !inB {
			removed = append(removed, k)
			r.print("%s REMOVED", k)
			continue
		}
		df := &differ{doc: k}
		df.walk(da, db, "")
		for _, rw := range df.rows {
			rows = append(rows, rw)
			if rw.verb == "REORDERED" {
				r.print("%s %s REORDERED old=[%s] new=[%s]", k, rw.path, strings.Join(rw.old, ","), strings.Join(rw.new, ","))
			} else {
				r.print("%s %s %s", k, rw.path, rw.verb)
			}
		}
	}
	count := func(f func(row) bool) int {
		n := 0
		for _, rw := range rows {
			if f(rw) {
				n++
			}
		}
		return n
	}
	changed := count(func(rw row) bool { return rw.verb == "CHANGED" })
	elAdd := count(func(rw row) bool { return rw.verb == "ELEMENT-ADDED" })
	elRm := count(func(rw row) bool { return rw.verb == "ELEMENT-REMOVED" })
	reord := count(func(rw row) bool { return rw.verb == "REORDERED" })
	podmeta := count(func(rw row) bool { return strings.Contains(rw.path, ".template.metadata.") })
	sts := count(func(rw row) bool { return rw.doc == postgresSTS })
	kinds := map[string]int{}
	for _, k := range a.order {
		kinds[strings.SplitN(k, "/", 2)[0]]++
	}
	kindNames := make([]string, 0, len(kinds))
	for k := range kinds {
		kindNames = append(kindNames, k)
	}
	sort.Strings(kindNames)
	kindParts := make([]string, len(kindNames))
	for i, k := range kindNames {
		kindParts[i] = fmt.Sprintf("%s=%d", k, kinds[k])
	}
	context := "hook-included: a helm template render"
	if opts.live {
		context = "hook-free: a live manifest"
	}
	r.print("docs A %d B %d (all kinds, Secrets as Kind/name only; %s); kinds in A: %s", len(a.order), len(b.order), context, strings.Join(kindParts, ", "))
	r.print("added %s; removed %s; changed field rows %d; element rows added %d removed %d; reordered rows %d; pod-template label/annotation rows %d; postgres sts rows %d",
		strListRepr(added), strListRepr(removed), changed, elAdd, elRm, reord, podmeta, sts)
	if opts.live {
		r.print("docs (non-Secret) A %d B %d; added %d removed %d changed fields %d", sa.liveCount, sb.liveCount, countNonSecret(added), countNonSecret(removed), changed)
	}
	namesOK := true
	if names != nil {
		plus, minus := setMinus(names[1], names[0]), setMinus(names[0], names[1])
		r.print("names: A %d B %d; added %s; removed %s", len(names[0]), len(names[1]), strListRepr(plus), strListRepr(minus))
		if opts.expectNames != nil {
			namesOK = slicesEqual(plus, opts.expectNames) && len(minus) == 0
		}
	}
	if opts.info {
		r.print("%s: INFO (no expectation; the floor held)", final)
		if namesOK {
			return 0
		}
		return 1
	}
	if opts.expectAdded != nil {
		sortedAdded := append([]string{}, added...)
		sort.Strings(sortedAdded)
		ok := slicesEqual(sortedAdded, opts.expectAdded) && len(removed) == 0 && len(rows) == 0 && namesOK
		word := "STOP"
		if ok {
			word = "PASS"
		}
		r.print("%s: %s (expected added %s)", final, word, strListRepr(opts.expectAdded))
		if ok {
			return 0
		}
		return 1
	}
	return r.rowsGate(*opts.expectRows, final, added, removed, rows, namesOK)
}

func countNonSecret(xs []string) int {
	n := 0
	for _, x := range xs {
		if !strings.HasPrefix(x, "Secret/") {
			n++
		}
	}
	return n
}

func setMinus(a, b map[string]bool) []string {
	var out []string
	for k := range a {
		if !b[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// pyBasename is os.path.basename: the text after the last '/'.
func pyBasename(p string) string {
	return p[strings.LastIndex(p, "/")+1:]
}

// docMap is the kit's side.docs: Kind/name -> doc, in load order.
type docMap struct {
	order []string
	docs  map[string]*pyDict
}

func (m *docMap) get(k string) (*pyDict, bool) {
	d, ok := m.docs[k]
	return d, ok
}

func unionDocKeys(a, b *docMap) []string {
	seen := map[string]bool{}
	var all []string
	for _, m := range []*docMap{a, b} {
		for _, k := range m.order {
			if !seen[k] {
				seen[k] = true
				all = append(all, k)
			}
		}
	}
	sort.Strings(all)
	return all
}

type side struct {
	label     string
	rc        *big.Int
	docs      *docMap
	err       string
	count     int
	liveCount int
}

func (s *side) shown(live bool) int {
	if live {
		return s.liveCount
	}
	return s.count
}

func (r *run) newSide(label, path string, rc *big.Int) *side {
	s := &side{label: label, rc: rc, docs: &docMap{docs: map[string]*pyDict{}}}
	raw, err := readFile(path)
	if err != nil {
		r.usage(fmt.Sprintf("cannot read %s: %s", path, strerror(err)))
	}
	text, err := pythonText(raw)
	if err != nil {
		r.crash(err, "docs_of")
	}
	var parsed []*pyDict
	all, err := loadAll(text, composeDepthFile)
	var ye *yamlError
	switch {
	case errors.As(err, &ye):
		s.err = fmt.Sprintf("render %s is not valid YAML", label)
	case err != nil:
		r.crash(err, "docs_of")
	default:
		parsed = docsOf(all)
	}
	s.count = len(parsed)
	for _, d := range parsed {
		k, err := key(d)
		if err != nil {
			r.crash(err, "key")
		}
		if _, dup := s.docs.docs[k]; dup {
			r.print("DUPLICATE doc %s in %s", k, path)
			panic(exitCode(2))
		}
		s.docs.order = append(s.docs.order, k)
		if kind, _ := d.getS("kind"); pyEq(kind, "Secret") {
			name, _ := docName(d)
			meta := newDict()
			_ = meta.set("name", name)
			stub := newDict()
			_ = stub.set("kind", "Secret")
			_ = stub.set("metadata", meta)
			s.docs.docs[k] = stub
		} else {
			s.docs.docs[k] = redact(d).(*pyDict)
			s.liveCount++
		}
	}
	return s
}

type options struct {
	live        bool
	minDocs     *big.Int
	names       *[2]string
	expectNames []string
	expectAdded []string
	expectRows  *string
	info        bool
}

var (
	intArg = regexp.MustCompile(`^-?[0-9]+$`)
	natArg = regexp.MustCompile(`^[0-9]+$`)
)

func splitList(v string) []string {
	out := []string{}
	for _, x := range strings.Split(v, ",") {
		if x != "" {
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

func (r *run) parseArgs(argv []string) ([2]string, *big.Int, *big.Int, options) {
	opts := options{minDocs: big.NewInt(1)}
	var pos []string
	rcs := map[string]*big.Int{}
	i := 0
	val := func(name string) string {
		if i+1 >= len(argv) {
			r.usage(name + " needs a value")
		}
		i++
		return argv[i]
	}
	for i < len(argv) {
		a := argv[i]
		switch {
		case a == "--rc-a" || a == "--rc-b":
			v := val(a)
			if !intArg.MatchString(v) {
				r.usage(a + " must be an integer")
			}
			rcs[a] = r.parseInt(v)
		case a == "--expect-added":
			opts.expectAdded = splitList(val(a))
		case a == "--expect-rows":
			v := val(a)
			opts.expectRows = &v
		case a == "--info":
			opts.info = true
		case a == "--live":
			opts.live = true
		case a == "--min-docs":
			v := val(a)
			if !natArg.MatchString(v) {
				r.usage("--min-docs must be a non-negative integer")
			}
			opts.minDocs = r.parseInt(v)
		case a == "--names":
			if i+2 >= len(argv) {
				r.usage("--names needs two files")
			}
			opts.names = &[2]string{argv[i+1], argv[i+2]}
			i += 2
		case a == "--expect-names-added":
			opts.expectNames = splitList(val(a))
		case a == "--expect":
			r.usage("--expect MODE was removed with the window modes (pin, pin11696, flip, flip11696, hbaonly): use --expect-rows FILE")
		case a == "-h" || a == "--help":
			r.print("%s", kitUsage)
			panic(exitCode(0))
		case strings.HasPrefix(a, "--"):
			r.usage("unknown option " + a)
		default:
			pos = append(pos, a)
		}
		i++
	}
	if len(pos) != 2 {
		r.usage("give exactly two render files")
	}
	if rcs["--rc-a"] == nil || rcs["--rc-b"] == nil {
		r.usage("--rc-a and --rc-b are required (the helm return code of each render)")
	}
	modes := 0
	for _, on := range []bool{opts.expectAdded != nil, opts.expectRows != nil, opts.info} {
		if on {
			modes++
		}
	}
	if modes != 1 {
		r.usage("give exactly one of --expect-added, --expect-rows, --info")
	}
	if opts.expectNames != nil && opts.names == nil {
		r.usage("--expect-names-added needs --names NA NB (in every mode)")
	}
	if opts.names != nil && opts.expectNames == nil && !opts.info {
		r.usage("--names needs --expect-names-added (only --info may print the names sets alone)")
	}
	return [2]string{pos[0], pos[1]}, rcs["--rc-a"], rcs["--rc-b"], opts
}

// parseInt is parse_args' int(v) on a value its regex already checked; past maxStrDigits digits Python raises.
func (r *run) parseInt(v string) *big.Int {
	if len(strings.TrimPrefix(v, "-")) > maxStrDigits {
		r.crash(&crashError{msg: "ValueError: Exceeds the limit (4300 digits) for integer string conversion"}, "parse_args")
	}
	n, _ := new(big.Int).SetString(v, 10)
	return n
}

// readNames is the kit's read_names: the set of stripped, non-empty lines.
func (r *run) readNames(path string) map[string]bool {
	raw, err := readFile(path)
	if err != nil {
		r.usage(fmt.Sprintf("cannot read %s: %s", path, strerror(err)))
	}
	text, err := pythonText(raw)
	if err != nil {
		r.crash(&crashError{msg: "UnicodeDecodeError: the names file is not UTF-8"}, "read_names")
	}
	out := map[string]bool{}
	for _, line := range strings.SplitAfter(text, "\n") {
		if s := strings.TrimFunc(line, pyIsSpace); s != "" {
			out[s] = true
		}
	}
	return out
}

// pyIsSpace is str.isspace() for one code point (what str.strip() removes).
func pyIsSpace(r rune) bool {
	switch {
	case r >= 0x09 && r <= 0x0d, r >= 0x1c && r <= 0x20, r == 0x85, r == 0xa0, r == 0x1680, r >= 0x2000 && r <= 0x200a,
		r == 0x2028, r == 0x2029, r == 0x202f, r == 0x205f, r == 0x3000:
		return true
	}
	return false
}

func duplicateLines(a, b *docMap) []string {
	var lines []string
	for _, s := range []struct {
		label string
		m     *docMap
	}{{"A", a}, {"B", b}} {
		keys := append([]string{}, s.m.order...)
		sort.Strings(keys)
		for _, k := range keys {
			findDups(s.m.docs[k], k, "", s.label, &lines)
		}
	}
	return lines
}
