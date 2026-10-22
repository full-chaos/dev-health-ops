package docstools

// Python 3.14 semantics the validate-inventory-review port relies on: argparse for options that take one value,
// pathlib path strings, UTF-8 decoding errors, OSError lines, and str()/repr()/==/truthiness of json.loads values.

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/big"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ivExc is an uncaught Python exception: the verb prints "Kind: msg" (the last part of the traceback) and exits 1.
type ivExc struct{ kind, msg string }

func (e ivExc) Error() string { return e.kind + ": " + e.msg }

// ivStricter is a refusal where Python went on (documented in testdata/README.md).
func ivStricter(msg string) ivExc {
	return ivExc{"cigate", "refused (stricter than the Python checker): " + msg}
}

// ---- argparse (3.14.7): -h/--help plus options with nargs=None, allow_abbrev ----

// ivNegNumber is argparse's _negative_number_matcher.match: re '-\.?\d' at the start.
func ivNegNumber(s string) bool {
	rest, ok := strings.CutPrefix(s, "-")
	if !ok {
		return false
	}
	rest = strings.TrimPrefix(rest, ".")
	r, size := utf8.DecodeRuneInString(rest)
	return size > 0 && pythonparity.IsDecimal(r)
}

type ivArgparse struct {
	prog, usage, help string
	opts              []string // option strings after -h, --help, in add_argument order
}

type ivOptTuple struct {
	opt           string // "" = no such option
	sep, explicit *string
}

type ivArgError struct{ msg string }

func (p ivArgparse) actions() []string { return append([]string{"-h", "--help"}, p.opts...) }

func (p ivArgparse) known(s string) bool {
	for _, o := range p.actions() {
		if o == s {
			return true
		}
	}
	return false
}

func strp(s string) *string { return &s }

// parseOptional is ArgumentParser._parse_optional; nil means a positional ('A').
func (p ivArgparse) parseOptional(arg string) []ivOptTuple {
	if arg == "" || arg[0] != '-' {
		return nil
	}
	if p.known(arg) {
		return []ivOptTuple{{opt: arg}}
	}
	if utf8.RuneCountInString(arg) == 1 {
		return nil
	}
	if head, tail, ok := strings.Cut(arg, "="); ok && p.known(head) {
		return []ivOptTuple{{opt: head, sep: strp("="), explicit: strp(tail)}}
	}
	var tuples []ivOptTuple
	prefix, tail, hasEq := strings.Cut(arg, "=")
	withEq := func(o string) ivOptTuple {
		t := ivOptTuple{opt: o}
		if hasEq {
			t.sep, t.explicit = strp("="), strp(tail)
		}
		return t
	}
	if arg[1] == '-' {
		for _, o := range p.actions() {
			if strings.HasPrefix(o, prefix) {
				tuples = append(tuples, withEq(o))
			}
		}
	} else {
		_, size := utf8.DecodeRuneInString(arg[1:])
		short := arg[:1+size]
		for _, o := range p.actions() {
			if o == short {
				tuples = append(tuples, ivOptTuple{opt: o, sep: strp(""), explicit: strp(arg[1+size:])})
			} else if strings.HasPrefix(o, prefix) {
				tuples = append(tuples, withEq(o))
			}
		}
	}
	if len(tuples) > 0 {
		return tuples
	}
	if ivNegNumber(arg) || strings.Contains(arg, " ") {
		return nil
	}
	return []ivOptTuple{{}}
}

func ivActionName(opt string) string {
	if opt == "-h" || opt == "--help" {
		return "-h/--help"
	}
	return opt
}

// parse is ArgumentParser.parse_args. It returns the values given (by option string) and (rc, true) when the
// process stops: help printed (0) or a usage error (2).
func (p ivArgparse) parse(args []string, stdout, stderr io.Writer) (map[string]string, int, bool) {
	fail := func(msg string) (map[string]string, int, bool) {
		fmt.Fprint(stderr, p.usage)
		fmt.Fprintf(stderr, "%s: error: %s\n", p.prog, msg)
		return nil, 2, true
	}
	tuples := map[int][]ivOptTuple{}
	var pattern []byte
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			pattern = append(pattern, '-')
			for i++; i < len(args); i++ {
				pattern = append(pattern, 'A')
			}
			break
		}
		if t := p.parseOptional(args[i]); t == nil {
			pattern = append(pattern, 'A')
		} else {
			tuples[i] = t
			pattern = append(pattern, 'O')
		}
	}
	vals := map[string]string{}
	var extras []string
	help := false
	consume := func(start int) (int, *ivArgError) {
		ts := tuples[start]
		if len(ts) > 1 {
			names := make([]string, len(ts))
			for i, t := range ts {
				names[i] = t.opt
			}
			return 0, &ivArgError{fmt.Sprintf("ambiguous option: %s could match %s", args[start], strings.Join(names, ", "))}
		}
		opt, sep, explicit := ts[0].opt, ts[0].sep, ts[0].explicit
		type taken struct {
			opt  string
			args []string
		}
		var actions []taken
		var stop int
		for {
			if opt == "" {
				extras = append(extras, args[start])
				return start + 1, nil
			}
			isHelp := opt == "-h" || opt == "--help"
			if explicit != nil {
				if isHelp {
					if opt[1] != '-' && *explicit != "" {
						if (sep != nil && *sep != "") || (*explicit)[0] == '-' {
							return 0, &ivArgError{"argument " + ivActionName(opt) + ": ignored explicit argument " + pythonparity.StrRepr(*explicit)}
						}
						actions = append(actions, taken{opt: opt})
						r, size := utf8.DecodeRuneInString(*explicit)
						next := "-" + string(r)
						if p.known(next) {
							opt = next
							rest := (*explicit)[size:]
							switch {
							case rest == "":
								sep, explicit = nil, nil
							case rest[0] == '=':
								sep, explicit = strp("="), strp(rest[1:])
							default:
								sep, explicit = strp(""), strp(rest)
							}
							continue
						}
						extras = append(extras, "-"+*explicit)
						stop = start + 1
						break
					}
					return 0, &ivArgError{"argument " + ivActionName(opt) + ": ignored explicit argument " + pythonparity.StrRepr(*explicit)}
				}
				actions = append(actions, taken{opt: opt, args: []string{*explicit}})
				stop = start + 1
				break
			}
			if isHelp {
				actions = append(actions, taken{opt: opt})
				stop = start + 1
				break
			}
			if start+1 >= len(pattern) || pattern[start+1] != 'A' {
				return 0, &ivArgError{"argument " + opt + ": expected one argument"}
			}
			actions = append(actions, taken{opt: opt, args: []string{args[start+1]}})
			stop = start + 2
			break
		}
		for _, a := range actions {
			if a.opt == "-h" || a.opt == "--help" {
				help = true
				return stop, nil
			}
			vals[a.opt] = a.args[0]
		}
		return stop, nil
	}
	maxOpt := -1
	for i := range tuples {
		if i > maxOpt {
			maxOpt = i
		}
	}
	start := 0
	for start <= maxOpt {
		next := start
		for next <= maxOpt {
			if _, ok := tuples[next]; ok {
				break
			}
			next++
		}
		if start != next {
			extras = append(extras, args[start:next]...)
			start = next
		}
		stop, err := consume(start)
		if err != nil {
			return fail(err.msg)
		}
		if help {
			fmt.Fprint(stdout, p.help)
			return nil, 0, true
		}
		start = stop
	}
	extras = append(extras, args[start:]...)
	if len(extras) > 0 {
		return fail("unrecognized arguments: " + strings.Join(extras, " "))
	}
	return vals, 0, false
}

// ---- pathlib ----

// ivPath is str(PurePosixPath(p)).
func ivPath(p string) string {
	root := ""
	switch {
	case strings.HasPrefix(p, "//") && !strings.HasPrefix(p, "///"):
		root = "//"
	case strings.HasPrefix(p, "/"):
		root = "/"
	}
	var parts []string
	for _, s := range strings.Split(p, "/") {
		if s != "" && s != "." {
			parts = append(parts, s)
		}
	}
	if len(parts) == 0 {
		if root != "" {
			return root
		}
		return "."
	}
	return root + strings.Join(parts, "/")
}

// ivJoin is str(Path(dir) / name) for a plain name.
func ivJoin(dir, name string) string {
	d := ivPath(dir)
	switch {
	case d == ".":
		return name
	case strings.HasSuffix(d, "/"):
		return d + name
	}
	return d + "/" + name
}

func ivSplitName(p string) (dir, name string) {
	s := ivPath(p)
	if s == "." || strings.Trim(s, "/") == "" {
		return s, ""
	}
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[:i+1], s[i+1:]
	}
	return "", s
}

func ivEmptyName(p string) ivExc {
	return ivExc{"ValueError", "PosixPath(" + pythonparity.StrRepr(ivPath(p)) + ") has an empty name"}
}

// ivWithSuffix is str(Path(p).with_suffix(suffix)) on Python 3.14: the suffix is os.path.splitext's (a trailing "."
// counts; leading dots do not).
func ivWithSuffix(p, suffix string) (string, error) {
	dir, name := ivSplitName(p)
	if name == "" {
		return "", ivEmptyName(p)
	}
	if i := strings.LastIndex(name, "."); strings.Trim(name[:max(i, 0)], ".") != "" {
		name = name[:i]
	}
	return dir + name + suffix, nil
}

// ivWithName is str(Path(p).with_name(name)).
func ivWithName(p, name string) (string, error) {
	dir, old := ivSplitName(p)
	if old == "" {
		return "", ivEmptyName(p)
	}
	return dir + name, nil
}

// ---- I/O errors ----

// ivOSError is the exception an open/read/write failure raises.
func ivOSError(err error) ivExc {
	var pe *fs.PathError
	if !errors.As(err, &pe) {
		return ivExc{"OSError", err.Error()}
	}
	at := ": " + pythonparity.StrRepr(pe.Path)
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return ivExc{"OSError", err.Error()}
	}
	kinds := map[syscall.Errno]string{
		syscall.ENOENT: "FileNotFoundError", syscall.EISDIR: "IsADirectoryError", syscall.EACCES: "PermissionError",
		syscall.EPERM: "PermissionError", syscall.ENOTDIR: "NotADirectoryError",
	}
	kind, ok := kinds[errno]
	if !ok {
		kind = "OSError"
	}
	msg := errno.Error()
	if msg != "" {
		msg = strings.ToUpper(msg[:1]) + msg[1:]
	}
	return ivExc{kind, fmt.Sprintf("[Errno %d] %s%s", int(errno), msg, at)}
}

// ivDecodeUTF8 is bytes.decode("utf-8") (strict): the UnicodeDecodeError CPython raises for the first bad sequence.
func ivDecodeUTF8(raw []byte) (string, error) {
	if utf8.Valid(raw) {
		return string(raw), nil
	}
	for i := 0; i < len(raw); {
		b := raw[i]
		if b < 0x80 {
			i++
			continue
		}
		var n int
		lo, hi := byte(0x80), byte(0xbf)
		switch {
		case b >= 0xc2 && b <= 0xdf:
			n = 1
		case b == 0xe0:
			n, lo = 2, 0xa0
		case b >= 0xe1 && b <= 0xec, b == 0xee, b == 0xef:
			n = 2
		case b == 0xed:
			n, hi = 2, 0x9f
		case b == 0xf0:
			n, lo = 3, 0x90
		case b >= 0xf1 && b <= 0xf3:
			n = 3
		case b == 0xf4:
			n, hi = 3, 0x8f
		default:
			return "", ivUnicodeDecode(raw, i, i+1, "invalid start byte")
		}
		for k := 1; k <= n; k++ {
			if i+k >= len(raw) {
				return "", ivUnicodeDecode(raw, i, len(raw), "unexpected end of data")
			}
			c := raw[i+k]
			if k == 1 && (c < lo || c > hi) || k > 1 && (c < 0x80 || c > 0xbf) {
				return "", ivUnicodeDecode(raw, i, i+k, "invalid continuation byte")
			}
		}
		i += n + 1
	}
	return "", ivExc{"UnicodeDecodeError", "'utf-8' codec can't decode"}
}

func ivUnicodeDecode(raw []byte, start, end int, reason string) ivExc {
	if end-start == 1 {
		return ivExc{"UnicodeDecodeError", fmt.Sprintf("'utf-8' codec can't decode byte 0x%02x in position %d: %s", raw[start], start, reason)}
	}
	return ivExc{"UnicodeDecodeError", fmt.Sprintf("'utf-8' codec can't decode bytes in position %d-%d: %s", start, end-1, reason)}
}

// ---- json.loads values ----

func ivTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "NoneType"
	case bool:
		return "bool"
	case *big.Int:
		return "int"
	case float64:
		return "float"
	case string:
		return "str"
	case []any:
		return "list"
	case pythonparity.OrderedObject:
		return "dict"
	}
	return fmt.Sprintf("%T", v)
}

func ivRepr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case *big.Int:
		return x.String()
	case float64:
		return pythonparity.Repr(x)
	case string:
		return pythonparity.StrRepr(x)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = ivRepr(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case pythonparity.OrderedObject:
		parts := make([]string, len(x))
		for i, m := range x {
			parts[i] = pythonparity.StrRepr(m.Key) + ": " + ivRepr(m.Value)
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprint(v)
}

// ivStr is str(v).
func ivStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ivRepr(v)
}

// ivTruthy is bool(v).
func ivTruthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case *big.Int:
		return x.Sign() != 0
	case float64:
		return x != 0 || math.IsNaN(x)
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case pythonparity.OrderedObject:
		return len(x) > 0
	}
	return true
}

func ivNumber(v any) (*big.Float, bool, bool) {
	switch x := v.(type) {
	case bool:
		if x {
			return big.NewFloat(1), true, true
		}
		return big.NewFloat(0), true, true
	case *big.Int:
		return new(big.Float).SetInt(x), true, true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, true, false
		}
		return big.NewFloat(x), true, true
	}
	return nil, false, false
}

// ivEq is a == b for json.loads values (two distinct objects: a NaN is never equal).
func ivEq(a, b any) bool {
	na, numA, finA := ivNumber(a)
	nb, numB, finB := ivNumber(b)
	if numA || numB {
		if !numA || !numB {
			return false
		}
		if !finA || !finB {
			fa, okA := a.(float64)
			fb, okB := b.(float64)
			return okA && okB && math.IsInf(fa, 0) && fa == fb
		}
		return na.Cmp(nb) == 0
	}
	switch x := a.(type) {
	case nil:
		return b == nil
	case string:
		y, ok := b.(string)
		return ok && x == y
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !ivEq(x[i], y[i]) {
				return false
			}
		}
		return true
	case pythonparity.OrderedObject:
		y, ok := b.(pythonparity.OrderedObject)
		if !ok || len(x) != len(y) {
			return false
		}
		for _, m := range x {
			w, found := ivGet(y, m.Key)
			if !found || !ivEq(m.Value, w) {
				return false
			}
		}
		return true
	}
	return false
}

func ivGet(o pythonparity.OrderedObject, key string) (any, bool) {
	for _, m := range o {
		if m.Key == key {
			return m.Value, true
		}
	}
	return nil, false
}

// ivItem is v[key] for a str key.
func ivItem(v any, key string) (any, error) {
	switch x := v.(type) {
	case pythonparity.OrderedObject:
		if w, ok := ivGet(x, key); ok {
			return w, nil
		}
		return nil, ivExc{"KeyError", pythonparity.StrRepr(key)}
	case []any:
		return nil, ivExc{"TypeError", "list indices must be integers or slices, not str"}
	case string:
		return nil, ivExc{"TypeError", "string indices must be integers, not 'str'"}
	}
	return nil, ivExc{"TypeError", fmt.Sprintf("'%s' object is not subscriptable", ivTypeName(v))}
}

// ivLen is len(v).
func ivLen(v any) (*big.Int, error) {
	switch x := v.(type) {
	case []any:
		return big.NewInt(int64(len(x))), nil
	case pythonparity.OrderedObject:
		return big.NewInt(int64(len(x))), nil
	case string:
		return big.NewInt(int64(utf8.RuneCountInString(x))), nil
	}
	return nil, ivExc{"TypeError", fmt.Sprintf("object of type '%s' has no len()", ivTypeName(v))}
}

// ivIterate is list(v) for the iterable shapes json.loads makes (a dict iterates its keys, a str its characters).
func ivIterate(v any) ([]any, error) {
	switch x := v.(type) {
	case []any:
		return x, nil
	case pythonparity.OrderedObject:
		out := make([]any, len(x))
		for i, m := range x {
			out[i] = m.Key
		}
		return out, nil
	case string:
		var out []any
		for _, r := range x {
			out = append(out, string(r))
		}
		return out, nil
	}
	return nil, ivExc{"TypeError", fmt.Sprintf("'%s' object is not iterable", ivTypeName(v))}
}

// ivHashable reports whether v can be a dict key (a Counter key).
func ivHashable(v any) error {
	switch v.(type) {
	case []any, pythonparity.OrderedObject:
		return ivExc{"TypeError", fmt.Sprintf("unhashable type: '%s'", ivTypeName(v))}
	}
	return nil
}
