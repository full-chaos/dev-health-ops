package docstools

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// dcFlag is one option of a ported script's argparse parser.
type dcFlag struct {
	name     string
	required bool
	def      string
}

// dcParseFlags reads "--name value" and "--name=value" options. argparse accepted abbreviations of an option, "-h",
// values that start with "-" and an empty value; this parser refuses them (STRICTER, exit 2). A repeated option keeps
// its last value, as argparse does. A usage error is exit 2 with the usage and one error line on stderr.
func dcParseFlags(verb string, args []string, flags []dcFlag, stderr io.Writer) (map[string]string, int) {
	known := map[string]*dcFlag{}
	var usage []string
	for i := range flags {
		known[flags[i].name] = &flags[i]
		item := "--" + flags[i].name + " " + strings.ToUpper(strings.ReplaceAll(flags[i].name, "-", "_"))
		if !flags[i].required {
			item = "[" + item + "]"
		}
		usage = append(usage, item)
	}
	fail := func(msg string) (map[string]string, int) {
		fmt.Fprintf(stderr, "usage: cigate docs %s %s\ncigate docs %s: error: %s\n", verb, strings.Join(usage, " "), verb, msg)
		return nil, 2
	}
	got := map[string]string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "--") || arg == "--" {
			return fail("unrecognized arguments: " + arg)
		}
		name, value, hasValue := strings.Cut(arg[2:], "=")
		if _, ok := known[name]; !ok {
			return fail("unrecognized arguments: " + arg)
		}
		if !hasValue {
			if i+1 >= len(args) {
				return fail("argument --" + name + ": expected one argument")
			}
			i++
			value = args[i]
		}
		if value == "" || strings.HasPrefix(value, "-") {
			return fail("argument --" + name + ": expected one argument")
		}
		got[name] = value
	}
	for _, f := range flags {
		if _, ok := got[f.name]; !ok {
			if f.required {
				return fail("the following arguments are required: --" + f.name)
			}
			got[f.name] = f.def
		}
	}
	return got, 0
}

// dcFail prints the one stderr line for an uncaught Python exception and returns exit code 1.
func dcFail(stderr io.Writer, err error) int {
	var ve dcValueError
	var de a1DecodeError
	var ex a1Exception
	switch {
	case errors.As(err, &ve):
		return a1Traceback(stderr, ve.kind, ve.msg)
	case errors.As(err, &de):
		return a1Traceback(stderr, "UnicodeDecodeError", de.Error())
	case errors.As(err, &ex):
		return a1Traceback(stderr, ex.kind, ex.msg)
	}
	kind, msg := a1OSError(err)
	return a1Traceback(stderr, kind, msg)
}

// dcIsDirPy is Path.is_dir (3.14): os.path.isdir, any error is false.
func dcIsDirPy(path string) (bool, error) {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir(), nil
}

// dcExistsPy is Path.exists (3.14): os.path.exists, any error is false.
func dcExistsPy(path string) (bool, error) {
	_, err := os.Stat(path)
	return err == nil, nil
}
