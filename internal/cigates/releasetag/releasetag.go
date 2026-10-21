// Package releasetag decides the next pre-release tag of the repository. It is the Go port of the former Python script
// scripts/auto_tag.py and keeps its output, its exit codes and its refusals; the checked-in inventory
// (testdata/refusals/auto_tag.tsv) lists every site of that script that can refuse and the Go answer to it.
//
//	cigate release next-tag [--package-json P] [--pre-release-suffix S]
//
// It prints what the script printed, and appends new_tag=<tag> to the file $GITHUB_OUTPUT names.
package releasetag

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const usage = "usage: cigate release next-tag [-h] [--package-json PACKAGE_JSON] [--pre-release-suffix PRE_RELEASE_SUFFIX]"

// Run is `cigate release <verb>`; only next-tag exists. env reads a process variable.
func Run(args []string, env func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "next-tag" {
		fmt.Fprintln(stderr, "usage: cigate release next-tag [flags]")
		return 2
	}
	opts, err := parseArgs(args[1:])
	if err != nil {
		fmt.Fprintln(stderr, usage)
		fmt.Fprintf(stderr, "cigate release next-tag: error: %v\n", err)
		return 2
	}
	if opts.help {
		fmt.Fprintln(stdout, usage)
		return 0
	}
	return nextTag(opts, env, stdout, stderr)
}

// refusal is an uncaught exception of the Python script: one "Kind: message" line on stderr, exit code 1.
func refusal(stderr io.Writer, kind, msg string) int {
	fmt.Fprintf(stderr, "%s: %s\n", kind, msg)
	return 1
}

func nextTag(opts options, env func(string) string, stdout, stderr io.Writer) int {
	latestTag, haveTag, code := latestGitTag(stderr)
	if code != 0 {
		return code
	}
	member, code := readVersionMember(opts.packageJSON, stderr)
	if code != 0 {
		return code
	}
	pkgVersion, pkgText, code := versionString(member, stderr)
	if code != 0 {
		return code
	}

	if haveTag {
		fmt.Fprintf(stdout, "Latest tag: %s\n", latestTag)
	} else {
		fmt.Fprintln(stdout, "Latest tag: None")
	}
	fmt.Fprintf(stdout, "Package.json version: %s\n", pkgText)

	clean := "0.0.0"
	if latestTag != "" {
		clean = strings.TrimLeft(latestTag, "v")
	}
	latest, ok, err := parseSemver(clean)
	if err != nil {
		return refusal(stderr, "ValueError", err.Error())
	}
	if !ok {
		fmt.Fprintf(stdout, "Error: Could not parse latest tag %s\n", tagText(latestTag, haveTag))
		latest = semver{major: new(big.Int), minor: new(big.Int), patch: new(big.Int)}
	}

	next := ""
	if pkgVersion != "" {
		pkg, ok, err := parseSemver(pkgVersion)
		if err != nil {
			return refusal(stderr, "ValueError", err.Error())
		}
		if ok && compareVersions(pkg, latest) > 0 {
			next = pkgVersion
			fmt.Fprintf(stdout, "Using version from package.json: %s\n", next)
		}
	}
	if next == "" {
		var err error
		if next, err = incremented(latest, opts.suffix); err != nil {
			return refusal(stderr, "ValueError", err.Error())
		}
	}

	tag := "v" + next
	fmt.Fprintf(stdout, "Next tag determined: %s\n", tag)

	if path := env("GITHUB_OUTPUT"); path != "" {
		if code := appendOutput(path, tag, stderr); code != 0 {
			return code
		}
	}
	return 0
}

func tagText(tag string, have bool) string {
	if !have {
		return "None"
	}
	return tag
}

func incremented(latest semver, suffix string) (string, error) {
	major, err := itoa(latest.major)
	if err != nil {
		return "", err
	}
	minor, err := itoa(latest.minor)
	if err != nil {
		return "", err
	}
	patch, err := itoa(latest.patch)
	if err != nil {
		return "", err
	}
	if latest.preType != "" {
		num, err := itoa(new(big.Int).Add(latest.preNum, big.NewInt(1)))
		if err != nil {
			return "", err
		}
		return major + "." + minor + "." + patch + "-" + latest.preType + "." + num, nil
	}
	bumped, err := itoa(new(big.Int).Add(latest.patch, big.NewInt(1)))
	if err != nil {
		return "", err
	}
	return major + "." + minor + "." + bumped + "-" + suffix + ".1", nil
}

// latestGitTag is get_latest_tag: `git describe --tags --abbrev=0`; a failing git (no tag, no repository) is no tag. git's
// own complaint reaches stderr, as it did under Python.
func latestGitTag(stderr io.Writer) (tag string, have bool, code int) {
	git, err := exec.LookPath("git")
	if err != nil {
		return "", false, refusal(stderr, "FileNotFoundError", "[Errno 2] No such file or directory: 'git'")
	}
	cmd := exec.Command(git, "describe", "--tags", "--abbrev=0")
	cmd.Stderr = stderr
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return "", false, 0
		}
		return "", false, refusal(stderr, "OSError", err.Error())
	}
	if !utf8.Valid(out) {
		return "", false, refusal(stderr, "UnicodeDecodeError", "the output of git describe is not UTF-8")
	}
	return pythonparity.Strip(string(out)), true, 0
}

// readVersionMember is get_package_json_version up to data.get("version"): a missing file is no version.
func readVersionMember(path string, stderr io.Writer) (versionMember, int) {
	if _, err := os.Stat(path); err != nil {
		return versionMember{kind: kindMissing}, 0
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return versionMember{}, osErrorLine(stderr, path, err)
	}
	if !utf8.Valid(raw) {
		return versionMember{}, refusal(stderr, "UnicodeDecodeError", path+": invalid UTF-8")
	}
	member, jerr := parsePackageJSON(string(raw))
	if jerr != nil {
		return versionMember{}, refusal(stderr, jerr.kind, jerr.msg)
	}
	return member, 0
}

func osErrorLine(stderr io.Writer, path string, err error) int {
	repr := pythonparity.StrRepr(path)
	switch {
	case errors.Is(err, syscall.EISDIR):
		return refusal(stderr, "IsADirectoryError", "[Errno 21] Is a directory: "+repr)
	case errors.Is(err, syscall.EACCES):
		return refusal(stderr, "PermissionError", "[Errno 13] Permission denied: "+repr)
	case errors.Is(err, syscall.ENOENT):
		return refusal(stderr, "FileNotFoundError", "[Errno 2] No such file or directory: "+repr)
	}
	return refusal(stderr, "OSError", err.Error()+": "+repr)
}

// versionString turns data.get("version") into what the script worked with. text is what its print shows. version is the
// string to parse, "" when the script saw a falsy value. A truthy value that is not a string made re.match raise TypeError;
// a string that carries a control character or a lone surrogate is refused here (D6129, stricter than Python): the script
// accepted "1.2.3\n" through "$" and wrote it to GITHUB_OUTPUT.
func versionString(member versionMember, stderr io.Writer) (version, text string, code int) {
	switch member.kind {
	case kindMissing, kindNull:
		return "", "None", 0
	case kindFalse:
		return "", "False", 0
	case kindInt:
		if member.zero {
			return "", "0", 0
		}
	case kindFloat:
		if member.zero {
			if member.negZero {
				return "", "-0.0", 0
			}
			return "", "0.0", 0
		}
	case kindList:
		if member.zero {
			return "", "[]", 0
		}
	case kindDict:
		if member.zero {
			return "", "{}", 0
		}
	case kindString:
		for _, r := range member.text {
			if unicode.Is(unicode.Cc, r) || (r >= 0xD800 && r <= 0xDFFF) {
				return "", "", refusal(stderr, "ValueError", "package.json version holds a control character or a lone surrogate")
			}
		}
		s := string(member.text)
		return s, s, 0
	}
	return "", "", refusal(stderr, "TypeError", "package.json version is not a string")
}

func appendOutput(path, tag string, stderr io.Writer) int {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return osErrorLine(stderr, path, err)
	}
	if !utf8.ValidString(tag) {
		_ = f.Close()
		return refusal(stderr, "UnicodeEncodeError", "the tag holds bytes that are not UTF-8 and the output file takes UTF-8 only")
	}
	if _, err := fmt.Fprintf(f, "new_tag=%s\n", tag); err != nil {
		_ = f.Close()
		return refusal(stderr, "OSError", err.Error())
	}
	if err := f.Close(); err != nil {
		return refusal(stderr, "OSError", err.Error())
	}
	return 0
}
