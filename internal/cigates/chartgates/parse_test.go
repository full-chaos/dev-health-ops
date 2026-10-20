package chartgates_test

import (
	"regexp"
	"strings"
)

// A guard that matches raw file text passes on a comment, an echo or a quoted string that merely names the
// thing it looks for. Where a guard can run the code it runs it (exec_test.go); these helpers cover the data
// files that cannot be run: Dockerfiles as instruction lists, templates with their comments removed.

// --- Dockerfile ---

type dockerInstr struct {
	stage string
	cmd   string
	args  string
}

func parseDockerfile(text string) []dockerInstr {
	var out []dockerInstr
	var logical []string
	var cur strings.Builder
	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimSpace(line)
		if cur.Len() == 0 && (t == "" || strings.HasPrefix(t, "#")) {
			continue
		}
		if strings.HasPrefix(t, "#") {
			continue
		}
		if strings.HasSuffix(t, `\`) {
			cur.WriteString(strings.TrimSuffix(t, `\`) + " ")
			continue
		}
		cur.WriteString(t)
		logical = append(logical, cur.String())
		cur.Reset()
	}
	stage := ""
	for _, l := range logical {
		cmd, args, _ := strings.Cut(l, " ")
		cmd = strings.ToUpper(cmd)
		args = strings.TrimSpace(args)
		if cmd == "FROM" {
			fields := strings.Fields(args)
			stage = ""
			for i, f := range fields {
				if strings.EqualFold(f, "as") && i+1 < len(fields) {
					stage = fields[i+1]
				}
			}
		}
		out = append(out, dockerInstr{stage: stage, cmd: cmd, args: args})
	}
	return out
}

// runCommands splits every RUN instruction into its shell commands (on ; and &&).
func runCommands(ins []dockerInstr) []string {
	var out []string
	for _, i := range ins {
		if i.cmd != "RUN" {
			continue
		}
		for _, part := range strings.FieldsFunc(strings.ReplaceAll(i.args, "&&", ";"), func(r rune) bool { return r == ';' }) {
			if c := strings.TrimSpace(part); c != "" {
				out = append(out, strings.Join(strings.Fields(c), " "))
			}
		}
	}
	return out
}

func hasInstr(ins []dockerInstr, cmd, args string) bool {
	for _, i := range ins {
		if i.cmd == cmd && strings.Join(strings.Fields(i.args), " ") == strings.Join(strings.Fields(args), " ") {
			return true
		}
	}
	return false
}

func hasRunCommand(ins []dockerInstr, command string) bool {
	for _, c := range runCommands(ins) {
		if c == command {
			return true
		}
	}
	return false
}

func hasStageInstr(ins []dockerInstr, stage, cmd, args string) bool {
	for _, i := range ins {
		if i.stage == stage && i.cmd == cmd && strings.Join(strings.Fields(i.args), " ") == args {
			return true
		}
	}
	return false
}

// --- templates ---

var tplComment = regexp.MustCompile(`(?s)\{\{-?\s*/\*.*?\*/\s*-?\}\}`)

// tplCode is a template's text with its {{/* */}} comments and its # comment lines removed.
func tplCode(text string) string {
	text = tplComment.ReplaceAllString(text, "")
	var keep []string
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		keep = append(keep, l)
	}
	return strings.Join(keep, "\n")
}
