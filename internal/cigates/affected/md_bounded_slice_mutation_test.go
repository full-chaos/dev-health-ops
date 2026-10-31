package affected_test

import (
	"strings"
	"testing"
)

// A package slice spread resolves to its initializer only when no code of the directory writes into it, aliases it, takes
// its address, stores it or passes it out. Every other use leaves the spread unresolved (pm D6610), so the md-bounded plan
// does not trust stale contents.
const sliceSpreadHead = "package p\n\nimport (\n\t\"os/exec\"\n\t\"slices\"\n\t\"sort\"\n)\n\nvar (\n\t_ = slices.Sort[[]string]\n\t_ = sort.Strings\n)\n\nvar args = []string{\"ls-files\", \"README.md\"}\n\n"

const sliceSpreadRun = "\nfunc run() error { return exec.Command(\"git\", args...).Run() }\n"

func TestPlanReadsPackageSliceSpreadNeedsAnUntouchedSlice(t *testing.T) {
	cells := []struct {
		name, extra string
		resolved    bool
	}{
		{"untouched", "", true},
		{"len_and_range_only", "func n() int {\n\tc := 0\n\tfor range args {\n\t\tc += len(args)\n\t}\n\treturn c\n}\n", true},
		{"spread_into_stdlib", "func j() []string { return append([]string{\"a\"}, args...) }\n", true},
		{"index_write", "func init() { args[1] = \"x\" }\n", false},
		{"index_write_in_test_file", "TESTFILE:func init() { args[1] = \"x\" }\n", false},
		{"append_back", "func init() { args = append(args, \"x\") }\n", false},
		{"alias_write", "func init() {\n\tt := args\n\tt[1] = \"x\"\n}\n", false},
		{"call_argument", "func mut(a []string) { a[1] = \"x\" }\n\nfunc init() { mut(args) }\n", false},
		{"variadic_into_local_callee", "func mut(a ...string) { a[1] = \"x\" }\n\nfunc init() { mut(args...) }\n", false},
		{"method_receiver_arg", "type w struct{}\n\nfunc (w) mut(a []string) { a[1] = \"x\" }\n\nfunc init() { w{}.mut(args) }\n", false},
		{"address_of_var", "func init() { p := &args; (*p)[1] = \"x\" }\n", false},
		{"address_of_element", "func init() { p := &args[1]; *p = \"x\" }\n", false},
		{"struct_field_escape", "type h struct{ a []string }\n\nvar held = h{a: args}\n", false},
		{"map_escape", "var held = map[string][]string{\"k\": args}\n", false},
		{"channel_escape", "func init() {\n\tch := make(chan []string, 1)\n\tch <- args\n}\n", false},
		{"other_var_escape", "var held = args\n", false},
		{"copy_into", "func init() { copy(args, []string{\"a\", \"b\"}) }\n", false},
		{"sort_in_place", "func init() { sort.Strings(args) }\n", false},
		{"slices_in_place", "func init() { slices.Sort(args) }\n", false},
		{"subslice", "func init() { t := args[:1]; t[0] = \"x\" }\n", false},
		{"exported_name", "RENAME:Args", false},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			src := sliceSpreadHead + sliceSpreadRun
			files := map[string]string{}
			switch {
			case strings.HasPrefix(c.extra, "TESTFILE:"):
				files["p/p_extra_test.go"] = "package p\n\n" + strings.TrimPrefix(c.extra, "TESTFILE:")
			case strings.HasPrefix(c.extra, "RENAME:"):
				src = strings.ReplaceAll(src, "args", strings.TrimPrefix(c.extra, "RENAME:"))
			default:
				src += "\n" + c.extra
			}
			files["p/p_spread_test.go"] = src
			root := readsFixture(t, files)
			res := cmdSpec{dir: root, env: gitEnv}.run(cigate(t), "plan-reads", "--root", root, "--list-unresolved-spreads")
			if res.code != 0 {
				t.Fatalf("exit %d, stderr %q", res.code, strings.TrimSpace(res.stderr))
			}
			listed := strings.Contains(res.stdout, "p/p_spread_test.go:")
			if listed == c.resolved {
				t.Fatalf("resolved=%v but unresolved list is %q", c.resolved, res.stdout)
			}
		})
	}
}
