package affected_test

import "testing"

// CHAOS-10440: a bare entry name from os.ReadDir read in the directory it was listed from (a package's own directory,
// os.ReadDir(".") in a test) is a file of that directory: the listing already carries the walk row, and the read adds
// no dynreads.tsv row. A name listed from any other directory, or read where the working directory is not known, keeps
// its row.
const entHead = "import \"path/filepath\"\n\nvar _ = filepath.Clean\n\nfunc TestEnt(t *testing.T) {\n"

func entryFlow(name, body string) nongoCase {
	head := flowFile(body)
	head[tablesDir+"walks.tsv"] = "x\tx_flow_test.go\tx\tdir\t*\n"
	return nongoCase{name: name + "_needs_no_dynreads_row", head: head, changed: []string{".claude/x.json"}, want: "NONE"}
}

func unrowedEntryFlow(name, body, walks string) nongoCase {
	head := flowFile(body)
	head[tablesDir+"walks.tsv"] = walks
	return nongoCase{name: name + "_needs_its_dynreads_row", head: head, changed: []string{".claude/x.json"}, want: "FULL the read tables differ*",
		readsCode: 1, readsWant: []string{"x\tx_flow_test.go"}}
}

func TestNonGoPlanEntryNamesReadInTheirOwnDirectoryAreBounded(t *testing.T) {
	runNongo(t, []nongoCase{
		entryFlow("bare_name", entHead+"\tentries, _ := os.ReadDir(\".\")\n\tfor _, e := range entries {\n\t\t_, _ = os.ReadFile(e.Name())\n\t}\n}\n"),
		entryFlow("name_in_a_variable", entHead+"\tentries, _ := os.ReadDir(\".\")\n\tfor _, e := range entries {\n\t\tname := e.Name()\n\t\t_, _ = os.ReadFile(name)\n\t}\n}\n"),
		entryFlow("name_through_clean", entHead+"\tentries, _ := os.ReadDir(\".\")\n\tfor _, e := range entries {\n\t\t_, _ = os.ReadFile(filepath.Clean(e.Name()))\n\t}\n}\n"),
	})
}

func TestNonGoPlanEntryNameNearMissesKeepTheirRow(t *testing.T) {
	const own = "x\tx_flow_test.go\tx\tdir\t*\n"
	runNongo(t, []nongoCase{
		unrowedEntryFlow("name_listed_from_a_subdirectory", entHead+"\tentries, _ := os.ReadDir(\"sub\")\n\tfor _, e := range entries {\n\t\t_, _ = os.ReadFile(e.Name())\n\t}\n}\n", "x\tx_flow_test.go\tx/sub\tdir\t*\n"),
		unrowedEntryFlow("name_listed_from_the_docs", entHead+"\tentries, _ := os.ReadDir(\"../docs\")\n\tfor _, e := range entries {\n\t\t_, _ = os.ReadFile(e.Name())\n\t}\n}\n", "x\tx_flow_test.go\tdocs\tdir\t*\n"),
		unrowedEntryFlow("name_read_after_a_chdir", entHead+"\tentries, _ := os.ReadDir(\".\")\n\tt.Chdir(os.Getenv(\"P\"))\n\tfor _, e := range entries {\n\t\t_, _ = os.ReadFile(e.Name())\n\t}\n}\n", own),
		unrowedEntryFlow("name_of_an_unlisted_value", entHead+"\tfor _, e := range os.Args {\n\t\t_, _ = os.ReadFile(e)\n\t}\n}\n", own),
	})
}
