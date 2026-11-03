package pgmigrate

import (
	"os"
	"testing"
)

// archivedChainFiles is the chain 0139..0162 as released, read from archive/sql in the archive manifest's order.
func archivedChainFiles(t testing.TB) []ChainFile {
	t.Helper()
	return readArchived(t, "archive/sql/")
}

// archivedDownFiles is the down SQL of the archived chain, read from archive/sql/down.
func archivedDownFiles(t testing.TB) []DownFile {
	t.Helper()
	var out []DownFile
	for _, file := range readArchived(t, "archive/sql/down/") {
		out = append(out, DownFile(file))
	}
	return out
}

func readArchived(t testing.TB, dir string) []ChainFile {
	t.Helper()
	var out []ChainFile
	for _, file := range ArchivedChain() {
		data, err := os.ReadFile(dir + file.Name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ChainFile{Revision: file.Revision, Name: file.Name, SQL: string(data)})
	}
	return out
}
