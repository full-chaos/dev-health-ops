package docstools

import (
	"os"
	"path/filepath"
)

// docsRoot is the repo root a docs verb works in. Every Python script behind these verbs took it from its own file path
// (Path(__file__).resolve().parents[1]), so a run from any directory inside the repo, however reached, used the same root.
// From the physical working directory upward, the nearest ancestor (the directory itself included) holding anchorRel
// as a regular file is the root; a symlink to a regular file counts, a directory of that name does not. With none on the
// way up the physical working directory is returned and the verb refuses its missing input.
func docsRoot(anchorRel string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if physical, err := filepath.EvalSymlinks(cwd); err == nil {
		cwd = physical
	}
	for d := cwd; ; d = filepath.Dir(d) {
		if info, err := os.Stat(filepath.Join(d, filepath.FromSlash(anchorRel))); err == nil && info.Mode().IsRegular() {
			return d, nil
		}
		if filepath.Dir(d) == d {
			return cwd, nil
		}
	}
}
