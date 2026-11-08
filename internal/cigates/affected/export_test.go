package affected

// TagHooks exposes tagHooks to the cells of affected_test: the tagged test files, the hook-superset directories, whether
// a non-test file names the tag, and the registering imports the superset uses.
func TagHooks(root, tag string) (tagged []string, hooks map[string]bool, nonTest bool, imports map[string]bool, err error) {
	th, err := tagHooks(root, tag)
	return th.tagged, th.hooks, th.nonTest, hookImports, err
}
