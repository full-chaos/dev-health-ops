package scripts_test

// shimBasePath is basePath with the python shims first: a child that runs with the fixed system PATH must still meet them.
func shimBasePath() string { return "PATH=" + withShims("/usr/bin:/bin:/usr/local/bin") }
