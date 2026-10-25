package docstools

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// check-validated-site: the built site in --site-dir was built and fully checked for --revision. Docs Guards writes the stamp
// file after every site check passed; the preview job runs this verb before it reuses the site, so a site of another revision
// is refused and never reused.

func init() { register("check-validated-site", validatedSiteVerb) }

func validatedSiteVerb(args []string, stdout, stderr io.Writer) int {
	flags, code := dcParseFlags("check-validated-site", args, []dcFlag{{name: "site-dir", required: true}, {name: "stamp", required: true}, {name: "revision", required: true}}, stderr)
	if flags == nil {
		return code
	}
	if err := checkValidatedSite(flags["site-dir"], flags["stamp"], flags["revision"]); err != nil {
		fmt.Fprintf(stderr, "ERROR: %s\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "Validated site %s is for revision %s\n", flags["site-dir"], flags["revision"])
	return 0
}

func checkValidatedSite(siteDir, stamp, revision string) error {
	if revision == "" || revision == "unknown" {
		return fmt.Errorf("no source revision to compare the validated site with")
	}
	info, err := os.Stat(siteDir + "/index.html")
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("no built site at %s", siteDir)
	}
	raw, err := os.ReadFile(stamp)
	if err != nil {
		return fmt.Errorf("no validated-site stamp %s: %v", stamp, err)
	}
	if got := strings.TrimSpace(string(raw)); got != revision {
		return fmt.Errorf("the validated site is for revision %q, not %q", got, revision)
	}
	return nil
}
