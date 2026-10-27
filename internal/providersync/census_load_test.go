package providersync

import (
	"fmt"
	"strings"
	"sync"

	"golang.org/x/tools/go/packages"
)

// The census tests read the typed syntax of the module's own packages. Each used to load them again; they share one load
// per test binary. The mode is the union of what they asked for. Every caller only reads the trees.
var census struct {
	once sync.Once
	pkgs []*packages.Package
	err  error
}

func censusLoad(patterns ...string) ([]*packages.Package, error) {
	census.once.Do(func() {
		census.pkgs, census.err = packages.Load(&packages.Config{
			Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
			Dir:  "../..",
		}, logSitePackages...)
	})
	if census.err != nil {
		return nil, census.err
	}
	var out []*packages.Package
	for _, pattern := range patterns {
		found := false
		for _, pkg := range census.pkgs {
			if strings.HasSuffix(pkg.PkgPath, strings.TrimPrefix(pattern, ".")) {
				out = append(out, pkg)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("census load: %s is not among the shared packages %v", pattern, logSitePackages)
		}
	}
	return out, nil
}
