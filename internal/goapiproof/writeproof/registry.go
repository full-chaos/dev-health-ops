package writeproof

import (
	"fmt"
	"sort"
	"sync"
)

var (
	registryMu sync.Mutex
	registered = map[string]Case{}
)

// Register adds a case to the registry the live verb resolves -case against. It
// panics on an invalid or duplicate case, at init time, so a broken case fails
// every build that links it instead of the one run that names it.
//
// No case ships registered by this package: the first real ones belong to the
// mutation ports (CHAOS-6098), which register theirs from their own package with
// their own Seeder, Tables and committed baseline digest.
func Register(c Case) {
	if err := c.Validate(); err != nil {
		panic(err)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registered[c.Name]; dup {
		panic(fmt.Sprintf("writeproof: case %q registered twice", c.Name))
	}
	registered[c.Name] = c
}

// Lookup returns a registered case by name.
func Lookup(name string) (Case, bool) {
	registryMu.Lock()
	defer registryMu.Unlock()
	c, ok := registered[name]
	return c, ok
}

// Names lists the registered case names, sorted.
func Names() []string {
	registryMu.Lock()
	defer registryMu.Unlock()
	names := make([]string, 0, len(registered))
	for name := range registered {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
