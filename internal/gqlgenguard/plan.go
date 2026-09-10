// Package gqlgenguard runs the gqlgen code generator inside a private copy of
// the module and copies back only the outputs the generator's own
// configuration says it may write.
//
// The safety property is structural, not asserted. The generator never runs in
// the working tree, so no configuration -- an absolute `resolver.dir`, a
// `filename_template` carrying `../`, a knob nobody enumerated -- can make it
// write there. Copy-back happens through an *os.Root opened on the module root,
// so a path that resolves outside the module is refused by the operating
// system's own resolution rather than by a check this package could forget.
//
// The output surface itself is read from gqlgen's own `codegen/config`
// package, never re-derived: the layout defaults, the `abs()` resolution of
// every filename, and the "which knobs are live under which layout" rules are
// gqlgen's, executed by gqlgen's code, so they cannot drift from what the
// generator actually does.
package gqlgenguard

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/99designs/gqlgen/codegen/config"
	"github.com/vektah/gqlparser/v2/validator"
	"gopkg.in/yaml.v3"
)

// Declaree names the configuration section an output path came from. It is
// reported on refusal so the operator can see which knob produced a path.
type Declaree string

const (
	DeclareeExec       Declaree = "exec"
	DeclareeModel      Declaree = "model"
	DeclareeResolver   Declaree = "resolver"
	DeclareeFederation Declaree = "federation"
)

// Output is one path the generator may write, expressed relative to the module
// root with forward slashes.
type Output struct {
	Path     string
	Declaree Declaree
}

// Plan is the complete enumerated output surface of one gqlgen configuration.
//
// Outputs is a SUPERSET of what a given run writes: gqlgen only emits a
// per-schema resolver file for a schema that actually declares resolvers, and
// only emits the "common!" exec file when a referenced type has no source
// position. Treating the plan as an upper bound is deliberate -- an output that
// appears is inside the plan, and anything outside the plan is a refusal.
type Plan struct {
	// ConfigPath is the module-relative path of the gqlgen config file.
	ConfigPath string
	// ConfigDir is the module-relative directory the generator runs in.
	ConfigDir string
	// Schemas lists the schema files the config globbed to, module-relative.
	Schemas []string
	// Outputs is sorted by Path and contains no duplicates.
	Outputs []Output
}

// PathSet returns the plan's output paths as a set for membership tests.
func (p *Plan) PathSet() map[string]Declaree {
	set := make(map[string]Declaree, len(p.Outputs))
	for _, o := range p.Outputs {
		set[o.Path] = o.Declaree
	}
	return set
}

// chdirMu serialises the working-directory change EnumerateOutputs needs.
//
// gqlgen resolves every relative path in a config -- the schema globs and each
// output filename -- against the process working directory, in its own
// `abs()`/`filepath.Glob` calls. Re-implementing that resolution is exactly the
// class of mistake this package exists to remove, so the working directory is
// moved instead and restored on the way out. Callers may therefore not
// enumerate concurrently; the mutex makes that a wait rather than a race.
var chdirMu sync.Mutex

// EnumerateOutputs loads the gqlgen config at moduleDir/configPath and returns
// every path the generator may write.
//
// configPath is module-relative. Every returned path is module-relative and
// slash-separated. An output that resolves outside moduleDir, or two different
// configuration sections that resolve to the same path, are refusals rather
// than results.
func EnumerateOutputs(moduleDir, configPath string) (*Plan, error) {
	if filepath.IsAbs(configPath) {
		return nil, fmt.Errorf("gqlgen config path must be module-relative, got %q", configPath)
	}
	moduleAbs, err := filepath.Abs(moduleDir)
	if err != nil {
		return nil, fmt.Errorf("resolve module dir %q: %w", moduleDir, err)
	}
	moduleAbs, err = filepath.EvalSymlinks(moduleAbs)
	if err != nil {
		return nil, fmt.Errorf("resolve module dir %q: %w", moduleDir, err)
	}

	configDirRel := filepath.Dir(filepath.FromSlash(configPath))
	configDirAbs := filepath.Join(moduleAbs, configDirRel)

	var (
		cfg        *config.Config
		absOutputs []absOutput
	)
	// Everything that resolves a path must happen with the working directory
	// set to the config's own directory, because gqlgen's config.abs() -- which
	// each Check() below calls on every filename -- resolves against the
	// PROCESS working directory. Doing the load inside the chdir and the checks
	// outside it silently produces paths rooted at wherever the guard was
	// started, which is the working tree rather than the private copy.
	err = withWorkingDir(configDirAbs, func() error {
		// gqlgen's schema globbing SWALLOWS every error on the way to a file:
		// filepath.Glob returns "no match" for a directory it cannot traverse,
		// and its `**` walk does not descend a link. In the private copy a link
		// that leaves the module is an inert self-loop (see CopyTree), so a
		// schema pattern that passes THROUGH one would be dropped by gqlgen
		// without a word and the generation would silently lose its types. The
		// pattern's own directory is therefore resolved first, by the operating
		// system, before gqlgen globs it.
		patterns, err := rawSchemaPatterns(filepath.Base(configPath))
		if err != nil {
			return fmt.Errorf("load gqlgen config %q: %w", configPath, err)
		}
		if err := refuseUnresolvableSchemaDirs(patterns); err != nil {
			return err
		}

		cfg, err = config.LoadConfig(filepath.Base(configPath))
		if err != nil {
			return fmt.Errorf("load gqlgen config %q: %w", configPath, err)
		}
		// gqlgen also accepts a configuration whose schema patterns match no
		// file at all, and generates an API from the built-in prelude alone --
		// which `generate` would then copy over every checked-in output. That is
		// never a meaningful generation.
		if len(cfg.SchemaFilename) == 0 {
			if len(patterns) == 0 {
				return fmt.Errorf(
					"refusing: %q names no schema, so gqlgen would generate an empty API over the checked-in outputs",
					configPath)
			}
			quoted := make([]string, len(patterns))
			for i, p := range patterns {
				quoted[i] = fmt.Sprintf("%q", p)
			}
			return fmt.Errorf(
				"refusing: the schema patterns in %q (%s) match no file, so gqlgen would generate an empty API over the checked-in outputs",
				configPath, strings.Join(quoted, ", "))
		}

		// gqlgen's own Check() methods apply the layout defaults and rewrite
		// every filename to an absolute path. Calling them is what makes this
		// enumeration gqlgen's answer rather than ours. They are called
		// individually (not through Config.Init) because Init also type-checks
		// the whole package graph, which needs a buildable module and is not
		// needed to learn where the generator writes.
		if err := cfg.Exec.Check(); err != nil {
			return fmt.Errorf("config.exec: %w", err)
		}
		if cfg.Model.IsDefined() {
			if err := cfg.Model.Check(); err != nil {
				return fmt.Errorf("config.model: %w", err)
			}
		}
		if cfg.Resolver.IsDefined() {
			if err := cfg.Resolver.Check(); err != nil {
				return fmt.Errorf("config.resolver: %w", err)
			}
		}
		if cfg.Federation.IsDefined() {
			if err := cfg.Federation.Check(); err != nil {
				return fmt.Errorf("config.federation: %w", err)
			}
		}

		absOutputs, err = absoluteOutputs(cfg, configDirAbs)
		return err
	})
	if err != nil {
		return nil, err
	}

	plan := &Plan{
		ConfigPath: filepath.ToSlash(filepath.Clean(filepath.FromSlash(configPath))),
		ConfigDir:  filepath.ToSlash(configDirRel),
	}
	if plan.ConfigDir == "" {
		plan.ConfigDir = "."
	}

	for _, s := range cfg.SchemaFilename {
		rel, err := moduleRelative(moduleAbs, absFrom(configDirAbs, s))
		if err != nil {
			return nil, fmt.Errorf("schema %q: %w", s, err)
		}
		plan.Schemas = append(plan.Schemas, rel)
	}
	sort.Strings(plan.Schemas)

	owner := map[string]Declaree{}
	for _, o := range absOutputs {
		rel, err := moduleRelative(moduleAbs, o.abs)
		if err != nil {
			return nil, fmt.Errorf("%s output %q: %w", o.declaree, o.abs, err)
		}
		if prev, seen := owner[rel]; seen {
			if prev != o.declaree {
				return nil, fmt.Errorf(
					"gqlgen config collision: %s and %s both resolve to %q; one would silently overwrite the other",
					prev, o.declaree, rel)
			}
			continue
		}
		owner[rel] = o.declaree
		plan.Outputs = append(plan.Outputs, Output{Path: rel, Declaree: o.declaree})
	}
	sort.Slice(plan.Outputs, func(i, j int) bool { return plan.Outputs[i].Path < plan.Outputs[j].Path })

	return plan, nil
}

// rawSchemaPatterns returns the `schema` entries of the config at file exactly
// as written, before gqlgen globs them. It decodes into gqlgen's own Config type
// with gqlgen's own decoder settings (config.ReadConfig: yaml.v3, KnownFields),
// so the list -- including the "schema.graphql" default when the key is absent
// -- is gqlgen's, not a re-parse with different rules.
func rawSchemaPatterns(file string) ([]string, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("unable to read config: %w", err)
	}
	raw := config.DefaultConfig()
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(raw); err != nil {
		return nil, fmt.Errorf("unable to parse config: %w", err)
	}
	return raw.SchemaFilename, nil
}

// globMeta are the characters filepath.Glob treats as pattern syntax on the
// platforms this guard runs on. A component containing one is matched against
// directory entries rather than resolved by name.
const globMeta = `*?[\\`

// refuseUnresolvableSchemaDirs walks every directory each schema pattern would
// traverse -- exactly the traversal filepath.Glob (and gqlgen's `**` walk
// root) performs, but with its errors KEPT instead of swallowed -- and refuses
// a pattern whose traversal hits anything other than simple absence.
//
// It decides no matches; gqlgen still does that. It only makes loud what
// gqlgen's globbing would make silent: a directory it cannot enter. In the
// private copy that is, above all, the inert self-loop CopyTree leaves where a
// link out of the module was, and a schema behind one would otherwise vanish
// from the generation without a word. Absence stays gqlgen's business -- a
// directory that does not exist matches nothing, and a configuration that
// matches nothing overall is refused separately. A LITERAL directory component
// that exists but is not a directory is refused too: it can match nothing, and
// gqlgen would not say so.
func refuseUnresolvableSchemaDirs(patterns []string) error {
	for _, p := range patterns {
		if err := schemaTraversalErr(p); err != nil {
			return fmt.Errorf(
				"refusing: schema pattern %q cannot be resolved: %w; gqlgen would skip it silently. A link out of the module is never followed -- move the schema inside the module",
				p, err)
		}
	}
	return nil
}

func schemaTraversalErr(pattern string) error {
	comps := strings.Split(filepath.ToSlash(pattern), "/")
	start := "."
	if comps[0] == "" && len(comps) > 1 {
		start, comps = "/", comps[1:]
	}
	dirs := []string{start}
	// Every component but the last names a directory to pass through. The
	// last is matched or Lstat'd by Glob itself, and a dropped link THERE is an
	// inert self-loop that fails gqlgen's own read loudly.
	for _, c := range comps[:len(comps)-1] {
		var next []string
		for _, d := range dirs {
			if !strings.ContainsAny(c, globMeta) {
				p := filepath.Join(d, c)
				info, err := os.Stat(p)
				switch {
				case err != nil && errors.Is(err, fs.ErrNotExist):
				case err != nil:
					return fmt.Errorf("directory %q: %w", filepath.ToSlash(p), err)
				case !info.IsDir():
					return fmt.Errorf("%q is not a directory", filepath.ToSlash(p))
				default:
					next = append(next, p)
				}
				continue
			}
			entries, err := os.ReadDir(d)
			if err != nil {
				return fmt.Errorf("directory %q: %w", filepath.ToSlash(d), err)
			}
			for _, e := range entries {
				ok, err := filepath.Match(c, e.Name())
				if err != nil {
					return fmt.Errorf("pattern component %q: %w", c, err)
				}
				if !ok {
					continue
				}
				p := filepath.Join(d, e.Name())
				info, err := os.Stat(p)
				switch {
				case err != nil && errors.Is(err, fs.ErrNotExist):
				case err != nil:
					return fmt.Errorf("directory %q: %w", filepath.ToSlash(p), err)
				case info.IsDir():
					next = append(next, p)
				}
			}
		}
		dirs = next
	}
	return nil
}

// withWorkingDir runs fn with the process working directory set to dir and
// restores it afterwards, holding chdirMu for the whole of it.
func withWorkingDir(dir string, fn func() error) (err error) {
	chdirMu.Lock()
	defer chdirMu.Unlock()

	prev, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("read working directory: %w", err)
	}
	if err := os.Chdir(dir); err != nil {
		return fmt.Errorf("enter gqlgen config dir %q: %w", dir, err)
	}
	defer func() {
		if cerr := os.Chdir(prev); cerr != nil && err == nil {
			err = fmt.Errorf("restore working directory %q: %w", prev, cerr)
		}
	}()

	return fn()
}

type absOutput struct {
	abs      string
	declaree Declaree
}

// absoluteOutputs mirrors, one for one, the file names gqlgen's generators
// render. Each branch cites the gqlgen source it follows so a version bump that
// changes a naming rule can be checked against this list.
func absoluteOutputs(cfg *config.Config, configDirAbs string) ([]absOutput, error) {
	var out []absOutput

	// codegen/generate.go GenerateCode.
	switch cfg.Exec.Layout {
	case config.ExecLayoutSingleFile:
		out = append(out, absOutput{absFrom(configDirAbs, cfg.Exec.Filename), DeclareeExec})
	case config.ExecLayoutFollowSchema:
		dir := absFrom(configDirAbs, cfg.Exec.DirName)
		// codegen/generate.go generateRootFile: a fixed name, never templated.
		out = append(out, absOutput{filepath.Join(dir, "root_.generated.go"), DeclareeExec})
		// codegen/generate.go filename() also uses the literal "common!" for a
		// referenced type that has no source position.
		for _, name := range append(schemaBaseNames(cfg), "common!") {
			out = append(out, absOutput{filepath.Join(dir, execFilename(cfg, name)), DeclareeExec})
		}
	default:
		return nil, fmt.Errorf("config.exec: unrecognised layout %q", cfg.Exec.Layout)
	}

	// plugin/modelgen/models.go: a single file, always.
	if cfg.Model.IsDefined() {
		out = append(out, absOutput{absFrom(configDirAbs, cfg.Model.Filename), DeclareeModel})
	}

	// plugin/federation/federation.go: the declared file plus the fixed
	// federation.requires.go beside it.
	if cfg.Federation.IsDefined() {
		fed := absFrom(configDirAbs, cfg.Federation.Filename)
		out = append(out, absOutput{fed, DeclareeFederation})
		out = append(out, absOutput{filepath.Join(filepath.Dir(fed), "federation.requires.go"), DeclareeFederation})
	}

	// plugin/resolvergen/resolver.go GenerateCode.
	if cfg.Resolver.IsDefined() {
		switch cfg.Resolver.Layout {
		case config.LayoutSingleFile:
			out = append(out, absOutput{absFrom(configDirAbs, cfg.Resolver.Filename), DeclareeResolver})
		case config.LayoutFollowSchema:
			dir := absFrom(configDirAbs, cfg.Resolver.Dir())
			// The dependency-injection stub, written only when absent.
			out = append(out, absOutput{absFrom(configDirAbs, cfg.Resolver.Filename), DeclareeResolver})
			for _, name := range schemaBaseNames(cfg) {
				out = append(out, absOutput{
					resolverFilename(dir, name, cfg.Resolver.FilenameTemplate),
					DeclareeResolver,
				})
			}
		default:
			return nil, fmt.Errorf("config.resolver: unrecognised layout %q", cfg.Resolver.Layout)
		}
	}

	return out, nil
}

// schemaBaseNames is the set of {name} substitutions a per-schema generator
// can produce: the extension-stripped base name of every schema source.
//
// The set includes gqlparser's built-in PRELUDE, because gqlparser.LoadSchema
// prepends validator.Prelude to the configured sources before gqlgen ever sees
// them -- so a follow-schema layout emits a prelude file that appears in no
// config key. The name is read from gqlparser's own value rather than written
// out here, so a version that renames it moves this with it.
func schemaBaseNames(cfg *config.Config) []string {
	seen := map[string]bool{}
	var names []string
	add := func(src string) {
		base := filepath.Base(src)
		name := strings.TrimSuffix(base, filepath.Ext(base))
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		names = append(names, name)
	}
	add(validator.Prelude.Name)
	for _, src := range cfg.SchemaFilename {
		add(src)
	}
	sort.Strings(names)
	return names
}

// execFilename follows codegen/generate.go filename(), including its default
// template. The template may carry any extension, or none, and may contain
// path separators -- all of which the caller resolves and bounds.
func execFilename(cfg *config.Config, name string) string {
	tmpl := cfg.Exec.FilenameTemplate
	if tmpl == "" {
		tmpl = "{name}.generated.go"
	}
	return strings.ReplaceAll(tmpl, "{name}", name)
}

// resolverFilename follows plugin/resolvergen/resolver.go gqlToResolverName,
// with the extension already stripped by schemaBaseNames.
func resolverFilename(base, name, tmpl string) string {
	if tmpl == "" {
		tmpl = "{name}.resolvers.go"
	}
	return filepath.Join(base, strings.ReplaceAll(tmpl, "{name}", name))
}

// absFrom resolves p the way gqlgen's config.abs does: relative paths against
// the directory the config was loaded from, absolute paths unchanged. An
// absolute path is not rejected here -- it is carried through so
// moduleRelative can refuse it by name, which reads better than a parse error.
func absFrom(dir, p string) string {
	if p == "" {
		return ""
	}
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	return filepath.Join(dir, p)
}

// moduleRelative converts an absolute path to a module-relative, slash
// separated one, refusing anything that leaves the module root.
func moduleRelative(moduleAbs, p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("empty path")
	}
	rel, err := filepath.Rel(moduleAbs, p)
	if err != nil {
		return "", fmt.Errorf("path %q is not inside the module root %q", p, moduleAbs)
	}
	rel = filepath.Clean(rel)
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q resolves outside the module root %q", p, moduleAbs)
	}
	if rel == "." {
		return "", fmt.Errorf("path %q is the module root itself, not a file", p)
	}
	return filepath.ToSlash(rel), nil
}
