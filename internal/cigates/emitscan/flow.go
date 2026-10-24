package emitscan

import "go/ast"

// The label flow reads one package at a time, by name, with no types: a function whose parameter reaches a metric
// attribute sink (metric.WithAttributes, metric.WithAttributeSet, attribute.NewSet), directly, through local names, or
// through a call to another such function, marks that parameter; every same-package call site then marks the attribute
// constructors that reach the argument (a literal, a name bound to constructors anywhere in the package, an append).
// Every step is charged to the package budget.

type fcall struct {
	callee string
	args   [][]string // names mentioned by each argument
}

type funcFlow struct {
	name   string
	slots  []slot
	srcOf  map[string][]string // a name bound in the function -> the names its right sides mention
	sinks  []string
	calls  []fcall
	callee map[string]bool
}

type slot struct {
	name     string
	index    int
	variadic bool
}

// carry walks the parts of an expression that can hold attribute constructors without computing a new value: a name, an
// attribute constructor call, a composite literal element, an append argument, a slice, an index, a parenthesis, a
// dereference or an address. Any other call or selector yields a new value and is not entered (a named limit).
func (info *pkgInfo) carry(expr ast.Expr, scan *fileScan, name func(string), ctor func(*ast.CallExpr)) {
	if expr == nil || !info.budget.charge(1) {
		return
	}
	switch e := expr.(type) {
	case *ast.Ident:
		if e.Name != "_" {
			name(e.Name)
		}
	case *ast.ParenExpr:
		info.carry(e.X, scan, name, ctor)
	case *ast.StarExpr:
		info.carry(e.X, scan, name, ctor)
	case *ast.UnaryExpr:
		info.carry(e.X, scan, name, ctor)
	case *ast.SliceExpr:
		info.carry(e.X, scan, name, ctor)
	case *ast.IndexExpr:
		info.carry(e.X, scan, name, ctor)
	case *ast.CompositeLit:
		for _, element := range e.Elts {
			if kv, ok := element.(*ast.KeyValueExpr); ok {
				element = kv.Value
			}
			info.carry(element, scan, name, ctor)
		}
	case *ast.CallExpr:
		if _, _, ok := scan.attrParts(e); ok {
			ctor(e)
		} else if fun, ok := e.Fun.(*ast.Ident); ok && fun.Name == "append" {
			for _, arg := range e.Args {
				info.carry(arg, scan, name, ctor)
			}
		}
	}
}

func (info *pkgInfo) carriedNames(expr ast.Expr, scan *fileScan) []string {
	var out []string
	info.carry(expr, scan, func(n string) { out = append(out, n) }, func(*ast.CallExpr) {})
	return out
}

// calleeName is the package-local function a call names: an identifier, or a selector that is not an import.
func calleeName(call *ast.CallExpr, imports map[string]string) (string, bool) {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name, true
	case *ast.SelectorExpr:
		if x, ok := fun.X.(*ast.Ident); ok {
			if _, imported := imports[x.Name]; imported {
				return "", false
			}
		}
		return fun.Sel.Name, true
	}
	return "", false
}

func (info *pkgInfo) collectLabelFlow(group []parsedFile) {
	var flows []*funcFlow
	byCallee := map[string][]int{}
	for _, parsed := range group {
		scan := &fileScan{pkg: info, imports: parsed.imports}
		for _, decl := range parsed.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || info.budget.over {
				continue
			}
			flow := info.readFlow(fn, scan, parsed.imports)
			for _, s := range flow.slots {
				if s.variadic {
					if info.variadic[flow.name] == nil {
						info.variadic[flow.name] = map[int]bool{}
					}
					info.variadic[flow.name][s.index] = true
				}
			}
			for callee := range flow.callee {
				byCallee[callee] = append(byCallee[callee], len(flows))
			}
			flows = append(flows, flow)
		}
	}
	queue := make([]int, 0, len(flows))
	queued := map[int]bool{}
	for index := range flows {
		queue = append(queue, index)
		queued[index] = true
	}
	for len(queue) > 0 && info.budget.charge(1) {
		index := queue[0]
		queue = queue[1:]
		queued[index] = false
		flow := flows[index]
		for _, newSlot := range info.labelSlots(flow) {
			if info.labelParams[flow.name] == nil {
				info.labelParams[flow.name] = map[int]bool{}
			}
			if info.labelParams[flow.name][newSlot] {
				continue
			}
			if !info.budget.charge(aliasEntryCost) {
				return
			}
			info.labelParams[flow.name][newSlot] = true
			for _, caller := range byCallee[flow.name] {
				if !queued[caller] {
					queued[caller] = true
					queue = append(queue, caller)
				}
			}
		}
	}
	if len(info.labelParams) == 0 || info.budget.over {
		return
	}
	info.markOrigins(group)
}

// readFlow reads one function once: the names its bindings link, the names that reach a sink, and its calls.
func (info *pkgInfo) readFlow(fn *ast.FuncDecl, scan *fileScan, imports map[string]string) *funcFlow {
	flow := &funcFlow{name: fn.Name.Name, srcOf: map[string][]string{}, callee: map[string]bool{}}
	position := 0
	for _, field := range fn.Type.Params.List {
		_, variadic := field.Type.(*ast.Ellipsis)
		if len(field.Names) == 0 {
			position++
			continue
		}
		for _, name := range field.Names {
			flow.slots = append(flow.slots, slot{name.Name, position, variadic})
			position++
		}
	}
	bind := func(lhs ast.Expr, rhs []ast.Expr) {
		ident, ok := lhs.(*ast.Ident)
		if !ok || ident.Name == "_" {
			return
		}
		for _, expr := range rhs {
			flow.srcOf[ident.Name] = append(flow.srcOf[ident.Name], info.carriedNames(expr, scan)...)
		}
	}
	inspectCharged(info.budget, fn.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				if len(n.Lhs) == len(n.Rhs) {
					bind(lhs, []ast.Expr{n.Rhs[i]})
				} else {
					bind(lhs, n.Rhs)
				}
			}
		case *ast.ValueSpec:
			for i, name := range n.Names {
				if len(n.Names) == len(n.Values) {
					bind(name, []ast.Expr{n.Values[i]})
				} else {
					bind(name, n.Values)
				}
			}
		case *ast.RangeStmt:
			if n.Key != nil {
				bind(n.Key, []ast.Expr{n.X})
			}
			if n.Value != nil {
				bind(n.Value, []ast.Expr{n.X})
			}
		case *ast.CallExpr:
			if isLabelSink(scan.targets(n.Fun)) {
				for _, arg := range n.Args {
					flow.sinks = append(flow.sinks, info.carriedNames(arg, scan)...)
				}
			}
			if callee, ok := calleeName(n, imports); ok {
				call := fcall{callee: callee}
				for _, arg := range n.Args {
					call.args = append(call.args, info.carriedNames(arg, scan))
				}
				flow.calls = append(flow.calls, call)
				flow.callee[callee] = true
			}
		}
		return true
	})
	return flow
}

func isLabelSink(targets [][2]string) bool {
	for _, target := range targets {
		if (target[0] == "metric" && (target[1] == "WithAttributes" || target[1] == "WithAttributeSet")) ||
			(target[0] == "attribute" && target[1] == "NewSet") {
			return true
		}
	}
	return false
}

// labelSlots returns the parameter slots of a function that reach a sink: the names that reach a sink directly or
// through a call to a function with a marked slot, then every name those names were bound from.
func (info *pkgInfo) labelSlots(flow *funcFlow) []int {
	reach := map[string]bool{}
	var queue []string
	add := func(name string) {
		if !reach[name] {
			reach[name] = true
			queue = append(queue, name)
		}
	}
	for _, name := range flow.sinks {
		add(name)
	}
	for _, call := range flow.calls {
		marked := info.labelParams[call.callee]
		if !info.budget.charge(1 + len(marked)*len(call.args)) {
			return nil
		}
		for slotIndex := range marked {
			for argIndex, names := range call.args {
				if argIndex == slotIndex || (argIndex > slotIndex && info.variadic[call.callee][slotIndex]) {
					for _, name := range names {
						add(name)
					}
				}
			}
		}
	}
	for len(queue) > 0 && info.budget.charge(1) {
		name := queue[0]
		queue = queue[1:]
		for _, src := range flow.srcOf[name] {
			add(src)
		}
	}
	var out []int
	for _, s := range flow.slots {
		if reach[s.name] {
			out = append(out, s.index)
		}
	}
	return out
}

// markOrigins marks, at every same-package call to a function with a marked parameter, the attribute constructors that
// reach the argument: those written in it, and those a name in it was bound to anywhere in the package.
func (info *pkgInfo) markOrigins(group []parsedFile) {
	type bindEdge struct {
		name  string
		ctors []*ast.CallExpr
		srcs  []string
	}
	var edges []bindEdge
	for _, parsed := range group {
		scan := &fileScan{pkg: info, imports: parsed.imports}
		bind := func(lhs ast.Expr, rhs ast.Expr) {
			ident, ok := lhs.(*ast.Ident)
			if !ok || ident.Name == "_" || rhs == nil {
				return
			}
			edge := bindEdge{name: ident.Name}
			info.carry(rhs, scan, func(n string) { edge.srcs = append(edge.srcs, n) }, func(c *ast.CallExpr) { edge.ctors = append(edge.ctors, c) })
			edges = append(edges, edge)
		}
		inspectCharged(info.budget, parsed.file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if len(n.Lhs) == len(n.Rhs) {
						bind(lhs, n.Rhs[i])
					} else if len(n.Rhs) > 0 {
						bind(lhs, n.Rhs[0])
					}
				}
			case *ast.ValueSpec:
				for i, name := range n.Names {
					if i < len(n.Values) {
						bind(name, n.Values[i])
					}
				}
			case *ast.RangeStmt:
				if n.Key != nil {
					bind(n.Key, n.X)
				}
				if n.Value != nil {
					bind(n.Value, n.X)
				}
			}
			return true
		})
		if info.budget.over {
			return
		}
	}
	origins := map[string]map[*ast.CallExpr]bool{}
	dependents := map[string][]int{}
	var queue []string
	queued := map[string]bool{}
	push := func(name string) {
		if !queued[name] {
			queued[name] = true
			queue = append(queue, name)
		}
	}
	add := func(name string, call *ast.CallExpr) bool {
		if origins[name] == nil {
			origins[name] = map[*ast.CallExpr]bool{}
		}
		if origins[name][call] {
			return true
		}
		if !info.budget.charge(aliasEntryCost) {
			return false
		}
		origins[name][call] = true
		push(name)
		return true
	}
	for index, edge := range edges {
		for _, src := range edge.srcs {
			dependents[src] = append(dependents[src], index)
		}
		for _, call := range edge.ctors {
			if !add(edge.name, call) {
				return
			}
		}
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		queued[name] = false
		for _, index := range dependents[name] {
			if !info.budget.charge(1 + len(origins[name])) {
				return
			}
			for call := range origins[name] {
				if !add(edges[index].name, call) {
					return
				}
			}
		}
	}
	for _, parsed := range group {
		scan := &fileScan{pkg: info, imports: parsed.imports}
		inspectCharged(info.budget, parsed.file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			callee, ok := calleeName(call, parsed.imports)
			if !ok || len(info.labelParams[callee]) == 0 {
				return true
			}
			for argIndex, arg := range call.Args {
				if info.labelParams[callee][argIndex] || info.reachesVariadic(callee, argIndex) {
					info.carry(arg, scan, func(n string) {
						for origin := range origins[n] {
							if info.budget.charge(1) {
								info.labelMarked[origin] = true
							}
						}
					}, func(c *ast.CallExpr) { info.labelMarked[c] = true })
				}
			}
			return true
		})
	}
}

// reachesVariadic reports an argument past a marked variadic parameter.
func (info *pkgInfo) reachesVariadic(callee string, argIndex int) bool {
	for slotIndex := range info.variadic[callee] {
		if argIndex >= slotIndex && info.labelParams[callee][slotIndex] {
			return true
		}
	}
	return false
}
