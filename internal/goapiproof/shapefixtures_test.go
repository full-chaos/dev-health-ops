package goapiproof

// Options values the shape tests (baselinecopysum, teamcoverageidentity, numericleaves, teamscope) run against the captured baselines.
// They are test fixtures only: no endpoint spec in restcorpus.go uses them.

var investmentFlowRepoDedupParity = Options{
	OrderInsensitiveLists: []OrderInsensitiveList{
		{
			Path:      "data.nodes",
			KeyFields: []string{"name"},
			Reason:    "investmentflow/builders.go's own nodePresence/nodeRunningTotal both build nodes in first-touch order over a row set neither plane's own SQL fully orders, the same tie-break class already declared for GET/POST /api/v1/sankey's own data.nodes (sankeyRepoDedupParity). name is unique per node on each plane's own result set.",
			Ticket:    "CHAOS-5873",
		},
		{
			Path:      "data.links",
			KeyFields: []string{"source", "target"},
			Reason:    "the SAME first-touch insertion order as data.nodes, over the SAME under-ordered row set, the same tie-break class already declared for GET/POST /api/v1/sankey's own data.links (sankeyRepoDedupParity). (source, target) is unique per edge on each plane's own result set.",
			Ticket:    "CHAOS-5873",
		},
	},
	BaselineDefects: []BaselineDefect{
		{
			Ticket:             "CHAOS-5803",
			Reason:             "repos is ReplacingMergeTree(last_synced) (000_raw_tables.sql); api/queries/investment.py's readers join it with no FINAL or org_id scoping at all, where this port (internal/investmentflow) reads it FINAL, org_id filtered inside the JOIN's own ON clause. An unmerged physical version of a repo row fans out a clean, uniform integer multiplier across the whole repo-rooted subtree that repo's own edges reach -- the same mechanism already declared for GET/POST /api/v1/sankey's own repos join (sankeyRepoDedupParity), extended here to also admit the anchor's own NODE value: unlike plain sankey, this route's own node builders (nodePresence/nodeRunningTotal) populate a real running total per node, so a genuine instance moves an anchor's node value by the SAME k as its incident edges, not only the edges themselves. Go is correct.",
			Paths:              []string{"data.nodes", "data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while repos holds an unmerged physical version inside the requested window; a comparison taken after the next background merge shows no divergence",
			SankeyRepoFanoutShape: &SankeyRepoFanoutShape{
				NodesListPath:       "data.nodes",
				LinksListPath:       "data.links",
				LinkValuePath:       "data.links.value",
				NodeValuePath:       "data.nodes.value",
				RepoNodeGroups:      []string{"repo"},
				FallbackAnchorNames: []string{"Unassigned repo", "Other repos", "unassigned"},
			},
		},
		{
			Ticket:             "CHAOS-4547",
			Reason:             "work_unit_investments.repo_id/work_unit_type/work_unit_name/provider are Nullable (017_investment_materialize_tables.sql, 019_work_unit_investment_labels.sql); api/queries/investment.py's own LATEST_WORK_UNIT_INVESTMENTS_CTE (the same definition api/queries/sankey.py imports by name for its own investment mode, already declared under this ticket as sankeyInvestmentParity) dedups them via a bare argMax(col, computed_at), which SKIPS a row whose column is NULL when picking the newest version, returning a STALE non-null value from an earlier generation instead of the true latest one. This port reuses analytics.LatestWorkUnitInvestmentsSource(), which tuple-wraps each of those columns -- (argMax(tuple(col), computed_at)).1 -- and therefore reads the true latest value. A work unit whose newest generation cleared one of those columns relative to an earlier one moves its effort from one edge to another -- a redistribution, not an addition, so the response's own total link value is CONSERVED even though no single edge's sign is predictable, the same mechanism already declared for sankeyInvestmentParity's own data.links. Go is correct. Paths names data.links alone: a node's own value is a SUM/max over several edges (finishPresenceEdges/nodeRunningTotal), so this redistribution can move an individual node's total in either direction with no whole-node conservation to check -- ConservationShape's own whole-comparison rule only proves the LIST total is conserved, not any one node's share of it, and that gap is knowingly left uncovered (see the blind-spot note below) rather than guessed at. BLIND SPOT: a node-value-only instance of this exact mechanism -- one whose links happen to stay within FloatTierB tolerance while the node total it feeds visibly moves -- is never covered by this entry; it would surface as an ordinary, uncovered data.nodes.value finding, which is correct and intended, not a gap in this citation's own scope.",
			Paths:              []string{"data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while at least one work unit in the requested window has a newer generation whose repo_id/work_unit_type/work_unit_name/provider differs (including a NULL transition) from an earlier generation's; a request whose work units never re-categorize shows no divergence",
			ConservationShape: &ConservationShape{
				ListPath:   "data.links",
				ValueField: "value",
				ValuePath:  "data.links.value",
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "every fetcher in this port composes analytics.LatestWorkUnitInvestmentsSource(), which appends `AND work_unit_id NOT IN (SELECT superseded_work_unit_id FROM work_unit_supersessions WHERE org_id = {org_id:String})` unconditionally -- a Go-only exclusion of work units a later regrouping run retired, making candidate's own row population a STRICT SUBSET of baseline's. A subtree/edge whose value sums that population can only be pulled UP by the rows Go excludes, never down, so baseline is admitted only when strictly greater than candidate at the same (source, target) key -- direction only, no magnitude bound. Go is correct.",
			Paths:              []string{"data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope; an org with no supersession rows in that window agrees on both planes",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:   "data.links",
				ValueField: "value",
				ValuePath:  "data.links.value",
				KeyFields:  []string{"source", "target"},
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "the same work_unit_supersessions exclusion as this ticket's data.links entry, over data.nodes: a node's own value sums the SAME strictly-subset row population, so it too can only be pulled up by an excluded row's effort, never down. Go is correct.",
			Paths:              []string{"data.nodes"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope; an org with no supersession rows in that window agrees on both planes",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:   "data.nodes",
				ValueField: "value",
				ValuePath:  "data.nodes.value",
				KeyFields:  []string{"name"},
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "the same work_unit_supersessions exclusion, over distinct_team_targets: coverageStats/edgeStats count DISTINCT assigned team names among the rows (builders.go's own teamSeen/seen sets). Removing a row from a population can only remove or preserve a distinct label already seen, never add one Go's own smaller population lacks -- a plain structural fact, not an empirical skew -- so baseline (over the LARGER population) is admitted only when strictly greater than candidate. Go is correct.",
			Paths:              []string{"data.distinct_team_targets"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope, AND that row's own team is not otherwise represented among the org's non-superseded rows; an org with no such row agrees on both planes",
			ScalarDirectionShape: &ScalarDirectionShape{
				Path:                  "data.distinct_team_targets",
				BaselineMustBeGreater: true,
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "the same work_unit_supersessions exclusion and the same distinct-count structural argument as this ticket's data.distinct_team_targets entry, over distinct_repo_targets. Go is correct.",
			Paths:              []string{"data.distinct_repo_targets"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope, AND that row's own repo is not otherwise represented among the org's non-superseded rows; an org with no such row agrees on both planes",
			ScalarDirectionShape: &ScalarDirectionShape{
				Path:                  "data.distinct_repo_targets",
				BaselineMustBeGreater: true,
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "the same work_unit_supersessions exclusion, over unassigned_reasons: fetchInvestmentUnassignedCounts composes the SAME strictly-subset population to count missing_team/missing_repo rows. Counting rows that share a key over a strict SUBSET of a population can only read less than or equal to the same count over the full population -- a structural fact, not an empirical skew -- so baseline is admitted only when strictly greater than candidate at each key. Go is correct.",
			Paths:              []string{"data.unassigned_reasons"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope, AND that row is itself missing a team or repo assignment; an org with no such row agrees on both planes",
			DictKeyDirectionShape: &DictKeyDirectionShape{
				DictPath:  "data.unassigned_reasons",
				ValuePath: "data.unassigned_reasons",
			},
		},
		{
			Ticket:             "CHAOS-5923",
			Reason:             "team_coverage (and its coverage.team_coverage duplicate) is assignedTeamValue/totalValue, summed straight over the fetched rows (coverageStats, investmentflow/builders.go; investment_flow.py's identical sum). A team-group sankey node's own value is finishPresenceEdges' max(incoming, outgoing) with incoming always zero for a team node (no edge ever targets a team label), so it equals that team's own row-value sum -- the SAME sum coverageStats accumulates per team. Summing every team-group node therefore reproduces totalValue on that SAME plane, and summing every one except the unassigned-team bucket (\"Unassigned team\") reproduces assignedTeamValue, so team_coverage is a pure function of that plane's OWN nodes. The sibling repos-join fan-out and supersession-exclusion entries above are what move one or more team-group node values between planes; when every team-group node difference in a comparison is already covered by one of those, the team_coverage difference has no source other than the covered nodes. Go is correct.",
			Paths:              []string{"data.team_coverage", "data.coverage.team_coverage"},
			Intermittent:       true,
			IntermittentReason: "present only under the same conditions the sibling fan-out/supersession-exclusion entries above state for their own node/link differences; a comparison with no covered team-group node difference agrees on both planes",
			TeamCoverageIdentityShape: &TeamCoverageIdentityShape{
				NodesListPath:          "data.nodes",
				TeamGroup:              "team",
				UnassignedTeamNodeName: "Unassigned team",
				CoveragePaths:          []string{"data.team_coverage", "data.coverage.team_coverage"},
			},
		},
	},
}

// POST /api/v1/investment/flow's own admissible scenarios
// reach THREE DIFFERENT numeric-leaf shapes depending on flow_mode --
// dynamic_default_org's own dynamic branch (buildDynamicModeResponse)
// leaves data.coverage/data.unassigned_reasons/data.top_n_repos nil
// (investmentflow.go's own comment: "Coverage/UnassignedReasons/
// FlowMode/DrillCategory/TopNRepos stay nil -- ... only the flow_mode
// branch does [set them]"), where the other three scenarios' shared
// flow_mode branch sets every one of them -- and POST /investment/flow/
// repo-team's own BuildRepoTeamFlowResponse sets NEITHER, only Nodes/
// Links/Mode/Unit/Label/Description/ChosenMode. Since UnusedTierB/
// IntegerLeaves enforcement is per COMPARISON (an entry unreached even
// once in a request using that Options reads as stale, no intermittent
// exemption the way BaselineDefects has one), one shared Options
// declaring all three routes' numeric leaves would refuse the two
// narrower routes' own runs the moment they don't reach a leaf the wider
// route's declaration names. Three Options values, one per reachable
// shape, all three still composing investmentFlowRepoDedupParity's own
// BaselineDefects/OrderInsensitiveLists BY VALUE (those tolerate the
// same variation already, via Intermittent).
//
// investmentFlowDynamicParity: dynamic_default_org. data.links.value/
// data.nodes.value are investmentflow/queries.go's own sum(subcategory_kv.2
// * effort_value) (all five fetchers share this shape) -- genuine merged
// ClickHouse float aggregate. team_coverage/repo_coverage are
// assignedValue/totalValue (investmentflow/builders.go edgeStats), a
// ratio of two sums of that SAME float aggregate -- float-valued for a
// less obvious reason than a bare aggregate: the numerator and
// denominator are each independently drift-prone, and dividing them does
// NOT cancel that drift (they are sums over different, data-dependent
// row subsets, not the same value divided by itself). distinct_team_
// targets/distinct_repo_targets are len(seen) set cardinalities -- integer.
var investmentFlowDynamicParity = Options{
	BaselineDefects:       investmentFlowRepoDedupParity.BaselineDefects,
	OrderInsensitiveLists: investmentFlowRepoDedupParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.links.value":   "sum(subcategory_kv.2 * effort_value) (investmentflow/queries.go, all five fetchers) -- genuine merged ClickHouse float aggregate.",
		"data.nodes.value":   "the same float Value as data.links.value, accumulated per node (investmentflow/builders.go nodePresence/nodeRunningTotal) -- unlike plain /api/v1/sankey, this route's own node builders DO populate a real value; see sankeyRepoDedupParity's own doc comment for that contrast.",
		"data.team_coverage": "assignedValue/totalValue (investmentflow/builders.go edgeStats) -- a ratio of two sums of the same float aggregate as data.links.value; the ratio does not cancel the sums' own drift, since numerator and denominator sum different, data-dependent row subsets.",
		"data.repo_coverage": "the same edgeStats ratio shape as data.team_coverage, over the repo-scoped row set.",
	},
	IntegerLeaves: map[string]string{
		"data.distinct_team_targets": "len(seen) (investmentflow/builders.go edgeStats) -- a set cardinality, not a ClickHouse aggregate.",
		"data.distinct_repo_targets": "the same set-cardinality shape as data.distinct_team_targets, over the repo-scoped row set.",
	},
}

// investmentFlowModeParity: team_category_repo_org, team_category_
// subcategory_repo_org, team_subcategory_repo_with_drill_org -- the
// three scenarios sharing BuildFlowResponse's own flow_mode branch.
// Extends investmentFlowDynamicParity's own leaves with the three this
// branch ALSO sets: data.coverage (map[string]float64{"team_coverage":
// teamCoverage, "repo_coverage": repoCoverage} -- investmentflow.go --
// the SAME two values as data.team_coverage/data.repo_coverage,
// duplicated; declared via the map's own parent path since coverage's
// keys, though fixed today, are string literals a corpus declaration
// would otherwise have to name twice for no benefit), data.
// unassigned_reasons (map[string]int{"missing_team":...,"missing_repo":
// ...} -- both countIf()-shaped, integer), and data.top_n_repos (*int,
// a pure echo of the request's own top_n_repos parameter/default,
// integer, never touches ClickHouse at all).
var investmentFlowModeParity = Options{
	BaselineDefects:       investmentFlowRepoDedupParity.BaselineDefects,
	OrderInsensitiveLists: investmentFlowRepoDedupParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.links.value":   investmentFlowDynamicParity.FloatTierB["data.links.value"],
		"data.nodes.value":   investmentFlowDynamicParity.FloatTierB["data.nodes.value"],
		"data.team_coverage": investmentFlowDynamicParity.FloatTierB["data.team_coverage"],
		"data.repo_coverage": investmentFlowDynamicParity.FloatTierB["data.repo_coverage"],
		"data.coverage":      "map[string]float64{team_coverage, repo_coverage} (investmentflow.go) -- the SAME two ratio values as data.team_coverage/data.repo_coverage, duplicated under fixed (if data-independent) keys.",
	},
	IntegerLeaves: map[string]string{
		"data.distinct_team_targets": investmentFlowDynamicParity.IntegerLeaves["data.distinct_team_targets"],
		"data.distinct_repo_targets": investmentFlowDynamicParity.IntegerLeaves["data.distinct_repo_targets"],
		"data.unassigned_reasons":    "map[string]int{missing_team, missing_repo} (investmentflow.go fetchInvestmentUnassignedCounts) -- both countIf()-shaped bare counts.",
		"data.top_n_repos":           "a pure echo of the request's own top_n_repos parameter/default (investmentflow.go) -- never touches ClickHouse.",
	},
}

// investmentFlowRepoTeamParity: POST /api/v1/investment/flow/repo-team's
// own default_org/theme_scoped_org. BuildRepoTeamFlowResponse
// (investmentflow.go) sets ONLY Nodes/Links/Mode/Unit/Label/Description/
// ChosenMode -- team_coverage/repo_coverage/coverage/distinct_*_targets/
// unassigned_reasons/top_n_repos all stay nil on both planes (never a
// numeric leaf either plane's comparison reaches), so declaring them
// here would make this Options value read as stale for every request
// that actually uses it -- see this section's own opening comment.
// data.links.value/data.nodes.value are fetchInvestmentRepoTeamEdges'
// own sum(subcategory_kv.2 * effort_value) (investmentflow/queries.go),
// the SAME shape every other investment-flow fetcher shares.
//
// data.links has its own declaration here, not investmentFlowRepoDedup
// Parity's: this route's rows are grouped by (subcategory, repo, team),
// one level finer than either link the builder draws, so the reference
// plane repeats a (source, target) pair once per row that reaches it
// (investmentFlowRepoTeamLinkCopiesDefect), while the candidate writes
// each pair once (buildRepoTeamSankey accumulates through orderedEdges).
// (source, target) is the identity of a link on the candidate plane and,
// after that defect's whole-key collapse, on the reference plane too.
var investmentFlowRepoTeamParity = Options{
	BaselineDefects: append(append([]BaselineDefect{}, investmentFlowRepoDedupParity.BaselineDefects...),
		investmentFlowRepoTeamLinkCopiesDefect),
	OrderInsensitiveLists: []OrderInsensitiveList{
		investmentFlowRepoDedupParity.OrderInsensitiveLists[0],
		{
			Path:      "data.links",
			KeyFields: []string{"source", "target"},
			Reason:    "the SAME first-touch insertion order as data.nodes, over the SAME under-ordered row set. (source, target) is unique per link on the candidate plane (orderedEdges, investmentflow/builders.go); the reference plane's repeated pairs are collapsed whole-key by investmentFlowRepoTeamLinkCopiesDefect before pairing.",
			Ticket:    "CHAOS-5873",
		},
	},
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.links.value": investmentFlowDynamicParity.FloatTierB["data.links.value"],
		"data.nodes.value": investmentFlowDynamicParity.FloatTierB["data.nodes.value"],
	},
}

// investmentFlowRepoTeamLinkCopiesDefect: build_investment_repo_team_
// flow_response (api/services/investment_flow.py) appends one link per
// fetch_investment_repo_team_edges row, and those rows are grouped by
// (subcategory, repo, team) -- so a repo->team link repeats once per
// subcategory that reaches the repo, and a subcategory->repo link once per
// team the repo maps to, each copy carrying one row's share. The candidate
// sums every row reaching a pair into one link, the way the reference
// plane's own link_totals builders do for every other investment flow.
// The copies are compared whole-key: every baseline copy of a key is
// consumed once, the copies must agree outside value, and their sum must
// meet the candidate's value under data.links.value's own FloatTierB tier.
var investmentFlowRepoTeamLinkCopiesDefect = BaselineDefect{
	Ticket:             "CHAOS-6024",
	Reason:             "build_investment_repo_team_flow_response appends one SankeyLink per (subcategory, repo, team) row, so the reference body repeats a (source, target) link once per row that reaches it, each copy carrying that row's share; buildRepoTeamSankey writes each (source, target) once, valued at the sum of the same rows (orderedEdges, the accumulator the link_totals builders share). A link is the aggregated contribution between two nodes (docs/use/investment/investment-flows.md); node values are equal on both planes. Go is correct.",
	Paths:              []string{"data.links"},
	Intermittent:       true,
	IntermittentReason: "present only while some (source, target) pair is reached by two or more (subcategory, repo, team) rows in the requested window and scope; a body whose every repository maps to one team and is reached from one subcategory repeats no link",
	BaselineCopySumShape: &BaselineCopySumShape{
		ListPath:  "data.links",
		KeyFields: []string{"source", "target"},
		SumField:  "value",
	},
}

// investmentFlowNodesTeamScopeSubsetDefect/investmentFlowLinksTeamScope
// SubsetDefect declare data.nodes/data.links as a bounded subset for a
// team-scoped POST /api/v1/investment/flow (+/repo-team) request:
// repoScopeFilterClause (investmentflow.go:143-171) splices
// teamscope.RepoCondition(orgID,"repo_id",scopeIDs,asOf) for a team
// scope, the same shared condition and the same team_repo_ownership
// source every repo-keyed route in this corpus reads (heatmapRepo
// TouchpointsTeamScopeSubsetDefect's own Reason states the two tables in
// full). The reference plane resolves the identical scope through
// resolve_repo_filter_ids -> resolve_repo_ids_for_teams
// (api/queries/scopes.py:72-89, the same user_metrics_daily.team_id read
// every sibling team-scope defect in this file cites) -- empty for this
// org's real team, so the reference answers organization-wide while the
// candidate answers over the team's own repositories.
//
// buildDynamicFlowSankey/buildRepoTeamSankey (investmentflow/builders.go)
// both build a node's own value as a RUNNING SUM over every row that
// touches it (nodeRunningTotal.add) and a link's own value directly off
// one qualifying row -- every row is already scoped to a single admitted
// repository (the SQL's own repo_id filter), so a SOURCE (subcategory)
// node's own value sums across however many repositories the request's
// scope admits, a non-negative per-repo contribution the SAME class the
// class ruling this ticket's own card states covers: a genuinely
// narrower team scope can only pull that sum down, never up, at a
// (sub)category node or link this response still carries -- ADMITTED
// via BoundedLeavesAllKeys, not enumerated BoundedLeafKeys, since every
// node/link this response can ever produce is built the same running-sum
// way, with no separate leaf-name-only-sometimes-bounded case to carve
// out. group is EqualLeaves: a matched node's own group label
// (subcategory/repo/team) is fixed by which BRANCH produced it, never by
// how many repositories fed its value.
var investmentFlowNodesTeamScopeSubsetDefect = BaselineDefect{
	Ticket:             "CHAOS-5940",
	Reason:             "POST /api/v1/investment/flow (+/repo-team)'s team scope resolves a team's repositories from team_repo_ownership through teamscope.RepoCondition on the candidate, user_metrics_daily.team_id (empty for this org's real team) on the reference -- the reference answers organization-wide, the candidate over the team's own repositories. A node's own value is a running sum over every admitted row that touches it (nodeRunningTotal.add, investmentflow/builders.go), a non-negative per-repo contribution, so a genuinely narrower team scope can only pull a matched node's own sum down, never up. Go is correct.",
	Paths:              []string{"data.nodes"},
	Intermittent:       true,
	IntermittentReason: "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{
		ListPath:             "data.nodes",
		KeyFields:            []string{"name"},
		EqualLeaves:          []string{"group"},
		BoundedLeavesAllKeys: []string{"value"},
	},
}

var investmentFlowLinksTeamScopeSubsetDefect = BaselineDefect{
	Ticket:             "CHAOS-5940",
	Reason:             "the same team-repository-resolution mechanism as investmentFlowNodesTeamScopeSubsetDefect, over data.links: a link's own value is one qualifying row's own contribution (or, for the flow_mode branch's own accumulators, a running sum over the rows sharing that (source, target) pair), always confined to the request's own admitted repositories, so a genuinely narrower team scope can only pull a matched link's own value down, never up. Go is correct.",
	Paths:              []string{"data.links"},
	Intermittent:       true,
	IntermittentReason: "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{
		ListPath:             "data.links",
		KeyFields:            []string{"source", "target"},
		BoundedLeavesAllKeys: []string{"value"},
	},
}

// investmentFlowDynamicTeamScopedParity: POST /api/v1/investment/flow's
// own team_scoped entry (the dynamic, non-flow_mode branch --
// investmentFlowDynamicParity's own doc comment states this branch's
// numeric leaves in full).
var investmentFlowDynamicTeamScopedParity = Options{
	OrderInsensitiveLists: investmentFlowDynamicParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB:            investmentFlowDynamicParity.FloatTierB,
	IntegerLeaves:         investmentFlowDynamicParity.IntegerLeaves,
	BaselineDefects: append(append([]BaselineDefect{}, investmentFlowDynamicParity.BaselineDefects...),
		investmentFlowNodesTeamScopeSubsetDefect, investmentFlowLinksTeamScopeSubsetDefect),
}

// investmentFlowRepoTeamTeamScopedParity: POST /api/v1/investment/flow/
// repo-team's own team_scoped entry.
var investmentFlowRepoTeamTeamScopedParity = Options{
	OrderInsensitiveLists: investmentFlowRepoTeamParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB:            investmentFlowRepoTeamParity.FloatTierB,
	BaselineDefects: append(append([]BaselineDefect{}, investmentFlowRepoTeamParity.BaselineDefects...),
		investmentFlowNodesTeamScopeSubsetDefect, investmentFlowLinksTeamScopeSubsetDefect),
}
