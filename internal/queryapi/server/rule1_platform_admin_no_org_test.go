package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-go/pep"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/aggflame"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/drilldown"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// Product rule 1 (chris, D6773): a platform admin needs no org membership row, and with no org every token class
// gets the answer the legacy path gives. rule1NoOrgGolden is what main (0bf51eb9) answers on each REST handler for a
// superuser with no org, recorded there by this test over rule1Client: status, body and a digest of every statement
// with its bindings.
const rule1NoOrgGolden = `{"edge token routed | drilldown_issues":{"body":"{\"items\":[]}",
"digest":"884760750f97cf60",
"statements":1,
"status":200},
"edge token routed | drilldown_prs":{"body":"{\"items\":[]}",
"digest":"42ba51aa626abc85",
"statements":1,
"status":200},
"edge token routed | explain":{"body":"{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"unit\":\"hours\",\"value\":0.0,\"delta_pct\":null,\"drivers\":[],\"contributors\":[],\"drilldown_links\":{\"prs\":\"/api/v1/drilldown/prs?metric=blocked_work\",\"issues\":\"/api/v1/drilldown/issues?metric=blocked_work\"},\"repositories\":null,\"source_url\":null,\"has_data\":false,\"has_prior_data\":false,\"source\":null,\"link_tier\":null,\"rate_state\":null,\"evidence_basis\":null}",
"digest":"a0950b0aeca0da55",
"statements":5,
"status":200},
"edge token routed | filteroptions":{"body":"{\"teams\":[],\"repos\":[],\"services\":[],\"developers\":[],\"work_category\":[\"feature_delivery\",\"maintenance\",\"operational\",\"quality\",\"risk\",\"feature_delivery.customer\",\"feature_delivery.enablement\",\"feature_delivery.roadmap\",\"maintenance.debt\",\"maintenance.refactor\",\"maintenance.upgrade\",\"operational.incident_response\",\"operational.on_call\",\"operational.support\",\"quality.bugfix\",\"quality.reliability\",\"quality.testing\",\"risk.compliance\",\"risk.security\",\"risk.vulnerability\"],\"issue_type\":[],\"flow_stage\":[]}",
"digest":"844346916457cb9d",
"statements":8,
"status":200},
"edge token routed | flame":{"body":"{\"detail\":\"Issue not found\"}",
"digest":"0c93624c7c068522",
"statements":1,
"status":404},
"edge token routed | flame_aggregated":{"body":"{\"mode\":\"throughput\",\"unit\":\"items\",\"root\":{\"name\":\"Work Delivered\",\"value\":0.0,\"children\":[]},\"meta\":{\"window_start\":\"2026-01-01\",\"window_end\":\"2026-01-15\",\"filters\":{},\"notes\":[\"Work type classification unavailable. Using team-based grouping.\",\"No throughput data available for this window/scope.\"],\"approximation\":{\"used\":true,\"method\":\"inferred\"}}}",
"digest":"476ad86b24082828",
"statements":2,
"status":200},
"edge token routed | heatmap":{"body":"{\"axes\":{\"x\":[\"00\",\"01\",\"02\",\"03\",\"04\",\"05\",\"06\",\"07\",\"08\",\"09\",\"10\",\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",\"17\",\"18\",\"19\",\"20\",\"21\",\"22\",\"23\"],\"y\":[\"Mon\",\"Tue\",\"Wed\",\"Thu\",\"Fri\",\"Sat\",\"Sun\"]},\"cells\":[],\"legend\":{\"unit\":\"hours\",\"scale\":\"linear\"},\"evidence\":null,\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"b6f3484c38784ef7",
"statements":1,
"status":200},
"edge token routed | home":{"body":"{\"freshness\":{\"last_ingested_at\":null,\"latest_successful_sync_at\":null,\"sources\":{},\"coverage\":{\"repos_covered_pct\":null,\"prs_linked_to_issues_pct\":null,\"issues_with_cycle_states_pct\":null}},\"deltas\":[{\"metric\":\"cycle_time\",\"label\":\"Cycle Time\",\"value\":0.0,\"unit\":\"days\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"review_latency\",\"label\":\"Review Latency\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"throughput\",\"label\":\"Throughput\",\"value\":0.0,\"unit\":\"items\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"deploy_freq\",\"label\":\"Deploy Frequency\",\"value\":0.0,\"unit\":\"deploys\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"churn\",\"label\":\"Code Churn\",\"value\":0.0,\"unit\":\"loc\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"wip_saturation\",\"label\":\"WIP Saturation\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"change_failure_rate\",\"label\":\"Change Failure Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"rework_ratio\",\"label\":\"Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"pr_rework_ratio\",\"label\":\"PR Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"ci_success\",\"label\":\"CI Success Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null}],\"rework_theme_allocation\":[],\"summary\":[],\"tiles\":{\"understand\":{\"title\":\"Understand\",\"subtitle\":\"Flow stages\",\"link\":\"/explore?view=understand\"},\"measure\":{\"title\":\"Measure\",\"subtitle\":\"Coverage & freshness\",\"link\":\"/explore?view=measure\"},\"align\":{\"title\":\"Align\",\"subtitle\":\"Investment mix\",\"link\":\"/investment\"},\"execute\":{\"title\":\"Execute\",\"subtitle\":\"Top opportunities\",\"link\":\"/opportunities\"}},\"constraint\":{\"title\":\"\",\"claim\":\"\",\"evidence\":[],\"experiments\":[]},\"events\":[],\"health_state\":{\"status\":\"no_data\",\"headline\":\"\",\"summary\":\"\",\"as_of\":null},\"signals\":[],\"limiting_factor\":{\"claim\":\"\",\"why_it_matters\":\"\",\"recommended_action\":\"\",\"confidence\":\"low\",\"evidence_ref\":null},\"data_confidence\":{\"level\":\"low\",\"coverage_pct\":null,\"connected_sources\":[],\"missing_sources\":[],\"caveats\":[\"Coverage could not be computed from available lineage fields.\"]},\"filter_empty_reason\":null}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"edge token routed | investment":{"body":"{\"theme_distribution\":{},\"subcategory_distribution\":{},\"evidence_quality_distribution\":{},\"evidence_quality_stats\":{\"mean\":null,\"stddev\":null,\"total\":0,\"band_counts\":{},\"quality_drivers\":[]},\"unit\":null,\"edges\":[],\"measure\":\"ISSUE_SIZE\",\"denominator\":0.0,\"unsized_unit_count\":0}",
"digest":"b5df5396114b20f6",
"statements":6,
"status":200},
"edge token routed | investment_explain":{"body":"{\"summary\":\"This mix suggests work leans toward the leading themes shown, with subcategories providing the specific intent behind that allocation.\",\"top_findings\":[],\"confidence\":{\"level\":\"unknown\",\"quality_mean\":null,\"quality_stddev\":null,\"band_mix\":{},\"drivers\":[]},\"what_to_check_next\":[{\"action\":\"Review the largest subcategories\",\"why\":\"They drive the overall theme distribution\",\"where\":\"Subcategory breakdown panel\"}],\"anti_claims\":[\"This does not measure individual productivity.\",\"This does not assign intent or correctness to any work.\"],\"status\":\"invalid_llm_output\"}",
"digest":"4a41f4fd2e5d84be",
"statements":4,
"status":200},
"edge token routed | investment_sunburst":{"body":"[]",
"digest":"402e8ed713b32417",
"statements":4,
"status":200},
"edge token routed | opportunities":{"body":"{\"items\":[{\"id\":\"opp-0\",\"title\":\"Maintain steady flow\",\"rationale\":\"Key metrics are stable. Focus on sustaining current practices.\",\"evidence_links\":[\"/api/v1/home?scope_type=org&scope_id=\"],\"suggested_experiments\":[\"Share the current playbook with new teams.\"],\"change_percent\":null,\"direction\":null,\"range_days\":14,\"compare_days\":14}]}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"edge token routed | people":{"body":"[]",
"digest":"e10d9e413a46484d",
"statements":1,
"status":200},
"edge token routed | people_drilldown_issues":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"edge token routed | people_drilldown_prs":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"edge token routed | people_metric":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"edge token routed | people_summary":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"edge token routed | quadrant":{"body":"{\"axes\":{\"x\":{\"metric\":\"wip\",\"label\":\"WIP\",\"unit\":\"items\"},\"y\":{\"metric\":\"throughput\",\"label\":\"Throughput\",\"unit\":\"items\"}},\"points\":[],\"annotations\":[],\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"e3c344211fd54ae4",
"statements":5,
"status":200},
"edge token routed | sankey":{"body":"{\"mode\":\"investment\",\"nodes\":[],\"links\":[],\"unit\":\"work_units\",\"label\":\"Investment flow\",\"description\":\"Where effort allocates across initiatives, areas, issue types, and work items.\",\"team_coverage\":null,\"repo_coverage\":null,\"distinct_team_targets\":null,\"distinct_repo_targets\":null,\"chosen_mode\":null,\"coverage\":null,\"unassigned_reasons\":null,\"flow_mode\":null,\"drill_category\":null,\"top_n_repos\":null}",
"digest":"4f7dc061cbb6787e",
"statements":3,
"status":200},
"edge token routed | work_unit_explain":{"body":"{\"detail\":\"Work unit wu-1 not found\"}",
"digest":"b07b9c93eced2a3a",
"statements":1,
"status":404},
"edge token routed | work_units":{"body":"[]",
"digest":"325371161d20e39f",
"statements":1,
"status":200},
"edge token | drilldown_issues":{"body":"{\"items\":[]}",
"digest":"884760750f97cf60",
"statements":1,
"status":200},
"edge token | drilldown_prs":{"body":"{\"items\":[]}",
"digest":"42ba51aa626abc85",
"statements":1,
"status":200},
"edge token | explain":{"body":"{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"unit\":\"hours\",\"value\":0.0,\"delta_pct\":null,\"drivers\":[],\"contributors\":[],\"drilldown_links\":{\"prs\":\"/api/v1/drilldown/prs?metric=blocked_work\",\"issues\":\"/api/v1/drilldown/issues?metric=blocked_work\"},\"repositories\":null,\"source_url\":null,\"has_data\":false,\"has_prior_data\":false,\"source\":null,\"link_tier\":null,\"rate_state\":null,\"evidence_basis\":null}",
"digest":"a0950b0aeca0da55",
"statements":5,
"status":200},
"edge token | filteroptions":{"body":"{\"teams\":[],\"repos\":[],\"services\":[],\"developers\":[],\"work_category\":[\"feature_delivery\",\"maintenance\",\"operational\",\"quality\",\"risk\",\"feature_delivery.customer\",\"feature_delivery.enablement\",\"feature_delivery.roadmap\",\"maintenance.debt\",\"maintenance.refactor\",\"maintenance.upgrade\",\"operational.incident_response\",\"operational.on_call\",\"operational.support\",\"quality.bugfix\",\"quality.reliability\",\"quality.testing\",\"risk.compliance\",\"risk.security\",\"risk.vulnerability\"],\"issue_type\":[],\"flow_stage\":[]}",
"digest":"844346916457cb9d",
"statements":8,
"status":200},
"edge token | flame":{"body":"{\"detail\":\"Issue not found\"}",
"digest":"0c93624c7c068522",
"statements":1,
"status":404},
"edge token | flame_aggregated":{"body":"{\"mode\":\"throughput\",\"unit\":\"items\",\"root\":{\"name\":\"Work Delivered\",\"value\":0.0,\"children\":[]},\"meta\":{\"window_start\":\"2026-01-01\",\"window_end\":\"2026-01-15\",\"filters\":{},\"notes\":[\"Work type classification unavailable. Using team-based grouping.\",\"No throughput data available for this window/scope.\"],\"approximation\":{\"used\":true,\"method\":\"inferred\"}}}",
"digest":"476ad86b24082828",
"statements":2,
"status":200},
"edge token | heatmap":{"body":"{\"axes\":{\"x\":[\"00\",\"01\",\"02\",\"03\",\"04\",\"05\",\"06\",\"07\",\"08\",\"09\",\"10\",\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",\"17\",\"18\",\"19\",\"20\",\"21\",\"22\",\"23\"],\"y\":[\"Mon\",\"Tue\",\"Wed\",\"Thu\",\"Fri\",\"Sat\",\"Sun\"]},\"cells\":[],\"legend\":{\"unit\":\"hours\",\"scale\":\"linear\"},\"evidence\":null,\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"b6f3484c38784ef7",
"statements":1,
"status":200},
"edge token | home":{"body":"{\"freshness\":{\"last_ingested_at\":null,\"latest_successful_sync_at\":null,\"sources\":{},\"coverage\":{\"repos_covered_pct\":null,\"prs_linked_to_issues_pct\":null,\"issues_with_cycle_states_pct\":null}},\"deltas\":[{\"metric\":\"cycle_time\",\"label\":\"Cycle Time\",\"value\":0.0,\"unit\":\"days\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"review_latency\",\"label\":\"Review Latency\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"throughput\",\"label\":\"Throughput\",\"value\":0.0,\"unit\":\"items\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"deploy_freq\",\"label\":\"Deploy Frequency\",\"value\":0.0,\"unit\":\"deploys\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"churn\",\"label\":\"Code Churn\",\"value\":0.0,\"unit\":\"loc\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"wip_saturation\",\"label\":\"WIP Saturation\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"change_failure_rate\",\"label\":\"Change Failure Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"rework_ratio\",\"label\":\"Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"pr_rework_ratio\",\"label\":\"PR Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"ci_success\",\"label\":\"CI Success Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null}],\"rework_theme_allocation\":[],\"summary\":[],\"tiles\":{\"understand\":{\"title\":\"Understand\",\"subtitle\":\"Flow stages\",\"link\":\"/explore?view=understand\"},\"measure\":{\"title\":\"Measure\",\"subtitle\":\"Coverage & freshness\",\"link\":\"/explore?view=measure\"},\"align\":{\"title\":\"Align\",\"subtitle\":\"Investment mix\",\"link\":\"/investment\"},\"execute\":{\"title\":\"Execute\",\"subtitle\":\"Top opportunities\",\"link\":\"/opportunities\"}},\"constraint\":{\"title\":\"\",\"claim\":\"\",\"evidence\":[],\"experiments\":[]},\"events\":[],\"health_state\":{\"status\":\"no_data\",\"headline\":\"\",\"summary\":\"\",\"as_of\":null},\"signals\":[],\"limiting_factor\":{\"claim\":\"\",\"why_it_matters\":\"\",\"recommended_action\":\"\",\"confidence\":\"low\",\"evidence_ref\":null},\"data_confidence\":{\"level\":\"low\",\"coverage_pct\":null,\"connected_sources\":[],\"missing_sources\":[],\"caveats\":[\"Coverage could not be computed from available lineage fields.\"]},\"filter_empty_reason\":null}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"edge token | investment":{"body":"{\"theme_distribution\":{},\"subcategory_distribution\":{},\"evidence_quality_distribution\":{},\"evidence_quality_stats\":{\"mean\":null,\"stddev\":null,\"total\":0,\"band_counts\":{},\"quality_drivers\":[]},\"unit\":null,\"edges\":[],\"measure\":\"ISSUE_SIZE\",\"denominator\":0.0,\"unsized_unit_count\":0}",
"digest":"b5df5396114b20f6",
"statements":6,
"status":200},
"edge token | investment_explain":{"body":"{\"summary\":\"This mix suggests work leans toward the leading themes shown, with subcategories providing the specific intent behind that allocation.\",\"top_findings\":[],\"confidence\":{\"level\":\"unknown\",\"quality_mean\":null,\"quality_stddev\":null,\"band_mix\":{},\"drivers\":[]},\"what_to_check_next\":[{\"action\":\"Review the largest subcategories\",\"why\":\"They drive the overall theme distribution\",\"where\":\"Subcategory breakdown panel\"}],\"anti_claims\":[\"This does not measure individual productivity.\",\"This does not assign intent or correctness to any work.\"],\"status\":\"invalid_llm_output\"}",
"digest":"4a41f4fd2e5d84be",
"statements":4,
"status":200},
"edge token | investment_sunburst":{"body":"[]",
"digest":"402e8ed713b32417",
"statements":4,
"status":200},
"edge token | opportunities":{"body":"{\"items\":[{\"id\":\"opp-0\",\"title\":\"Maintain steady flow\",\"rationale\":\"Key metrics are stable. Focus on sustaining current practices.\",\"evidence_links\":[\"/api/v1/home?scope_type=org&scope_id=\"],\"suggested_experiments\":[\"Share the current playbook with new teams.\"],\"change_percent\":null,\"direction\":null,\"range_days\":14,\"compare_days\":14}]}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"edge token | people":{"body":"[]",
"digest":"e10d9e413a46484d",
"statements":1,
"status":200},
"edge token | people_drilldown_issues":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"edge token | people_drilldown_prs":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"edge token | people_metric":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"edge token | people_summary":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"edge token | quadrant":{"body":"{\"axes\":{\"x\":{\"metric\":\"wip\",\"label\":\"WIP\",\"unit\":\"items\"},\"y\":{\"metric\":\"throughput\",\"label\":\"Throughput\",\"unit\":\"items\"}},\"points\":[],\"annotations\":[],\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"e3c344211fd54ae4",
"statements":5,
"status":200},
"edge token | sankey":{"body":"{\"mode\":\"investment\",\"nodes\":[],\"links\":[],\"unit\":\"work_units\",\"label\":\"Investment flow\",\"description\":\"Where effort allocates across initiatives, areas, issue types, and work items.\",\"team_coverage\":null,\"repo_coverage\":null,\"distinct_team_targets\":null,\"distinct_repo_targets\":null,\"chosen_mode\":null,\"coverage\":null,\"unassigned_reasons\":null,\"flow_mode\":null,\"drill_category\":null,\"top_n_repos\":null}",
"digest":"4f7dc061cbb6787e",
"statements":3,
"status":200},
"edge token | work_unit_explain":{"body":"{\"detail\":\"Work unit wu-1 not found\"}",
"digest":"b07b9c93eced2a3a",
"statements":1,
"status":404},
"edge token | work_units":{"body":"[]",
"digest":"325371161d20e39f",
"statements":1,
"status":200},
"envelope | drilldown_issues":{"body":"{\"items\":[]}",
"digest":"884760750f97cf60",
"statements":1,
"status":200},
"envelope | drilldown_prs":{"body":"{\"items\":[]}",
"digest":"42ba51aa626abc85",
"statements":1,
"status":200},
"envelope | explain":{"body":"{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"unit\":\"hours\",\"value\":0.0,\"delta_pct\":null,\"drivers\":[],\"contributors\":[],\"drilldown_links\":{\"prs\":\"/api/v1/drilldown/prs?metric=blocked_work\",\"issues\":\"/api/v1/drilldown/issues?metric=blocked_work\"},\"repositories\":null,\"source_url\":null,\"has_data\":false,\"has_prior_data\":false,\"source\":null,\"link_tier\":null,\"rate_state\":null,\"evidence_basis\":null}",
"digest":"a0950b0aeca0da55",
"statements":5,
"status":200},
"envelope | filteroptions":{"body":"{\"teams\":[],\"repos\":[],\"services\":[],\"developers\":[],\"work_category\":[\"feature_delivery\",\"maintenance\",\"operational\",\"quality\",\"risk\",\"feature_delivery.customer\",\"feature_delivery.enablement\",\"feature_delivery.roadmap\",\"maintenance.debt\",\"maintenance.refactor\",\"maintenance.upgrade\",\"operational.incident_response\",\"operational.on_call\",\"operational.support\",\"quality.bugfix\",\"quality.reliability\",\"quality.testing\",\"risk.compliance\",\"risk.security\",\"risk.vulnerability\"],\"issue_type\":[],\"flow_stage\":[]}",
"digest":"844346916457cb9d",
"statements":8,
"status":200},
"envelope | flame":{"body":"{\"detail\":\"Issue not found\"}",
"digest":"0c93624c7c068522",
"statements":1,
"status":404},
"envelope | flame_aggregated":{"body":"{\"mode\":\"throughput\",\"unit\":\"items\",\"root\":{\"name\":\"Work Delivered\",\"value\":0.0,\"children\":[]},\"meta\":{\"window_start\":\"2026-01-01\",\"window_end\":\"2026-01-15\",\"filters\":{},\"notes\":[\"Work type classification unavailable. Using team-based grouping.\",\"No throughput data available for this window/scope.\"],\"approximation\":{\"used\":true,\"method\":\"inferred\"}}}",
"digest":"476ad86b24082828",
"statements":2,
"status":200},
"envelope | heatmap":{"body":"{\"axes\":{\"x\":[\"00\",\"01\",\"02\",\"03\",\"04\",\"05\",\"06\",\"07\",\"08\",\"09\",\"10\",\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",\"17\",\"18\",\"19\",\"20\",\"21\",\"22\",\"23\"],\"y\":[\"Mon\",\"Tue\",\"Wed\",\"Thu\",\"Fri\",\"Sat\",\"Sun\"]},\"cells\":[],\"legend\":{\"unit\":\"hours\",\"scale\":\"linear\"},\"evidence\":null,\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"b6f3484c38784ef7",
"statements":1,
"status":200},
"envelope | home":{"body":"{\"freshness\":{\"last_ingested_at\":null,\"latest_successful_sync_at\":null,\"sources\":{},\"coverage\":{\"repos_covered_pct\":null,\"prs_linked_to_issues_pct\":null,\"issues_with_cycle_states_pct\":null}},\"deltas\":[{\"metric\":\"cycle_time\",\"label\":\"Cycle Time\",\"value\":0.0,\"unit\":\"days\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"review_latency\",\"label\":\"Review Latency\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"throughput\",\"label\":\"Throughput\",\"value\":0.0,\"unit\":\"items\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"deploy_freq\",\"label\":\"Deploy Frequency\",\"value\":0.0,\"unit\":\"deploys\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"churn\",\"label\":\"Code Churn\",\"value\":0.0,\"unit\":\"loc\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"wip_saturation\",\"label\":\"WIP Saturation\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"change_failure_rate\",\"label\":\"Change Failure Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"rework_ratio\",\"label\":\"Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"pr_rework_ratio\",\"label\":\"PR Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"ci_success\",\"label\":\"CI Success Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null}],\"rework_theme_allocation\":[],\"summary\":[],\"tiles\":{\"understand\":{\"title\":\"Understand\",\"subtitle\":\"Flow stages\",\"link\":\"/explore?view=understand\"},\"measure\":{\"title\":\"Measure\",\"subtitle\":\"Coverage & freshness\",\"link\":\"/explore?view=measure\"},\"align\":{\"title\":\"Align\",\"subtitle\":\"Investment mix\",\"link\":\"/investment\"},\"execute\":{\"title\":\"Execute\",\"subtitle\":\"Top opportunities\",\"link\":\"/opportunities\"}},\"constraint\":{\"title\":\"\",\"claim\":\"\",\"evidence\":[],\"experiments\":[]},\"events\":[],\"health_state\":{\"status\":\"no_data\",\"headline\":\"\",\"summary\":\"\",\"as_of\":null},\"signals\":[],\"limiting_factor\":{\"claim\":\"\",\"why_it_matters\":\"\",\"recommended_action\":\"\",\"confidence\":\"low\",\"evidence_ref\":null},\"data_confidence\":{\"level\":\"low\",\"coverage_pct\":null,\"connected_sources\":[],\"missing_sources\":[],\"caveats\":[\"Coverage could not be computed from available lineage fields.\"]},\"filter_empty_reason\":null}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"envelope | investment":{"body":"{\"theme_distribution\":{},\"subcategory_distribution\":{},\"evidence_quality_distribution\":{},\"evidence_quality_stats\":{\"mean\":null,\"stddev\":null,\"total\":0,\"band_counts\":{},\"quality_drivers\":[]},\"unit\":null,\"edges\":[],\"measure\":\"ISSUE_SIZE\",\"denominator\":0.0,\"unsized_unit_count\":0}",
"digest":"b5df5396114b20f6",
"statements":6,
"status":200},
"envelope | investment_explain":{"body":"{\"summary\":\"This mix suggests work leans toward the leading themes shown, with subcategories providing the specific intent behind that allocation.\",\"top_findings\":[],\"confidence\":{\"level\":\"unknown\",\"quality_mean\":null,\"quality_stddev\":null,\"band_mix\":{},\"drivers\":[]},\"what_to_check_next\":[{\"action\":\"Review the largest subcategories\",\"why\":\"They drive the overall theme distribution\",\"where\":\"Subcategory breakdown panel\"}],\"anti_claims\":[\"This does not measure individual productivity.\",\"This does not assign intent or correctness to any work.\"],\"status\":\"invalid_llm_output\"}",
"digest":"4a41f4fd2e5d84be",
"statements":4,
"status":200},
"envelope | investment_sunburst":{"body":"[]",
"digest":"402e8ed713b32417",
"statements":4,
"status":200},
"envelope | opportunities":{"body":"{\"items\":[{\"id\":\"opp-0\",\"title\":\"Maintain steady flow\",\"rationale\":\"Key metrics are stable. Focus on sustaining current practices.\",\"evidence_links\":[\"/api/v1/home?scope_type=org&scope_id=\"],\"suggested_experiments\":[\"Share the current playbook with new teams.\"],\"change_percent\":null,\"direction\":null,\"range_days\":14,\"compare_days\":14}]}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"envelope | people":{"body":"[]",
"digest":"e10d9e413a46484d",
"statements":1,
"status":200},
"envelope | people_drilldown_issues":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"envelope | people_drilldown_prs":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"envelope | people_metric":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"envelope | people_summary":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"envelope | quadrant":{"body":"{\"axes\":{\"x\":{\"metric\":\"wip\",\"label\":\"WIP\",\"unit\":\"items\"},\"y\":{\"metric\":\"throughput\",\"label\":\"Throughput\",\"unit\":\"items\"}},\"points\":[],\"annotations\":[],\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"e3c344211fd54ae4",
"statements":5,
"status":200},
"envelope | sankey":{"body":"{\"mode\":\"investment\",\"nodes\":[],\"links\":[],\"unit\":\"work_units\",\"label\":\"Investment flow\",\"description\":\"Where effort allocates across initiatives, areas, issue types, and work items.\",\"team_coverage\":null,\"repo_coverage\":null,\"distinct_team_targets\":null,\"distinct_repo_targets\":null,\"chosen_mode\":null,\"coverage\":null,\"unassigned_reasons\":null,\"flow_mode\":null,\"drill_category\":null,\"top_n_repos\":null}",
"digest":"4f7dc061cbb6787e",
"statements":3,
"status":200},
"envelope | work_unit_explain":{"body":"{\"detail\":\"Work unit wu-1 not found\"}",
"digest":"b07b9c93eced2a3a",
"statements":1,
"status":404},
"envelope | work_units":{"body":"[]",
"digest":"325371161d20e39f",
"statements":1,
"status":200},
"user access token routed | drilldown_issues":{"body":"{\"items\":[]}",
"digest":"884760750f97cf60",
"statements":1,
"status":200},
"user access token routed | drilldown_prs":{"body":"{\"items\":[]}",
"digest":"42ba51aa626abc85",
"statements":1,
"status":200},
"user access token routed | explain":{"body":"{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"unit\":\"hours\",\"value\":0.0,\"delta_pct\":null,\"drivers\":[],\"contributors\":[],\"drilldown_links\":{\"prs\":\"/api/v1/drilldown/prs?metric=blocked_work\",\"issues\":\"/api/v1/drilldown/issues?metric=blocked_work\"},\"repositories\":null,\"source_url\":null,\"has_data\":false,\"has_prior_data\":false,\"source\":null,\"link_tier\":null,\"rate_state\":null,\"evidence_basis\":null}",
"digest":"a0950b0aeca0da55",
"statements":5,
"status":200},
"user access token routed | filteroptions":{"body":"{\"teams\":[],\"repos\":[],\"services\":[],\"developers\":[],\"work_category\":[\"feature_delivery\",\"maintenance\",\"operational\",\"quality\",\"risk\",\"feature_delivery.customer\",\"feature_delivery.enablement\",\"feature_delivery.roadmap\",\"maintenance.debt\",\"maintenance.refactor\",\"maintenance.upgrade\",\"operational.incident_response\",\"operational.on_call\",\"operational.support\",\"quality.bugfix\",\"quality.reliability\",\"quality.testing\",\"risk.compliance\",\"risk.security\",\"risk.vulnerability\"],\"issue_type\":[],\"flow_stage\":[]}",
"digest":"844346916457cb9d",
"statements":8,
"status":200},
"user access token routed | flame":{"body":"{\"detail\":\"Issue not found\"}",
"digest":"0c93624c7c068522",
"statements":1,
"status":404},
"user access token routed | flame_aggregated":{"body":"{\"mode\":\"throughput\",\"unit\":\"items\",\"root\":{\"name\":\"Work Delivered\",\"value\":0.0,\"children\":[]},\"meta\":{\"window_start\":\"2026-01-01\",\"window_end\":\"2026-01-15\",\"filters\":{},\"notes\":[\"Work type classification unavailable. Using team-based grouping.\",\"No throughput data available for this window/scope.\"],\"approximation\":{\"used\":true,\"method\":\"inferred\"}}}",
"digest":"476ad86b24082828",
"statements":2,
"status":200},
"user access token routed | heatmap":{"body":"{\"axes\":{\"x\":[\"00\",\"01\",\"02\",\"03\",\"04\",\"05\",\"06\",\"07\",\"08\",\"09\",\"10\",\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",\"17\",\"18\",\"19\",\"20\",\"21\",\"22\",\"23\"],\"y\":[\"Mon\",\"Tue\",\"Wed\",\"Thu\",\"Fri\",\"Sat\",\"Sun\"]},\"cells\":[],\"legend\":{\"unit\":\"hours\",\"scale\":\"linear\"},\"evidence\":null,\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"b6f3484c38784ef7",
"statements":1,
"status":200},
"user access token routed | home":{"body":"{\"freshness\":{\"last_ingested_at\":null,\"latest_successful_sync_at\":null,\"sources\":{},\"coverage\":{\"repos_covered_pct\":null,\"prs_linked_to_issues_pct\":null,\"issues_with_cycle_states_pct\":null}},\"deltas\":[{\"metric\":\"cycle_time\",\"label\":\"Cycle Time\",\"value\":0.0,\"unit\":\"days\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"review_latency\",\"label\":\"Review Latency\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"throughput\",\"label\":\"Throughput\",\"value\":0.0,\"unit\":\"items\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"deploy_freq\",\"label\":\"Deploy Frequency\",\"value\":0.0,\"unit\":\"deploys\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"churn\",\"label\":\"Code Churn\",\"value\":0.0,\"unit\":\"loc\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"wip_saturation\",\"label\":\"WIP Saturation\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"change_failure_rate\",\"label\":\"Change Failure Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"rework_ratio\",\"label\":\"Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"pr_rework_ratio\",\"label\":\"PR Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"ci_success\",\"label\":\"CI Success Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null}],\"rework_theme_allocation\":[],\"summary\":[],\"tiles\":{\"understand\":{\"title\":\"Understand\",\"subtitle\":\"Flow stages\",\"link\":\"/explore?view=understand\"},\"measure\":{\"title\":\"Measure\",\"subtitle\":\"Coverage & freshness\",\"link\":\"/explore?view=measure\"},\"align\":{\"title\":\"Align\",\"subtitle\":\"Investment mix\",\"link\":\"/investment\"},\"execute\":{\"title\":\"Execute\",\"subtitle\":\"Top opportunities\",\"link\":\"/opportunities\"}},\"constraint\":{\"title\":\"\",\"claim\":\"\",\"evidence\":[],\"experiments\":[]},\"events\":[],\"health_state\":{\"status\":\"no_data\",\"headline\":\"\",\"summary\":\"\",\"as_of\":null},\"signals\":[],\"limiting_factor\":{\"claim\":\"\",\"why_it_matters\":\"\",\"recommended_action\":\"\",\"confidence\":\"low\",\"evidence_ref\":null},\"data_confidence\":{\"level\":\"low\",\"coverage_pct\":null,\"connected_sources\":[],\"missing_sources\":[],\"caveats\":[\"Coverage could not be computed from available lineage fields.\"]},\"filter_empty_reason\":null}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"user access token routed | investment":{"body":"{\"theme_distribution\":{},\"subcategory_distribution\":{},\"evidence_quality_distribution\":{},\"evidence_quality_stats\":{\"mean\":null,\"stddev\":null,\"total\":0,\"band_counts\":{},\"quality_drivers\":[]},\"unit\":null,\"edges\":[],\"measure\":\"ISSUE_SIZE\",\"denominator\":0.0,\"unsized_unit_count\":0}",
"digest":"b5df5396114b20f6",
"statements":6,
"status":200},
"user access token routed | investment_explain":{"body":"{\"summary\":\"This mix suggests work leans toward the leading themes shown, with subcategories providing the specific intent behind that allocation.\",\"top_findings\":[],\"confidence\":{\"level\":\"unknown\",\"quality_mean\":null,\"quality_stddev\":null,\"band_mix\":{},\"drivers\":[]},\"what_to_check_next\":[{\"action\":\"Review the largest subcategories\",\"why\":\"They drive the overall theme distribution\",\"where\":\"Subcategory breakdown panel\"}],\"anti_claims\":[\"This does not measure individual productivity.\",\"This does not assign intent or correctness to any work.\"],\"status\":\"invalid_llm_output\"}",
"digest":"4a41f4fd2e5d84be",
"statements":4,
"status":200},
"user access token routed | investment_sunburst":{"body":"[]",
"digest":"402e8ed713b32417",
"statements":4,
"status":200},
"user access token routed | opportunities":{"body":"{\"items\":[{\"id\":\"opp-0\",\"title\":\"Maintain steady flow\",\"rationale\":\"Key metrics are stable. Focus on sustaining current practices.\",\"evidence_links\":[\"/api/v1/home?scope_type=org&scope_id=\"],\"suggested_experiments\":[\"Share the current playbook with new teams.\"],\"change_percent\":null,\"direction\":null,\"range_days\":14,\"compare_days\":14}]}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"user access token routed | people":{"body":"[]",
"digest":"e10d9e413a46484d",
"statements":1,
"status":200},
"user access token routed | people_drilldown_issues":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"user access token routed | people_drilldown_prs":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"user access token routed | people_metric":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"user access token routed | people_summary":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"user access token routed | quadrant":{"body":"{\"axes\":{\"x\":{\"metric\":\"wip\",\"label\":\"WIP\",\"unit\":\"items\"},\"y\":{\"metric\":\"throughput\",\"label\":\"Throughput\",\"unit\":\"items\"}},\"points\":[],\"annotations\":[],\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"e3c344211fd54ae4",
"statements":5,
"status":200},
"user access token routed | sankey":{"body":"{\"mode\":\"investment\",\"nodes\":[],\"links\":[],\"unit\":\"work_units\",\"label\":\"Investment flow\",\"description\":\"Where effort allocates across initiatives, areas, issue types, and work items.\",\"team_coverage\":null,\"repo_coverage\":null,\"distinct_team_targets\":null,\"distinct_repo_targets\":null,\"chosen_mode\":null,\"coverage\":null,\"unassigned_reasons\":null,\"flow_mode\":null,\"drill_category\":null,\"top_n_repos\":null}",
"digest":"4f7dc061cbb6787e",
"statements":3,
"status":200},
"user access token routed | work_unit_explain":{"body":"{\"detail\":\"Work unit wu-1 not found\"}",
"digest":"b07b9c93eced2a3a",
"statements":1,
"status":404},
"user access token routed | work_units":{"body":"[]",
"digest":"325371161d20e39f",
"statements":1,
"status":200},
"user access token routed, PDP shadow, cell off | drilldown_issues":{"body":"{\"items\":[]}",
"digest":"884760750f97cf60",
"statements":1,
"status":200},
"user access token routed, PDP shadow, cell off | drilldown_prs":{"body":"{\"items\":[]}",
"digest":"42ba51aa626abc85",
"statements":1,
"status":200},
"user access token routed, PDP shadow, cell off | explain":{"body":"{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"unit\":\"hours\",\"value\":0.0,\"delta_pct\":null,\"drivers\":[],\"contributors\":[],\"drilldown_links\":{\"prs\":\"/api/v1/drilldown/prs?metric=blocked_work\",\"issues\":\"/api/v1/drilldown/issues?metric=blocked_work\"},\"repositories\":null,\"source_url\":null,\"has_data\":false,\"has_prior_data\":false,\"source\":null,\"link_tier\":null,\"rate_state\":null,\"evidence_basis\":null}",
"digest":"a0950b0aeca0da55",
"statements":5,
"status":200},
"user access token routed, PDP shadow, cell off | filteroptions":{"body":"{\"teams\":[],\"repos\":[],\"services\":[],\"developers\":[],\"work_category\":[\"feature_delivery\",\"maintenance\",\"operational\",\"quality\",\"risk\",\"feature_delivery.customer\",\"feature_delivery.enablement\",\"feature_delivery.roadmap\",\"maintenance.debt\",\"maintenance.refactor\",\"maintenance.upgrade\",\"operational.incident_response\",\"operational.on_call\",\"operational.support\",\"quality.bugfix\",\"quality.reliability\",\"quality.testing\",\"risk.compliance\",\"risk.security\",\"risk.vulnerability\"],\"issue_type\":[],\"flow_stage\":[]}",
"digest":"844346916457cb9d",
"statements":8,
"status":200},
"user access token routed, PDP shadow, cell off | flame":{"body":"{\"detail\":\"Issue not found\"}",
"digest":"0c93624c7c068522",
"statements":1,
"status":404},
"user access token routed, PDP shadow, cell off | flame_aggregated":{"body":"{\"mode\":\"throughput\",\"unit\":\"items\",\"root\":{\"name\":\"Work Delivered\",\"value\":0.0,\"children\":[]},\"meta\":{\"window_start\":\"2026-01-01\",\"window_end\":\"2026-01-15\",\"filters\":{},\"notes\":[\"Work type classification unavailable. Using team-based grouping.\",\"No throughput data available for this window/scope.\"],\"approximation\":{\"used\":true,\"method\":\"inferred\"}}}",
"digest":"476ad86b24082828",
"statements":2,
"status":200},
"user access token routed, PDP shadow, cell off | heatmap":{"body":"{\"axes\":{\"x\":[\"00\",\"01\",\"02\",\"03\",\"04\",\"05\",\"06\",\"07\",\"08\",\"09\",\"10\",\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",\"17\",\"18\",\"19\",\"20\",\"21\",\"22\",\"23\"],\"y\":[\"Mon\",\"Tue\",\"Wed\",\"Thu\",\"Fri\",\"Sat\",\"Sun\"]},\"cells\":[],\"legend\":{\"unit\":\"hours\",\"scale\":\"linear\"},\"evidence\":null,\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"b6f3484c38784ef7",
"statements":1,
"status":200},
"user access token routed, PDP shadow, cell off | home":{"body":"{\"freshness\":{\"last_ingested_at\":null,\"latest_successful_sync_at\":null,\"sources\":{},\"coverage\":{\"repos_covered_pct\":null,\"prs_linked_to_issues_pct\":null,\"issues_with_cycle_states_pct\":null}},\"deltas\":[{\"metric\":\"cycle_time\",\"label\":\"Cycle Time\",\"value\":0.0,\"unit\":\"days\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"review_latency\",\"label\":\"Review Latency\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"throughput\",\"label\":\"Throughput\",\"value\":0.0,\"unit\":\"items\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"deploy_freq\",\"label\":\"Deploy Frequency\",\"value\":0.0,\"unit\":\"deploys\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"churn\",\"label\":\"Code Churn\",\"value\":0.0,\"unit\":\"loc\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"wip_saturation\",\"label\":\"WIP Saturation\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"blocked_work\",\"label\":\"Blocked Work\",\"value\":0.0,\"unit\":\"hours\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"change_failure_rate\",\"label\":\"Change Failure Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"rework_ratio\",\"label\":\"Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"pr_rework_ratio\",\"label\":\"PR Rework Ratio\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null},{\"metric\":\"ci_success\",\"label\":\"CI Success Rate\",\"value\":0.0,\"unit\":\"%\",\"delta_pct\":null,\"spark\":[],\"has_data\":false,\"has_prior_data\":false,\"rate_state\":null,\"repo_filter_applied\":null}],\"rework_theme_allocation\":[],\"summary\":[],\"tiles\":{\"understand\":{\"title\":\"Understand\",\"subtitle\":\"Flow stages\",\"link\":\"/explore?view=understand\"},\"measure\":{\"title\":\"Measure\",\"subtitle\":\"Coverage & freshness\",\"link\":\"/explore?view=measure\"},\"align\":{\"title\":\"Align\",\"subtitle\":\"Investment mix\",\"link\":\"/investment\"},\"execute\":{\"title\":\"Execute\",\"subtitle\":\"Top opportunities\",\"link\":\"/opportunities\"}},\"constraint\":{\"title\":\"\",\"claim\":\"\",\"evidence\":[],\"experiments\":[]},\"events\":[],\"health_state\":{\"status\":\"no_data\",\"headline\":\"\",\"summary\":\"\",\"as_of\":null},\"signals\":[],\"limiting_factor\":{\"claim\":\"\",\"why_it_matters\":\"\",\"recommended_action\":\"\",\"confidence\":\"low\",\"evidence_ref\":null},\"data_confidence\":{\"level\":\"low\",\"coverage_pct\":null,\"connected_sources\":[],\"missing_sources\":[],\"caveats\":[\"Coverage could not be computed from available lineage fields.\"]},\"filter_empty_reason\":null}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"user access token routed, PDP shadow, cell off | investment":{"body":"{\"theme_distribution\":{},\"subcategory_distribution\":{},\"evidence_quality_distribution\":{},\"evidence_quality_stats\":{\"mean\":null,\"stddev\":null,\"total\":0,\"band_counts\":{},\"quality_drivers\":[]},\"unit\":null,\"edges\":[],\"measure\":\"ISSUE_SIZE\",\"denominator\":0.0,\"unsized_unit_count\":0}",
"digest":"b5df5396114b20f6",
"statements":6,
"status":200},
"user access token routed, PDP shadow, cell off | investment_explain":{"body":"{\"summary\":\"This mix suggests work leans toward the leading themes shown, with subcategories providing the specific intent behind that allocation.\",\"top_findings\":[],\"confidence\":{\"level\":\"unknown\",\"quality_mean\":null,\"quality_stddev\":null,\"band_mix\":{},\"drivers\":[]},\"what_to_check_next\":[{\"action\":\"Review the largest subcategories\",\"why\":\"They drive the overall theme distribution\",\"where\":\"Subcategory breakdown panel\"}],\"anti_claims\":[\"This does not measure individual productivity.\",\"This does not assign intent or correctness to any work.\"],\"status\":\"invalid_llm_output\"}",
"digest":"4a41f4fd2e5d84be",
"statements":4,
"status":200},
"user access token routed, PDP shadow, cell off | investment_sunburst":{"body":"[]",
"digest":"402e8ed713b32417",
"statements":4,
"status":200},
"user access token routed, PDP shadow, cell off | opportunities":{"body":"{\"items\":[{\"id\":\"opp-0\",\"title\":\"Maintain steady flow\",\"rationale\":\"Key metrics are stable. Focus on sustaining current practices.\",\"evidence_links\":[\"/api/v1/home?scope_type=org&scope_id=\"],\"suggested_experiments\":[\"Share the current playbook with new teams.\"],\"change_percent\":null,\"direction\":null,\"range_days\":14,\"compare_days\":14}]}",
"digest":"5a624f4c74df2aec",
"statements":40,
"status":200},
"user access token routed, PDP shadow, cell off | people":{"body":"[]",
"digest":"e10d9e413a46484d",
"statements":1,
"status":200},
"user access token routed, PDP shadow, cell off | people_drilldown_issues":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"user access token routed, PDP shadow, cell off | people_drilldown_prs":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"user access token routed, PDP shadow, cell off | people_metric":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"user access token routed, PDP shadow, cell off | people_summary":{"body":"{\"detail\":\"Person not found\"}",
"digest":"9d132ea4b5a411b8",
"statements":1,
"status":404},
"user access token routed, PDP shadow, cell off | quadrant":{"body":"{\"axes\":{\"x\":{\"metric\":\"wip\",\"label\":\"WIP\",\"unit\":\"items\"},\"y\":{\"metric\":\"throughput\",\"label\":\"Throughput\",\"unit\":\"items\"}},\"points\":[],\"annotations\":[],\"repo_filter_applied\":null,\"filter_empty_reason\":null}",
"digest":"e3c344211fd54ae4",
"statements":5,
"status":200},
"user access token routed, PDP shadow, cell off | sankey":{"body":"{\"mode\":\"investment\",\"nodes\":[],\"links\":[],\"unit\":\"work_units\",\"label\":\"Investment flow\",\"description\":\"Where effort allocates across initiatives, areas, issue types, and work items.\",\"team_coverage\":null,\"repo_coverage\":null,\"distinct_team_targets\":null,\"distinct_repo_targets\":null,\"chosen_mode\":null,\"coverage\":null,\"unassigned_reasons\":null,\"flow_mode\":null,\"drill_category\":null,\"top_n_repos\":null}",
"digest":"4f7dc061cbb6787e",
"statements":3,
"status":200},
"user access token routed, PDP shadow, cell off | work_unit_explain":{"body":"{\"detail\":\"Work unit wu-1 not found\"}",
"digest":"b07b9c93eced2a3a",
"statements":1,
"status":404},
"user access token routed, PDP shadow, cell off | work_units":{"body":"[]",
"digest":"325371161d20e39f",
"statements":1,
"status":200}}`

// rule1Client records every statement and answers as an empty ClickHouse does: no rows, except the two
// system-catalog probes (they echo their bound names, so the routes go on) and the aggregate statements with no
// GROUP BY, which answer one row of zero values. It is this cell's own, so a change to another test's fake moves
// no row here.
type rule1Client struct {
	mu         sync.Mutex
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func (c *rule1Client) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	c.mu.Unlock()
	switch {
	case strings.Contains(statement, "FROM system.tables"):
		return &rule1Rows{values: append(rule1BindingStrings(bindings, "table"), rule1BindingStrings(bindings, "tables")...)}, nil
	case strings.Contains(statement, "FROM system.columns"):
		return &rule1Rows{values: append(rule1BindingStrings(bindings, "column"), rule1BindingStrings(bindings, "columns")...)}, nil
	case strings.Contains(statement, "AS sized_units") || strings.Contains(statement, "toString(max(computed_at))"):
		return &rule1Rows{zeroRow: true}, nil
	}
	return &rule1Rows{}, nil
}

func rule1BindingStrings(bindings []dhclickhouse.Binding, name string) []string {
	for _, binding := range bindings {
		if binding.Name != name {
			continue
		}
		switch value := binding.Value.(type) {
		case string:
			return []string{value}
		case []string:
			return value
		}
	}
	return nil
}

// rule1Rows answers values one per row into the first string column, or (zeroRow) one row that leaves every column
// at its zero value.
type rule1Rows struct {
	values  []string
	zeroRow bool
	index   int
}

func (r *rule1Rows) Next() bool {
	limit := len(r.values)
	if r.zeroRow {
		limit = 1
	}
	if r.index >= limit {
		return false
	}
	r.index++
	return true
}

func (r *rule1Rows) Scan(dest ...any) error {
	if r.zeroRow || len(dest) == 0 {
		return nil
	}
	if target, ok := dest[0].(*string); ok {
		*target = r.values[r.index-1]
	}
	return nil
}

func (r *rule1Rows) Err() error   { return nil }
func (r *rule1Rows) Close() error { return nil }

type rule1Route struct {
	class, method, query, body string
	pathValues                 map[string]string
	handler                    func(*rule1Client) http.HandlerFunc
}

func rule1Need[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func rule1Routes() []rule1Route {
	dates := "start_date=2026-01-01&end_date=2026-01-15"
	person := map[string]string{"person_id": "person-1"}
	return []rule1Route{
		{"quadrant", http.MethodGet, "type=wip_throughput&" + dates, "", nil, func(c *rule1Client) http.HandlerFunc { return newQuadrantWorkHandler(c) }},
		{"heatmap", http.MethodGet, "type=temporal_load&metric=review_wait_density&range_days=30", "", nil, func(c *rule1Client) http.HandlerFunc { return newHeatmapWorkHandler(c) }},
		{"sankey", http.MethodGet, dates, "", nil, func(c *rule1Client) http.HandlerFunc { return newSankeyGetHandler(c) }},
		{"home", http.MethodGet, dates, "", nil, func(c *rule1Client) http.HandlerFunc { return newHomeGetHandler(c, nil) }},
		{"opportunities", http.MethodGet, dates, "", nil, func(c *rule1Client) http.HandlerFunc { return newOpportunitiesGetHandler(c) }},
		{"filteroptions", http.MethodGet, "", "", nil, func(c *rule1Client) http.HandlerFunc { return newFilterOptionsWorkHandler(c) }},
		{"investment", http.MethodGet, dates, "", nil, func(c *rule1Client) http.HandlerFunc {
			return newInvestmentGetHandler(rule1Need(investment.NewReader(c)))
		}},
		{"investment_sunburst", http.MethodGet, dates, "", nil, func(c *rule1Client) http.HandlerFunc {
			return newInvestmentSunburstGetHandler(rule1Need(investment.NewReader(c)))
		}},
		{"investment_explain", http.MethodPost, "llm_provider=mock&" + dates, "", nil, func(c *rule1Client) http.HandlerFunc {
			return newInvestmentExplainWorkHandler(rule1Need(investmentexplain.NewReader(c)), nil, nil, nil, testPlatformGate{})
		}},
		{"drilldown_prs", http.MethodGet, dates, "", nil, func(c *rule1Client) http.HandlerFunc {
			return newDrilldownPRsGetHandler(rule1Need(drilldown.NewReader(c)))
		}},
		{"drilldown_issues", http.MethodGet, dates, "", nil, func(c *rule1Client) http.HandlerFunc {
			return newDrilldownIssuesGetHandler(rule1Need(drilldown.NewReader(c)))
		}},
		{"work_units", http.MethodGet, dates, "", nil, func(c *rule1Client) http.HandlerFunc {
			return newWorkUnitsGetHandler(rule1Need(investmentexplain.NewReader(c)))
		}},
		{"work_unit_explain", http.MethodPost, "llm_provider=mock&" + dates, "", map[string]string{"work_unit_id": "wu-1"}, func(c *rule1Client) http.HandlerFunc {
			return newWorkUnitExplainHandler(rule1Need(investmentexplain.NewReader(c)), nil, nil, nil, testPlatformGate{})
		}},
		{"people", http.MethodGet, "q=ali", "", nil, func(c *rule1Client) http.HandlerFunc {
			return newPeopleSearchHandler(rule1Need(people.NewReader(c)))
		}},
		{"people_summary", http.MethodGet, dates, "", person, func(c *rule1Client) http.HandlerFunc {
			return newPeopleSummaryHandler(rule1Need(people.NewReader(c)))
		}},
		{"people_metric", http.MethodGet, "metric=churn&" + dates, "", person, func(c *rule1Client) http.HandlerFunc {
			return newPeopleMetricHandler(rule1Need(people.NewReader(c)))
		}},
		{"people_drilldown_prs", http.MethodGet, dates, "", person, func(c *rule1Client) http.HandlerFunc {
			return newPeopleDrilldownPRsHandler(rule1Need(people.NewReader(c)))
		}},
		{"people_drilldown_issues", http.MethodGet, dates, "", person, func(c *rule1Client) http.HandlerFunc {
			return newPeopleDrilldownIssuesHandler(rule1Need(people.NewReader(c)))
		}},
		{"explain", http.MethodGet, "metric=" + explain.MetricNames()[0] + "&" + dates, "", nil, func(c *rule1Client) http.HandlerFunc {
			return newExplainGetHandler(rule1Need(explain.NewReader(c)))
		}},
		{"flame", http.MethodGet, "entity_type=issue&entity_id=abc", "", nil, func(c *rule1Client) http.HandlerFunc { return newFlameWorkHandler(c) }},
		{"flame_aggregated", http.MethodGet, "mode=throughput&" + dates, "", nil, func(c *rule1Client) http.HandlerFunc {
			var client aggflame.QueryClient = c
			return newFlameAggregatedWorkHandler(client)
		}},
	}
}

// rule1Class is one token class of a superuser with no org: the bearer, whether the edge verifier is routed, and
// the PDP gate mode and 5.17 cells the request runs under ("" = no gate).
type rule1Class struct {
	name        string
	routed      bool
	mode, cells string
	bearer      func(t *testing.T, m *accessMinter, f *restFixture) string
}

func rule1Gate(t *testing.T, mode, cells string) *pdp.Gate {
	t.Helper()
	srv := pdptest.New(t)
	client, err := pep.New(pep.Config{BaseURL: srv.URL, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return pdp.New(pdp.Config{Mode: mode, Switch: authzenforce.Parse(authzenforce.QueryAPI, cells), Meter: provider.Meter("test"),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Decider: client})
}

func rule1Classes() []rule1Class {
	edge := func(t *testing.T, _ *accessMinter, _ *restFixture) string {
		return ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), tokenVersion: 5})
	}
	user := func(_ *testing.T, m *accessMinter, _ *restFixture) string {
		return m.mint(ecUser, map[string]any{"org_id": nil}, nil)
	}
	return []rule1Class{
		{"edge token", false, "", "", edge},
		{"edge token routed", true, "", "", edge},
		{"user access token routed", true, "", "", user},
		{"user access token routed, PDP shadow, cell off", true, pdp.ModeShadow, "", user},
		{"envelope", false, "", "", func(t *testing.T, _ *accessMinter, f *restFixture) string {
			return iaEnvelope(t, f.priv, principal.Claims{Role: "admin", IsSuperuser: true, IsSuperuserVerified: true})
		}},
	}
}

type rule1Answer struct {
	Status     int    `json:"status"`
	Body       string `json:"body"`
	Statements int    `json:"statements"`
	Digest     string `json:"digest"`
}

// rule1Serve runs route behind authenticateRESTRequest (the REST fixture) as a superuser with no org of class.
func rule1Serve(t *testing.T, route rule1Route, class rule1Class) rule1Answer {
	t.Helper()
	m := newAccessMinter(t)
	store := newAccessStore()
	store.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	var setup *accessSetup
	if class.routed {
		setup = testAccess(m.keys, nil, accessTestNow)
	}
	client := &rule1Client{}
	handler := route.handler(client)
	f := newRESTFixtureServing(t, store, setup, true, func(w http.ResponseWriter, r *http.Request) {
		for name, value := range route.pathValues {
			r.SetPathValue(name, value)
		}
		if _, ok := authctx.FromContext(r.Context()); !ok {
			t.Fatal("no claims on the request")
		}
		handler(w, r)
	})
	target := "/api/v1/home"
	if route.query != "" {
		target += "?" + route.query
	}
	request := httptest.NewRequest(route.method, target, strings.NewReader(route.body))
	request.Header.Set("Authorization", "Bearer "+class.bearer(t, m, f))
	recorder := httptest.NewRecorder()
	if class.mode == "" {
		f.handler.ServeHTTP(recorder, request)
	} else {
		gate := rule1Gate(t, class.mode, class.cells)
		planeHandler(f.handler.(*http.ServeMux), gate).ServeHTTP(recorder, request)
		gate.Wait()
	}
	body, _ := io.ReadAll(recorder.Result().Body)
	client.mu.Lock()
	defer client.mu.Unlock()
	// Handlers run some reads concurrently: the digest is over the sorted statement blocks.
	blocks := make([]string, 0, len(client.statements))
	for i, statement := range client.statements {
		var b strings.Builder
		fmt.Fprintf(&b, "%s\n", statement)
		for _, binding := range client.bindings[i] {
			if _, isTime := binding.Value.(time.Time); isTime {
				fmt.Fprintf(&b, "  %s=<time>\n", binding.Name)
				continue
			}
			fmt.Fprintf(&b, "  %s=%v\n", binding.Name, binding.Value)
		}
		blocks = append(blocks, b.String())
	}
	sort.Strings(blocks)
	digest := sha256.New()
	for _, block := range blocks {
		_, _ = io.WriteString(digest, block)
	}
	return rule1Answer{Status: recorder.Code, Body: strings.TrimSpace(string(body)), Statements: len(client.statements),
		Digest: hex.EncodeToString(digest.Sum(nil))[:16]}
}

func TestRule1PlatformAdminWithNoOrgGetsMainsRESTAnswers(t *testing.T) {
	quietStdLog(t)
	got := map[string]rule1Answer{}
	for _, class := range rule1Classes() {
		for _, route := range rule1Routes() {
			got[class.name+" | "+route.class] = rule1Serve(t, route, class)
		}
	}
	keys := make([]string, 0, len(got))
	for key := range got {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var want map[string]rule1Answer
	if err := json.Unmarshal([]byte(rule1NoOrgGolden), &want); err != nil {
		raw, _ := json.Marshal(got)
		t.Fatalf("golden: %v; answers: %s", err, raw)
	}
	if len(got) != len(rule1Classes())*21 || len(want) != len(got) {
		t.Fatalf("%d answers, golden has %d, want %d", len(got), len(want), len(rule1Classes())*21)
	}
	for _, key := range keys {
		if got[key] != want[key] {
			t.Errorf("%s: got %+v, main answered %+v", key, got[key], want[key])
		}
	}
}
