# Part 1 — Existing Platforms: What They Measure

## System Flow (Connect → Sync → Calculate)

- connect: Github, GitLab, IDEs, CI/CD, SCM, Databases
- sync: code events, PRs, reviews, issues, deployments
- calculate: metrics, scores, trends, risk indicators, health signals

---

## LinearB — Flow & Team Efficiency

Focus: flow, predictability, PR health, resource allocation.

**Metrics**

- Cycle Time

  - Coding Time
  - Pickup Time
  - Review Time
  - Deploy Time

- Deployment Frequency
- Lead Time for Changes
- Work Breakdown

  - New work vs rework vs unplanned vs refactor

- Investment Profile

  - Strategic work %
  - Maintenance %
  - Unplanned work %

- PR Process Health

  - PR size
  - Review depth
  - Review response time
  - Time-to-approve

- Bottlenecks

  - Review congestion
  - Idle WIP

- Review Silos / Single-reviewer dependencies

---

## GitPrime / Pluralsight Flow — Engineering Behavior

Focus: individual contribution patterns and risk.

**Metrics**

- Impact Score (weighted contribution proxy)
- Efficiency (merged vs reworked code)
- Code Fundamentals

  - Active days
  - Commits/day
  - PRs opened/merged
  - Lines changed
  - Rework rate
  - Churn %

- Collaboration

  - Review load
  - Review turnaround

- Risk Indicators

  - High-churn files
  - Ownership hotspots

- Health Signals

  - Sustained intensity
  - Weekend / off-hours work
  - Irregular contribution patterns

---

## GitLab Analytics — Value Stream & DevOps

Focus: delivery pipeline and DORA.

**Metrics**

- Lead Time for Changes
- Value Stream Stages

  - Issue → Code → Review → Merge → Deploy

- Merge Request Analytics

  - MR size
  - Approval count
  - Discussion count
  - Time to merge

- CI/CD

  - Pipeline duration
  - Success rate
  - MTTR
  - Change failure rate

- Contribution Analytics
- Security / Vulnerability introduction rate

---

## typeapp.app — Cognitive Load & Well-being

Focus: IDE-level behavior and burnout risk.

**Metrics**

- Flow State Duration
- Context Switching

  - File switches
  - Tab switches
  - Project switches

- Typing Behavior

  - Error rate
  - Undo/redo density

- Distraction Index
- Wellness Signals

  - Late-night streaks
  - Burst–burnout cycles

- Cognitive Load Index

---

# Part 2 — Unified Metrics Framework

## 1. Delivery & Velocity

- Coding Time
- Review Time
- Rework Time
- Deploy Time
- Deployment Frequency
- Throughput (PRs, issues, story points)
- Work Composition
- Investment Profile (Strategic vs Maintenance)

---

## 2. Code Quality & Risk

- Code Risk Index
  - Churn
  - Ownership concentration
  - Hotspots
  - Cyclomatic Complexity (radon; snapshots persisted and loaded via `store.get_complexity_snapshots()`)
- Rework Rate
- PR Quality

  - Size distribution
  - Comment density
  - Rejection rate

- Stability

  - Bugs introduced
  - Defect escape rate

---

## 3. Collaboration & Team Dynamics

- Review Responsiveness
- Reviewer Load
- Review Network Graph
- Knowledge Distribution

  - Bus factor
  - Contribution entropy

- Communication Density

---

## 4. Developer Health & Cognitive Load

- Context Switching Score
- Focus Time Index
- Work Pattern Health

  - Late-night activity
  - Weekend work

- Editing & Typing Behavior

---

## 5. Systemic & Process Health

- Bottleneck Index
- WIP Congestion
- Stale Work Detection
- Predictability Index

---

# Part 3 — Developer Health Model

**Four Dimensions**

1. Delivery — speed, throughput, predictability
2. Durability — quality, stability, long-term risk
3. Developer Well-being — flow, burnout risk, load
4. Dynamics — collaboration, knowledge spread

This framework fully covers and extends LinearB, GitPrime, GitLab, and typeapp.app with a single coherent analytics model.

Absolutely — I’ll continue _without pause_ and expand the entire system:

- **Data model**
- **Events and ingestion**
- **Scoring & formulas**
- **Detailed metric definitions**
- **Architecture**
- **Dashboards & views**
- **Roadmap**

This will give you a complete **draft developer health platform** capable of rivaling LinearB, GitPrime, TypeApp, and GitLab Analytics _in one unified product_.

---

# 🚧 PART 4 — Detailed Data Model

1. **Git Events** (commits, branches, PRs, reviews)
2. **Issue/Task Events** (create, update, close, transitions)
3. **CI/CD Events** (pipelines, deploys, failures, restores)
4. **IDE Telemetry Events** (typing, context switching, focus time)

---

# 🧮 PART 5 — Metric Definitions & Formulas

Everything the earlier platforms measure + extended metrics.

---

# 1️⃣ DELIVERY METRICS (Flow & Velocity)

## **Cycle Time**

```
cycle_time = merged_at - first_commit_timestamp_in_branch
```

### **Coding Time**

```
coding_time = pr_created_at - first_commit_timestamp
```

### **Review Time**

```
review_time = first_review_timestamp - pr_created_at
```

### **Rework Time**

```
rework_time = merged_at - first_approval_timestamp
```

### **Deploy Time**

```
deploy_time = first_prod_deploy_timestamp - merged_at
```

---

## **Throughput**

```
features_completed = count(tasks.type=="feature")
bugs_fixed = count(tasks.type=="bug")
prs_merged = count(prs where state="merged")
```

---

## **Work Mix**

```
rework_percent = churn_loc_last_30_days / total_loc_last_30_days
new_work_percent = new_features_loc / total_loc
bugfix_percent = bugfix_loc / total_loc
refactor_percent = refactor_loc / total_loc
```

---

## **DORA Metrics**

- Deployment frequency = deploy_count / time_period
- Lead time for changes = average(coding_time + review_time + deploy_time)
- MTTR = mean(time_from_incident_to_restore)
- Change failure rate = failed_deploys / total_deploys

---

# 2️⃣ CODE QUALITY & RISK METRICS

## **Churn**

```
commit_churn = (loc_added + loc_deleted)
rework_rate = churn_in_30_days / total_loc_touched
```

## **Hotspot Risk**

A weighted model:

```
hotspot_score = file_churn * number_of_contributors * commit_frequency
```

## **PR Size**

```
pr_size = lines_added + lines_deleted
```

## **Review Quality**

```
comments_per_review = total_review_comments / number_of_reviews
review_coverage = files_reviewed / files_changed
```

---

# 3️⃣ COLLABORATION & TEAM DYNAMICS

## **Review Responsiveness**

```
time_to_first_review = min(review.submitted_at) - pr_created_at
```

## **Collaboration Network**

Track edges:
`author → reviewer`

Metrics:

- reciprocity
- centrality
- isolated contributors
- bottleneck reviewers

## **Knowledge Distribution (Bus Factor)**

```
ownership_score(file) = commits_by_user / total_commits_on_file
bus_factor = number_of_files_with_ownership>0.75
```

---

# 4️⃣ DEVELOPER HEALTH & COGNITIVE LOAD

## **Flow Score (0–100)**

Uses editor telemetry:

```
focus_blocks = sequences of 10+ min uninterrupted editing
context_switch_penalty = file_switches + tab_switches
flow_score = focus_blocks * 10 - context_switch_penalty * 2
```

## **Cognitive Load**

```
load_index = (avg_time_between_edits + tab_switch_rate + undo_density)
```

## **Burnout Indicators**

```
late_night_activity = activity between 12am–5am
weekend_activity = sat/sun commits
burst_cycles = # of commit storms within <2h
risk_score = weighted_sum(late_night, weekend, bursts)
```

---

# 5️⃣ SYSTEM HEALTH METRICS

## **WIP Congestion**

```
stale_prs = prs open > X days
queued_reviews = prs waiting for review > Y hours
build_queue_length = count(pipelines waiting)
```

## **Predictability**

```
estimate_accuracy = |estimate - actual| / estimate
cycle_time_variance = variance(cycle_time)
```

---

# 📊 PART 7 — Dashboard Drafts

## 1. **Org Dashboard**

- Cycle Time
- Deployment Frequency
- Hotspot files
- Bottleneck teams
- Investment Profile
- Predictability Index

## 2. **Team Dashboard**

- PR review health
- Collaboration graph
- Throughput
- Roadmap completion forecast
- Team burnout risk

## 3. **Developer Dashboard**

- Flow score
- Deep work patterns
- Impact score
- Review contribution
- Churn & rework (healthy or high)

## 4. **Repo Dashboard**

- Hotspots
- Bus factor
- Risky files
- Churn trends
- Contribution activity

---

# Developer Health Platform – Full Draft Specification

## 1. Metric formulations & weights

### 1.1 Normalization & scoring pattern

General pattern (so everything fits into 0–100):

- **Raw metric** → normalize into \[0,1\] using either
  - min–max based on historical window, or
  - percentile within org/team.
- **Direction**: some metrics “lower is better”.
  - If lower is better: `score = 1 - normalized_value`
  - If higher is better: `score = normalized_value`
- **Metric Score (0–100)**: `metric_score = score * 100`

All metrics below follow: **raw formula → normalized score** and then roll into dimension scores.

---

### 1.2 Delivery metrics (Flow & DORA)

#### 1.2.1 Cycle time

Per PR:

```text
cycle_time(pr) = merged_at - first_commit_time_on_branch(pr)
```

Normalized (lower = better):

```text
ct_norm = clip( (cycle_time - CT_min) / (CT_max - CT_min), 0, 1 )
ct_score = (1 - ct_norm) * 100
```

#### 1.2.2 Coding / Review / Deploy times

For each PR:

```text
coding_time  = pr_created_at - first_commit_time
review_time  = time_of_first_review - pr_created_at
rework_time  = merged_at - time_of_first_approval
deploy_time  = first_prod_deploy_at - merged_at
```

Flow Balance score (detect one-stage dominance):

```text
flow_balance_ratio = max(coding_time, review_time, rework_time, deploy_time) / cycle_time
# If one stage > 60% of total, that’s a bottleneck
balance_norm = clip( (flow_balance_ratio - 0.25) / (0.75 - 0.25), 0, 1 )
flow_balance_score = (1 - balance_norm) * 100
```

#### 1.2.3 Deployment frequency

For a team in a period T (e.g., 14 days):

```text
deploy_freq = number_of_prod_deploys / T_days
```

Normalize vs org history:

```text
df_norm = clip( (deploy_freq - DF_min) / (DF_max - DF_min), 0, 1 )
deploy_freq_score = df_norm * 100
```

#### 1.2.4 DORA metrics

- **Lead time for changes** = average cycle_time for prod-bound PRs → same pattern as cycle time.

- **MTTR** (Mean Time To Restore):

```text
MTTR = avg(incident_resolved_at - incident_detected_at)
mttr_norm, mttr_score = lower-is-better normalization
```

- **Change failure rate**:

```text
change_failure_rate = failed_deploys / total_deploys
cfr_norm = clip( (change_failure_rate - CFR_min) / (CFR_max - CFR_min), 0, 1 )
cfr_score = (1 - cfr_norm) * 100
```

#### 1.2.5 Delivery Dimension Score

For a team or org:

- `Cycle Time score` (CT)
- `Deploy Frequency score` (DF)
- `Lead Time score` (LT)
- `MTTR score` (MT)
- `Change Failure Rate score` (CF)

Weights (example):

```text
DeliveryScore = 0.30*CT + 0.20*DF + 0.20*LT + 0.15*MT + 0.15*CF
```

---

### 1.3 Code quality & risk metrics

#### 1.3.1 Churn & rework

Per file over window W (e.g., 30 days):

```text
loc_touched = Σ(|loc_added| + |loc_deleted|)
loc_reworked_soon = Σ(loc_modified_within_30d_of_being_added)

rework_rate = loc_reworked_soon / loc_touched
```

Normalize (lower is better):

```text
rw_norm = clip( (rework_rate - RW_min) / (RW_max - RW_min), 0, 1 )
rework_score = (1 - rw_norm) * 100
```

#### 1.3.2 File hotspot score

For each file f:

```text
churn_f        = loc_touched_in_W
contributors_f = number_of_distinct_authors_in_W
commit_freq_f  = commits_touching_file_in_W / days_in_W

hotspot_raw = α*log(1 + churn_f) + β*contributors_f + γ*commit_freq_f
# α,β,γ ~ 0.4, 0.3, 0.3 initially
```

Normalize across files:

```text
hs_norm = (hotspot_raw - HS_min) / (HS_max - HS_min)
hotspot_score_file = hs_norm * 100
```

Team-level **Code Risk score** can be e.g. 80th percentile of hotspot scores in that team’s modules, inverted:

```text
risk_raw = P80(hotspot_score_file_for_team)
risk_score = 100 - risk_raw
```

Ownership concentration (for hotspot drivers) is derived from git blame data:

```text
ownership_concentration = max(lines_by_author) / total_lines
```

Synthetic fixtures include an expanded file set to improve blame-driven ownership coverage.
Blame-only sync is available via `cli.py sync blame --provider <local|github|gitlab>`.

#### 1.3.3 PR Size

```text
pr_size = loc_added + loc_deleted
```

Normalize with a sweet spot (e.g. 20–400 LOC):

```text
if pr_size < target_min:
    size_score = 40 + 60*(pr_size / target_min)   # Very tiny PRs not ideal
elif pr_size <= target_max:
    size_score = 100
else:
    overflow = min(pr_size - target_max, cap)
    size_score = max(40, 100 - overflow / scale) # penalize very large ones
```

#### 1.3.4 Review depth & coverage

```text
review_comments_per_pr = total_comments_on_pr / 1
review_coverage = reviewed_files_count / files_changed
```

Combined **PR Review Health score**:

```text
depth_score = sigmoid( a * (review_comments_per_pr - target_comments) )
coverage_score = review_coverage * 100

review_health_score = 0.4*size_score + 0.3*depth_score + 0.3*coverage_score
```

Where `sigmoid(x) = 100 / (1 + exp(-x))` scaled to \[0,100\].

#### 1.3.5 Quality & Durability Dimension Score

Metrics:

- Rework score
- Code risk score
- Review health score
- Defect introduction rate score

Weights:

```text
DurabilityScore = 0.35*ReworkScore + 0.30*CodeRiskScore
                  + 0.20*ReviewHealthScore + 0.15*DefectRateScore
```

---

### 1.4 Collaboration & team dynamics metrics

#### 1.4.1 Review responsiveness

```text
time_to_first_review = avg( first_review_timestamp - pr_created_at )

# lower better
resp_norm = (time_to_first_review - RESP_min) / (RESP_max - RESP_min)
ReviewResponsivenessScore = (1 - resp_norm) * 100
```

#### 1.4.2 Review load & reciprocity

For each user u in window W:

```text
reviews_given_u  = count(reviews where reviewer_id = u)
reviews_received_u = count(prs authored_by_u that had reviews)
review_balance_u = reviews_given_u / (reviews_received_u + 1)
```

Team reciprocity via dispersion:

```text
team_balance = variance(review_balance_u across team)
ReciprocityScore = 100 - normalized_variance(team_balance)
```

#### 1.4.3 Knowledge distribution / Bus factor

- **Bus Factor (Truck Factor)**: The smallest number of developers that account for >= 50% of the total code churn in the window.
- **Code Ownership Gini**: Gini coefficient of code contribution (churn) distribution. 0.0 = perfect equality, 1.0 = perfect inequality.

```text
bus_factor = number_of_devs_contributing_50_percent_churn
gini = (2 * sum(i * y_i) / (n * sum(y_i))) - (n + 1) / n
```

#### 1.4.4 Dynamics Dimension Score

Use:

- ReviewResponsivenessScore
- ReciprocityScore
- BusFactorScore
- Cross-team-review score (optional)

```text
DynamicsScore = 0.30*ReviewResponsivenessScore
              + 0.25*ReciprocityScore
              + 0.30*BusFactorScore
              + 0.15*CrossTeamReviewScore
```

---

### 1.5 Developer well-being & cognitive load metrics

#### 1.5.1 Flow Score

From editor events:

```text
focus_block = a continuous period ≥ 10 min
              with no context_switch events (tab/file/project) and
              active edits every ≤ 2 min

num_blocks   = count(focus_block) in day
avg_block_len = avg(duration of focus_block)
context_switches = count(context_switch events per hour)
interruptions = count(non-editor-window-focus events per hour)

flow_raw = w1*num_blocks + w2*avg_block_len - w3*context_switches - w4*interruptions
```

Normalize:

```text
flow_norm = (flow_raw - FLOW_min) / (FLOW_max - FLOW_min)
FlowScore = clip(flow_norm, 0, 1) * 100
```

Start with `w1=2, w2=0.1, w3=1, w4=1` as tunables.

#### 1.5.2 Cognitive load index

Signals:

- avg time between edits in same file
- undo/redo density
- error bursts (rapid changes + reverts)

Example:

```text
avg_edit_gap = avg(time_between_edits_same_file)
undo_density = total_undo_ops / total_edits
switch_density = tab_switches / hour

load_raw = a1*avg_edit_gap + a2*undo_density + a3*switch_density
LoadScore = (1 - normalized(load_raw)) * 100
```

Higher score = better (manageable load).

#### 1.5.3 Burnout risk score

Signals in last 14–30 days:

```text
late_night_ratio = commits_or_edits_between_00_05 / total_commits_or_edits
weekend_ratio    = weekend_commits / total_commits
streak_length    = longest_consecutive_days_with_activity
burst_index      = fraction_of_commits_in_top_20% busiest_hours
```

Combine:

```text
burnout_raw = b1*late_night_ratio + b2*weekend_ratio + b3*streak_length_norm + b4*burst_index
BurnoutRiskScore = (1 - normalized(burnout_raw)) * 100
```

(High score = low risk.)

#### 1.5.4 Well-being Dimension Score

Combine:

```text
WellBeingScore = 0.40*FlowScore + 0.25*LoadScore + 0.35*BurnoutRiskScore
```

---

### 1.6 System & process health metrics

#### 1.6.1 Bottleneck index

For each stage s in {coding, review, qa, deploy}:

```text
stage_time_s = avg(time_spent_in_stage_s)
stage_fraction_s = stage_time_s / total_cycle_time
bottleneck_raw = max(stage_fraction_s)
BottleneckScore = (1 - normalized(bottleneck_raw)) * 100
```

#### 1.6.2 WIP congestion

```text
stale_prs_ratio = stale_prs / total_open_prs
queue_length    = queued_reviews / team_size
build_queue     = queued_builds / historical_median

congestion_raw = c1*stale_prs_ratio + c2*queue_length + c3*build_queue
CongestionScore = (1 - normalized(congestion_raw)) * 100
```

#### 1.6.3 Predictability

Defined as **Completion Rate** (how well the team clears its plate).

```text
predictability_score = items_completed / (items_completed + wip_count_end_of_day)
```

#### 1.6.4 System Health Dimension Score

```text
SystemHealthScore = 0.40*BottleneckScore
                  + 0.30*CongestionScore
                  + 0.30*PredictabilityScore
```

---

## 2. Healthy vs unhealthy thresholds (initial draft)

These are initial org-agnostic defaults; tune them to your context.

### Delivery

- **Cycle time (PR → deploy)**

  - Excellent: < 24h
  - Healthy: 24–72h
  - At risk: 3–7 days
  - Unhealthy: > 7 days

- **Time to first review**

  - Excellent: < 2h
  - Healthy: 2–8h
  - At risk: 8–24h
  - Unhealthy: > 24h

- **Deployment frequency**

  - Excellent: multiple times per day
  - Healthy: daily–few times/week
  - At risk: < 1/week
  - Unhealthy: < 1/month

- **Change failure rate**
  - Excellent: < 10%
  - Healthy: 10–20%
  - At risk: 20–30%
  - Unhealthy: > 30%

### Code Quality & Risk

- **Rework rate (loc re-touched within 30 days)**

  - Excellent: < 10%
  - Healthy: 10–20%
  - At risk: 20–35%
  - Unhealthy: > 35%

- **Average PR size (median)**

  - Healthy: 50–300 LOC
  - At risk: 300–600 LOC
  - Unhealthy: > 600 LOC regularly

- **Hotspot concentration (files with high risk)**
  - Healthy: < 5% of files
  - At risk: 5–15%
  - Unhealthy: > 15%

### Dynamics & Collaboration

- **Bus factor (single-owner files >75%)**

  - Healthy: < 25% of team files
  - At risk: 25–50%
  - Unhealthy: > 50%

- **Review reciprocity**
  - Healthy: most devs in [.5, 2] given/received ratio
  - Unhealthy: many devs only receiving or only giving

### Well-being

- **Late-night ratio**

  - Healthy: < 5%
  - At risk: 5–15%
  - Unhealthy: > 15%

- **Weekend ratio**

  - Healthy: < 5–10%
  - At risk: 10–25%
  - Unhealthy: > 25%

- **Flow time** (uninterrupted focus per day)
  - Excellent: ≥ 2–3 hours
  - Healthy: 1–2 hours
  - At risk: < 1 hour
  - Unhealthy: mostly fragmented 10–15 min blocks

---

## 3. Org-wide “Developer Health Score”

Use the **4D model**:

1. **DeliveryScore**
2. **DurabilityScore**
3. **WellBeingScore**
4. **DynamicsScore**

Optionally add **SystemHealthScore** as a 5th dimension.

### 3.1 Dimension aggregation

Example weights:

```text
DH_Delivery   = DeliveryScore
DH_Durability = DurabilityScore
DH_WellBeing  = WellBeingScore
DH_Dynamics   = DynamicsScore
DH_System     = SystemHealthScore

DeveloperHealthScore = 0.25*DH_Delivery
                      + 0.25*DH_Durability
                      + 0.20*DH_WellBeing
                      + 0.20*DH_Dynamics
                      + 0.10*DH_System
```

Compute per org, per team, per repo, per individual (with some metric substitutions).

---

## 7. Predictive AI models

### 7.1 Delivery risk prediction (PR-level)

**Goal:** Predict whether a PR will be “slow” or “problematic” at creation time.

- **Label:** `slow_pr = cycle_time > org_p75_cycle_time` (binary)
- **Features:** PR size, file count, hotspot involvement, author historical cycle time, team WIP, time-of-day/day-of-week, reviewer count
- **Model:** Gradient boosting (XGBoost/LightGBM) or logistic regression
- **Output:** `P(slow_pr)` + suggested mitigations (split PR, add reviewer, etc.)

### 7.2 Burnout risk prediction (user-level)

**Goal:** Predict burnout risk for each dev next 2–4 weeks.

- **Label:** Proxy: future spike in late-night/weekend + drop in FlowScore (or HR flag if integrated)
- **Features:** late-night ratio, weekend ratio, FlowScore mean/variance, streak length, review load, context switching, team stress/incident load
- **Model:** time-series classification or GBM on rolling window features
- **Output:** `BurnoutRiskProbability` 0–1 → 0–100 display

### 7.3 Expected cycle time / delivery date

**Goal:** Predict cycle time for a new PR or task.

- **Label:** numeric cycle time
- **Features:** 7.1 + repo/component + team throughput context
- **Model:** regression (GBM, random forest, or linear w/ interactions)
- **Output:** predicted cycle time + confidence interval

### 7.4 Hotspot evolution

**Goal:** Predict which files will become high-risk hotspots soon.

- **Label:** `future_hotspot = hotspot_score_file > threshold in next W`
- **Features:** churn, entropy, commit frequency, bug density, ownership volatility
- **Model:** GBM / logistic regression
- **Output:** “emerging risky modules” highlights

---

## 8. Dashboard mockups (textual Figma)

### 8.1 Org Dashboard (“Executive View”)

**Top bar**

- Time range selector (Last 7 / 30 / 90 days)
- Org dropdown
- Overall Developer Health Score pill (e.g. 82/100) + sparkline

**Row 1: 4 dimension cards**

- Delivery: score + median cycle time, deploy frequency, change failure rate
- Durability: score + rework %, hotspot count, defect rate
- Well-being: score + flow hrs/dev/day, burnout risk index
- Dynamics: score + review responsiveness, bus factor

**Row 2: Charts**

- Cycle Time Breakdown (stacked: coding, review, deploy) over time
- Deploy Frequency vs Change Failure Rate (dual-axis)

**Row 3: Tables**

- Teams ranked by Developer Health Score
- Top 10 hotspot repos/services

### 8.2 Team Dashboard (“Eng Lead View”)

**Header**

- Team name + date range + Team Health Score

**Row 1: KPIs**

- Delivery: cycle time, time to first review, deploys/week
- Quality: rework %, defect rate, top hotspots
- Well-being: flow hrs/dev/day, late-night %, burnout risk
- Dynamics: review delay, bus factor, review balance

**Row 2: Charts**

- PR Flow Timeline (scatter: PRs by age vs cycle time, colored by size)
- Team Review Network (graph: nodes=devs, edges=reviews)

**Row 3: People table**

- Dev, Impact proxy, FlowScore, BurnoutRisk, ReviewLoad, Churn%
- Flags: high review load, high late-night work, isolation

### 8.3 Developer Dashboard (“Personal View”)

**Header**

- Dev name + “Your Health this month: 78/100”

**Row 1: Summary**

- Delivery vs team, Quality vs team, Flow, Burnout risk

**Row 2: Charts**

- Flow over time
- Work mix (new vs bugfix vs refactor)

**Row 3: Suggestions**

- Auto-generated coaching insights (split PRs, protect focus blocks, reduce late-night pattern, etc.)

### 8.4 Repo / Service Dashboard

**Header**

- Repo name + risk indicator

**Row 1: Risk & Quality**

- Risk gauge
- Hotspot file count
- Defects tied to repo

**Row 2: Hotspots table**

- file path, risk, churn, contributors, linked bugs

**Row 3: Ownership & Bus factor**

- contributor distribution
- single-owner file visualization
