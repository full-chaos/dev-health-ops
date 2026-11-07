package shadowvenue

// DefaultRows is the row table of the go-api shadow rows at this commit, plus the control cells. It is Go source, not a
// data file, so a change to it runs the Go workflow.
var DefaultRows = []byte(`# The venue driver's row table: one go-api shadow row per line (row = PLAN-10851-2 section 2 number), or a control cell (C<n>).
# Columns: row method route org destructive ext principal expect path body. org = a principals-file org label, or T on
# a destructive row: the throwaway pool, one fresh pool org per k (-throwaway-pool). Placeholders in path and body:
# {org} {org:<label>} {owner} {user:<principal>} {uuid:<kind>} {name:<kind>} {missing}; a destructive row may use only
# {org}, {owner} and {missing}. {state} (a body string value only) is the slot's minted single-use state (-states).
# The external rows (ext != -) act in org E, the seeded org of the external evidence (ADDENDUM-13267-1 section 5).
# ext names the outside service a legacy-allowed request can reach; the driver sends it only with -allow-external <ext>.
row	method	route	org	destructive	ext	principal	expect	path	body
C1	GET	/api/v1/admin/teams	A	no	-	drift-S	mismatch	/api/v1/admin/teams	-
C2	GET	/api/v1/admin/integrations	A	no	-	drift-S	mismatch	/api/v1/admin/integrations	-
1	POST	/api/v1/admin/orgs/{org_id}/invites	T	yes	mail	-	match	/api/v1/admin/orgs/{org}/invites	{"email":"p13140-invitee@venue-proof.fullchaos.dev","role":"member"}
2	GET	/api/v1/admin/orgs/{org_id}/members	A	no	-	-	match	/api/v1/admin/orgs/{org}/members	-
3	POST	/api/v1/admin/orgs/{org_id}/members	A	no	-	-	match	/api/v1/admin/orgs/{org}/members	{"user_id":"{missing}","role":"member"}
4	DELETE	/api/v1/admin/orgs/{org_id}/members/{user_id}	A	no	-	-	match	/api/v1/admin/orgs/{org}/members/{missing}	-
5	PATCH	/api/v1/admin/orgs/{org_id}/members/{user_id}	A	no	-	-	match	/api/v1/admin/orgs/{org}/members/{missing}	{"role":"member"}
6	POST	/api/v1/admin/orgs/{org_id}/transfer-ownership	T	yes	-	-	match	/api/v1/admin/orgs/{org}/transfer-ownership	{"new_owner_id":"{missing}"}
7	GET	/api/v1/admin/feature-flags	A	no	-	-	match	/api/v1/admin/feature-flags	-
8	PATCH	/api/v1/admin/feature-flags/{flag_id}	A	no	-	-	match	/api/v1/admin/feature-flags/{missing}	{}
9	POST	/api/v1/admin/impersonate	T	yes	-	-	match	/api/v1/admin/impersonate	{"user_id":"{owner}"}
10	POST	/api/v1/admin/impersonate/stop	T	yes	-	-	match	/api/v1/admin/impersonate/stop	-
11	GET	/api/v1/admin/orgs	A	no	-	-	match	/api/v1/admin/orgs	-
12	POST	/api/v1/admin/orgs	A	no	-	-	match	/api/v1/admin/orgs	{}
13	DELETE	/api/v1/admin/orgs/{org_id}	T	yes	-	-	match	/api/v1/admin/orgs/{org}	-
14	GET	/api/v1/admin/orgs/{org_id}	A	no	-	-	match	/api/v1/admin/orgs/{org}	-
15	PATCH	/api/v1/admin/orgs/{org_id}	A	no	-	-	match	/api/v1/admin/orgs/{org}	{}
16	GET	/api/v1/admin/orgs/{org_id}/feature-overrides	A	no	-	-	match	/api/v1/admin/orgs/{org}/feature-overrides	-
17	POST	/api/v1/admin/orgs/{org_id}/feature-overrides	A	no	-	-	match	/api/v1/admin/orgs/{org}/feature-overrides	{}
18	DELETE	/api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}	A	no	-	-	match	/api/v1/admin/orgs/{org}/feature-overrides/{missing}	-
19	PATCH	/api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}	A	no	-	-	match	/api/v1/admin/orgs/{org}/feature-overrides/{uuid:feature_override}	{}
20	GET	/api/v1/admin/platform/audit-logs	A	no	-	-	match	/api/v1/admin/platform/audit-logs	-
21	GET	/api/v1/admin/platform/stats	A	no	-	-	match	/api/v1/admin/platform/stats	-
22	POST	/api/v1/billing/plans/pull-stripe	E	no	stripe	-	match	/api/v1/billing/plans/pull-stripe	-
23	GET	/api/v1/billing/invoices	A	no	-	-	match	/api/v1/billing/invoices	-
24	GET	/api/v1/billing/invoices/{invoice_id}	A	no	-	-	match	/api/v1/billing/invoices/{missing}	-
25	POST	/api/v1/billing/invoices/{invoice_id}/void	E	no	stripe	-	match	/api/v1/billing/invoices/{uuid:invoice_open}/void	-
26	GET	/api/v1/billing/subscriptions	A	no	-	-	match	/api/v1/billing/subscriptions	-
27	POST	/api/v1/billing/subscriptions/cancel	E	no	stripe	-	match	/api/v1/billing/subscriptions/cancel	{}
28	POST	/api/v1/billing/subscriptions/change-plan	E	no	stripe	-	match	/api/v1/billing/subscriptions/change-plan	{"price_id":"price_venue_stub"}
29	GET	/api/v1/billing/subscriptions/history	A	no	-	-	match	/api/v1/billing/subscriptions/history	-
30	GET	/api/v1/billing/subscriptions/list	A	no	-	-	match	/api/v1/billing/subscriptions/list	-
31	POST	/api/v1/billing/subscriptions/reactivate	E	no	stripe	-	match	/api/v1/billing/subscriptions/reactivate	{}
32	GET	/api/v1/admin/integrations	A	no	-	-	match	/api/v1/admin/integrations	-
33	POST	/api/v1/admin/integrations	A	no	-	-	match	/api/v1/admin/integrations	{}
34	POST	/api/v1/admin/integrations/github/install-callback	E	no	github	-	match	/api/v1/admin/integrations/github/install-callback	{"installation_id":1,"state":"{state}","code":"venue-stub-code"}
35	POST	/api/v1/admin/integrations/pagerduty/api-token	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/api-token	{"subdomain":"venue-stub","api_token":"venue-stub-api-token"}
36	POST	/api/v1/admin/integrations/pagerduty/authorize	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/authorize	{}
37	POST	/api/v1/admin/integrations/pagerduty/callback	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/callback	{"state":"{state}","code":"venue-stub-code"}
38	POST	/api/v1/admin/integrations/pagerduty/client-credentials	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/client-credentials	{"subdomain":"venue-stub","client_id":"venue-stub-client","client_secret":"venue-stub-client-secret"}
39	POST	/api/v1/admin/integrations/pagerduty/disconnect	A	no	-	-	match	/api/v1/admin/integrations/pagerduty/disconnect	{}
40	GET	/api/v1/admin/integrations/pagerduty/services	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/services	-
41	GET	/api/v1/admin/integrations/pagerduty/status	A	no	-	-	match	/api/v1/admin/integrations/pagerduty/status	-
42	POST	/api/v1/admin/integrations/pagerduty/webhook-bindings	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/webhook-bindings	{}
43	GET	/api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}	A	no	-	-	match	/api/v1/admin/integrations/pagerduty/webhook-bindings/{missing}	-
44	POST	/api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/webhook-bindings/{missing}/activate	{}
45	POST	/api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/webhook-bindings/{missing}/revoke	{}
46	POST	/api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/webhook-bindings/{missing}/rotate	{}
47	GET	/api/v1/admin/integrations/{integration_id}	A	no	-	-	match	/api/v1/admin/integrations/{uuid:integration}	-
48	PATCH	/api/v1/admin/integrations/{integration_id}	A	no	-	-	match	/api/v1/admin/integrations/{uuid:integration}	{}
49	GET	/api/v1/admin/integrations/{integration_id}/datasets	A	no	-	-	match	/api/v1/admin/integrations/{uuid:integration}/datasets	-
50	PATCH	/api/v1/admin/integrations/{integration_id}/datasets	A	no	-	-	match	/api/v1/admin/integrations/{uuid:integration}/datasets	{}
51	GET	/api/v1/admin/integrations/{integration_id}/sources	A	no	-	-	match	/api/v1/admin/integrations/{uuid:integration}/sources	-
52	PATCH	/api/v1/admin/integrations/{integration_id}/sources/{source_id}	A	no	-	-	match	/api/v1/admin/integrations/{uuid:integration}/sources/{missing}	{}
53	POST	/api/v1/admin/settings	A	no	-	-	match	/api/v1/admin/settings	{}
54	GET	/api/v1/admin/settings/{category}	A	no	-	-	match	/api/v1/admin/settings/general	-
55	DELETE	/api/v1/admin/settings/{category}/{key}	A	no	-	-	match	/api/v1/admin/settings/general/p13140_missing_key	-
56	GET	/api/v1/admin/settings/{category}/{key}	A	no	-	-	match	/api/v1/admin/settings/{name:setting}	-
57	PUT	/api/v1/admin/settings/{category}/{key}	A	no	-	-	match	/api/v1/admin/settings/{name:setting}	{"value":"p13140"}
58	GET	/api/v1/admin/teams	A	no	-	-	match	/api/v1/admin/teams	-
59	POST	/api/v1/admin/teams	A	no	-	-	match	/api/v1/admin/teams	{}
60	DELETE	/api/v1/admin/teams/{team_id}	A	no	-	-	match	/api/v1/admin/teams/{missing}	-
61	GET	/api/v1/admin/teams/{team_id}	A	no	-	-	match	/api/v1/admin/teams/{uuid:team}	-
62	PATCH	/api/v1/admin/teams/{team_id}	A	no	-	-	match	/api/v1/admin/teams/{uuid:team}	{}
63	GET	/api/v1/billing/entitlements/{org_id}	A	no	-	-	match	/api/v1/billing/entitlements/{org}	-
64	GET	/api/v1/licensing/entitlements/{org_id}	A	no	-	-	match	/api/v1/licensing/entitlements/{org}	-
65	DELETE	/api/v1/admin/sync-configs/{config_id}	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}	-
66	GET	/api/v1/admin/backfill-jobs	A	no	-	-	match	/api/v1/admin/backfill-jobs	-
67	GET	/api/v1/admin/backfill-jobs/{job_id}	A	no	-	-	match	/api/v1/admin/backfill-jobs/{missing}	-
68	GET	/api/v1/admin/sync-configs	A	no	-	-	match	/api/v1/admin/sync-configs	-
69	GET	/api/v1/admin/sync-configs/auto-import-capabilities	A	no	-	-	match	/api/v1/admin/sync-configs/auto-import-capabilities	-
70	GET	/api/v1/admin/sync-configs/{config_id}	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}	-
71	GET	/api/v1/admin/sync-configs/{config_id}/coverage	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}/coverage	-
72	GET	/api/v1/admin/sync-configs/{config_id}/jobs	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}/jobs	-
73	GET	/api/v1/admin/sync-configs/{config_id}/repositories	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}/repositories	-
74	GET	/api/v1/admin/sync-runs/{run_id}	A	no	-	-	match	/api/v1/admin/sync-runs/{missing}	-
75	GET	/api/v1/admin/sync-runs/{run_id}/units	A	no	-	-	match	/api/v1/admin/sync-runs/{missing}/units	-
76	GET	/api/v1/admin/sync-targets	A	no	-	-	match	/api/v1/admin/sync-targets	-
77	GET	/api/v1/admin/teams/{team_id}/discover-members	A	no	-	-	match	/api/v1/admin/teams/{missing}/discover-members	-
78	GET	/api/v1/admin/teams/{team_id}/infer-members	A	no	-	-	match	/api/v1/admin/teams/{missing}/infer-members	-
79	PATCH	/api/v1/admin/sync-configs/{config_id}	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}	{}
80	POST	/api/v1/admin/impersonate/handoff/consume	C	no	-	-	match	/api/v1/admin/impersonate/handoff/consume	{}
81	POST	/api/v1/admin/integrations/github/install-url	E	no	github	-	match	/api/v1/admin/integrations/github/install-url	{}
82	POST	/api/v1/admin/integrations/pagerduty/preflight	E	no	pagerduty	-	match	/api/v1/admin/integrations/pagerduty/preflight	{}
83	POST	/api/v1/admin/integrations/{integration_id}/backfill	A	no	-	-	match	/api/v1/admin/integrations/{missing}/backfill	{}
84	POST	/api/v1/admin/integrations/{integration_id}/discover	A	no	-	-	match	/api/v1/admin/integrations/{missing}/discover	{}
85	POST	/api/v1/admin/integrations/{integration_id}/sync	A	no	-	-	match	/api/v1/admin/integrations/{missing}/sync	{}
86	POST	/api/v1/admin/sync-configs	A	no	-	-	match	/api/v1/admin/sync-configs	{}
87	POST	/api/v1/admin/sync-configs/batch	A	no	-	-	match	/api/v1/admin/sync-configs/batch	{}
88	POST	/api/v1/admin/sync-configs/{config_id}/backfill	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}/backfill	{}
89	POST	/api/v1/admin/sync-configs/{config_id}/trigger	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}/trigger	{}
90	POST	/api/v1/admin/teams/{team_id}	A	no	-	-	match	/api/v1/admin/teams/import	{}
91	POST	/api/v1/admin/teams/{team_id}/approve-changes	A	no	-	-	match	/api/v1/admin/teams/{missing}/approve-changes	{}
92	POST	/api/v1/admin/teams/{team_id}/confirm-inferred-members	A	no	-	-	match	/api/v1/admin/teams/{missing}/confirm-inferred-members	{}
93	POST	/api/v1/admin/teams/{team_id}/confirm-members	A	no	-	-	match	/api/v1/admin/teams/{missing}/confirm-members	{}
94	POST	/api/v1/admin/teams/{team_id}/dismiss-changes	A	no	-	-	match	/api/v1/admin/teams/{missing}/dismiss-changes	{}
95	PUT	/api/v1/admin/sync-configs/{config_id}/repositories	A	no	-	-	match	/api/v1/admin/sync-configs/{missing}/repositories	{}
`)
