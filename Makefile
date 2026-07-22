DOCS_V2_CONFIG := mkdocs.prototype.yml
DOCS_V2_SITE_DIR := .build/docs-prototype
DOCS_CLOUDFLARE_DIR := .build/docs-cloudflare
DOCS_REDIRECTS := .github/documentation-program/phase-9/redirects.tsv
WRANGLER_VERSION := 4.112.0

.PHONY: docs\:check docs\:check-drift docs\:check-links docs\:generate-taxonomy docs\:build docs\:check-built-site docs\:check-external-links docs\:check-freshness docs\:check-code-prerequisites docs\:v2-serve docs\:v2-build docs\:v2-check docs\:cloudflare-preview docs\:cloudflare-dev install test\:unit test\:integration test\:e2e test\:live-e2e test\:ci

docs\:generate-taxonomy:
	python3 scripts/gen_taxonomy_docs.py

docs\:check-drift:
	python3 scripts/check_investment_docs_drift.py

docs\:check-links:
	python3 scripts/check_docs_links.py

docs\:check:
	python3 scripts/check_investment_docs_drift.py
	python3 scripts/check_docs_links.py

docs\:build:
	mkdocs build --strict --site-dir .build/site

docs\:check-built-site:
	mkdocs build --strict --site-dir .build/site
	python3 scripts/check_built_site_links.py --site-dir .build/site

docs\:check-external-links:
	mkdocs build --strict --site-dir .build/site
	python3 scripts/check_external_links.py --built-site .build/site --allowlist docs/external-link-allowlist.yml --site-url https://docs.fullchaos.dev

docs\:check-freshness:
	python3 scripts/check_freshness_inventory.py

docs\:check-code-prerequisites:
	python3 scripts/check_code_prerequisites.py

# Canonical documentation v2 authoring loop with live reload.
docs\:v2-serve:
	python3 -m mkdocs serve --strict --config-file $(DOCS_V2_CONFIG) --dev-addr 127.0.0.1:8000

# Build the canonical documentation v2 candidate after validating IA/publication state.
docs\:v2-build:
	mkdir -p .build
	python3 scripts/validate_docs_v2_publication.py
	python3 -m mkdocs build --strict --config-file $(DOCS_V2_CONFIG)

# Run the reader-critical Phase 10 gate locally.
docs\:v2-check: docs\:v2-build
	python3 scripts/check_built_site_links.py --site-dir $(DOCS_V2_SITE_DIR)
	python3 scripts/check_docs_candidate_search.py --site-dir $(DOCS_V2_SITE_DIR) --queries .github/documentation-program/phase-10/search-acceptance.json
	python3 scripts/check_docs_candidate_accessibility.py --site-dir $(DOCS_V2_SITE_DIR) --css docs-prototype/stylesheets/extra.css
	python3 scripts/check_docs_candidate_facts.py

# Prepare the exact static asset tree used for an Access-protected preview.
docs\:cloudflare-preview: docs\:v2-check
	python3 scripts/prepare_docs_cloudflare.py \
		--source $(DOCS_V2_SITE_DIR) \
		--output $(DOCS_CLOUDFLARE_DIR) \
		--mode preview \
		--redirects $(DOCS_REDIRECTS) \
		--source-revision "$$(git rev-parse HEAD)"

# Serve the prepared Cloudflare asset tree with the pinned local Workers runtime.
docs\:cloudflare-dev: docs\:cloudflare-preview
	npx --yes wrangler@$(WRANGLER_VERSION) dev --config wrangler.jsonc

test\:unit:
	@./ci/run_tests.sh unit

test\:integration:
	@./ci/run_tests.sh integration

test\:e2e:
	@./ci/run_tests.sh e2e

test\:live-e2e:
	@./ci/run_tests.sh live-e2e

test\:ci:
	@./ci/run_tests.sh ci

install:
	pip install -r requirements.txt
	lefthook install
