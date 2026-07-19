PYTHON ?= python3
WRANGLER ?= npx --yes wrangler@4.112.0

.PHONY: docs\:check docs\:check-drift docs\:check-links docs\:generate-taxonomy docs\:build docs\:check-built-site docs\:check-external-links docs\:check-freshness docs\:check-code-prerequisites docs\:serve docs\:check-v2 docs\:build-preview docs\:preview docs\:deploy docs\:v2-serve docs\:v2-check docs\:cloudflare-preview docs\:cloudflare-dev docs\:cloudflare-deploy install test\:unit test\:integration test\:e2e test\:live-e2e test\:ci

docs\:generate-taxonomy:
	$(PYTHON) scripts/gen_taxonomy_docs.py

docs\:check-drift:
	$(PYTHON) scripts/check_investment_docs_drift.py

docs\:check-links:
	$(PYTHON) scripts/check_docs_links.py

docs\:check:
	$(PYTHON) scripts/check_investment_docs_drift.py
	$(PYTHON) scripts/check_docs_links.py

docs\:build:
	$(PYTHON) -m mkdocs build --strict --site-dir .build/site

docs\:check-built-site:
	$(PYTHON) -m mkdocs build --strict --site-dir .build/site
	$(PYTHON) scripts/check_built_site_links.py --site-dir .build/site

docs\:check-external-links:
	$(PYTHON) -m mkdocs build --strict --site-dir .build/site
	$(PYTHON) scripts/check_external_links.py --built-site .build/site --allowlist docs/external-link-allowlist.yml --site-url https://docs.fullchaos.dev

docs\:check-freshness:
	$(PYTHON) scripts/check_freshness_inventory.py

docs\:check-code-prerequisites:
	$(PYTHON) scripts/check_code_prerequisites.py

# Documentation v2: three commands cover the normal workflow.
#   make docs:serve   - fast MkDocs live reload
#   make docs:preview - build preview assets and run Wrangler locally
#   make docs:deploy  - full validation, production build, and deployment

docs\:serve:
	$(PYTHON) -m mkdocs serve --strict --config-file mkdocs.prototype.yml --dev-addr 127.0.0.1:8000

docs\:check-v2:
	$(PYTHON) scripts/build_docs_cloudflare.py --mode preview --full-check

docs\:build-preview:
	$(PYTHON) scripts/build_docs_cloudflare.py --mode preview

docs\:preview: docs\:build-preview
	$(WRANGLER) dev --config wrangler.jsonc

docs\:deploy:
	$(PYTHON) scripts/build_docs_cloudflare.py --mode production
	$(WRANGLER) deploy --config wrangler.jsonc

# Backward-compatible aliases used by earlier review notes.
docs\:v2-serve: docs\:serve

docs\:v2-check: docs\:check-v2

docs\:cloudflare-preview: docs\:build-preview

docs\:cloudflare-dev: docs\:preview

docs\:cloudflare-deploy: docs\:deploy

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
