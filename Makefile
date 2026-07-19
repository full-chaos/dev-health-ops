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

# Documentation v2 authoring loop. These targets intentionally use the v2
# configuration while the legacy docs tree remains available during migration.
docs\:v2-serve:
	python -m mkdocs serve --strict --config-file mkdocs.prototype.yml --dev-addr 127.0.0.1:8000

docs\:v2-build:
	python -m mkdocs build --strict --config-file mkdocs.prototype.yml

docs\:v2-check:
	mkdir -p .build
	python scripts/validate_docs_v2_publication.py
	python -m mkdocs build --strict --config-file mkdocs.prototype.yml
	python scripts/check_built_site_links.py --site-dir .build/docs-prototype
	python scripts/check_docs_candidate_search.py --site-dir .build/docs-prototype --queries .github/documentation-program/phase-10/search-acceptance.json
	python scripts/check_docs_candidate_accessibility.py --site-dir .build/docs-prototype --css docs-prototype/stylesheets/extra.css
	python scripts/check_docs_candidate_facts.py

docs\:cloudflare-preview: docs\:v2-check
	rm -rf .build/docs-cloudflare
	python scripts/prepare_docs_cloudflare.py \
		--source .build/docs-prototype \
		--output .build/docs-cloudflare \
		--mode preview \
		--redirects .github/documentation-program/phase-9/redirects.tsv \
		--source-revision "$$(git rev-parse HEAD)"

docs\:cloudflare-dev: docs\:cloudflare-preview
	npx --yes wrangler@4.112.0 dev --config wrangler.jsonc

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
