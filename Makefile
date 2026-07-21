.PHONY: docs\:check docs\:check-drift docs\:check-links docs\:generate-taxonomy docs\:build docs\:check-built-site docs\:check-external-links docs\:check-freshness docs\:check-code-prerequisites go\:fmt go\:vet go\:test go\:race go\:build go\:contract go\:integration go\:container-smoke go\:container-reproducible go\:container go\:verify go\:check-fast go\:check install test\:unit test\:integration test\:e2e test\:live-e2e test\:ci

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

go\:fmt:
	@./ci/check_go.sh fmt

go\:vet:
	@./ci/check_go.sh vet

go\:test:
	@./ci/check_go.sh test

go\:race:
	@./ci/check_go.sh race

go\:build:
	@./ci/check_go.sh build

go\:contract:
	@./ci/check_go.sh contract

go\:integration:
	@./ci/check_go.sh integration

go\:container-smoke:
	@./ci/check_go_containers.sh smoke

go\:container-reproducible:
	@./ci/check_go_containers.sh reproducible

go\:container:
	@./ci/check_go_containers.sh all

go\:verify:
	@./ci/check_go.sh all
	@./ci/check_go.sh integration
	@./ci/check_go_containers.sh all

go\:check-fast:
	@./ci/check_go.sh fast

go\:check:
	@./ci/check_go.sh all

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
