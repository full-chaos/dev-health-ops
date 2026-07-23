PYTHON ?= python3
WRANGLER ?= npx --yes wrangler
PREVIEW_ALIAS ?=
VERSION_ID ?=
DOCS_REVISION := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
DOCS_TAG := $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)

.PHONY: docs\:check docs\:check-drift docs\:check-links docs\:generate-taxonomy docs\:build docs\:check-built-site docs\:check-external-links docs\:check-freshness docs\:check-code-prerequisites docs\:serve docs\:check-v2 docs\:build-preview docs\:preview docs\:version docs\:upload-preview docs\:deploy docs\:rollback docs\:versions docs\:deployments docs\:v2-serve docs\:v2-check docs\:cloudflare-preview docs\:cloudflare-dev docs\:cloudflare-version docs\:cloudflare-deploy go\:fmt go\:vet go\:test go\:race go\:build go\:contract go\:integration go\:container-smoke go\:container-reproducible go\:container go\:verify go\:check-fast go\:check install test\:unit test\:integration test\:e2e test\:live-e2e test\:ci

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
	$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml

docs\:check-built-site:
	$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml
	$(PYTHON) scripts/check_built_site_links.py --site-dir .build/docs

docs\:check-external-links:
	$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml
	$(PYTHON) scripts/check_external_links.py --built-site .build/docs --allowlist .github/docs-legacy/external-link-allowlist.yml --site-url https://docs.fullchaos.dev

docs\:check-freshness:
	$(PYTHON) scripts/check_freshness_inventory.py

docs\:check-code-prerequisites:
	$(PYTHON) scripts/check_code_prerequisites.py

# Canonical documentation lifecycle:
#   make docs:serve
#       Fast MkDocs live reload at http://127.0.0.1:8000.
#   make docs:preview
#       Local Cloudflare-shaped preview at http://localhost:8787.
#   make docs:version PREVIEW_ALIAS=pr-1256
#       Upload an immutable, non-production Worker version and stable preview URL.
#   make docs:deploy
#       Full validation, production build, and immediate 100% production deployment.
#   make docs:rollback VERSION_ID=<known-good-version-id>
#       Roll production back to an explicit Worker version.

docs\:serve:
	$(PYTHON) -m mkdocs serve --strict --config-file mkdocs.yml --dev-addr 127.0.0.1:8000

docs\:check-v2:
	$(PYTHON) scripts/build_docs_cloudflare.py --mode preview --full-check

docs\:build-preview:
	$(PYTHON) scripts/build_docs_cloudflare.py --mode preview

docs\:preview: docs\:build-preview
	$(WRANGLER) dev --config wrangler.jsonc

# Remote preview version. This never changes the active production deployment.
docs\:version:
	@test -n "$(PREVIEW_ALIAS)" || { \
		echo 'PREVIEW_ALIAS is required, for example: make docs:version PREVIEW_ALIAS=pr-1256'; \
		exit 2; \
	}
	@printf '%s\n' "$(PREVIEW_ALIAS)" | grep -Eq '^[a-z][a-z0-9-]*$$' || { \
		echo 'PREVIEW_ALIAS must begin with a lowercase letter and contain only lowercase letters, numbers, and dashes.'; \
		exit 2; \
	}
	@test $$(printf '%s' "$(PREVIEW_ALIAS)" | wc -c | tr -d ' ') -le 47 || { \
		echo 'PREVIEW_ALIAS must be 47 characters or fewer for the dev-health-docs preview hostname.'; \
		exit 2; \
	}
	$(PYTHON) scripts/build_docs_cloudflare.py --mode preview --full-check
	$(WRANGLER) versions upload \
		--config wrangler.jsonc \
		--preview-alias "$(PREVIEW_ALIAS)" \
		--message "Documentation preview $(PREVIEW_ALIAS) at $(DOCS_REVISION)"

docs\:upload-preview: docs\:version

# Production creates a separate production-mode version without preview noindex headers,
# then immediately deploys that version to 100% of traffic.
docs\:deploy:
	$(PYTHON) scripts/build_docs_cloudflare.py --mode production --full-check
	$(WRANGLER) deploy \
		--config wrangler.jsonc \
		--strict \
		--message "Documentation production deployment at $(DOCS_REVISION)" \
		--tag "docs-$(DOCS_TAG)"

docs\:rollback:
	@test -n "$(VERSION_ID)" || { \
		echo 'VERSION_ID is required, for example: make docs:rollback VERSION_ID=<known-good-version-id>'; \
		exit 2; \
	}
	$(WRANGLER) rollback "$(VERSION_ID)" \
		--config wrangler.jsonc \
		--message "Documentation rollback to $(VERSION_ID) requested from $(DOCS_REVISION)"

docs\:versions:
	$(WRANGLER) versions list --config wrangler.jsonc

docs\:deployments:
	$(WRANGLER) deployments list --config wrangler.jsonc

# Backward-compatible aliases used by earlier review notes.
docs\:v2-serve: docs\:serve

docs\:v2-check: docs\:check-v2

docs\:cloudflare-preview: docs\:build-preview

docs\:cloudflare-dev: docs\:preview

docs\:cloudflare-version: docs\:version

docs\:cloudflare-deploy: docs\:deploy

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
