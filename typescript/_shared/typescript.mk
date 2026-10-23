# Each plugin installs and locks its own published dependencies.
PNPM ?= pnpm
TEST_ARGS ?=

.PHONY: sync sync-latest lint test build clean help
sync:
	$(PNPM) install --frozen-lockfile

sync-latest:
	$(PNPM) update --latest=false

lint:
	$(PNPM) run lint

test:
	$(PNPM) run build
	$(PNPM) run test $(TEST_ARGS)

build:
	$(PNPM) run build
	$(PNPM) run pack

clean:
	$(PNPM) exec tsc --build --clean
	node -e "for (const path of ['lib', 'dist', 'tests/helpers/lib']) require('node:fs').rmSync(path, {recursive: true, force: true})"

help:
	@echo "sync sync-latest lint test build clean"
