.PHONY: test mutants check

MUTANTS_BASE ?= HEAD

test:
	cargo nextest run

mutants:
	@set -e; \
	diff_file=$$(mktemp); \
	trap 'rm -f "$$diff_file"' 0 1 2 3 15; \
	git diff --no-ext-diff --binary $(MUTANTS_BASE) > "$$diff_file"; \
	cargo mutants --in-diff "$$diff_file" --jobs 2

check:
	cargo fmt --check
	cargo clippy --all-targets --all-features -- -D clippy::all
