.PHONY: test mutants mutants-diff check

MUTANTS_BASE ?= HEAD
MUTANTS_JOBS ?= 4
MUTANTS_FILE_ARG = $(if $(strip $(FILE)),--file "$(FILE)",)

# src/sim.rs compiles the discovery binary into the node's test binary to
# run discovery in process, and with it discovery's own unit tests, which its
# binary already runs: skip that second copy (twice the time, twice the
# exposure to port-binding flakes).
test:
	cargo nextest run -E 'not test(sim::discovery::)'

mutants:
	cargo mutants $(MUTANTS_FILE_ARG) --jobs $(MUTANTS_JOBS)

mutants-diff:
	@set -e; \
	diff_file=$$(mktemp); \
	trap 'rm -f "$$diff_file"' 0 1 2 3 15; \
	git diff --no-ext-diff --binary $(MUTANTS_BASE) > "$$diff_file"; \
	cargo mutants $(MUTANTS_FILE_ARG) --in-diff "$$diff_file" --jobs $(MUTANTS_JOBS)

check:
	cargo fmt --check
	cargo clippy --all-targets --all-features -- -D clippy::all
