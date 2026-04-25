.PHONY: lint typecheck check

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run pyright

check:
	@uv run ruff check . && uv run ruff format --check . & LINT_PID=$$!; \
	uv run pyright & TYPE_PID=$$!; \
	wait $$LINT_PID || exit 1; \
	wait $$TYPE_PID || exit 1
