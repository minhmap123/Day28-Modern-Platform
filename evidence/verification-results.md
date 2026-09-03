# Verification results

The commands below were run against the Docker full profile before preparing
this submission. GPU and LangSmith gates are intentionally excluded where the
required external endpoint or credential is unavailable; their evidence records
that state as `UNVERIFIED` rather than substituting a mock.

```text
uv run pytest starter-tests -q
4 passed

uv run pytest tests -q
83 passed

uv run pytest integration-tests/test_j1_golden_path.py -q
12 passed, 3 skipped (GPU gate)

uv run pytest integration-tests/test_j2_idempotent_replay.py -q
9 passed

uv run pytest integration-tests -m "not gpu and not langsmith" -q
56 passed, 16 deselected

uv run ruff check .
All checks passed

uv run python scripts/verify_matrix.py
245 checks passed

uv run python scripts/check_portability.py
supported workflow is host-path and shell independent

uv run python scripts/validate_manifests.py
Kubernetes and GitOps manifest contracts passed
```
