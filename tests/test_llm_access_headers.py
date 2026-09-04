"""Authentication headers for a protected remote vLLM endpoint."""

from __future__ import annotations

import pytest

from lab28_platform.llm_client import _auth_headers
from lab28_platform.settings import VLLMSettings


def test_vllm_headers_include_bearer_and_cloudflare_service_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LAB28_VLLM_API_KEY", "vllm-test-key")
    monkeypatch.setenv("LAB28_CF_ACCESS_CLIENT_ID", "service-token-id")
    monkeypatch.setenv("LAB28_CF_ACCESS_CLIENT_SECRET", "service-token-secret")

    assert _auth_headers(VLLMSettings.from_env()) == {
        "Authorization": "Bearer vllm-test-key",
        "CF-Access-Client-Id": "service-token-id",
        "CF-Access-Client-Secret": "service-token-secret",
    }


def test_partial_cloudflare_service_token_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAB28_CF_ACCESS_CLIENT_ID", "service-token-id")
    monkeypatch.delenv("LAB28_CF_ACCESS_CLIENT_SECRET", raising=False)

    with pytest.raises(ValueError, match="must be set together"):
        _auth_headers(VLLMSettings.from_env())
