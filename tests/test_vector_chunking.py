from __future__ import annotations

from vault_service_defaults import DEFAULT_LOCAL_MODEL_BASE_URL
from vault_vector_index import (
    DEFAULT_EMBED_MAX_TEXT_CHARS,
    EmbeddingConfig,
    OpenAIEmbeddingClient,
    PreparedEmbeddingText,
    RetryableEmbeddingSizeError,
    chunk_text,
)


def test_chunk_text_falls_back_to_char_windows_for_huge_tokens() -> None:
    raw = "X" * (DEFAULT_EMBED_MAX_TEXT_CHARS * 2 + 400)
    chunks = chunk_text(raw, max_chars=DEFAULT_EMBED_MAX_TEXT_CHARS)
    assert len(chunks) >= 2
    assert all(len(chunk) <= DEFAULT_EMBED_MAX_TEXT_CHARS for chunk in chunks)
    assert chunks[0].startswith("X" * 128)
    assert chunks[-1].endswith("X" * 128)


def test_embedding_batches_respect_token_budget() -> None:
    cfg = EmbeddingConfig(
        base_url=DEFAULT_LOCAL_MODEL_BASE_URL,
        model="test",
        api_key="local",
        timeout_seconds=30,
        batch_size=16,
        batch_tokens=3000,
        max_text_chars=3000,
    )
    client = OpenAIEmbeddingClient(cfg)
    texts = [PreparedEmbeddingText(original_index=i, text="alpha " * 220) for i in range(16)]
    batches = client._build_batches(texts)
    assert len(batches) > 1
    for batch in batches:
        approx_tokens = sum(client._estimate_text_tokens(entry.text) for entry in batch)
        assert approx_tokens <= cfg.batch_tokens
        assert len(batch) <= cfg.batch_size


class AdaptiveTestEmbeddingClient(OpenAIEmbeddingClient):
    def _request_batch(self, batch: list[PreparedEmbeddingText]) -> tuple[list[bytes], int]:
        if len(batch) > 2:
            raise RetryableEmbeddingSizeError(
                status_code=500,
                approx_tokens=4000,
                batch_items=len(batch),
                message="embedding HTTP 500: Context size has been exceeded.",
            )
        if any(len(entry.text) > 512 for entry in batch):
            raise RetryableEmbeddingSizeError(
                status_code=500,
                approx_tokens=2200,
                batch_items=len(batch),
                message="embedding HTTP 500: Context size has been exceeded.",
            )
        return ([b"vec"] * len(batch), 3)


def test_embedding_client_adapts_to_batch_and_single_item_failures() -> None:
    cfg = EmbeddingConfig(
        base_url=DEFAULT_LOCAL_MODEL_BASE_URL,
        model="test",
        api_key="local",
        timeout_seconds=30,
        batch_size=8,
        batch_tokens=5000,
        max_text_chars=2000,
    )
    client = AdaptiveTestEmbeddingClient(cfg)
    texts = [
        "normal text " * 20,
        "Y" * 1800,
        "normal text " * 25,
        "normal text " * 30,
    ]
    blobs, dim = client.embed_texts(texts)
    assert dim == 3
    assert blobs == [b"vec", b"vec", b"vec", b"vec"]
    assert client._adaptive_batch_tokens < cfg.batch_tokens
    assert client._adaptive_single_text_tokens < cfg.batch_tokens
    assert client._adaptive_max_text_chars < cfg.max_text_chars
