# Lilac Fork TODO

This file tracks persistent issues and future improvements for the stabilized Lilac fork.

## High Priority: GGUF Stabilization
- [ ] **Fix `embeddings required but some input tokens were not marked as outputs`**:
    - Despite adding `pooling_type` support, some models still fail with this error in `llama-cpp-python` 0.3.18.
    - Research if specific build flags (like `LLAMA_CURL=ON`) or different `llama-cpp-python` versions are needed.
    - Investigate if the models themselves are correctly flagged as embedding models in GGUF metadata.

## High Priority: Jina v5 Nano Dependency Upgrade
- [ ] **Enable `JinaV5Nano` signal** (currently disabled in `default_signals.py`):
    - Requires `transformers >= 4.57.0` (currently pinned to `^4.37.2` / installed 4.38.1).
    - Upgrading `transformers` cascades to requiring newer `huggingface_hub`, `peft`, `sentence-transformers`, etc.
    - The signal code is ready in `jina.py` with multi-task support — just needs the dependency upgrade.


## Medium Priority: Performance & IO
- [ ] **Windows IO Stress Test**: Verify the `delete_file` retry logic and DuckDB connection reset during very large clustering/map operations (1M+ rows) on Windows.
- [ ] **Cross-Signal Tokenization**: Extend the `Union[str, list[int]]` input support to other embedding signals (SBERT, OpenAI, etc.) for full API parity.

## Low Priority: DX Improvements
- [ ] **Auto-Config Enhancements**: Automatically try to detect the correct `pooling_type` by reading GGUF metadata via `llama_cpp` during the initial scan.
