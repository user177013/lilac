import numpy as np
import pytest

from lilac.embeddings import llamacpp as llamacpp_mod
from lilac.embeddings.llamacpp import LlamaCppEmbedding
from lilac.schema import EMBEDDING_KEY, SPAN_KEY, TEXT_SPAN_START_FEATURE, TEXT_SPAN_END_FEATURE


class FakeModel:
    def __init__(self, n_embd, kind='data_embedding', raise_on=False):
        self._n = n_embd
        self.kind = kind
        self.raise_on = raise_on

    def n_embd(self):
        return self._n

    def create_embedding(self, text: str):
        if self.raise_on:
            raise RuntimeError('simulated failure')
        if self.kind == 'data_embedding':
            return {'data': [{'embedding': [float(i + 1) for i in range(self._n)]}]}
        if self.kind == 'embedding_root':
            return {'embedding': [float(i + 1) for i in range(self._n)]}
        if self.kind == 'list':
            return [float(i + 1) for i in range(self._n)]
        # unknown shape
        return {'unknown': 'x'}


def _run_with_fake(fake: FakeModel):
    # Monkeypatch the cached loader to return our fake model.
    llamacpp_mod._get_and_cache_model = lambda *args, **kwargs: fake
    emb = LlamaCppEmbedding(split=False, model_filename='fake.gguf')
    return emb.compute(['hello'])[0]


@pytest.mark.parametrize('kind', ['data_embedding', 'embedding_root', 'list'])
def test_llamacpp_various_return_shapes(kind):
    fake = FakeModel(8, kind=kind)
    out = _run_with_fake(fake)

    assert out is not None
    assert isinstance(out, list)
    item = out[0]
    assert EMBEDDING_KEY in item
    vec = item[EMBEDDING_KEY]
    assert isinstance(vec, np.ndarray)
    assert vec.dtype == np.float32
    assert vec.shape[0] == 8
    # vector should be normalized (non-zero)
    norm = np.linalg.norm(vec)
    assert pytest.approx(1.0, rel=1e-5) == norm
    # span present
    assert SPAN_KEY in item
    span = item[SPAN_KEY]
    assert span[TEXT_SPAN_START_FEATURE] == 0
    assert span[TEXT_SPAN_END_FEATURE] >= 0


def test_llamacpp_bad_return_falls_back_to_zero():
    fake = FakeModel(6, kind='unknown')
    out = _run_with_fake(fake)
    item = out[0]
    vec = item[EMBEDDING_KEY]
    assert isinstance(vec, np.ndarray)
    assert vec.dtype == np.float32
    # fallback to zeros (norm == 0)
    assert np.linalg.norm(vec) == 0.0


def test_llamacpp_create_embedding_exception_fallbacks():
    fake = FakeModel(5, kind='data_embedding', raise_on=True)
    out = _run_with_fake(fake)
    item = out[0]
    vec = item[EMBEDDING_KEY]
    assert isinstance(vec, np.ndarray)
    assert vec.dtype == np.float32
    assert np.linalg.norm(vec) == 0.0
