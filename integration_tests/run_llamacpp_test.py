import sys
import numpy as np
import importlib.util
from pathlib import Path

# Load modules directly by file path to avoid importing the top-level package
ROOT = Path(__file__).resolve().parents[1]
LLAMA_CPP_PY = ROOT / 'lilac' / 'embeddings' / 'llamacpp.py'
SCHEMA_PY = ROOT / 'lilac' / 'schema.py'

def _load_module_from_path(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod

llamacpp_mod = _load_module_from_path(LLAMA_CPP_PY, 'llamacpp_mod')
schema_mod = _load_module_from_path(SCHEMA_PY, 'schema_mod')
LlamaCppEmbedding = getattr(llamacpp_mod, 'LlamaCppEmbedding')
EMBEDDING_KEY = getattr(schema_mod, 'EMBEDDING_KEY')


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
        return {'unknown': 'x'}


def run_case(kind, expected_norm_nonzero=True):
    fake = FakeModel(8, kind=kind)
    llamacpp_mod._get_and_cache_model = lambda *a, **k: fake
    emb = LlamaCppEmbedding(split=False, model_filename='fake.gguf')
    out = emb.compute(['hello'])[0]
    if out is None:
        print(f'FAIL: output None for kind={kind}')
        return False
    item = out[0]
    vec = item[EMBEDDING_KEY]
    if not isinstance(vec, np.ndarray):
        print(f'FAIL: vector not ndarray for kind={kind}: {type(vec)}')
        return False
    norm = float(np.linalg.norm(vec))
    if expected_norm_nonzero and not (abs(norm - 1.0) < 1e-3):
        print(f'FAIL: unexpected norm {norm} for kind={kind}')
        return False
    if not expected_norm_nonzero and norm != 0.0:
        print(f'FAIL: expected zero norm but got {norm} for kind={kind}')
        return False
    print(f'PASS: kind={kind} norm={norm}')
    return True


def main():
    cases = [
        ('data_embedding', True),
        ('embedding_root', True),
        ('list', True),
        ('unknown', False),
    ]
    all_ok = True
    for kind, exp in cases:
        ok = run_case(kind, expected_norm_nonzero=exp)
        all_ok = all_ok and ok
    if not all_ok:
        sys.exit(2)
    print('All checks passed')


if __name__ == '__main__':
    main()
