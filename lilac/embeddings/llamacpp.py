"""Llama.cpp embeddings using GGUF models.

This module provides a BYOM (Bring Your Own Model) embedding signal for Lilac.
It scans the models directory for .gguf files, reads their config.yaml files,
and dynamically registers each model as a separate signal in the Lilac UI dropdown.

Folder structure:
  models/gguf/
  ├── my-model.gguf
  ├── my-model.config.yaml       ← auto-generated on first scan if missing
  ├── another-model.gguf
  └── another-model.config.yaml
"""
import functools
import gc
import os
from pathlib import Path
from typing import Any, Callable, ClassVar, Optional, Union, cast

import numpy as np
from pydantic import Field as PydanticField
from typing_extensions import override

from ..env import get_llamacpp_models_dir
from ..schema import Item
from ..signal import TextEmbeddingSignal, register_signal
from ..splitters.chunk_splitter import TextChunk
from ..splitters.spacy_splitter import clustering_spacy_chunker
from ..tasks import TaskExecutionType
from ..utils import log
from .embedding import chunked_compute_embedding, identity_chunker

# ─── Default config values ────────────────────────────────────────────────────
_DEFAULT_N_CTX = 2048
_DEFAULT_N_GPU_LAYERS = 0
_DEFAULT_OUTPUT_DIM = None
_DEFAULT_BATCH_SIZE = 512
_DEFAULT_POOLING_TYPE = -1  # Default to auto-detect from model.


def _make_display_name(stem: str) -> str:
  """Generate a human-readable display name from a gguf filename stem.

  Example: 'embeddinggemma-300m-Q8_0' → 'Embeddinggemma 300m (Q8 0)'
  """
  return stem.replace('-', ' ').replace('_', ' ').title()


def _config_path_for(gguf_path: str) -> str:
  """Return the .config.yaml path that corresponds to a .gguf file."""
  return os.path.splitext(gguf_path)[0] + '.config.yaml'


def _read_or_create_config(gguf_path: str) -> dict[str, Any]:
  """Read a model's config.yaml, or create one with defaults if it doesn't exist.

  Args:
    gguf_path: The full path to the .gguf file.

  Returns:
    A dictionary with the model configuration.
  """
  try:
    import yaml
  except ImportError:
    # pyyaml is already a required dependency of lilac.
    raise ImportError('Could not import "yaml". Please install pyyaml.')

  config_path = _config_path_for(gguf_path)
  stem = Path(gguf_path).stem

  if os.path.exists(config_path):
    with open(config_path, 'r', encoding='utf-8') as f:
      config = yaml.safe_load(f) or {}
  else:
    # Auto-generate a config with sensible defaults.
    config = {
      'display_name': _make_display_name(stem),
      'n_ctx': _DEFAULT_N_CTX,
      'n_gpu_layers': _DEFAULT_N_GPU_LAYERS,
      'output_dim': _DEFAULT_OUTPUT_DIM,
      'pooling_type': _DEFAULT_POOLING_TYPE,
    }
    with open(config_path, 'w', encoding='utf-8') as f:
      yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    log(f'[llamacpp] Created default config: {config_path}')

  # Ensure all keys exist (for configs that were manually created with missing keys).
  config.setdefault('display_name', _make_display_name(stem))
  config.setdefault('n_ctx', _DEFAULT_N_CTX)
  config.setdefault('n_gpu_layers', _DEFAULT_N_GPU_LAYERS)
  config.setdefault('output_dim', _DEFAULT_OUTPUT_DIM)
  config.setdefault('pooling_type', _DEFAULT_POOLING_TYPE)

  return config


@functools.cache
def _get_and_cache_model(
  model_filename: str, n_ctx: int, n_gpu_layers: int, pooling_type: int = _DEFAULT_POOLING_TYPE
) -> Any:
  """Load and cache the Llama.cpp model."""
  try:
    from llama_cpp import Llama
    
    # ------------- MONKEY PATCH BUG FIX -------------
    # llama-cpp-python <0.3.x throws AssertionError for 'Memory is not initialized'
    # and TypeError during __del__ of internal classes for some embedding-only models
    # like Gemma. This crashes the Loky worker process in Lilac.
    # Additionally, 0.3.x throws AssertionError during computation if kv_cache_clear 
    # is called on embedding-only models.
    def _apply_patch(cls_to_patch: Any) -> None:
      if not cls_to_patch or hasattr(cls_to_patch, '_is_patched_for_lilac'):
        return

      # 1. Patch __del__ to ignore AssertionError and TypeError during shutdown.
      if hasattr(cls_to_patch, '__del__'):
        _orig_del = getattr(cls_to_patch, '__del__')
        def _safe_del(self: Any) -> None:
          try:
            _orig_del(self)
          except (AssertionError, TypeError, Exception):
            pass
        setattr(cls_to_patch, '__del__', _safe_del)

      # 2. Patch kv_cache_clear to ignore assertion if memory is not initialized.
      # This is critical for 0.3.x embedding-only models during create_embedding.
      if hasattr(cls_to_patch, 'kv_cache_clear'):
        _orig_clear = getattr(cls_to_patch, 'kv_cache_clear')
        def _safe_clear(self: Any) -> None:
          try:
            _orig_clear(self)
          except AssertionError as e:
            if 'Memory is not initialized' not in str(e):
              raise
          except Exception:
            pass
        setattr(cls_to_patch, 'kv_cache_clear', _safe_clear)

      setattr(cls_to_patch, '_is_patched_for_lilac', True)

    try:
      from llama_cpp import _internals
      # Handle 0.2.x naming
      if hasattr(_internals, '_LlamaContext'): _apply_patch(_internals._LlamaContext)
      if hasattr(_internals, '_LlamaModel'): _apply_patch(_internals._LlamaModel)
      # Handle 0.3.x naming
      if hasattr(_internals, 'LlamaContext'): _apply_patch(_internals.LlamaContext)
      if hasattr(_internals, 'LlamaModel'): _apply_patch(_internals.LlamaModel)
    except ImportError:
      pass
    _apply_patch(Llama)
    # ------------------------------------------------

  except ImportError:
    raise ImportError(
      'Could not import the "llama-cpp-python" python package. '
      'Please install it with `pip install llama-cpp-python`.'
    )

  models_dir = get_llamacpp_models_dir()
  os.makedirs(models_dir, exist_ok=True)
  model_path = os.path.join(models_dir, model_filename)
  if not os.path.exists(model_path):
    raise ValueError(
      f'Model not found at {model_path}. '
      f'Please place your GGUF model in {models_dir} or set LILAC_LLAMACPP_MODELS_DIR.'
    )

  llama_kwargs: dict[str, Any] = {
    'model_path': model_path,
    'embedding': True,
    'n_ctx': n_ctx,
    'n_gpu_layers': n_gpu_layers,
    'verbose': False,
  }
  if pooling_type != -1:
    llama_kwargs['pooling_type'] = pooling_type

  return Llama(**llama_kwargs)


class LlamaCppEmbedding(TextEmbeddingSignal):
  """Computes embeddings using llama.cpp and GGUF models.

  <br>This allows you to use any GGUF model for embeddings, including quantized versions.
  Place your .gguf files in the directory specified by `LILAC_LLAMACPP_MODELS_DIR`
  (defaults to `models/gguf/` in your project directory).
  """

  # Allow 'model_filename' without Pydantic complaining about the 'model_' prefix.
  model_config = {'protected_namespaces': ()}

  name: ClassVar[str] = 'llamacpp'
  display_name: ClassVar[str] = 'Llama.cpp GGUF Embedding'
  local_batch_size: ClassVar[int] = _DEFAULT_BATCH_SIZE
  local_parallelism: ClassVar[int] = 1
  local_strategy: ClassVar[TaskExecutionType] = 'threads'

  model_filename: str = PydanticField(
    description='The filename of the .gguf model in your models directory.'
  )
  n_ctx: int = PydanticField(
    default=_DEFAULT_N_CTX, description='The maximum context size for the model.'
  )
  n_gpu_layers: int = PydanticField(
    default=_DEFAULT_N_GPU_LAYERS,
    description='Number of layers to offload to GPU (-1 for all).',
  )
  output_dim: Optional[int] = PydanticField(
    default=_DEFAULT_OUTPUT_DIM,
    description='The output dimension to truncate to (for Matryoshka models).',
  )
  pooling_type: int = PydanticField(
    default=_DEFAULT_POOLING_TYPE,
    description='Explicit pooling type for llama.cpp. '
    '(-1: Auto, 0: None, 1: Mean, 2: CLS, 3: Last, 4: Rank).',
  )

  @override
  def setup(self) -> None:
    """Pre-load the model."""
    _get_and_cache_model(self.model_filename, self.n_ctx, self.n_gpu_layers, self.pooling_type)

  @override
  def compute(self, docs: list[Union[str, list[int]]]) -> list[Optional[Item]]:
    """Compute embeddings for the given documents."""
    model = _get_and_cache_model(
      self.model_filename, self.n_ctx, self.n_gpu_layers, self.pooling_type
    )

    def _embed_fn(batch_docs: list[Union[str, list[int]]]) -> list[np.ndarray]:
      # Try to infer embedding length from the model if available; otherwise
      # we'll determine it from the first successful embedding we receive.
      embedding_length: Optional[int] = None
      try:
        n_attr = getattr(model, 'n_embd', None)
        if n_attr is not None:
          embedding_length = int(n_attr() if callable(n_attr) else n_attr)
      except Exception:
        embedding_length = None

      def _zeros() -> np.ndarray:
        return np.zeros(embedding_length or 1, dtype=np.float32)

      vectors: list[np.ndarray] = []

      for d in batch_docs:
        if isinstance(d, str) and (not d or not d.strip()):
          vectors.append(_zeros())
          continue
        if isinstance(d, list) and not d:
          vectors.append(_zeros())
          continue

        try:
          # Process one by one to avoid batching bugs in newer architectures.
          res = model.create_embedding(d)

          # Robustly extract an embedding list from different return shapes.
          raw = None
          if isinstance(res, dict):
            if 'data' in res and isinstance(res['data'], list) and res['data']:
              first = res['data'][0]
              if isinstance(first, dict) and 'embedding' in first:
                raw = first['embedding']
            if raw is None and 'embedding' in res:
              raw = res['embedding']
            if raw is None:
              # Fallback: find first numeric list-like value in the dict
              for v in res.values():
                if isinstance(v, (list, tuple)) and v and isinstance(v[0], (int, float)):
                  raw = v
                  break
          elif isinstance(res, (list, tuple)) and res and isinstance(res[0], (int, float)):
            raw = res

          if raw is None:
            raise ValueError(f'Could not parse embedding from llama-cpp response: {type(res)}')

          vector = np.array(raw, dtype=np.float32)

          # If we didn't know the embedding length yet, set it now.
          if embedding_length is None:
            embedding_length = int(vector.shape[0])

          # Apply matryoshka truncation if requested.
          if self.output_dim:
            vector = vector[: self.output_dim]

          # Normalize.
          norm = np.linalg.norm(vector)
          if norm > 0:
            vector = vector / norm

          vectors.append(vector)
        except Exception as e:
          # Log the error and fallback to a zero vector of the inferred length.
          try:
            log(f'[llamacpp] create_embedding failed for {self.model_filename}: {e}')
          except Exception:
            pass
          vectors.append(_zeros())

      return vectors

    # If any input is a list of tokens, we bypass chunking for the entire batch to avoid
    # SpaCy errors. Tokenized inputs are already "chunked" in a sense.
    contains_tokens = any(isinstance(d, list) for d in docs)
    chunker = cast(
      Callable[[str], list[TextChunk]],
      clustering_spacy_chunker if (self._split and not contains_tokens) else identity_chunker,
    )
    return chunked_compute_embedding(_embed_fn, docs, self.local_batch_size, chunker=chunker)

  @override
  def teardown(self) -> None:
    """Release the model from memory."""
    # We do not delete the cached model since it's global, just clear gc.
    gc.collect()


def scan_and_register_llamacpp_models() -> None:
  """Scan the models directory for .gguf files and register each as a Lilac signal.

  For each .gguf file found, this function:
    1. Reads (or auto-creates) a matching .config.yaml with model settings.
    2. Dynamically creates a Python subclass of LlamaCppEmbedding with unique
       name and display_name ClassVars.
    3. Registers that subclass in the Lilac signal registry so it appears in the UI.
  """
  models_dir = get_llamacpp_models_dir()

  if not os.path.isdir(models_dir):
    # Directory doesn't exist yet; nothing to scan.
    return

  gguf_files = sorted(
    f for f in os.listdir(models_dir) if f.lower().endswith('.gguf')
  )

  if not gguf_files:
    return

  for filename in gguf_files:
    gguf_path = os.path.join(models_dir, filename)
    stem = Path(filename).stem
    config = _read_or_create_config(gguf_path)

    # Build a unique signal name like "llamacpp/embeddinggemma-300m-Q8_0".
    signal_name = f'llamacpp/{stem}'

    # Dynamically create a subclass with baked-in defaults from the config.
    # We use `type()` to create a new class at runtime.
    subclass = type(
      f'LlamaCpp_{stem.replace("-", "_").replace(".", "_")}',
      (LlamaCppEmbedding,),
      {
        'name': signal_name,
        'display_name': config['display_name'],
        # Override the default field values via __init__ defaults.
        '__annotations__': {},
      },
    )

    # Override pydantic field defaults for this specific model.
    subclass.model_fields['model_filename'].default = filename
    subclass.model_fields['n_ctx'].default = config['n_ctx']
    subclass.model_fields['n_gpu_layers'].default = config['n_gpu_layers']
    subclass.model_fields['output_dim'].default = config.get('output_dim')
    subclass.model_fields['pooling_type'].default = config.get('pooling_type', _DEFAULT_POOLING_TYPE)

    # Rebuild the pydantic model so the new defaults take effect.
    subclass.model_rebuild(force=True)

    register_signal(subclass, exists_ok=True)
    log(f'[llamacpp] Registered model: {signal_name} ({config["display_name"]})')
