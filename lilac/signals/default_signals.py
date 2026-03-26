"""Registers all available default signals."""

from ..embeddings.bge import BGEM3
from ..embeddings.cohere import Cohere
from ..embeddings.llamacpp import scan_and_register_llamacpp_models
from ..embeddings.nomic_embed import NomicEmbedV2MoE, NomicEmbedV2MoE_256
from ..embeddings.openai import OpenAIEmbedding
from ..signal import register_signal
from .concept_labels import ConceptLabelsSignal
from .concept_scorer import ConceptSignal
from .lang_detection import LangDetectionSignal
from .markdown_code_block import MarkdownCodeBlockSignal
from .near_dup import NearDuplicateSignal
from .ner import SpacyNER
from .pii import PIISignal
from .text_statistics import TextStatisticsSignal


def register_default_signals() -> None:
  """Register all the default signals.

  Legacy models (GTE, SBERT, Jina V2, Nomic 1.5) were deprecated in favor of Nomic v2 MoE.
  See deprecation_analysis.md for details. JinaV5Nano is blocked on transformers >= 4.57.
  """
  # Concepts.
  register_signal(ConceptSignal, exists_ok=True)
  register_signal(ConceptLabelsSignal, exists_ok=True)

  # Text.
  register_signal(PIISignal, exists_ok=True)
  register_signal(TextStatisticsSignal, exists_ok=True)
  register_signal(SpacyNER, exists_ok=True)
  register_signal(NearDuplicateSignal, exists_ok=True)
  register_signal(LangDetectionSignal, exists_ok=True)
  register_signal(MarkdownCodeBlockSignal, exists_ok=True)

  # Embeddings — API-based.
  register_signal(Cohere, exists_ok=True)
  register_signal(OpenAIEmbedding, exists_ok=True)

  # Embeddings — On-device.
  register_signal(BGEM3, exists_ok=True)
  register_signal(NomicEmbedV2MoE, exists_ok=True)
  register_signal(NomicEmbedV2MoE_256, exists_ok=True)

  # Llama.cpp GGUF models (dynamically scanned from the models directory).
  scan_and_register_llamacpp_models()
