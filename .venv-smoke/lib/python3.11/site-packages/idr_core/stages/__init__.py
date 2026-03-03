"""
IDR Pipeline Stages.

Each stage handles a specific phase of the identity resolution pipeline.
"""

from .base import BaseStage, StageContext
from .extraction import ExtractionStage
from .graph import GraphStage
from .output import OutputStage
from .preflight import PreflightStage

__all__ = [
    "StageContext",
    "BaseStage",
    "PreflightStage",
    "ExtractionStage",
    "GraphStage",
    "OutputStage",
]
