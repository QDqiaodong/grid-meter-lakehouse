"""Domain data pipeline."""
from .pipeline import build
from .incremental import revise

__all__ = ["build", "revise"]
