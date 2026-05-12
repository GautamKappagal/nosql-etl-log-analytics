"""
pipelines package
─────────────────
ETL pipeline implementations.
"""

from .base import BasePipeline, RunMetadata, QueryResult, BatchRecord, ALL_QUERIES
from .mapreduce.pipeline import MapReducePipeline
from .pig.pipeline import PigPipeline
from .hive.pipeline import HivePipeline

# Optional: MongoDB pipeline (requires pymongo)
try:
    from .mongodb.pipeline import MongoDBPipeline
    _mongodb_available = True
except ImportError:
    _mongodb_available = False
    MongoDBPipeline = None  # type: ignore

# Registry used by the orchestrator to instantiate pipelines by name
PIPELINE_REGISTRY = {
    "mapreduce": MapReducePipeline,
    "pig": PigPipeline,
    "hive": HivePipeline,
}

# Add MongoDB to registry if available
if _mongodb_available:
    PIPELINE_REGISTRY["mongodb"] = MongoDBPipeline

__all__ = [
    "BasePipeline",
    "RunMetadata",
    "QueryResult",
    "BatchRecord",
    "ALL_QUERIES",
    "MapReducePipeline",
    "PigPipeline",
    "HivePipeline",
    "PIPELINE_REGISTRY",
]

if _mongodb_available:
    __all__.append("MongoDBPipeline")
