from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, Dict, List
import json


@dataclass
class IngestionMetrics:
    """
    Metrics collected during data ingestion for a single table.
    """
    success: bool = False
    eltId: str = None
    runId: str = None
    startTime: datetime = None
    endTime: datetime = None
    lakehouse: Optional[str] = None
    deltaSchema: Optional[str] = None
    deltaObject: Optional[str] = None
    error: Optional[str] = None
    sourceDataCount: int = 0
    recordInserts: int = 0
    recordUpdates: int = 0
    recordDeletes: int = 0
    outputBytes: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


@dataclass
class GroupMetrics:
    """
    Aggregated metrics across the ingestion process.
    """
    ingestion: dict = None
    quarantine: list[dict] = field(default_factory=lambda: [])
    expectations: dict = None
    table_columns: Dict[str, List[str]] = field(default_factory=dict)   # ← NEW

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


# Code Change in Ingest Class
# === Capture final columns for OneLake security roles ===
try:
    final_columns = self._source_data.columns if self._source_data is not None else []
    self.ALLMETRICS.table_columns = {
        self.target_table: final_columns
    }
    self.logger.debug(f"Captured {len(final_columns)} columns for table '{self.target_table}'")
except Exception as e:
    self.logger.warning(f"Could not capture table columns: {e}")
    self.ALLMETRICS.table_columns = {}

# Full Updated metrics() Method
def metrics(self, frmt: str = "json"):
    """
    Get the metrics that were generated during the ingestion process.
    """
    if frmt == "json":
        return self.ALLMETRICS.to_json()
    if frmt == "dict":
        return self.ALLMETRICS.to_dict()
    raise ValueError(f"Unsupported format: {frmt}")
