import json
from typing import Optional
from dataclasses import asdict, dataclass, field
from datetime import datetime


@dataclass
class IngestionMetrics:
    """
    Metrics collected during data ingestion.
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
        return json.dumps(self.to_dict())
    
@dataclass
class GroupMetrics:
    """
    Metrics collected during data ingestion.
    """

    ingestion: dict = None
    quarantine: list[dict] = field(default_factory=lambda: [])
    expectations: dict = None

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
