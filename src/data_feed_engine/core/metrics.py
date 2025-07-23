from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class ExecutionMetrics:
    """Standardized metrics for feed execution"""

    feed_name: str
    run_date: str
    execution_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    input_row_count: int = 0
    output_row_count: int = 0
    processing_engine: str = "python"
    status: str = "running"
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feed_name": self.feed_name,
            "run_date": self.run_date,
            "execution_id": self.execution_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": (
                (self.end_time - self.start_time).total_seconds()
                if self.end_time
                else None
            ),
            "input_row_count": self.input_row_count,
            "output_row_count": self.output_row_count,
            "processing_engine": self.processing_engine,
            "status": self.status,
            "error_message": self.error_message,
        }
