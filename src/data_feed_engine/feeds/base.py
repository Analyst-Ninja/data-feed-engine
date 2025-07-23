from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
from datetime import datetime
import pandas as pd
from pyspark.sql import DataFrame
from typing import Union
import json
from data_feed_engine.core.metrics import ExecutionMetrics


class BaseFeed(ABC):
    def __init__(self, config: Dict[str, Any]):
        from data_feed_engine.factory import factory

        self.config = config
        self.feed_name = config["feed_name"]
        self.input_datasource = factory.create_datasource(config["input_datasource"])
        self.output_datasource = factory.create_datasource(config["output_datasource"])
        self.processing_config = config.get("processing_config", {})
        self.logger = logging.getLogger(f"Feed_{self.feed_name}")

        self.metrics: Optional[ExecutionMetrics] = None

    def _generate_execution_id(self, run_date: str) -> str:
        """Generate unique execution_id for idempotency"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{self.feed_name}_{run_date}_{timestamp}"

    @abstractmethod
    def process(
        self, data: Union[pd.DataFrame, DataFrame], run_date: str
    ) -> Union[pd.DataFrame, DataFrame]:
        """Override this method to implement custom processing logic"""
        return data

    @abstractmethod
    def perform_data_quality_checks(
        self,
        input_data: Union[pd.DataFrame, DataFrame],
        output_data: Union[pd.DataFrame, DataFrame],
    ) -> None:
        """Override this method to perform data quality checks"""
        pass

    def run(self, run_date: str, full_load: bool = False):
        """Main execution method"""
        execution_id = self._generate_execution_id(run_date)

        # Initialize Metrics
        self.metrics = ExecutionMetrics(
            feed_name=self.feed_name,
            run_date=run_date,
            execution_id=execution_id,
            start_time=datetime.now(),
        )

        try:
            self.logger.info(
                f"Starting feed execution: {self.feed_name} for date: {run_date}"
            )

            # Validate Connection
            if not self.input_datasource.validate_connection():
                raise ConnectionError("Input source connection validation failed")
            if not self.output_datasource.validate_connection():
                raise ConnectionError("Output source connection validation failed")

            # Extract data
            incremental = not full_load
            data = self.input_datasource.get_data(run_date, incremental)
            self.metrics.input_row_count = len(data)

            if data.empty:
                self.logger.warning("No data received from source")
                self.metrics.status = "success_no_data"
                self.metrics.end_time = datetime.now()
                return self.metrics

            processed_data = self.process(data, run_date)

            print(processed_data.shape)

            self.perform_data_quality_checks(data, processed_data)

            self.output_datasource.put_data(processed_data, run_date, execution_id)

            # Log to metrics
            self.metrics.status = "success"
            self.metrics.end_time = datetime.now()
            self.metrics.output_row_count = len(processed_data)

            self.logger.info("Feed execution completed successfully")

        except Exception as e:
            # Log to metrics
            self.metrics.status = "failed"
            self.metrics.error_message = str(e)
            self.metrics.end_time = datetime.now()

            self.logger.error(f"Feed execution failed: {str(e)}")

        finally:
            self.input_datasource.disconnect()
            self.output_datasource.disconnect()

            self._emit_metrics()

        return self.metrics

    def _emit_metrics(self) -> None:
        if self.metrics:
            self.logger.info(
                f"------Metrics:------\n{json.dumps(self.metrics.to_dict(), indent=2)}"
            )
