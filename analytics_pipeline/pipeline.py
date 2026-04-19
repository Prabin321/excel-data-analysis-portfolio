from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from analytics_pipeline.analysis import AnalysisResult, AnalysisService
from analytics_pipeline.cleaning import DataCleaningService, TransformationLog
from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.ingestion import FileIngestionService, IngestionResult
from analytics_pipeline.insights import InsightGenerationService, InsightReport
from analytics_pipeline.validation import DataQualityReport, ValidationService
from analytics_pipeline.visualization import VisualizationService


@dataclass
class PipelineResult:
    ingestion: IngestionResult
    quality_report: DataQualityReport
    transformations: TransformationLog
    analysis: AnalysisResult
    insights: InsightReport
    charts: list[Path]
    runtime_seconds: float


class AnalyticsPipeline:
    def __init__(self, config: PipelineConfig | None = None):
        self.config = config or PipelineConfig()
        self.config.ensure_output_dirs()
        self.ingestion_service = FileIngestionService(self.config)
        self.validation_service = ValidationService(self.config)
        self.cleaning_service = DataCleaningService(self.config)
        self.analysis_service = AnalysisService(self.config)
        self.visualization_service = VisualizationService(self.config)
        self.insight_service = InsightGenerationService()

    def run(self, file_path: str | Path) -> PipelineResult:
        start = time.time()
        ingestion = self.ingestion_service.ingest(file_path)
        quality = self.validation_service.build_quality_report(ingestion.dataframe)
        schema = quality.inferred_schema
        cleaned_df, log = self.cleaning_service.clean(ingestion.dataframe, schema)
        analysis = self.analysis_service.analyze(cleaned_df, schema)
        charts = self.visualization_service.generate(cleaned_df, schema)
        insights = self.insight_service.generate(quality, analysis)

        result = PipelineResult(
            ingestion=ingestion,
            quality_report=quality,
            transformations=log,
            analysis=analysis,
            insights=insights,
            charts=charts,
            runtime_seconds=time.time() - start,
        )
        self._persist_outputs(result)
        return result

    def _persist_outputs(self, result: PipelineResult) -> None:
        report_path = self.config.output_dir / "report.json"
        payload = {
            "ingestion": {
                "detected_encoding": result.ingestion.detected_encoding,
                "malformed_rows_skipped": result.ingestion.malformed_rows_skipped,
            },
            "quality_report": {
                "row_count": result.quality_report.row_count,
                "column_count": result.quality_report.column_count,
                "missing_counts": result.quality_report.missing_counts,
                "duplicate_count": result.quality_report.duplicate_count,
                "outlier_counts": result.quality_report.outlier_counts,
                "inferred_schema": asdict(result.quality_report.inferred_schema),
            },
            "transformations": result.transformations.actions,
            "analysis": asdict(result.analysis),
            "insights": asdict(result.insights),
            "charts": [str(p) for p in result.charts],
            "runtime_seconds": result.runtime_seconds,
        }
        report_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
