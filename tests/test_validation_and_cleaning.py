import pandas as pd

from analytics_pipeline.cleaning import DataCleaningService
from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.validation import ValidationService


def test_schema_detection_identifies_numeric_categorical_and_date():
    df = pd.DataFrame(
        {
            "amount": [1.0, 2.5, 3.5],
            "category": ["A", "B", "C"],
            "event_date": ["2025-01-01", "2025-01-02", "2025-01-03"],
        }
    )
    config = PipelineConfig(output_dir="test_outputs")
    service = ValidationService(config)

    schema = service.infer_schema(df)

    assert "amount" in schema.numeric
    assert "category" in schema.categorical
    assert "event_date" in schema.date


def test_missing_value_handling_applies_median_for_numeric():
    df = pd.DataFrame({"value": [1.0, None, 5.0]})
    config = PipelineConfig(output_dir="test_outputs", missing_numeric_strategy="median")

    validation_service = ValidationService(config)
    schema = validation_service.infer_schema(df)

    cleaning = DataCleaningService(config)
    cleaned, _ = cleaning.clean(df, schema)

    assert cleaned["value"].isna().sum() == 0
    assert cleaned.loc[1, "value"] == 3.0


def test_outlier_detection_uses_zscore_threshold():
    df = pd.DataFrame({"metric": [10, 11, 9, 10, 12, 11, 10, 500]})
    config = PipelineConfig(output_dir="test_outputs", zscore_threshold=2.0)
    service = ValidationService(config)

    report = service.build_quality_report(df)

    assert report.outlier_counts["metric"] == 1
