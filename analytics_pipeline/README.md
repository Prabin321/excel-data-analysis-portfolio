# Production-Style CSV/Excel Analytics Pipeline

This module provides an end-to-end analytics pipeline for CSV and Excel data with:

- file ingestion and validation,
- schema and quality checks,
- configurable cleaning,
- analysis + optional predictive modeling,
- auto-visualizations,
- and executive insight generation.

## Architecture

```
ingestion -> validation -> cleaning -> analysis -> visualization -> insight generation
```

### Modules

- `config.py`: central runtime configuration (size limits, missing-value strategy, outlier threshold, model settings).
- `ingestion.py`: CSV/XLSX ingestion with file type/size validation, encoding fallback, and malformed row handling.
- `validation.py`: schema inference, missing/inconsistent/duplicate checks, and z-score outlier detection.
- `cleaning.py`: configurable missing-value handling, categorical normalization, numeric/date standardization, transformation logging.
- `analysis.py`: summary stats, correlations, segmentation, trend/anomaly detection, optional regression/classification metrics.
- `visualization.py`: type-driven chart generation with edge-case handling for empty/small datasets.
- `insights.py`: executive summary with key drivers, risks, and recommended actions.
- `pipeline.py`: orchestration, runtime tracking, and final report persistence.
- `cli.py`: command-line entrypoint.
- `app.py`: Streamlit app interface for interactive uploads, filtering, charts, and insights.

## Reliability and Edge Cases

- File size limit enforcement (`max_file_size_mb`) to simulate large-file protection.
- Empty dataset guardrails in validation and visualization layers.
- Single-column datasets are safely analyzed (skip unsupported analyses).
- Invalid file paths, unsupported extensions, parsing errors raise descriptive `FileIngestionError` messages.
- Transformation and operational logs are persisted for debugging and auditability.

## Run

```bash
python -m analytics_pipeline.cli <path_to_csv_or_xlsx> --output-dir outputs
```

### Streamlit App

```bash
streamlit run analytics_pipeline/app.py
```

Outputs are stored in `outputs/`:

- `report.json`
- `charts/*.png`
- `logs/*.log`

## Tests

`tests/test_validation_and_cleaning.py` includes checks for:

- schema detection,
- missing value handling,
- outlier detection.
