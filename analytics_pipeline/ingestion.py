from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.logging_utils import get_logger


@dataclass
class IngestionResult:
    dataframe: pd.DataFrame
    detected_encoding: str
    malformed_rows_skipped: int


class FileIngestionError(Exception):
    pass


class FileIngestionService:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = get_logger(
            self.__class__.__name__, config.output_dir / "logs"
        )

    def ingest(self, file_path: str | Path) -> IngestionResult:
        path = Path(file_path)
        self._validate_path(path)

        suffix = path.suffix.lower()
        if suffix not in self.config.accepted_extensions:
            raise FileIngestionError(
                f"Unsupported file type '{suffix}'. Supported types: {self.config.accepted_extensions}."
            )

        size_mb = path.stat().st_size / (1024 * 1024)
        if size_mb > self.config.max_file_size_mb:
            raise FileIngestionError(
                f"File is {size_mb:.2f}MB; max allowed is {self.config.max_file_size_mb}MB."
            )

        if suffix == ".csv":
            return self._read_csv_with_encoding_detection(path)

        try:
            df = pd.read_excel(path)
            self.logger.info("Loaded Excel file '%s' with %d rows", path, len(df))
            return IngestionResult(dataframe=df, detected_encoding="binary-excel", malformed_rows_skipped=0)
        except Exception as exc:  # noqa: BLE001
            raise FileIngestionError(f"Failed to parse Excel file: {exc}") from exc

    def _validate_path(self, path: Path) -> None:
        if not path.exists():
            raise FileIngestionError(f"File not found: {path}")
        if not path.is_file():
            raise FileIngestionError(f"Expected a file but got: {path}")

    def _read_csv_with_encoding_detection(self, path: Path) -> IngestionResult:
        encodings_to_try = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
        malformed_rows_skipped = 0

        for encoding in encodings_to_try:
            try:
                bad_lines_count = 0

                def on_bad_lines(_: list[str]) -> None:
                    nonlocal bad_lines_count
                    bad_lines_count += 1
                    return None

                df = pd.read_csv(
                    path,
                    encoding=encoding,
                    on_bad_lines=on_bad_lines,
                    engine="python",
                )
                malformed_rows_skipped = bad_lines_count
                self.logger.info(
                    "Loaded CSV file '%s' encoding=%s rows=%d skipped_bad_rows=%d",
                    path,
                    encoding,
                    len(df),
                    malformed_rows_skipped,
                )
                return IngestionResult(
                    dataframe=df,
                    detected_encoding=encoding,
                    malformed_rows_skipped=malformed_rows_skipped,
                )
            except UnicodeDecodeError:
                continue
            except Exception as exc:  # noqa: BLE001
                raise FileIngestionError(f"Failed to parse CSV file: {exc}") from exc

        raise FileIngestionError(
            "Unable to decode CSV with common encodings (utf-8, utf-8-sig, latin1, cp1252)."
        )
