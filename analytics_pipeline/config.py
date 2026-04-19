from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


MissingStrategy = Literal["mean", "median", "mode", "drop", "constant"]


@dataclass
class PipelineConfig:
    """Configuration object for the end-to-end analytics pipeline."""

    max_file_size_mb: int = 100
    output_dir: Path = Path("outputs")
    missing_numeric_strategy: MissingStrategy = "median"
    missing_categorical_strategy: MissingStrategy = "mode"
    missing_constant: str = "UNKNOWN"
    zscore_threshold: float = 3.0
    duplicate_subset: list[str] | None = None
    target_column: str | None = None
    model_type: Literal["regression", "classification"] = "regression"
    random_state: int = 42
    monitoring_enabled: bool = True
    accepted_extensions: tuple[str, ...] = field(
        default_factory=lambda: (".csv", ".xlsx", ".xls")
    )

    def __post_init__(self) -> None:
        if not isinstance(self.output_dir, Path):
            self.output_dir = Path(self.output_dir)

    def ensure_output_dirs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "charts").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "logs").mkdir(parents=True, exist_ok=True)
